using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading;
using System.Linq;
using Microsoft.Extensions.Logging;
using Microsoft.Graph.Communications.Calls;
using Microsoft.Graph.Communications.Resources;
using Microsoft.Skype.Bots.Media;

namespace TeamsMediaBot;

public sealed class ParticipantAudioRouter
{
    private readonly AudioProcessor _audioProcessor;
    private readonly AwsTranscribeService _awsTranscribeService;
    private readonly MeetingParticipantService _meetingParticipants;
    private readonly ParticipantManager _participantManager;
    private readonly ILogger<ParticipantAudioRouter> _logger;

    /// <summary>Teams media source id for the current dominant speaker (from <see cref="IAudioSocket.DominantSpeakerChanged"/>).</summary>
    private uint _dominantSourceId = (uint)DominantSpeakerChangedEventArgs.None;

    private int _loggedMixedMode;
    private int _loggedUnmappedDominantMixed;
    private int _loggedDominantNotYetMixed;
    private int _loggedMultiParticipantInferenceSkipped;

    private readonly object _inferLock = new();

    private readonly ConcurrentDictionary<uint, byte> _warnedUnmappedSourceIds = new();

    public ParticipantAudioRouter(
        AudioProcessor audioProcessor,
        AwsTranscribeService awsTranscribeService,
        MeetingParticipantService meetingParticipants,
        ParticipantManager participantManager,
        ILogger<ParticipantAudioRouter> logger)
    {
        _audioProcessor = audioProcessor;
        _awsTranscribeService = awsTranscribeService;
        _meetingParticipants = meetingParticipants;
        _participantManager = participantManager;
        _logger = logger;
    }

    public void AttachToCall(ICall call, string botClientId)
    {
        call.Participants.OnUpdated += (_, args) =>
        {
            foreach (var p in args.AddedResources)
            {
                UpsertParticipantMappings(p, botClientId);
            }
            foreach (var p in args.UpdatedResources)
            {
                UpsertParticipantMappings(p, botClientId);
            }
            foreach (var p in args.RemovedResources)
            {
                RemoveParticipantMappings(p);
            }
        };

        // Roster may already contain participants before delta events; hydrate bindings immediately.
        TryHydrateFromCurrentRoster(call, botClientId);
    }

    private void TryHydrateFromCurrentRoster(ICall call, string botClientId)
    {
        try
        {
            foreach (var p in call.Participants)
            {
                UpsertParticipantMappings(p, botClientId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not hydrate participant source bindings from current roster.");
        }
    }

    public async Task HandleAudioAsync(AudioMediaReceivedEventArgs args)
    {
        var unmixed = args.Buffer.UnmixedAudioBuffers;
        if (unmixed is null || !unmixed.Any())
        {
            // Many Teams builds/skus still deliver only the main (mixed) buffer; unmixed may be empty forever.
            await TrySendMainBufferMixedDominantAsync(args);
            return;
        }

        foreach (var ub in unmixed)
        {
            var sourceId = Convert.ToUInt32(ub.ActiveSpeakerId);
            if (sourceId == (uint)DominantSpeakerChangedEventArgs.None)
            {
                continue;
            }

            if (!_participantManager.TryResolveAudioSource(sourceId, out var participantId, out var displayName))
            {
                var roster = _meetingParticipants.GetRosterSnapshot();
                if (!TryInferBindingForUnmappedSource(sourceId, roster, out participantId, out displayName))
                {
                    LogUnmappedSourceIdOnce(sourceId);
                    continue;
                }
            }

            var payload = CopyUnmixedBuffer(ub.Data, ub.Length);
            if (payload.Length == 0)
            {
                continue;
            }

            var pcm = _audioProcessor.ConvertToPcm(new AudioFrame(
                Data: payload,
                Timestamp: ub.OriginalSenderTimestamp,
                Length: (int)ub.Length,
                Format: AudioFormat.Pcm16K));

            if (pcm.Length == 0)
            {
                continue;
            }

            _logger.LogDebug("Audio received from {ParticipantName} ({ParticipantId}).", displayName, participantId);
            await _awsTranscribeService.SendAudioChunkAsync(
                participantId,
                displayName,
                pcm,
                ub.OriginalSenderTimestamp);
        }
    }

    /// <summary>
    /// Mixed meeting audio (single buffer) — attribute text to the participant mapped from Teams <strong>dominant speaker</strong>
    /// source id (MSI), using Graph <c>mediaStreams[].sourceId</c> → Entra user. If the dominant id is not mapped yet,
    /// we fall back to the first roster entry (degraded) so you still get transcripts.
    /// </summary>
    private async Task TrySendMainBufferMixedDominantAsync(AudioMediaReceivedEventArgs args)
    {
        var declaredLength = (int)args.Buffer.Length;
        var extracted = AudioProcessor.ExtractBytes(args.Buffer);
        if (declaredLength > 0 && extracted.Length == 0)
        {
            _logger.LogTrace("Main audio buffer had Length={Len} but ExtractBytes returned 0.", declaredLength);
            return;
        }

        var pcm = _audioProcessor.ConvertToPcm(new AudioFrame(
            Data: extracted,
            Timestamp: args.Buffer.Timestamp,
            Length: declaredLength,
            Format: AudioFormat.Pcm16K));
        if (pcm.Length == 0)
        {
            return;
        }

        var roster = _meetingParticipants.GetRosterSnapshot();
        if (roster.Count == 0)
        {
            _logger.LogDebug("Main audio buffer received but roster is empty (participants not ingested yet).");
            return;
        }

        if (!TryResolveMixedAttribution(roster, out var mixedParticipantId, out var mixedDisplayName))
        {
            return;
        }

        if (Interlocked.Increment(ref _loggedMixedMode) == 1)
        {
            _logger.LogInformation(
                "Using mixed main audio buffer with dominant-speaker labeling (sourceId map + Teams dominant MSI). " +
                "For per-person audio without mixing, enable unmixed meeting audio when the client supports it.");
        }

        await _awsTranscribeService.SendMixedDominantAudioAsync(
            mixedParticipantId,
            mixedDisplayName,
            pcm,
            args.Buffer.Timestamp);
    }

    /// <summary>Teams raises dominant speaker MSI; must align with participant mediaStreams sourceId for correct names.</summary>
    public void SetDominantSpeaker(uint sourceId)
    {
        _dominantSourceId = sourceId;
    }

    /// <summary>
    /// When Graph omits <c>mediaStreams[].sourceId</c>, we only infer identity if exactly one human has no binding yet.
    /// With two or more unmapped users, guessing (e.g. join order vs name sort) swaps speakers — we refuse and wait for Graph.
    /// </summary>
    private bool TryInferBindingForUnmappedSource(
        uint sourceId,
        IReadOnlyList<RosterParticipantDto> roster,
        out string participantId,
        out string displayName)
    {
        participantId = string.Empty;
        displayName = string.Empty;
        lock (_inferLock)
        {
            if (_participantManager.TryResolveAudioSource(sourceId, out participantId, out displayName))
            {
                return true;
            }

            var mappedUserIds = _participantManager.GetParticipantIdsWithAudioSourceBindings();

            var unmappedHumans = roster
                .Where(r => !mappedUserIds.Contains(r.AzureAdObjectId))
                .ToList();

            if (unmappedHumans.Count == 0)
            {
                return false;
            }

            if (unmappedHumans.Count == 1)
            {
                var p = unmappedHumans[0];
                var removed = _participantManager.TryBindAudioSource(sourceId, p.AzureAdObjectId, p.DisplayName, "InferenceSingleRoster");
                if (removed is not null)
                {
                    _awsTranscribeService.RemoveParticipant(removed);
                }

                _awsTranscribeService.UpsertParticipant(p.AzureAdObjectId, p.DisplayName);
                _logger.LogInformation(
                    "Inferred sourceId {SourceId} → {DisplayName} (only roster user without a Graph mediaStreams sourceId).",
                    sourceId,
                    p.DisplayName);
                return _participantManager.TryResolveAudioSource(sourceId, out participantId, out displayName);
            }

            // Multiple humans: use a non-Entra placeholder per source id (no cross-participant guessing). Graph can upgrade later.
            var syntheticId = ParticipantManager.SyntheticParticipantId(sourceId);
            var syntheticName = $"Speaker ({sourceId})";
            if (Interlocked.Increment(ref _loggedMultiParticipantInferenceSkipped) == 1)
            {
                _logger.LogInformation(
                    "Graph has not mapped mediaStreams source ids yet; using per-stream placeholders {Placeholder} until Entra mappings arrive.",
                    syntheticName);
            }

            var removedPlaceholder = _participantManager.TryBindAudioSource(sourceId, syntheticId, syntheticName, "SyntheticUntilGraph");
            if (removedPlaceholder is not null)
            {
                _awsTranscribeService.RemoveParticipant(removedPlaceholder);
            }

            _awsTranscribeService.UpsertParticipant(syntheticId, syntheticName);
            return _participantManager.TryResolveAudioSource(sourceId, out participantId, out displayName);
        }
    }

    private void LogUnmappedSourceIdOnce(uint sourceId)
    {
        if (_warnedUnmappedSourceIds.TryAdd(sourceId, 0))
        {
            _logger.LogWarning(
                "Could not infer Entra user for sourceId {SourceId}. Check roster vs participants, or Graph mediaStreams payload.",
                sourceId);
        }
    }

    private bool TryResolveMixedAttribution(IReadOnlyList<RosterParticipantDto> roster, out string participantId, out string displayName)
    {
        participantId = string.Empty;
        displayName = string.Empty;
        var none = (uint)DominantSpeakerChangedEventArgs.None;
        var dom = _dominantSourceId;

        if (dom != none && _participantManager.TryResolveAudioSource(dom, out participantId, out displayName))
        {
            return true;
        }

        if (roster.Count == 0)
        {
            return false;
        }

        if (roster.Count == 1)
        {
            participantId = roster[0].AzureAdObjectId;
            displayName = roster[0].DisplayName;
            return true;
        }

        // 2+ humans, dominant MSI known but not yet mapped to Entra: use same per-MSI placeholder as unmixed (upgrades when Graph arrives).
        if (dom != none)
        {
            if (Interlocked.Increment(ref _loggedUnmappedDominantMixed) == 1)
            {
                _logger.LogInformation(
                    "Mixed audio: dominant sourceId {SourceId} not mapped to Entra yet (multiple participants). " +
                    "Using placeholder label until Graph sends mediaStreams.",
                    dom);
            }

            var syntheticId = ParticipantManager.SyntheticParticipantId(dom);
            var syntheticName = $"Speaker ({dom})";
            var removedPh = _participantManager.TryBindAudioSource(dom, syntheticId, syntheticName, "SyntheticDominantMixed");
            if (removedPh is not null)
            {
                _awsTranscribeService.RemoveParticipant(removedPh);
            }

            _awsTranscribeService.UpsertParticipant(syntheticId, syntheticName);
            return _participantManager.TryResolveAudioSource(dom, out participantId, out displayName);
        }

        if (Interlocked.Increment(ref _loggedDominantNotYetMixed) == 1)
        {
            _logger.LogWarning(
                "Mixed audio: dominant speaker not reported yet with multiple participants; dropping frames until MSI maps to a user.");
        }

        return false;
    }

    private void UpsertParticipantMappings(IParticipant participant, string botClientId)
    {
        var resource = participant.Resource;
        var identity = resource?.Info?.Identity;
        var appId = identity?.Application?.Id;
        if (!string.IsNullOrWhiteSpace(appId) &&
            string.Equals(appId.Trim(), botClientId, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        var participantId = identity?.User?.Id;
        if (string.IsNullOrWhiteSpace(participantId))
        {
            return;
        }

        var displayName = identity?.User?.DisplayName;
        if (string.IsNullOrWhiteSpace(displayName))
        {
            displayName = participantId;
        }

        var pid = participantId.Trim();
        var dn = displayName.Trim();
        _participantManager.RegisterParticipant(pid, dn, DateTime.UtcNow);

        foreach (var sourceId in TryExtractSourceIds(resource))
        {
            var removedSyn = _participantManager.TryBindAudioSource(sourceId, pid, dn, "Graph");
            if (removedSyn is not null)
            {
                _awsTranscribeService.RemoveParticipant(removedSyn);
            }

            _logger.LogInformation("Bound sourceId {SourceId} -> {DisplayName} ({ParticipantId}).", sourceId, dn, pid);
        }

        _awsTranscribeService.UpsertParticipant(pid, dn);
    }

    private void RemoveParticipantMappings(IParticipant participant)
    {
        var participantId = participant.Resource?.Info?.Identity?.User?.Id;
        if (string.IsNullOrWhiteSpace(participantId))
        {
            return;
        }

        _awsTranscribeService.RemoveParticipant(participantId.Trim());
    }

    private static List<uint> TryExtractSourceIds(Microsoft.Graph.Models.Participant? participant)
    {
        var list = new List<uint>();
        if (participant?.AdditionalData is null)
        {
            return list;
        }

        object? msObj = null;
        foreach (var kvp in participant.AdditionalData)
        {
            if (string.Equals(kvp.Key, "mediaStreams", StringComparison.OrdinalIgnoreCase))
            {
                msObj = kvp.Value;
                break;
            }
        }

        if (msObj is null)
        {
            return list;
        }

        if (msObj is JsonElement je && je.ValueKind == JsonValueKind.Array)
        {
            foreach (var stream in je.EnumerateArray())
            {
                if (stream.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }

                if (stream.TryGetProperty("sourceId", out var src))
                {
                    if (src.ValueKind == JsonValueKind.Number && src.TryGetUInt32(out var n))
                    {
                        list.Add(n);
                    }
                    else if (src.ValueKind == JsonValueKind.String &&
                             uint.TryParse(src.GetString(), out var s))
                    {
                        list.Add(s);
                    }
                }
            }
        }
        else if (msObj is JsonElement js && js.ValueKind == JsonValueKind.String)
        {
            var raw = js.GetString();
            if (!string.IsNullOrWhiteSpace(raw) && TryParseFromJson(raw, list))
            {
                return list;
            }
        }
        else if (msObj is string str && TryParseFromJson(str, list))
        {
            return list;
        }

        return list;
    }

    private static bool TryParseFromJson(string json, List<uint> list)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind != JsonValueKind.Array)
            {
                return false;
            }

            foreach (var stream in doc.RootElement.EnumerateArray())
            {
                if (stream.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }

                if (!stream.TryGetProperty("sourceId", out var src))
                {
                    continue;
                }

                if (src.ValueKind == JsonValueKind.Number && src.TryGetUInt32(out var n))
                {
                    list.Add(n);
                }
                else if (src.ValueKind == JsonValueKind.String &&
                         uint.TryParse(src.GetString(), out var s))
                {
                    list.Add(s);
                }
            }

            return list.Count > 0;
        }
        catch
        {
            return false;
        }
    }

    private static byte[] CopyUnmixedBuffer(IntPtr ptr, long length)
    {
        if (ptr == IntPtr.Zero || length <= 0 || length > int.MaxValue)
        {
            return Array.Empty<byte>();
        }

        var bytes = new byte[(int)length];
        Marshal.Copy(ptr, bytes, 0, (int)length);
        return bytes;
    }

}
