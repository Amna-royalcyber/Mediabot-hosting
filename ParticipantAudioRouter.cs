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
    private readonly ILogger<ParticipantAudioRouter> _logger;
    private readonly ConcurrentDictionary<uint, ParticipantAudioBinding> _bindingBySourceId = new();

    /// <summary>Teams media source id for the current dominant speaker (from <see cref="IAudioSocket.DominantSpeakerChanged"/>).</summary>
    private uint _dominantSourceId = (uint)DominantSpeakerChangedEventArgs.None;

    private int _loggedMixedMode;
    private int _loggedUnmappedDominantMixed;
    private int _loggedDominantNotYetMixed;
    private int _loggedHeuristicMultiInference;

    /// <summary>Order in which previously unknown source ids first appeared (for pairing with roster when Graph omits mediaStreams).</summary>
    private readonly object _inferLock = new();

    private readonly List<uint> _sourceIdDiscoveryOrder = new();
    private readonly ConcurrentDictionary<uint, byte> _warnedUnmappedSourceIds = new();

    public ParticipantAudioRouter(
        AudioProcessor audioProcessor,
        AwsTranscribeService awsTranscribeService,
        MeetingParticipantService meetingParticipants,
        ILogger<ParticipantAudioRouter> logger)
    {
        _audioProcessor = audioProcessor;
        _awsTranscribeService = awsTranscribeService;
        _meetingParticipants = meetingParticipants;
        _logger = logger;
    }

    public void AttachToCall(ICall call, string botClientId)
    {
        lock (_inferLock)
        {
            _sourceIdDiscoveryOrder.Clear();
        }

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

            if (!_bindingBySourceId.TryGetValue(sourceId, out var binding))
            {
                var roster = _meetingParticipants.GetRosterSnapshot();
                if (!TryInferBindingForUnmappedSource(sourceId, roster, out binding))
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

            _logger.LogDebug("Audio received from {ParticipantName} ({ParticipantId}).", binding.DisplayName, binding.ParticipantId);
            await _awsTranscribeService.SendAudioChunkAsync(
                binding.ParticipantId,
                binding.DisplayName,
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

        if (!TryResolveMixedAttribution(roster, out var binding))
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
            binding.ParticipantId,
            binding.DisplayName,
            pcm,
            args.Buffer.Timestamp);
    }

    /// <summary>Teams raises dominant speaker MSI; must align with participant mediaStreams sourceId for correct names.</summary>
    public void SetDominantSpeaker(uint sourceId)
    {
        _dominantSourceId = sourceId;
    }

    /// <summary>
    /// Graph often omits <c>mediaStreams[].sourceId</c> for some participants. Map unmixed <paramref name="sourceId"/>
    /// to the roster user who is not yet tied to any source, or pair by discovery order vs sorted display names.
    /// </summary>
    private bool TryInferBindingForUnmappedSource(
        uint sourceId,
        IReadOnlyList<RosterParticipantDto> roster,
        out ParticipantAudioBinding binding)
    {
        binding = default!;
        lock (_inferLock)
        {
            if (_bindingBySourceId.TryGetValue(sourceId, out var race))
            {
                binding = race;
                return true;
            }

            var mappedUserIds = new HashSet<string>(
                _bindingBySourceId.Values.Select(v => v.ParticipantId),
                StringComparer.OrdinalIgnoreCase);

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
                binding = new ParticipantAudioBinding(p.AzureAdObjectId, p.DisplayName);
                _bindingBySourceId[sourceId] = binding;
                _awsTranscribeService.UpsertParticipant(binding.ParticipantId, binding.DisplayName);
                _logger.LogInformation(
                    "Inferred sourceId {SourceId} → {DisplayName} (only roster user without a Graph mediaStreams sourceId).",
                    sourceId,
                    binding.DisplayName);
                return true;
            }

            if (!_sourceIdDiscoveryOrder.Contains(sourceId))
            {
                _sourceIdDiscoveryOrder.Add(sourceId);
            }

            var sorted = unmappedHumans.OrderBy(h => h.DisplayName, StringComparer.OrdinalIgnoreCase).ToList();
            var idx = _sourceIdDiscoveryOrder.IndexOf(sourceId);
            if (idx < 0)
            {
                return false;
            }

            idx = Math.Min(idx, sorted.Count - 1);
            var pick = sorted[idx];
            binding = new ParticipantAudioBinding(pick.AzureAdObjectId, pick.DisplayName);
            _bindingBySourceId[sourceId] = binding;
            _awsTranscribeService.UpsertParticipant(binding.ParticipantId, binding.DisplayName);
            if (Interlocked.Increment(ref _loggedHeuristicMultiInference) == 1)
            {
                _logger.LogWarning(
                    "Several participants lack mediaStreams sourceId in Graph. Pairing new source ids to roster users by first-seen order vs sorted display names (best-effort).");
            }

            _logger.LogDebug(
                "Inferred sourceId {SourceId} → {DisplayName} (heuristic index {Idx}).",
                sourceId,
                binding.DisplayName,
                idx);
            return true;
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

    private bool TryResolveMixedAttribution(IReadOnlyList<RosterParticipantDto> roster, out ParticipantAudioBinding binding)
    {
        binding = default!;
        var none = (uint)DominantSpeakerChangedEventArgs.None;
        var dom = _dominantSourceId;

        if (dom != none && _bindingBySourceId.TryGetValue(dom, out var mapped))
        {
            binding = mapped;
            return true;
        }

        if (roster.Count == 1)
        {
            binding = new ParticipantAudioBinding(roster[0].AzureAdObjectId, roster[0].DisplayName);
            return true;
        }

        if (dom != none)
        {
            if (Interlocked.Increment(ref _loggedUnmappedDominantMixed) == 1)
            {
                _logger.LogWarning(
                    "Mixed audio: dominant sourceId {SourceId} is not bound to an Entra user yet. " +
                    "Ensure Graph participant updates include mediaStreams with matching sourceId. " +
                    "Using first roster participant as temporary label.",
                    dom);
            }

            binding = new ParticipantAudioBinding(roster[0].AzureAdObjectId, roster[0].DisplayName);
            return true;
        }

        if (Interlocked.Increment(ref _loggedDominantNotYetMixed) == 1)
        {
            _logger.LogWarning(
                "Mixed audio: dominant speaker not reported yet; labeling transcripts as {Name} until MSI arrives.",
                roster[0].DisplayName);
        }

        binding = new ParticipantAudioBinding(roster[0].AzureAdObjectId, roster[0].DisplayName);
        return true;
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

        foreach (var sourceId in TryExtractSourceIds(resource))
        {
            var binding = new ParticipantAudioBinding(participantId.Trim(), displayName.Trim());
            _bindingBySourceId[sourceId] = binding;
            _awsTranscribeService.UpsertParticipant(binding.ParticipantId, binding.DisplayName);
            _logger.LogInformation("Bound sourceId {SourceId} -> {DisplayName} ({ParticipantId}).", sourceId, binding.DisplayName, binding.ParticipantId);
        }
    }

    private void RemoveParticipantMappings(IParticipant participant)
    {
        var participantId = participant.Resource?.Info?.Identity?.User?.Id;
        if (string.IsNullOrWhiteSpace(participantId))
        {
            return;
        }

        foreach (var kvp in _bindingBySourceId.Where(k => string.Equals(k.Value.ParticipantId, participantId.Trim(), StringComparison.OrdinalIgnoreCase)).ToList())
        {
            _bindingBySourceId.TryRemove(kvp.Key, out _);
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

    private sealed record ParticipantAudioBinding(string ParticipantId, string DisplayName);
}
