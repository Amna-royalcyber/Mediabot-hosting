using System.Collections.Concurrent;
using System.Runtime.InteropServices;
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
    private readonly BotSettings _settings;
    private readonly ILogger<ParticipantAudioRouter> _logger;

    /// <summary>Distinct MSIs seen while Graph omitted mediaStreams; paired to <see cref="MeetingParticipantService"/> roster order.</summary>
    private readonly List<uint> _joinOrderFallbackSourceIds = new();

    /// <summary>Teams media source id for the current dominant speaker (from <see cref="IAudioSocket.DominantSpeakerChanged"/>).</summary>
    private uint _dominantSourceId = (uint)DominantSpeakerChangedEventArgs.None;

    /// <summary>Last non-<see cref="DominantSpeakerChangedEventArgs.None"/> dominant MSI (Teams sometimes reports None between packets).</summary>
    private uint _lastNonNoneDominantSourceId = (uint)DominantSpeakerChangedEventArgs.None;

    private ICall? _attachedCall;
    private string _botClientId = string.Empty;
    private readonly object _rescanLock = new();
    private DateTime _lastParticipantRescanUtc = DateTime.MinValue;

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
        BotSettings settings,
        ILogger<ParticipantAudioRouter> logger)
    {
        _audioProcessor = audioProcessor;
        _awsTranscribeService = awsTranscribeService;
        _meetingParticipants = meetingParticipants;
        _participantManager = participantManager;
        _settings = settings;
        _logger = logger;
    }

    public void AttachToCall(ICall call, string botClientId)
    {
        _attachedCall = call;
        _botClientId = botClientId ?? string.Empty;
        var none = (uint)DominantSpeakerChangedEventArgs.None;
        _dominantSourceId = none;
        _lastNonNoneDominantSourceId = none;
        lock (_rescanLock)
        {
            _lastParticipantRescanUtc = DateTime.MinValue;
        }

        lock (_inferLock)
        {
            _joinOrderFallbackSourceIds.Clear();
        }

        var bot = _botClientId;
        call.Participants.OnUpdated += (_, args) =>
        {
            foreach (var p in args.AddedResources)
            {
                UpsertParticipantMappings(p, bot);
            }
            foreach (var p in args.UpdatedResources)
            {
                UpsertParticipantMappings(p, bot);
            }
            foreach (var p in args.RemovedResources)
            {
                RemoveParticipantMappings(p);
            }
        };

        // Roster may already contain participants before delta events; hydrate bindings immediately.
        TryHydrateFromCurrentRoster(call, bot);
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
        MaybeRescanParticipantMediaStreams();

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
                if (!TryApplyRosterMediaStreamMap(sourceId, out participantId, out displayName))
                {
                    var roster = _meetingParticipants.GetRosterSnapshot();
                    if (!TryInferBindingForUnmappedSource(sourceId, roster, out participantId, out displayName))
                    {
                        LogUnmappedSourceIdOnce(sourceId);
                        continue;
                    }
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
        var none = (uint)DominantSpeakerChangedEventArgs.None;
        if (sourceId != none)
        {
            _lastNonNoneDominantSourceId = sourceId;
        }
    }

    /// <summary>
    /// Graph often adds <c>mediaStreams</c> after the first audio frames. Delta callbacks can be missed; re-scan fixes Entra names for demos.
    /// </summary>
    private void MaybeRescanParticipantMediaStreams()
    {
        var call = _attachedCall;
        var botId = _botClientId;
        if (call is null || string.IsNullOrWhiteSpace(botId))
        {
            return;
        }

        lock (_rescanLock)
        {
            if ((DateTime.UtcNow - _lastParticipantRescanUtc).TotalSeconds < 2.5)
            {
                return;
            }

            _lastParticipantRescanUtc = DateTime.UtcNow;
        }

        try
        {
            _meetingParticipants.ResyncParticipantMediaStreamsFromCall(call, botId);
            foreach (var p in call.Participants)
            {
                UpsertParticipantMappings(p, botId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Periodic participant mediaStreams rescan failed.");
        }
    }

    /// <summary>
    /// When <see cref="MeetingParticipantService"/> has already correlated MSI → Entra from <c>mediaStreams</c> (same parse as Graph), upgrade <see cref="ParticipantManager"/> from synthetic.
    /// </summary>
    private bool TryApplyRosterMediaStreamMap(uint sourceId, out string participantId, out string displayName)
    {
        participantId = string.Empty;
        displayName = string.Empty;
        if (!_meetingParticipants.TryResolveAudioSourceToEntra(sourceId, out var oid, out var dn))
        {
            return false;
        }

        lock (_inferLock)
        {
            var removed = _participantManager.TryBindAudioSource(sourceId, oid, dn, "RosterMediaStreamsMap");
            if (removed is not null)
            {
                _awsTranscribeService.RemoveParticipant(removed);
            }

            _awsTranscribeService.UpsertParticipant(oid, dn);
            return _participantManager.TryResolveAudioSource(sourceId, out participantId, out displayName);
        }
    }

    /// <summary>
    /// Maps the Nth distinct MSI (first-seen order) to the Nth human in roster ingest order. Caller must hold <see cref="_inferLock"/>.
    /// </summary>
    private bool TryAllocateJoinOrderRosterNoLock(uint sourceId, IReadOnlyList<RosterParticipantDto> roster, out string participantId, out string displayName)
    {
        participantId = string.Empty;
        displayName = string.Empty;
        if (!_settings.MsiToRosterJoinOrderFallback || roster.Count == 0)
        {
            return false;
        }

        var idx = _joinOrderFallbackSourceIds.IndexOf(sourceId);
        if (idx < 0)
        {
            _joinOrderFallbackSourceIds.Add(sourceId);
            idx = _joinOrderFallbackSourceIds.Count - 1;
            if (idx == 0)
            {
                _logger.LogWarning(
                    "Graph did not provide mediaStreams source ids; using join-order fallback (Nth new MSI → Nth roster participant by ingest order). " +
                    "Entra display names are real; speaker↔name alignment is best-effort. Disable with Bot:MsiToRosterJoinOrderFallback=false.");
            }
        }

        var rIdx = Math.Min(idx, roster.Count - 1);
        var p = roster[rIdx];
        participantId = p.AzureAdObjectId;
        displayName = string.IsNullOrWhiteSpace(p.DisplayName) ? participantId : p.DisplayName.Trim();
        _participantManager.SetJoinOrderEntraHint(sourceId, p.AzureAdObjectId);
        return true;
    }

    /// <summary>
    /// When Graph omits <c>mediaStreams[].sourceId</c> for a stream, we never map that MSI to a roster user by headcount
    /// (e.g. "only one person in roster") — that mis-assigns the first packets before others join. Use a per-source placeholder;
    /// <see cref="ParticipantManager.TryBindAudioSource"/> upgrades to Entra when Graph sends mediaStreams.
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

            if (roster.Count == 0)
            {
                return false;
            }

            if (_settings.MsiToRosterJoinOrderFallback &&
                TryAllocateJoinOrderRosterNoLock(sourceId, roster, out _, out var fjDn))
            {
                var synId = ParticipantManager.SyntheticParticipantId(sourceId);
                var removedFj = _participantManager.TryBindAudioSource(sourceId, synId, fjDn, "JoinOrderDisplayFallback");
                if (removedFj is not null)
                {
                    _awsTranscribeService.RemoveParticipant(removedFj);
                }

                _awsTranscribeService.UpsertParticipant(synId, fjDn);
                return _participantManager.TryResolveAudioSource(sourceId, out participantId, out displayName);
            }

            var syntheticId = ParticipantManager.SyntheticParticipantId(sourceId);
            var syntheticName = $"Speaker ({sourceId})";
            if (Interlocked.Increment(ref _loggedMultiParticipantInferenceSkipped) == 1)
            {
                _logger.LogInformation(
                    "Graph has not mapped mediaStreams for some streams yet; using per-source placeholders until Entra mappings arrive (set Bot:MsiToRosterJoinOrderFallback=true for Entra names by join order).");
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
        if (dom == none && _lastNonNoneDominantSourceId != none)
        {
            dom = _lastNonNoneDominantSourceId;
        }

        if (dom != none && _participantManager.TryResolveAudioSource(dom, out participantId, out displayName))
        {
            return true;
        }

        if (dom != none && TryApplyRosterMediaStreamMap(dom, out participantId, out displayName))
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

        // 2+ humans, dominant MSI known but not yet mapped to Entra: join-order fallback or per-MSI placeholder.
        if (dom != none)
        {
            lock (_inferLock)
            {
                if (_settings.MsiToRosterJoinOrderFallback &&
                    TryAllocateJoinOrderRosterNoLock(dom, roster, out _, out var fjDn))
                {
                    var synId = ParticipantManager.SyntheticParticipantId(dom);
                    var removedFj = _participantManager.TryBindAudioSource(dom, synId, fjDn, "JoinOrderDisplayFallbackMixed");
                    if (removedFj is not null)
                    {
                        _awsTranscribeService.RemoveParticipant(removedFj);
                    }

                    _awsTranscribeService.UpsertParticipant(synId, fjDn);
                    return _participantManager.TryResolveAudioSource(dom, out participantId, out displayName);
                }

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

        foreach (var sourceId in GraphParticipantMediaStreams.ExtractSourceIds(resource))
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
