using System.Collections.Concurrent;
using System.Reflection;
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
    private readonly ILogger<ParticipantAudioRouter> _logger;

    /// <summary>Teams media source id for the current dominant speaker (from <see cref="IAudioSocket.DominantSpeakerChanged"/>).</summary>
    private uint _dominantSourceId = (uint)DominantSpeakerChangedEventArgs.None;

    private ICall? _attachedCall;
    private string _botClientId = string.Empty;
    private readonly object _rescanLock = new();
    private DateTime _lastParticipantRescanUtc = DateTime.MinValue;

    private int _loggedMixedMode;
    private int _loggedDominantNotYetMixed;
    private int _loggedUnknownMixedFallback;

    private readonly object _inferLock = new();

    /// <summary>Last time we saw PCM for this MSI (unmixed chunk or dominant-speaker event). Used for mixed-mode “recent speakers” only.</summary>
    private readonly ConcurrentDictionary<uint, DateTime> _lastSeenAudio = new();

    private static readonly TimeSpan RecentSpeakerWindow = TimeSpan.FromSeconds(2);

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
        _attachedCall = call;
        _botClientId = botClientId ?? string.Empty;
        var none = (uint)DominantSpeakerChangedEventArgs.None;
        _dominantSourceId = none;
        _lastSeenAudio.Clear();
        lock (_rescanLock)
        {
            _lastParticipantRescanUtc = DateTime.MinValue;
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
            var sourceId = ResolveUnmixedStreamSourceId(ub);
            if (sourceId == (uint)DominantSpeakerChangedEventArgs.None)
            {
                continue;
            }

            _lastSeenAudio[sourceId] = DateTime.UtcNow;

            if (!_participantManager.TryResolveAudioSource(sourceId, out var participantId, out var displayName))
            {
                if (!TryApplyRosterMediaStreamMap(sourceId, out participantId, out displayName))
                {
                    EnsureSyntheticBinding(sourceId);
                    participantId = ParticipantManager.SyntheticParticipantId(sourceId);
                    displayName = string.Empty;
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
                sourceId,
                displayName,
                pcm,
                ub.OriginalSenderTimestamp);
        }
    }

    /// <summary>
    /// Mixed meeting audio (single buffer) — attribute text only via <c>sourceId</c> → <see cref="ParticipantManager"/> (no roster name guessing).
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

        if (!TryResolveMixedAttribution(
                roster,
                out var mixedSourceId,
                out var mixedDisplayName,
                out var mixedUserIdWhenNoStream))
        {
            mixedSourceId = null;
            mixedDisplayName = string.Empty;
            mixedUserIdWhenNoStream = null;
            if (Interlocked.Increment(ref _loggedUnknownMixedFallback) == 1)
            {
                _logger.LogWarning("No authoritative mixed attribution — sending audio without speaker identity (unknown source).");
            }
        }

        if (Interlocked.Increment(ref _loggedMixedMode) == 1)
        {
            _logger.LogInformation(
                "Using mixed main audio buffer with dominant-speaker labeling (sourceId map + Teams dominant MSI). " +
                "For per-person audio without mixing, enable unmixed meeting audio when the client supports it.");
        }

        await _awsTranscribeService.SendMixedDominantAudioAsync(
            mixedSourceId,
            mixedDisplayName,
            mixedUserIdWhenNoStream,
            pcm,
            args.Buffer.Timestamp);
    }

    /// <summary>Teams raises dominant speaker MSI; must align with participant mediaStreams sourceId for correct names.</summary>
    public void SetDominantSpeaker(uint sourceId)
    {
        _dominantSourceId = sourceId;
        if (sourceId != (uint)DominantSpeakerChangedEventArgs.None)
        {
            _lastSeenAudio[sourceId] = DateTime.UtcNow;
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
            _participantManager.TryBindAudioSource(sourceId, oid, dn, "RosterMediaStreamsMap");
            _awsTranscribeService.UpsertParticipant(oid, dn);
            return _participantManager.TryResolveAudioSource(sourceId, out participantId, out displayName);
        }
    }

    private void EnsureSyntheticBinding(uint sourceId)
    {
        lock (_inferLock)
        {
            if (!_participantManager.HasBinding(sourceId))
            {
                _participantManager.TryBindAudioSource(sourceId, null, string.Empty, "SyntheticUntilGraph");
            }
        }
    }

    /// <summary>
    /// Resolves mixed-audio attribution with strict determinism:
    /// only when exactly one active sourceId is known. Otherwise emit unknown (no source assignment).
    /// </summary>
    private bool TryResolveMixedAttribution(
        IReadOnlyList<RosterParticipantDto> roster,
        out uint? sourceStreamId,
        out string displayName,
        out string? userIdWhenNoSourceStream)
    {
        sourceStreamId = null;
        displayName = string.Empty;
        userIdWhenNoSourceStream = null;
        _ = roster;
        var activeSources = GetActiveSourceIds();
        if (activeSources.Count != 1)
        {
            if (Interlocked.Increment(ref _loggedDominantNotYetMixed) == 1)
            {
                _logger.LogWarning(
                    "Mixed audio has {ActiveCount} active sourceIds; attribution disabled to prevent identity guessing.",
                    activeSources.Count);
            }
            return false;
        }

        var onlySourceId = activeSources[0];
        if (_participantManager.TryResolveAudioSource(onlySourceId, out _, out displayName))
        {
            sourceStreamId = onlySourceId;
            return true;
        }

        if (TryApplyRosterMediaStreamMap(onlySourceId, out _, out displayName))
        {
            sourceStreamId = onlySourceId;
            return true;
        }

        _participantManager.TryBindAudioSource(onlySourceId, null, string.Empty, "SyntheticDominantMixed");
        if (_participantManager.TryResolveAudioSource(onlySourceId, out _, out displayName))
        {
            sourceStreamId = onlySourceId;
            return true;
        }

        return false;
    }

    private List<uint> GetActiveSourceIds()
    {
        var now = DateTime.UtcNow;
        return _lastSeenAudio
            .Where(kvp => (now - kvp.Value) <= RecentSpeakerWindow)
            .Select(kvp => kvp.Key)
            .ToList();
    }

    /// <summary>
    /// Prefer a dedicated stream/source id when the SDK exposes it; otherwise fall back to <see cref="UnmixedAudioBuffer.ActiveSpeakerId"/> (dominant).
    /// </summary>
    private static uint ResolveUnmixedStreamSourceId(UnmixedAudioBuffer ub)
    {
        var none = (uint)DominantSpeakerChangedEventArgs.None;
        try
        {
            foreach (var propName in new[] { "SourceId", "StreamSourceId", "MediaSourceId" })
            {
                var p = ub.GetType().GetProperty(propName, BindingFlags.Public | BindingFlags.Instance);
                if (p is null)
                {
                    continue;
                }

                var val = p.GetValue(ub);
                switch (val)
                {
                    case uint u when u != 0 && u != none:
                        return u;
                    case int i when i > 0:
                        return (uint)i;
                }
            }
        }
        catch
        {
            // fall through to ActiveSpeakerId
        }

        return Convert.ToUInt32(ub.ActiveSpeakerId);
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
            _participantManager.TryBindAudioSource(sourceId, pid, dn, "Graph");
            _logger.LogInformation("Bound sourceId {SourceId} -> {DisplayName} ({ParticipantId}).", sourceId, dn, pid);
        }

        _awsTranscribeService.UpsertParticipant(pid, dn);
    }

    private void RemoveParticipantMappings(IParticipant participant)
    {
        // AWS Transcribe sessions are keyed by immutable sourceId; do not tear down streams on roster leave
        // (avoids identity churn and matches bind-once semantics).
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
