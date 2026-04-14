using System.Reflection;
using System.Runtime.InteropServices;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;
using Microsoft.Graph.Communications.Calls;
using Microsoft.Graph.Communications.Resources;
using Microsoft.Skype.Bots.Media;

namespace TeamsMediaBot;

/// <summary>
/// Routes each unmixed Teams media stream (stable <c>sourceId</c>) to Azure Speech. Mixed-only meetings are not transcribed.
/// Identity comes from Graph roster <c>mediaStreams</c> before transcription (see <see cref="MeetingParticipantService"/>).
/// </summary>
public sealed class ParticipantAudioRouter
{
    private readonly AudioProcessor _audioProcessor;
    private readonly AzureSpeechTranscriptionService _azureSpeech;
    private readonly MeetingParticipantService _meetingParticipants;
    private readonly ParticipantManager _participantManager;
    private readonly ILogger<ParticipantAudioRouter> _logger;

    private ICall? _attachedCall;
    private string _botClientId = string.Empty;
    private readonly object _rescanLock = new();
    private DateTime _lastParticipantRescanUtc = DateTime.MinValue;
    private int _loggedNoUnmixed;

    public ParticipantAudioRouter(
        AudioProcessor audioProcessor,
        AzureSpeechTranscriptionService azureSpeech,
        MeetingParticipantService meetingParticipants,
        ParticipantManager participantManager,
        ILogger<ParticipantAudioRouter> logger)
    {
        _audioProcessor = audioProcessor;
        _azureSpeech = azureSpeech;
        _meetingParticipants = meetingParticipants;
        _participantManager = participantManager;
        _logger = logger;
    }

    public void AttachToCall(ICall call, string botClientId)
    {
        _attachedCall = call;
        _botClientId = botClientId ?? string.Empty;
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
            if (Interlocked.Increment(ref _loggedNoUnmixed) == 1)
            {
                _logger.LogInformation(
                    "No unmixed audio buffers in this meeting; transcription requires unmixed participant audio (ReceiveUnmixedMeetingAudio). Mixed-only capture is not transcribed.");
            }

            return;
        }

        foreach (var ub in unmixed)
        {
            var sourceId = ResolveUnmixedStreamSourceId(ub);
            if (sourceId == (uint)DominantSpeakerChangedEventArgs.None)
            {
                continue;
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

            if (_meetingParticipants.TryGetParticipantForMediaStream(sourceId, out _, out _, out var dn))
            {
                _logger.LogDebug("PCM for stream {SourceId} ({Name}).", sourceId, dn);
            }

            await _azureSpeech.ProcessPcm16Async(sourceId, pcm, ub.OriginalSenderTimestamp);
        }
    }

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
            // fall through
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
    }

    private void RemoveParticipantMappings(IParticipant participant)
    {
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
