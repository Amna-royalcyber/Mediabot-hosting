using Microsoft.Extensions.Logging;
using Microsoft.Graph.Communications.Calls;
using Microsoft.Graph.Communications.Calls.Media;
using Microsoft.Graph.Communications.Client;
using Microsoft.Skype.Bots.Media;

namespace TeamsMediaBot;

public sealed class MediaHandler
{
    private readonly ILogger<MediaHandler> _logger;
    private readonly ParticipantAudioRouter _participantAudioRouter;
    private IAudioSocket? _audioSocket;

    public MediaHandler(
        ILogger<MediaHandler> logger,
        ParticipantAudioRouter participantAudioRouter)
    {
        _logger = logger;
        _participantAudioRouter = participantAudioRouter;
    }

    public IMediaSession CreateMediaSession(ICommunicationsClient communicationsClient)
    {
        var mediaConfiguration = new AudioSocketSettings
        {
            StreamDirections = StreamDirection.Recvonly,
            SupportedAudioFormat = AudioFormat.Pcm16K,
            ReceiveUnmixedMeetingAudio = true,
            EnableAudioHealingForUnmixed = true
        };

        var mediaSession = communicationsClient.CreateMediaSession(
            audioSocketSettings: mediaConfiguration,
            videoSocketSettings: (IEnumerable<VideoSocketSettings>?)null,
            vbssSocketSettings: null,
            dataSocketSettings: null,
            mediaSessionId: Guid.NewGuid());

        _audioSocket = mediaSession.AudioSocket;
        _audioSocket.AudioMediaReceived += OnAudioMediaReceived;
        _audioSocket.DominantSpeakerChanged += (_, e) =>
        {
            var sid = Convert.ToUInt32(e.CurrentDominantSpeaker);
            _participantAudioRouter.SetDominantSpeaker(sid);
            _logger.LogDebug("Dominant speaker source id (MSI) set to {SourceId}", sid);
        };

        _logger.LogInformation(
            "Media session initialized with unmixed meeting audio. Speaker labels use immutable Teams source-id → Entra bindings (ParticipantManager).");
        return mediaSession;
    }

    private async void OnAudioMediaReceived(object? sender, AudioMediaReceivedEventArgs args)
    {
        try
        {
            await _participantAudioRouter.HandleAudioAsync(args);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed handling unmixed participant audio.");
        }
    }
}
