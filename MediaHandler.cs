using Microsoft.Extensions.Logging;
using Microsoft.Graph.Communications.Calls;
using Microsoft.Graph.Communications.Calls.Media;
using Microsoft.Graph.Communications.Client;
using Microsoft.Skype.Bots.Media;

namespace TeamsMediaBot;

public sealed class MediaHandler
{
    private readonly ILogger<MediaHandler> _logger;
    private readonly ParticipantAudioStreamHandler _participantAudioStreamHandler;
    private IAudioSocket? _audioSocket;

    public MediaHandler(
        ILogger<MediaHandler> logger,
        ParticipantAudioStreamHandler participantAudioStreamHandler)
    {
        _logger = logger;
        _participantAudioStreamHandler = participantAudioStreamHandler;
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
            _logger.LogDebug("Dominant speaker MSI changed: {SourceId}", e.CurrentDominantSpeaker);
        };
        _logger.LogInformation("Media session initialized and audio event subscribed.");
        return mediaSession;
    }

    private async void OnAudioMediaReceived(object? sender, AudioMediaReceivedEventArgs args)
    {
        try
        {
            await _participantAudioStreamHandler.HandleAsync(args);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed handling unmixed participant audio.");
        }
    }
}
