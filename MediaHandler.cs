using Microsoft.Extensions.Logging;
using Microsoft.Graph.Communications.Calls;
using Microsoft.Graph.Communications.Calls.Media;
using Microsoft.Graph.Communications.Client;
using Microsoft.Skype.Bots.Media;

namespace TeamsMediaBot;

public sealed class MediaHandler
{
    private readonly ILogger<MediaHandler> _logger;
    private readonly AudioProcessor _audioProcessor;
    private readonly AwsTranscribeService _awsTranscribeService;
    private IAudioSocket? _audioSocket;
    private bool _loggedFirstAudioFrame;

    public MediaHandler(
        ILogger<MediaHandler> logger,
        AudioProcessor audioProcessor,
        AwsTranscribeService awsTranscribeService)
    {
        _logger = logger;
        _audioProcessor = audioProcessor;
        _awsTranscribeService = awsTranscribeService;
    }

    public IMediaSession CreateMediaSession(ICommunicationsClient communicationsClient)
    {
        var mediaConfiguration = new AudioSocketSettings
        {
            StreamDirections = StreamDirection.Recvonly,
            SupportedAudioFormat = AudioFormat.Pcm16K
        };

        var mediaSession = communicationsClient.CreateMediaSession(
            audioSocketSettings: mediaConfiguration,
            videoSocketSettings: (IEnumerable<VideoSocketSettings>?)null,
            vbssSocketSettings: null,
            dataSocketSettings: null,
            mediaSessionId: Guid.NewGuid());

        _audioSocket = mediaSession.AudioSocket;
        _audioSocket.AudioMediaReceived += OnAudioMediaReceived;
        _loggedFirstAudioFrame = false;

        _logger.LogInformation("Media session initialized and audio event subscribed.");
        return mediaSession;
    }

    private void OnAudioMediaReceived(object? sender, AudioMediaReceivedEventArgs args)
    {
        var incomingFrame = new AudioFrame(
            Data: AudioProcessor.ExtractBytes(args.Buffer),
            Timestamp: args.Buffer.Timestamp,
            Length: (int)args.Buffer.Length,
            Format: AudioFormat.Pcm16K);

        byte[] pcmChunk = _audioProcessor.ConvertToPcm(incomingFrame);
        _audioProcessor.BufferChunk(pcmChunk);
        _awsTranscribeService.SendAudioChunk(pcmChunk);

        if (!_loggedFirstAudioFrame && pcmChunk.Length > 0)
        {
            _loggedFirstAudioFrame = true;
            _logger.LogInformation(
                "First PCM chunk from Teams to Transcribe (length={Length} bytes). Speak in the meeting to drive transcription.",
                pcmChunk.Length);
        }

        _logger.LogDebug(
            "Audio frame received and buffered. Timestamp: {Timestamp}, Length: {Length}",
            incomingFrame.Timestamp,
            incomingFrame.Length);
    }
}
