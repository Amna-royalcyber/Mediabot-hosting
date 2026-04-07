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
    private int _pcmFrameCount;
    private long _pcmBytesTotal;

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
        _pcmFrameCount = 0;
        _pcmBytesTotal = 0;

        _logger.LogInformation("Media session initialized and audio event subscribed.");
        return mediaSession;
    }

    private void OnAudioMediaReceived(object? sender, AudioMediaReceivedEventArgs args)
    {
        int declaredLength = (int)args.Buffer.Length;
        byte[] extracted = AudioProcessor.ExtractBytes(args.Buffer);
        if (declaredLength > 0 && extracted.Length == 0)
        {
            _logger.LogError(
                "AudioMediaReceived: buffer Length={DeclaredLen} but ExtractBytes returned 0 bytes (reflection path may not match buffer type).",
                declaredLength);
        }

        var incomingFrame = new AudioFrame(
            Data: extracted,
            Timestamp: args.Buffer.Timestamp,
            Length: declaredLength,
            Format: AudioFormat.Pcm16K);

        byte[] pcmChunk = _audioProcessor.ConvertToPcm(incomingFrame);
        if (incomingFrame.Length > 0 && pcmChunk.Length == 0)
        {
            _logger.LogError(
                "AudioMediaReceived: non-zero frame length ({FrameLen}) produced empty PCM after ConvertToPcm (format {Format}).",
                incomingFrame.Length,
                incomingFrame.Format);
        }

        // AWS Transcribe path uses AwsTranscribeService only. Do not enqueue to AudioProcessor.BufferChunk — nothing consumes it (memory leak on long calls).
        _awsTranscribeService.SendAudioChunk(pcmChunk);

        if (pcmChunk.Length > 0)
        {
            _pcmFrameCount++;
            _pcmBytesTotal += pcmChunk.Length;

            if (!_loggedFirstAudioFrame)
            {
                _loggedFirstAudioFrame = true;
                _logger.LogInformation(
                    "First PCM chunk from Teams to Transcribe (length={Length} bytes). Speak in the meeting to drive transcription.",
                    pcmChunk.Length);
            }
            else if (_pcmFrameCount % 100 == 0)
            {
                _logger.LogInformation(
                    "Teams audio still flowing: {Frames} PCM frames, {Kilobytes} KB total sent to Transcribe.",
                    _pcmFrameCount,
                    _pcmBytesTotal / 1024);
            }
        }

        _logger.LogDebug(
            "Audio frame received and buffered. Timestamp: {Timestamp}, Length: {Length}",
            incomingFrame.Timestamp,
            incomingFrame.Length);
    }
}
