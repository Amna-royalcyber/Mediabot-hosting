using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;
using Microsoft.Skype.Bots.Media;

namespace TeamsMediaBot;

/// <summary>
/// Receives unmixed audio buffers and routes each source-id stream to TranscriptionManager.
/// </summary>
public sealed class ParticipantAudioStreamHandler
{
    private readonly AudioProcessor _audioProcessor;
    private readonly TranscriptionManager _transcriptionManager;
    private readonly ILogger<ParticipantAudioStreamHandler> _logger;

    public ParticipantAudioStreamHandler(
        AudioProcessor audioProcessor,
        TranscriptionManager transcriptionManager,
        ILogger<ParticipantAudioStreamHandler> logger)
    {
        _audioProcessor = audioProcessor;
        _transcriptionManager = transcriptionManager;
        _logger = logger;
    }

    public async Task HandleAsync(AudioMediaReceivedEventArgs args)
    {
        var unmixed = args.Buffer.UnmixedAudioBuffers;
        if (unmixed is null || !unmixed.Any())
        {
            return;
        }

        foreach (var ub in unmixed)
        {
            var sourceId = Convert.ToUInt32(ub.ActiveSpeakerId);
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

            await _transcriptionManager.ProcessParticipantAudioAsync(sourceId, pcm, ub.OriginalSenderTimestamp);
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
