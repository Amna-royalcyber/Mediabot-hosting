using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;
using Microsoft.Skype.Bots.Media;

namespace TeamsMediaBot;

public sealed record AudioFrame(byte[] Data, long Timestamp, int Length, AudioFormat Format);

public sealed class AudioProcessor
{
    private readonly ILogger<AudioProcessor> _logger;
    private readonly ConcurrentQueue<byte[]> _bufferedChunks = new();
    private readonly SemaphoreSlim _signal = new(0);

    public AudioProcessor(ILogger<AudioProcessor> logger)
    {
        _logger = logger;
    }

    public byte[] ConvertToPcm(AudioFrame frame)
    {
        if (frame.Length <= 0 || frame.Data.Length == 0)
        {
            return Array.Empty<byte>();
        }

        if (frame.Format == AudioFormat.Pcm16K)
        {
            return frame.Data;
        }

        // For non-PCM16K frames, a transcoder step should be added here.
        // Current implementation keeps raw bytes to preserve stream continuity.
        _logger.LogWarning("Unsupported source format {Format}; passing through raw frame.", frame.Format);
        return frame.Data;
    }

    public void BufferChunk(byte[] pcmChunk)
    {
        if (pcmChunk.Length == 0)
        {
            return;
        }

        _bufferedChunks.Enqueue(pcmChunk);
        _signal.Release();
    }

    public async IAsyncEnumerable<ReadOnlyMemory<byte>> GetAwsCompatibleAudioStreamAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await _signal.WaitAsync(cancellationToken);

            while (_bufferedChunks.TryDequeue(out byte[]? chunk))
            {
                // AWS Transcribe streaming accepts linear PCM chunks over a continuous stream.
                yield return chunk;
            }
        }
    }

    public static byte[] ExtractBytes(object mediaBuffer)
    {
        var bufferType = mediaBuffer.GetType();
        var dataProperty = bufferType.GetProperty("Data");
        var lengthProperty = bufferType.GetProperty("Length");

        if (dataProperty is null || lengthProperty is null)
        {
            return Array.Empty<byte>();
        }

        object? dataValue = dataProperty.GetValue(mediaBuffer);
        int length = Convert.ToInt32(lengthProperty.GetValue(mediaBuffer) ?? 0);

        if (length <= 0 || dataValue is null)
        {
            return Array.Empty<byte>();
        }

        if (dataValue is byte[] managed)
        {
            if (managed.Length == length)
            {
                return managed;
            }

            var copy = new byte[length];
            Buffer.BlockCopy(managed, 0, copy, 0, Math.Min(length, managed.Length));
            return copy;
        }

        if (dataValue is IntPtr ptr)
        {
            var copy = new byte[length];
            Marshal.Copy(ptr, copy, 0, length);
            return copy;
        }

        return Array.Empty<byte>();
    }
}
