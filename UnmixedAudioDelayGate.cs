using System.Collections.Concurrent;
using System.Linq;

namespace TeamsMediaBot;

/// <summary>
/// Buffers PCM per <c>sourceId</c> until Entra is mapped or <see cref="BotSettings.IdentityAudioBufferMilliseconds"/> elapses.
/// </summary>
internal sealed class UnmixedAudioDelayGate : IAsyncDisposable
{
    private readonly BotSettings _settings;
    private readonly ParticipantManager _participantManager;
    private readonly Func<uint, string, byte[], long, Task> _forwardDirectAsync;
    private readonly ConcurrentDictionary<uint, PendingBuffer> _pending = new();
    private readonly Timer _timer;
    private volatile bool _disposed;
    private volatile bool _disabling;

    private sealed class PendingBuffer
    {
        public readonly Queue<(byte[] Pcm, long Ts)> Chunks = new();
        public DateTime FirstUtc;
    }

    public UnmixedAudioDelayGate(
        BotSettings settings,
        ParticipantManager participantManager,
        Func<uint, string, byte[], long, Task> forwardDirectAsync)
    {
        _settings = settings;
        _participantManager = participantManager;
        _forwardDirectAsync = forwardDirectAsync;
        _timer = new Timer(Tick, null, TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(200));
    }

    public async Task EnqueueAsync(uint sourceId, string displayName, byte[] pcmAudio, long timestamp)
    {
        if (_disposed || _disabling || pcmAudio.Length == 0)
        {
            return;
        }

        if (ShouldForwardWithoutDelay(sourceId))
        {
            await _forwardDirectAsync(sourceId, displayName, pcmAudio, timestamp);
            return;
        }

        var buf = _pending.GetOrAdd(sourceId, _ => new PendingBuffer());
        lock (buf.Chunks)
        {
            if (buf.Chunks.Count == 0)
            {
                buf.FirstUtc = DateTime.UtcNow;
            }

            buf.Chunks.Enqueue((pcmAudio, timestamp));
        }

        if (ShouldFlush(sourceId))
        {
            await FlushSourceAsync(sourceId, displayName);
        }
    }

    /// <summary>Called when Graph maps Entra to this <paramref name="sourceId"/>; drains any buffered PCM.</summary>
    public async Task OnIdentityResolvedAsync(uint sourceId)
    {
        if (_disposed || !_pending.ContainsKey(sourceId))
        {
            return;
        }

        await FlushSourceAsync(sourceId, string.Empty);
    }

    private bool ShouldForwardWithoutDelay(uint sourceId) =>
        _participantManager.HasEntraOidForSource(sourceId);

    private bool ShouldFlush(uint sourceId)
    {
        if (ShouldForwardWithoutDelay(sourceId))
        {
            return true;
        }

        if (!_pending.TryGetValue(sourceId, out var buf))
        {
            return false;
        }

        return (DateTime.UtcNow - buf.FirstUtc).TotalMilliseconds >= _settings.IdentityAudioBufferMilliseconds;
    }

    private async Task FlushSourceAsync(uint sourceId, string displayNameFallback)
    {
        if (!_pending.TryRemove(sourceId, out var buf))
        {
            return;
        }

        List<(byte[] Pcm, long Ts)> batch;
        lock (buf.Chunks)
        {
            batch = buf.Chunks.ToList();
        }

        var dn = displayNameFallback;
        if (string.IsNullOrWhiteSpace(dn))
        {
            dn = _participantManager.GetTranscriptSpeakerLabel(sourceId);
        }

        foreach (var (pcm, ts) in batch)
        {
            await _forwardDirectAsync(sourceId, dn, pcm, ts);
        }
    }

    private void Tick(object? _)
    {
        if (_disposed || _disabling)
        {
            return;
        }

        foreach (var sid in _pending.Keys.ToArray())
        {
            if (ShouldFlush(sid))
            {
                _ = FlushSourceAsync(sid, string.Empty);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        _disabling = true;
        _timer.Dispose();
        foreach (var sid in _pending.Keys.ToArray())
        {
            await FlushSourceAsync(sid, string.Empty);
        }

        _pending.Clear();
        _disposed = true;
    }
}
