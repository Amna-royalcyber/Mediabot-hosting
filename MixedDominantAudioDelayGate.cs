using Microsoft.Skype.Bots.Media;

namespace TeamsMediaBot;

/// <summary>
/// Buffers mixed (single-buffer) PCM until Entra exists for the active <c>sourceId</c> or the buffer window elapses (then may apply dominant MSI).
/// </summary>
internal sealed class MixedDominantAudioDelayGate : IAsyncDisposable
{
    private static readonly uint DominantNone = (uint)DominantSpeakerChangedEventArgs.None;

    private readonly BotSettings _settings;
    private readonly ParticipantManager _participantManager;
    private readonly Func<uint?, string, string?, byte[], long, Task> _forwardMixedDirectAsync;
    private readonly object _lock = new();
    private readonly Timer _timer;
    private List<(byte[] Pcm, long Ts)>? _chunks;
    private DateTime? _firstUtc;
    private uint? _pendingSourceId;
    private string _pendingDisplay = "";
    private string? _pendingFallbackUserId;
    private uint _pendingDominantMsi = DominantNone;
    private volatile bool _disposed;

    public MixedDominantAudioDelayGate(
        BotSettings settings,
        ParticipantManager participantManager,
        Func<uint?, string, string?, byte[], long, Task> forwardMixedDirectAsync)
    {
        _settings = settings;
        _participantManager = participantManager;
        _forwardMixedDirectAsync = forwardMixedDirectAsync;
        _timer = new Timer(Tick, null, TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(200));
    }

    public async Task EnqueueAsync(
        uint? sourceStreamId,
        string displayName,
        string? userIdWhenNoSourceStream,
        byte[] pcmAudio,
        long timestamp,
        uint dominantSpeakerMsi)
    {
        if (_disposed || pcmAudio.Length == 0)
        {
            return;
        }

        if (sourceStreamId is uint sid && _participantManager.HasEntraOidForSource(sid))
        {
            await _forwardMixedDirectAsync(sourceStreamId, displayName, userIdWhenNoSourceStream, pcmAudio, timestamp);
            return;
        }

        lock (_lock)
        {
            _chunks ??= new List<(byte[], long)>();
            if (_chunks.Count == 0)
            {
                _firstUtc = DateTime.UtcNow;
            }

            _pendingSourceId = sourceStreamId;
            _pendingDisplay = displayName;
            _pendingFallbackUserId = userIdWhenNoSourceStream;
            _pendingDominantMsi = dominantSpeakerMsi;
            _chunks.Add((pcmAudio, timestamp));
        }

        if (ShouldFlush())
        {
            await FlushAsync();
        }
    }

    private bool ShouldFlush()
    {
        if (_firstUtc is not DateTime first)
        {
            return false;
        }

        if (_pendingSourceId is uint sid && _participantManager.HasEntraOidForSource(sid))
        {
            return true;
        }

        return (DateTime.UtcNow - first).TotalMilliseconds >= _settings.IdentityAudioBufferMilliseconds;
    }

    private async Task FlushAsync()
    {
        List<(byte[] Pcm, long Ts)>? batch;
        uint? sourceId;
        string displayName;
        string? fallbackUserId;
        uint dominantMsi;

        lock (_lock)
        {
            if (_chunks is null || _chunks.Count == 0)
            {
                return;
            }

            batch = _chunks;
            _chunks = null;
            _firstUtc = null;
            sourceId = _pendingSourceId;
            displayName = _pendingDisplay;
            fallbackUserId = _pendingFallbackUserId;
            dominantMsi = _pendingDominantMsi;
        }

        var effectiveSource = sourceId;
        if (effectiveSource is null && dominantMsi != DominantNone)
        {
            effectiveSource = dominantMsi;
            _participantManager.TryBindAudioSource(dominantMsi, null, string.Empty, "SyntheticDominantMixed");
        }

        foreach (var (pcm, ts) in batch)
        {
            await _forwardMixedDirectAsync(effectiveSource, displayName, fallbackUserId, pcm, ts);
        }
    }

    private void Tick(object? _)
    {
        if (_disposed)
        {
            return;
        }

        if (ShouldFlush())
        {
            _ = FlushAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        _timer.Dispose();
        await FlushAsync();
        _disposed = true;
    }
}
