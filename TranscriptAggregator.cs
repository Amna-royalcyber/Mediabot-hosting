using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

public sealed record TranscriptFragment(
    long AudioTimestamp,
    DateTime EmittedAtUtc,
    string Kind,
    string Text,
    string UserId,
    string DisplayName,
    uint? SourceStreamId = null);

/// <summary>
/// Merges transcripts from multiple participant streams into a single timeline.
/// </summary>
public sealed class TranscriptAggregator : BackgroundService
{
    private readonly BotSettings _settings;
    private readonly TranscriptBroadcaster _broadcaster;
    private readonly TranscriptIdentityResolver _identityResolver;
    private readonly IParticipantManager _participantManager;
    private readonly TranscriptBuffer _buffer;
    private readonly TranscriptDeduplicator _deduplicator;
    private readonly ILogger<TranscriptAggregator> _logger;
    private readonly Channel<TranscriptFragment> _incoming = Channel.CreateUnbounded<TranscriptFragment>();
    private readonly PriorityQueue<TranscriptFragment, long> _timeline = new();
    private readonly object _lock = new();

    public TranscriptAggregator(
        BotSettings settings,
        TranscriptBroadcaster broadcaster,
        TranscriptIdentityResolver identityResolver,
        IParticipantManager participantManager,
        TranscriptBuffer buffer,
        TranscriptDeduplicator deduplicator,
        ILogger<TranscriptAggregator> logger)
    {
        _settings = settings;
        _broadcaster = broadcaster;
        _identityResolver = identityResolver;
        _participantManager = participantManager;
        _buffer = buffer;
        _deduplicator = deduplicator;
        _logger = logger;
    }

    public ValueTask PublishAsync(TranscriptFragment fragment, CancellationToken cancellationToken = default) =>
        _incoming.Writer.WriteAsync(fragment, cancellationToken);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        while (!stoppingToken.IsCancellationRequested)
        {
            var waitReadTask = _incoming.Reader.WaitToReadAsync(stoppingToken).AsTask();
            var tickTask = timer.WaitForNextTickAsync(stoppingToken).AsTask();
            var completed = await Task.WhenAny(waitReadTask, tickTask);
            if (completed == waitReadTask && await waitReadTask)
            {
                while (_incoming.Reader.TryRead(out var next))
                {
                    lock (_lock)
                    {
                        _timeline.Enqueue(next, next.AudioTimestamp);
                    }
                }

                await DrainAsync(stoppingToken);
            }

            await FlushResolvedBufferedAsync();
        }
    }

    private async Task DrainAsync(CancellationToken cancellationToken)
    {
        var mergeMs = Math.Clamp(_settings.TranscriptTimelineMergeMilliseconds, 0, 200);
        if (mergeMs > 0)
        {
            await Task.Delay(mergeMs, cancellationToken);
        }

        while (true)
        {
            TranscriptFragment item;
            lock (_lock)
            {
                if (_timeline.Count == 0)
                {
                    break;
                }

                item = _timeline.Dequeue();
            }

            await HandleFragmentAsync(item);
        }
    }

    private async Task HandleFragmentAsync(TranscriptFragment item)
    {
        if (string.Equals(item.Kind, "Final", StringComparison.OrdinalIgnoreCase))
        {
            if (!_deduplicator.ShouldPass(item.SourceStreamId, item.Text, item.EmittedAtUtc))
            {
                return;
            }

            if (item.SourceStreamId is uint sid &&
                (!_participantManager.TryGetBinding(sid, out var binding) || binding is null || binding.State != IdentityState.Resolved))
            {
                _buffer.Buffer(item);
                await _broadcaster.BroadcastAsync(
                    item.Kind,
                    item.Text,
                    item.EmittedAtUtc,
                    item.AudioTimestamp,
                    speakerLabel: $"Unresolved Speaker ({sid})",
                    azureAdObjectId: ParticipantManager.SyntheticParticipantId(sid),
                    sourceStreamId: sid);
                return;
            }
        }

        var (resolvedUserId, resolvedDisplayName) = _identityResolver.Resolve(
            item.UserId,
            item.DisplayName,
            item.SourceStreamId);

        if (string.Equals(item.Kind, "Final", StringComparison.OrdinalIgnoreCase) &&
            (resolvedUserId.StartsWith(ParticipantManager.SyntheticIdPrefix, StringComparison.OrdinalIgnoreCase) ||
             resolvedDisplayName.StartsWith("Unresolved Speaker", StringComparison.OrdinalIgnoreCase)))
        {
            if (item.SourceStreamId is uint sid)
            {
                _buffer.Buffer(item);
                await _broadcaster.BroadcastAsync(
                    item.Kind,
                    item.Text,
                    item.EmittedAtUtc,
                    item.AudioTimestamp,
                    speakerLabel: $"Unresolved Speaker ({sid})",
                    azureAdObjectId: ParticipantManager.SyntheticParticipantId(sid),
                    sourceStreamId: sid);
            }
            return;
        }

        await _broadcaster.BroadcastAsync(
            item.Kind,
            item.Text,
            item.EmittedAtUtc,
            item.AudioTimestamp,
            speakerLabel: resolvedDisplayName,
            azureAdObjectId: resolvedUserId,
            sourceStreamId: item.SourceStreamId);
    }

    private async Task FlushResolvedBufferedAsync()
    {
        var flushed = _buffer.DrainResolved(_participantManager);
        foreach (var item in flushed.OrderBy(f => f.AudioTimestamp))
        {
            await HandleFragmentAsync(item);
        }
    }

    public async Task ResolvePending(uint sourceId, string? _displayName = null)
    {
        var flushed = _buffer.ResolvePending(sourceId, _participantManager);
        foreach (var item in flushed.OrderBy(f => f.AudioTimestamp))
        {
            await HandleFragmentAsync(item);
        }
    }
}
