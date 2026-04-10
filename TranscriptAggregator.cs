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
    string DisplayName);

/// <summary>
/// Merges transcripts from multiple participant streams into a single timeline.
/// </summary>
public sealed class TranscriptAggregator : BackgroundService
{
    private readonly BotSettings _settings;
    private readonly TranscriptBroadcaster _broadcaster;
    private readonly TranscriptIdentityResolver _identityResolver;
    private readonly ILogger<TranscriptAggregator> _logger;
    private readonly Channel<TranscriptFragment> _incoming = Channel.CreateUnbounded<TranscriptFragment>();
    private readonly PriorityQueue<TranscriptFragment, long> _timeline = new();
    private readonly object _lock = new();

    public TranscriptAggregator(
        BotSettings settings,
        TranscriptBroadcaster broadcaster,
        TranscriptIdentityResolver identityResolver,
        ILogger<TranscriptAggregator> logger)
    {
        _settings = settings;
        _broadcaster = broadcaster;
        _identityResolver = identityResolver;
        _logger = logger;
    }

    public ValueTask PublishAsync(TranscriptFragment fragment, CancellationToken cancellationToken = default) =>
        _incoming.Writer.WriteAsync(fragment, cancellationToken);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var next = await _incoming.Reader.ReadAsync(stoppingToken);
            lock (_lock)
            {
                _timeline.Enqueue(next, next.AudioTimestamp);
            }

            await DrainAsync(stoppingToken);
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

            var (resolvedUserId, resolvedDisplayName) = _identityResolver.Resolve(item.UserId, item.DisplayName);

            await _broadcaster.BroadcastAsync(
                item.Kind,
                item.Text,
                item.EmittedAtUtc,
                item.AudioTimestamp,
                speakerLabel: resolvedDisplayName,
                azureAdObjectId: resolvedUserId);
        }
    }
}
