using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

public sealed record TranscriptFragment(
    long AudioTimestamp,
    string Kind,
    string Text,
    string UserId,
    string DisplayName);

/// <summary>
/// Merges transcripts from multiple participant streams into a single timeline.
/// </summary>
public sealed class TranscriptAggregator : BackgroundService
{
    private readonly TranscriptBroadcaster _broadcaster;
    private readonly ILogger<TranscriptAggregator> _logger;
    private readonly Channel<TranscriptFragment> _incoming = Channel.CreateUnbounded<TranscriptFragment>();
    private readonly PriorityQueue<TranscriptFragment, long> _timeline = new();
    private readonly object _lock = new();

    public TranscriptAggregator(TranscriptBroadcaster broadcaster, ILogger<TranscriptAggregator> logger)
    {
        _broadcaster = broadcaster;
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
        await Task.Delay(120, cancellationToken);

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

            await _broadcaster.BroadcastAsync(
                item.Kind,
                item.Text,
                speakerLabel: item.DisplayName,
                azureAdObjectId: item.UserId);
        }
    }
}
