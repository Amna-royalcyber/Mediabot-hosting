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
    uint? SourceStreamId = null,
    bool FromBufferedReplay = false);

/// <summary>
/// Merges transcripts from multiple participant streams into a single timeline.
/// </summary>
public sealed class TranscriptAggregator : BackgroundService
{
    private readonly BotSettings _settings;
    private readonly TranscriptBroadcaster _broadcaster;
    private readonly TranscriptIdentityResolver _identityResolver;
    private readonly IParticipantManager _participantManager;
    private readonly SpeakerIdentityStore _speakerIdentityStore;
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
        SpeakerIdentityStore speakerIdentityStore,
        TranscriptBuffer buffer,
        TranscriptDeduplicator deduplicator,
        ILogger<TranscriptAggregator> logger)
    {
        _settings = settings;
        _broadcaster = broadcaster;
        _identityResolver = identityResolver;
        _participantManager = participantManager;
        _speakerIdentityStore = speakerIdentityStore;
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
            if (!item.FromBufferedReplay && !_deduplicator.ShouldPass(item.SourceStreamId, item.Text, item.EmittedAtUtc))
            {
                return;
            }

            if (!item.FromBufferedReplay &&
                item.SourceStreamId is uint sidUnres &&
                (!_participantManager.TryGetBinding(sidUnres, out var bindingUnres) ||
                 bindingUnres is null ||
                 bindingUnres.State != IdentityState.Resolved))
            {
                _buffer.Buffer(item);
                _speakerIdentityStore.RegisterPendingTranscript(
                    sidUnres,
                    new SpeakerTranscriptRecord
                    {
                        Text = item.Text,
                        SourceId = sidUnres,
                        Timestamp = item.EmittedAtUtc
                    });
                await _broadcaster.BroadcastTempFinalAsync(
                    item.Kind,
                    item.Text,
                    item.EmittedAtUtc,
                    item.AudioTimestamp,
                    sidUnres);
                return;
            }
        }

        var (resolvedUserId, resolvedDisplayName) = _identityResolver.Resolve(
            item.UserId,
            item.DisplayName,
            item.SourceStreamId);

        if (string.Equals(item.Kind, "Final", StringComparison.OrdinalIgnoreCase) &&
            !item.FromBufferedReplay &&
            (resolvedUserId.StartsWith(ParticipantManager.SyntheticIdPrefix, StringComparison.OrdinalIgnoreCase) ||
             string.IsNullOrWhiteSpace(resolvedDisplayName)))
        {
            if (item.SourceStreamId is uint sid)
            {
                _buffer.Buffer(item);
                _speakerIdentityStore.RegisterPendingTranscript(
                    sid,
                    new SpeakerTranscriptRecord
                    {
                        Text = item.Text,
                        SourceId = sid,
                        Timestamp = item.EmittedAtUtc
                    });
                await _broadcaster.BroadcastTempFinalAsync(
                    item.Kind,
                    item.Text,
                    item.EmittedAtUtc,
                    item.AudioTimestamp,
                    sid);
            }

            return;
        }

        if (item.FromBufferedReplay)
        {
            if (string.Equals(item.Kind, "Final", StringComparison.OrdinalIgnoreCase) &&
                !_deduplicator.ShouldPass(item.SourceStreamId, item.Text, item.EmittedAtUtc))
            {
                return;
            }

            await _broadcaster.RecordAlbFinalChunkAsync(
                item.Kind,
                item.Text,
                item.EmittedAtUtc,
                item.AudioTimestamp,
                resolvedUserId,
                resolvedDisplayName,
                item.SourceStreamId);
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
            await HandleFragmentAsync(item with { FromBufferedReplay = true });
        }
    }

    public async Task ResolvePendingAsync(uint sourceId, string? _displayName = null)
    {
        var flushed = _buffer.ResolvePending(sourceId, _participantManager);
        foreach (var item in flushed.OrderBy(f => f.AudioTimestamp))
        {
            await HandleFragmentAsync(item with { FromBufferedReplay = true });
        }
    }
}
