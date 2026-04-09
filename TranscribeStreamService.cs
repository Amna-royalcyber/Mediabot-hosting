using System.Collections.Concurrent;
using System.Globalization;
using Amazon;
using Amazon.TranscribeStreaming;
using Amazon.TranscribeStreaming.Model;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

public sealed record ParticipantIdentity(string UserId, string DisplayName);

/// <summary>
/// One AWS Transcribe streaming session per participant/source.
/// </summary>
public sealed class TranscribeStreamService : IAsyncDisposable
{
    private readonly BotSettings _settings;
    private readonly TranscriptAggregator _aggregator;
    private readonly ILogger<TranscribeStreamService> _logger;
    private readonly bool _broadcastPartials;
    private readonly ConcurrentQueue<(byte[] Bytes, long Timestamp)> _audioQueue = new();
    private readonly SemaphoreSlim _signal = new(0);
    private readonly CancellationTokenSource _cts = new();
    private readonly object _participantLock = new();
    private readonly object _runLock = new();

    private ParticipantIdentity _participant;
    private Task? _sessionTask;
    private string? _lastFinalDedupeKey;
    private string? _lastPartialTranscript;
    private DateTime _lastPartialSentAtUtc = DateTime.MinValue;

    private DateTime _lastRealAudioUtc;
    private Timer? _silenceKeepAliveTimer;

    public TranscribeStreamService(
        BotSettings settings,
        TranscriptAggregator aggregator,
        ParticipantIdentity participant,
        ILogger<TranscribeStreamService> logger)
    {
        _settings = settings;
        _aggregator = aggregator;
        _participant = participant;
        _logger = logger;
        _broadcastPartials = settings.TranscriptBroadcastPartials;
        _lastRealAudioUtc = DateTime.UtcNow;
    }

    public void UpdateParticipant(ParticipantIdentity participant)
    {
        lock (_participantLock)
        {
            _participant = participant;
        }
    }

    public Task EnsureStartedAsync()
    {
        lock (_runLock)
        {
            if (_sessionTask is not null && !_sessionTask.IsCompleted)
            {
                return Task.CompletedTask;
            }

            _sessionTask = RunSessionLoopAsync();

            if (_silenceKeepAliveTimer is null)
            {
                _silenceKeepAliveTimer = new Timer(
                    EnqueueSilenceKeepAliveIfNeeded,
                    null,
                    dueTime: TimeSpan.FromSeconds(4),
                    period: TimeSpan.FromSeconds(4));
            }
        }

        return Task.CompletedTask;
    }

    public void EnqueueAudio(byte[] pcm16kMono, long timestamp)
    {
        if (pcm16kMono.Length == 0)
        {
            return;
        }

        _lastRealAudioUtc = DateTime.UtcNow;
        _audioQueue.Enqueue((pcm16kMono, timestamp));
        _signal.Release();
    }

    private void EnqueueSilenceKeepAliveIfNeeded(object? _)
    {
        if (_cts.IsCancellationRequested)
        {
            return;
        }

        try
        {
            if ((DateTime.UtcNow - _lastRealAudioUtc).TotalSeconds < 3.5)
            {
                return;
            }

            var chunkMs = Math.Clamp(_settings.TranscribeAudioChunkMilliseconds, 50, 500);
            var bytes = 16_000 * 2 * chunkMs / 1000;
            _audioQueue.Enqueue((new byte[bytes], 0L));
            _signal.Release();
        }
        catch (ObjectDisposedException)
        {
        }
    }

    private async Task RunSessionLoopAsync()
    {
        var attempt = 0;
        while (!_cts.IsCancellationRequested)
        {
            attempt++;
            using var client = new AmazonTranscribeStreamingClient(RegionEndpoint.GetBySystemName(_settings.AwsRegion));
            var req = new StartStreamTranscriptionRequest
            {
                LanguageCode = LanguageCode.EnUS,
                MediaEncoding = MediaEncoding.Pcm,
                MediaSampleRateHertz = 16000,
                ShowSpeakerLabel = false,
                EnablePartialResultsStabilization = true,
                PartialResultsStability = PartialResultsStability.Medium,
                AudioStreamPublisher = GetNextAudioEventAsync
            };

            try
            {
                using var res = await client.StartStreamTranscriptionAsync(req, _cts.Token);
                var stream = res.TranscriptResultStream;
                stream.ExceptionReceived += (_, ev) =>
                {
                    _logger.LogError(ev.EventStreamException, "Transcribe result stream exception for {UserId}.", _participant.UserId);
                };
                stream.TranscriptEventReceived += (_, e) =>
                {
                    if (e.EventStreamEvent is TranscriptEvent te)
                    {
                        _ = HandleTranscriptAsync(te);
                    }
                };

                stream.StartProcessing();
                await Task.Delay(Timeout.Infinite, _cts.Token);
                return;
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Per-participant Transcribe stream failed for {UserId}; retrying.", _participant.UserId);
                try
                {
                    await Task.Delay(Math.Min(5000, 250 * attempt), _cts.Token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    private async Task HandleTranscriptAsync(TranscriptEvent te)
    {
        if (te.Transcript?.Results is null)
        {
            return;
        }

        ParticipantIdentity participant;
        lock (_participantLock)
        {
            participant = _participant;
        }

        foreach (var result in te.Transcript.Results)
        {
            if (result.Alternatives?.Count is not > 0)
            {
                continue;
            }

            var text = result.Alternatives[0].Transcript ?? string.Empty;
            if (string.IsNullOrWhiteSpace(text))
            {
                continue;
            }

            if (result.IsPartial == true)
            {
                if (!_broadcastPartials)
                {
                    continue;
                }

                var now = DateTime.UtcNow;
                if (string.Equals(_lastPartialTranscript, text, StringComparison.Ordinal))
                {
                    continue;
                }

                var minGap = Math.Clamp(_settings.TranscribePartialMinIntervalMilliseconds, 30, 500);
                if ((now - _lastPartialSentAtUtc).TotalMilliseconds < minGap)
                {
                    continue;
                }

                _lastPartialTranscript = text;
                _lastPartialSentAtUtc = now;
                await _aggregator.PublishAsync(new TranscriptFragment(
                    AudioTimestamp: (long)((result.StartTime ?? 0) * 10_000_000),
                    EmittedAtUtc: DateTime.UtcNow,
                    Kind: "Partial",
                    Text: text,
                    UserId: participant.UserId,
                    DisplayName: participant.DisplayName));
                continue;
            }

            var start = (double)(result.StartTime ?? 0);
            var end = (double)(result.EndTime ?? 0);
            var dedupeKey =
                start.ToString("F6", CultureInfo.InvariantCulture) + "|" +
                end.ToString("F6", CultureInfo.InvariantCulture) + "|" + text;
            if (string.Equals(_lastFinalDedupeKey, dedupeKey, StringComparison.Ordinal))
            {
                continue;
            }

            _lastFinalDedupeKey = dedupeKey;

            await _aggregator.PublishAsync(new TranscriptFragment(
                AudioTimestamp: (long)((result.StartTime ?? 0) * 10_000_000),
                EmittedAtUtc: DateTime.UtcNow,
                Kind: "Final",
                Text: text,
                UserId: participant.UserId,
                DisplayName: participant.DisplayName));
        }
    }

    private async Task<IAudioStreamEvent> GetNextAudioEventAsync()
    {
        var chunkMs = Math.Clamp(_settings.TranscribeAudioChunkMilliseconds, 50, 500);
        var targetChunkBytes = 16_000 * 2 * chunkMs / 1000;
        var merged = new List<byte>(targetChunkBytes);

        while (merged.Count < targetChunkBytes && !_cts.IsCancellationRequested)
        {
            await _signal.WaitAsync(_cts.Token);
            while (_audioQueue.TryDequeue(out var frame))
            {
                merged.AddRange(frame.Bytes);
                if (merged.Count >= targetChunkBytes)
                {
                    break;
                }
            }
        }

        if (merged.Count == 0)
        {
            throw new OperationCanceledException(_cts.Token);
        }

        return new AudioEvent
        {
            AudioChunk = new MemoryStream(merged.ToArray(), writable: false)
        };
    }

    public async ValueTask DisposeAsync()
    {
        _silenceKeepAliveTimer?.Dispose();
        _silenceKeepAliveTimer = null;
        _cts.Cancel();
        if (_sessionTask is not null)
        {
            await _sessionTask;
        }

        _signal.Dispose();
        _cts.Dispose();
    }
}
