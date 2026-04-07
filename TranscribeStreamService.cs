using System.Collections.Concurrent;
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
    private readonly AmazonTranscribeStreamingClient _client;
    private readonly TranscriptAggregator _aggregator;
    private readonly ILogger<TranscribeStreamService> _logger;
    private readonly ConcurrentQueue<(byte[] Bytes, long Timestamp)> _audioQueue = new();
    private readonly SemaphoreSlim _signal = new(0);
    private readonly CancellationTokenSource _cts = new();
    private readonly object _participantLock = new();

    private ParticipantIdentity _participant;
    private Task? _sessionTask;

    public TranscribeStreamService(
        BotSettings settings,
        TranscriptAggregator aggregator,
        ParticipantIdentity participant,
        ILogger<TranscribeStreamService> logger)
    {
        _aggregator = aggregator;
        _participant = participant;
        _logger = logger;
        _client = new AmazonTranscribeStreamingClient(RegionEndpoint.GetBySystemName(settings.AwsRegion));
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
        if (_sessionTask is not null)
        {
            return Task.CompletedTask;
        }

        _sessionTask = RunSessionAsync();
        return Task.CompletedTask;
    }

    public void EnqueueAudio(byte[] pcm16kMono, long timestamp)
    {
        if (pcm16kMono.Length == 0)
        {
            return;
        }

        _audioQueue.Enqueue((pcm16kMono, timestamp));
        _signal.Release();
    }

    private async Task RunSessionAsync()
    {
        var req = new StartStreamTranscriptionRequest
        {
            LanguageCode = LanguageCode.EnUS,
            MediaEncoding = MediaEncoding.Pcm,
            MediaSampleRateHertz = 16000,
            ShowSpeakerLabel = false,
            EnablePartialResultsStabilization = true,
            PartialResultsStability = PartialResultsStability.High,
            AudioStreamPublisher = GetNextAudioEventAsync
        };

        try
        {
            using var res = await _client.StartStreamTranscriptionAsync(req, _cts.Token);
            var stream = res.TranscriptResultStream;
            stream.TranscriptEventReceived += (_, e) =>
            {
                if (e.EventStreamEvent is TranscriptEvent te)
                {
                    _ = HandleTranscriptAsync(te);
                }
            };

            stream.StartProcessing();
            await Task.Delay(Timeout.Infinite, _cts.Token);
        }
        catch (OperationCanceledException)
        {
            // expected on dispose
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Per-participant Transcribe stream failed for {UserId}.", _participant.UserId);
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

            await _aggregator.PublishAsync(new TranscriptFragment(
                AudioTimestamp: (long)((result.StartTime ?? 0) * 10_000_000),
                Kind: result.IsPartial == true ? "Partial" : "Final",
                Text: text,
                UserId: participant.UserId,
                DisplayName: participant.DisplayName));
        }
    }

    private async Task<IAudioStreamEvent> GetNextAudioEventAsync()
    {
        const int targetChunkBytes = 16_000 * 2 * 320 / 1000; // 320ms of 16KHz mono PCM16
        var merged = new List<byte>(targetChunkBytes);
        long earliestTimestamp = 0;

        while (merged.Count < targetChunkBytes && !_cts.IsCancellationRequested)
        {
            await _signal.WaitAsync(_cts.Token);
            while (_audioQueue.TryDequeue(out var frame))
            {
                if (earliestTimestamp == 0)
                {
                    earliestTimestamp = frame.Timestamp;
                }

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
        _cts.Cancel();
        if (_sessionTask is not null)
        {
            await _sessionTask;
        }

        _signal.Dispose();
        _cts.Dispose();
        _client.Dispose();
    }
}
