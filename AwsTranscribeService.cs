using System.Collections.Concurrent;
using System.Threading;
using Amazon;
using Amazon.TranscribeStreaming;
using Amazon.TranscribeStreaming.Model;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

public sealed class AwsTranscribeService : IAsyncDisposable
{
    private readonly ILogger<AwsTranscribeService> _logger;
    private readonly TranscriptBroadcaster _transcriptBroadcaster;
    private readonly ConcurrentQueue<byte[]> _audioQueue = new();
    private readonly SemaphoreSlim _audioSignal = new(0);
    private readonly CancellationTokenSource _cts = new();
    private readonly AmazonTranscribeStreamingClient _client;

    private Task? _streamingTask;
    private TaskCompletionSource<bool>? _sessionReady;
    private StartStreamTranscriptionResponse? _activeResponse;
    private int _eventReceivedLogBudget = 40;
    private long _chunksPublishedToSdk;
    private int _transcriptEventCount;
    private int _emptyTranscriptStreak;

    public AwsTranscribeService(
        ILogger<AwsTranscribeService> logger,
        TranscriptBroadcaster transcriptBroadcaster,
        BotSettings settings)
    {
        _logger = logger;
        _transcriptBroadcaster = transcriptBroadcaster;
        var regionName = !string.IsNullOrWhiteSpace(settings.AwsRegion)
            ? settings.AwsRegion
            : (Environment.GetEnvironmentVariable("AWS_REGION") ?? "us-east-1");
        _logger.LogInformation("AWS Transcribe Streaming client region: {Region}", regionName);
        _client = new AmazonTranscribeStreamingClient(RegionEndpoint.GetBySystemName(regionName));
    }

    /// <summary>
    /// Starts a single long-lived Transcribe session. Awaits until the stream is accepted or fails (credentials, region, network).
    /// </summary>
    public async Task StartStreamingAsync(CancellationToken cancellationToken = default)
    {
        if (_streamingTask is not null)
        {
            return;
        }

        _sessionReady = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var request = new StartStreamTranscriptionRequest
        {
            LanguageCode = LanguageCode.EnUS,
            MediaEncoding = MediaEncoding.Pcm,
            MediaSampleRateHertz = 16000,
            EnablePartialResultsStabilization = true,
            PartialResultsStability = PartialResultsStability.Medium,
            AudioStreamPublisher = GetNextAudioEventAsync
        };

        _streamingTask = RunStreamingSessionAsync(request, _sessionReady, cancellationToken);

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);
        try
        {
            await _sessionReady.Task.WaitAsync(linked.Token);
        }
        catch
        {
            _streamingTask = null;
            _sessionReady = null;
            _activeResponse = null;
            throw;
        }
    }

    private async Task RunStreamingSessionAsync(
        StartStreamTranscriptionRequest request,
        TaskCompletionSource<bool> sessionReady,
        CancellationToken cancellationToken)
    {
        try
        {
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);
            _activeResponse = await _client.StartStreamTranscriptionAsync(request, linkedCts.Token);

            Interlocked.Exchange(ref _eventReceivedLogBudget, 40);
            Interlocked.Exchange(ref _chunksPublishedToSdk, 0);
            Interlocked.Exchange(ref _transcriptEventCount, 0);
            Interlocked.Exchange(ref _emptyTranscriptStreak, 0);

            var stream = _activeResponse.TranscriptResultStream;
            stream.ExceptionReceived += (_, e) =>
            {
                _logger.LogError(e.EventStreamException, "AWS Transcribe stream exception");
            };

            stream.InitialResponseReceived += (_, _) =>
            {
                _logger.LogInformation("AWS Transcribe initial response received (session active).");
            };

            // Prefer EventReceived: covers all event types; some SDK paths only surface TranscriptEvent here.
            stream.EventReceived += (_, e) =>
            {
                try
                {
                    if (e.EventStreamEvent is TranscriptEvent te)
                    {
                        var c = Interlocked.Increment(ref _transcriptEventCount);
                        if (c == 1 || c % 25 == 0)
                        {
                            _logger.LogInformation(
                                "Transcribe TranscriptEvent #{Ordinal}: {ResultCount} result(s).",
                                c,
                                te.Transcript?.Results?.Count ?? 0);
                        }

                        HandleTranscriptResult(te);
                    }
                    else if (Interlocked.Decrement(ref _eventReceivedLogBudget) >= 0)
                    {
                        _logger.LogInformation(
                            "Transcribe EventReceived (non-transcript): {Type}",
                            e.EventStreamEvent?.GetType().FullName ?? "(null)");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error handling Transcribe EventReceived");
                }
            };

            sessionReady.TrySetResult(true);

            // Keep the session task alive until shutdown; the SDK processes the response stream on background threads.
            try
            {
                await Task.Delay(Timeout.Infinite, _cts.Token);
            }
            catch (OperationCanceledException)
            {
                // Expected on dispose.
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to start AWS Transcribe streaming session");
            sessionReady.TrySetException(ex);
        }
    }

    public void SendAudioChunk(byte[] data)
    {
        if (data.Length == 0)
        {
            return;
        }

        _audioQueue.Enqueue(data);
        _audioSignal.Release();
    }

    public void HandleTranscriptResult(TranscriptEvent transcriptEvent)
    {
        if (transcriptEvent.Transcript?.Results is null)
        {
            _logger.LogDebug("TranscriptEvent had null Transcript.Results.");
            return;
        }

        var anyText = false;
        foreach (var result in transcriptEvent.Transcript.Results)
        {
            if (result.Alternatives.Count == 0)
            {
                continue;
            }

            string text = result.Alternatives[0].Transcript ?? string.Empty;
            if (string.IsNullOrWhiteSpace(text))
            {
                continue;
            }

            anyText = true;
            var kind = result.IsPartial == true ? "Partial" : "Final";
            if (kind == "Final")
            {
                _logger.LogInformation("Transcribe final: {Text}", text);
            }
            else
            {
                _logger.LogDebug("Transcribe partial: {Text}", text);
            }

            _ = _transcriptBroadcaster.BroadcastAsync(kind, text);
        }

        if (!anyText && transcriptEvent.Transcript.Results.Count > 0)
        {
            var n = Interlocked.Increment(ref _emptyTranscriptStreak);
            if (n <= 3 || n % 200 == 0)
            {
                _logger.LogWarning(
                    "Transcribe had {ResultCount} result row(s) but no usable text (occurrence {Occurrence}). If this persists, verify 16 kHz mono PCM (stereo mislabeled as mono breaks transcription).",
                    transcriptEvent.Transcript.Results.Count,
                    n);
            }
        }
    }

    /// <summary>
    /// Pulls the next non-empty PCM chunk. Never sends an empty chunk (can confuse or end the Transcribe stream).
    /// </summary>
    private async Task<IAudioStreamEvent> GetNextAudioEventAsync()
    {
        while (!_cts.IsCancellationRequested)
        {
            await _audioSignal.WaitAsync(_cts.Token);

            if (_audioQueue.TryDequeue(out byte[]? chunk) && chunk.Length > 0)
            {
                var n = Interlocked.Increment(ref _chunksPublishedToSdk);
                if (n % 1000 == 0)
                {
                    _logger.LogInformation(
                        "AWS Transcribe publisher delivered {Chunks} audio chunks to the SDK (upload path OK).",
                        n);
                }

                return new AudioEvent
                {
                    AudioChunk = new MemoryStream(chunk, writable: false)
                };
            }
        }

        throw new OperationCanceledException(_cts.Token);
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        if (_streamingTask is not null)
        {
            await _streamingTask;
        }

        _client.Dispose();
        _cts.Dispose();
        _audioSignal.Dispose();
    }
}
