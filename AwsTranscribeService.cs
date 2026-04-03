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
                    if (Interlocked.Decrement(ref _eventReceivedLogBudget) >= 0)
                    {
                        _logger.LogInformation(
                            "Transcribe EventReceived: {Type}",
                            e.EventStreamEvent?.GetType().FullName ?? "(null)");
                    }

                    if (e.EventStreamEvent is TranscriptEvent te)
                    {
                        HandleTranscriptResult(te);
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
            return;
        }

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
