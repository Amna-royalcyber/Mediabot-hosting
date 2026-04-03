using System.Collections.Concurrent;
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
    private StartStreamTranscriptionResponse? _activeResponse;

    public AwsTranscribeService(ILogger<AwsTranscribeService> logger, TranscriptBroadcaster transcriptBroadcaster)
    {
        _logger = logger;
        _transcriptBroadcaster = transcriptBroadcaster;
        var regionName = Environment.GetEnvironmentVariable("AWS_REGION") ?? "us-east-1";
        _client = new AmazonTranscribeStreamingClient(RegionEndpoint.GetBySystemName(regionName));
    }

    public Task StartStreaming()
    {
        if (_streamingTask is not null)
        {
            return Task.CompletedTask;
        }

        var request = new StartStreamTranscriptionRequest
        {
            LanguageCode = LanguageCode.EnUS,
            MediaEncoding = MediaEncoding.Pcm,
            MediaSampleRateHertz = 16000,
            AudioStreamPublisher = GetNextAudioEventAsync
        };

        _streamingTask = Task.Run(async () =>
        {
            _activeResponse = await _client.StartStreamTranscriptionAsync(request, _cts.Token);
            _activeResponse.TranscriptResultStream.TranscriptEventReceived += (_, e) =>
            {
                HandleTranscriptResult(e.EventStreamEvent);
            };

            while (!_cts.IsCancellationRequested)
            {
                await Task.Delay(1000, _cts.Token);
            }
        }, _cts.Token);

        return Task.CompletedTask;
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
            Console.WriteLine($"[{kind}] {text}");
            _ = _transcriptBroadcaster.BroadcastAsync(kind, text);
        }
    }

    private async Task<IAudioStreamEvent> GetNextAudioEventAsync()
    {
        await _audioSignal.WaitAsync(_cts.Token);

        if (_audioQueue.TryDequeue(out byte[]? chunk))
        {
            return new AudioEvent
            {
                AudioChunk = new MemoryStream(chunk, writable: false)
            };
        }

        return new AudioEvent
        {
            AudioChunk = new MemoryStream(Array.Empty<byte>(), writable: false)
        };
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
