using System.Collections.Concurrent;
using Amazon;
using Amazon.TranscribeStreaming;
using Amazon.TranscribeStreaming.Model;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

/// <summary>
/// Manages one AWS Transcribe streaming session per participant.
/// </summary>
public sealed class AwsTranscribeService : IAsyncDisposable
{
    private readonly BotSettings _settings;
    private readonly TranscriptBroadcaster _transcriptBroadcaster;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<AwsTranscribeService> _logger;
    private readonly ConcurrentDictionary<string, ParticipantTranscribeSession> _sessions = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, string> _displayNames = new(StringComparer.OrdinalIgnoreCase);

    public AwsTranscribeService(
        BotSettings settings,
        TranscriptBroadcaster transcriptBroadcaster,
        ILoggerFactory loggerFactory,
        ILogger<AwsTranscribeService> logger)
    {
        _settings = settings;
        _transcriptBroadcaster = transcriptBroadcaster;
        _loggerFactory = loggerFactory;
        _logger = logger;
    }

    public void UpsertParticipant(string participantId, string displayName)
    {
        _displayNames[participantId] = displayName;
        if (_sessions.TryGetValue(participantId, out var session))
        {
            session.UpdateDisplayName(displayName);
        }
    }

    public async Task SendAudioChunkAsync(string participantId, string displayName, byte[] pcmAudio, long timestamp)
    {
        if (pcmAudio.Length == 0)
        {
            return;
        }

        UpsertParticipant(participantId, displayName);

        var session = _sessions.GetOrAdd(participantId, _ =>
            new ParticipantTranscribeSession(
                _settings,
                participantId,
                displayName,
                _transcriptBroadcaster,
                _loggerFactory.CreateLogger<ParticipantTranscribeSession>()));

        await session.EnsureStartedAsync();
        session.EnqueueAudio(pcmAudio, timestamp);
    }

    public void RemoveParticipant(string participantId)
    {
        _displayNames.TryRemove(participantId, out _);
        if (_sessions.TryRemove(participantId, out var session))
        {
            _ = session.DisposeAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var session in _sessions.Values)
        {
            await session.DisposeAsync();
        }

        _sessions.Clear();
    }
}

internal sealed class ParticipantTranscribeSession : IAsyncDisposable
{
    private readonly AmazonTranscribeStreamingClient _client;
    private readonly TranscriptBroadcaster _transcriptBroadcaster;
    private readonly ILogger<ParticipantTranscribeSession> _logger;
    private readonly bool _broadcastPartials;
    private readonly string _participantId;
    private readonly ConcurrentQueue<byte[]> _audioQueue = new();
    private readonly SemaphoreSlim _audioSignal = new(0);
    private readonly CancellationTokenSource _cts = new();
    private readonly object _stateLock = new();

    private string _displayName;
    private Task? _sessionTask;
    private string? _lastPartial;
    private string? _lastFinal;
    private DateTime _lastPartialUtc = DateTime.MinValue;

    public ParticipantTranscribeSession(
        BotSettings settings,
        string participantId,
        string displayName,
        TranscriptBroadcaster transcriptBroadcaster,
        ILogger<ParticipantTranscribeSession> logger)
    {
        _participantId = participantId;
        _displayName = displayName;
        _transcriptBroadcaster = transcriptBroadcaster;
        _logger = logger;
        _broadcastPartials = settings.TranscriptBroadcastPartials;
        _client = new AmazonTranscribeStreamingClient(RegionEndpoint.GetBySystemName(settings.AwsRegion));
    }

    public void UpdateDisplayName(string displayName)
    {
        lock (_stateLock)
        {
            _displayName = displayName;
        }
    }

    public Task EnsureStartedAsync()
    {
        if (_sessionTask is not null)
        {
            return Task.CompletedTask;
        }

        _sessionTask = RunAsync();
        return Task.CompletedTask;
    }

    public void EnqueueAudio(byte[] pcmAudio, long _)
    {
        _audioQueue.Enqueue(pcmAudio);
        _audioSignal.Release();
    }

    private async Task RunAsync()
    {
        var request = new StartStreamTranscriptionRequest
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
            using var response = await _client.StartStreamTranscriptionAsync(request, _cts.Token);
            var resultStream = response.TranscriptResultStream;
            resultStream.TranscriptEventReceived += (_, e) =>
            {
                if (e.EventStreamEvent is TranscriptEvent te)
                {
                    _ = HandleTranscriptAsync(te);
                }
            };
            resultStream.StartProcessing();
            await Task.Delay(Timeout.Infinite, _cts.Token);
        }
        catch (OperationCanceledException)
        {
            // expected
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Transcribe session failed for participant {ParticipantId}.", _participantId);
        }
    }

    private async Task HandleTranscriptAsync(TranscriptEvent te)
    {
        if (te.Transcript?.Results is null)
        {
            return;
        }

        string displayName;
        lock (_stateLock)
        {
            displayName = _displayName;
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

                if (string.Equals(_lastPartial, text, StringComparison.Ordinal))
                {
                    continue;
                }

                if ((DateTime.UtcNow - _lastPartialUtc).TotalMilliseconds < 250)
                {
                    continue;
                }

                _lastPartial = text;
                _lastPartialUtc = DateTime.UtcNow;
                await _transcriptBroadcaster.BroadcastAsync(
                    "Partial",
                    text,
                    speakerLabel: displayName,
                    azureAdObjectId: _participantId);
                continue;
            }

            if (string.Equals(_lastFinal, text, StringComparison.Ordinal))
            {
                continue;
            }

            _lastFinal = text;
            _logger.LogInformation("Transcript mapped to {ParticipantName}: {Text}", displayName, text);
            await _transcriptBroadcaster.BroadcastAsync(
                "Final",
                text,
                speakerLabel: displayName,
                azureAdObjectId: _participantId);
        }
    }

    private async Task<IAudioStreamEvent> GetNextAudioEventAsync()
    {
        const int targetChunkBytes = 16_000 * 2 * 320 / 1000;
        var merged = new List<byte>(targetChunkBytes);

        while (merged.Count < targetChunkBytes && !_cts.IsCancellationRequested)
        {
            await _audioSignal.WaitAsync(_cts.Token);
            while (_audioQueue.TryDequeue(out var chunk))
            {
                merged.AddRange(chunk);
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

        _client.Dispose();
        _cts.Dispose();
        _audioSignal.Dispose();
    }
}
