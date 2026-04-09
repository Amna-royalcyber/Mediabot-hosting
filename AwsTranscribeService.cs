using System.Collections.Concurrent;
using System.Globalization;
using Amazon;
using Amazon.TranscribeStreaming;
using Amazon.TranscribeStreaming.Model;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

/// <summary>
/// Manages AWS Transcribe streaming: one session per participant when unmixed audio is available,
/// and one optional "dominant mixed" session when only the main (mixed) buffer is present.
/// </summary>
public sealed class AwsTranscribeService : IAsyncDisposable
{
    /// <summary>Single session used for mixed meeting audio; identity is updated per chunk from Teams dominant speaker.</summary>
    public const string DominantMixedSessionKey = "__dominant_mixed__";

    private readonly BotSettings _settings;
    private readonly TranscriptAggregator _transcriptAggregator;
    private readonly ParticipantManager _participantManager;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<AwsTranscribeService> _logger;
    private readonly ConcurrentDictionary<string, ParticipantTranscribeSession> _sessions = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, string> _displayNames = new(StringComparer.OrdinalIgnoreCase);

    public AwsTranscribeService(
        BotSettings settings,
        TranscriptAggregator transcriptAggregator,
        ParticipantManager participantManager,
        ILoggerFactory loggerFactory,
        ILogger<AwsTranscribeService> logger)
    {
        _settings = settings;
        _transcriptAggregator = transcriptAggregator;
        _participantManager = participantManager;
        _loggerFactory = loggerFactory;
        _logger = logger;
    }

    public void UpsertParticipant(string participantId, string displayName)
    {
        if (string.IsNullOrWhiteSpace(participantId))
        {
            return;
        }

        var pid = participantId.Trim();
        _participantManager.RegisterParticipant(pid, displayName, DateTime.UtcNow);
        var canonical = _participantManager.GetCanonicalDisplayName(pid) ?? displayName.Trim();
        _displayNames[pid] = canonical;
        if (_sessions.TryGetValue(pid, out var session))
        {
            session.UpdateDisplayName(canonical);
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
                _transcriptAggregator,
                _participantManager,
                _loggerFactory.CreateLogger<ParticipantTranscribeSession>()));

        await session.EnsureStartedAsync();
        session.EnqueueAudio(pcmAudio, timestamp);
    }

    /// <summary>
    /// Sends PCM from the main (mixed) buffer to one Transcribe stream. Call
    /// <see cref="ParticipantTranscribeSession.UpdateTranscriptIdentity"/> before each chunk so transcripts use the current speaker (Teams dominant MSI → Entra user).
    /// </summary>
    public async Task SendMixedDominantAudioAsync(string participantId, string displayName, byte[] pcmAudio, long timestamp)
    {
        if (pcmAudio.Length == 0)
        {
            return;
        }

        UpsertParticipant(DominantMixedSessionKey, displayName);

        var session = _sessions.GetOrAdd(
            DominantMixedSessionKey,
            _ => new ParticipantTranscribeSession(
                _settings,
                DominantMixedSessionKey,
                displayName,
                _transcriptAggregator,
                _participantManager,
                _loggerFactory.CreateLogger<ParticipantTranscribeSession>()));

        session.UpdateTranscriptIdentity(participantId, displayName);
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
    private readonly BotSettings _settings;
    private readonly TranscriptAggregator _transcriptAggregator;
    private readonly ParticipantManager _participantManager;
    private readonly ILogger<ParticipantTranscribeSession> _logger;
    private readonly bool _broadcastPartials;
    private string _participantId;
    private readonly ConcurrentQueue<byte[]> _audioQueue = new();
    private readonly SemaphoreSlim _audioSignal = new(0);
    private readonly CancellationTokenSource _cts = new();
    private readonly object _stateLock = new();
    private readonly object _runLock = new();

    private string _displayName;
    private Task? _sessionTask;
    private string? _lastPartial;
    /// <summary>Dedupe only identical AWS segment replays (same start/end/text), not repeated words in a new utterance.</summary>
    private string? _lastFinalDedupeKey;
    private DateTime _lastPartialUtc = DateTime.MinValue;

    /// <summary>Last time real (non-keepalive) PCM arrived from Teams. AWS streaming can stall if no audio for several seconds.</summary>
    private DateTime _lastRealAudioUtc;

    private Timer? _silenceKeepAliveTimer;

    public ParticipantTranscribeSession(
        BotSettings settings,
        string participantId,
        string displayName,
        TranscriptAggregator transcriptAggregator,
        ParticipantManager participantManager,
        ILogger<ParticipantTranscribeSession> logger)
    {
        _settings = settings;
        _participantId = participantId;
        _displayName = displayName;
        _transcriptAggregator = transcriptAggregator;
        _participantManager = participantManager;
        _logger = logger;
        _broadcastPartials = settings.TranscriptBroadcastPartials;
        _lastRealAudioUtc = DateTime.UtcNow;
    }

    public void UpdateDisplayName(string displayName)
    {
        lock (_stateLock)
        {
            _displayName = displayName;
        }
    }

    /// <summary>Used for the dominant-mixed session so transcript lines carry the current Teams speaker (Entra id + name).</summary>
    public void UpdateTranscriptIdentity(string participantId, string displayName)
    {
        lock (_stateLock)
        {
            _participantId = participantId;
            _displayName = displayName;
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

            if (_sessionTask?.IsFaulted == true)
            {
                _logger.LogWarning(
                    "Restarting AWS Transcribe stream for session key {SessionKey} after prior failure.",
                    _participantId);
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

    public void EnqueueAudio(byte[] pcmAudio, long _)
    {
        _lastRealAudioUtc = DateTime.UtcNow;
        _audioQueue.Enqueue(pcmAudio);
        _audioSignal.Release();
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
            _audioQueue.Enqueue(new byte[bytes]);
            _audioSignal.Release();
        }
        catch (ObjectDisposedException)
        {
            // shutting down
        }
    }

    /// <summary>Retries the streaming connection after errors so speech works again after silence or transient AWS failures.</summary>
    private async Task RunSessionLoopAsync()
    {
        var attempt = 0;
        while (!_cts.IsCancellationRequested)
        {
            attempt++;
            using var client = new AmazonTranscribeStreamingClient(RegionEndpoint.GetBySystemName(_settings.AwsRegion));
            var request = new StartStreamTranscriptionRequest
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
                if (attempt > 1)
                {
                    _logger.LogInformation(
                        "Transcribe stream attempt {Attempt} for session {SessionKey}.",
                        attempt,
                        _participantId);
                }

                using var response = await client.StartStreamTranscriptionAsync(request, _cts.Token);
                var resultStream = response.TranscriptResultStream;
                resultStream.ExceptionReceived += (_, ev) =>
                {
                    _logger.LogError(ev.EventStreamException, "Transcribe result stream exception for session {SessionKey}.", _participantId);
                };
                resultStream.TranscriptEventReceived += (_, e) =>
                {
                    if (e.EventStreamEvent is TranscriptEvent te)
                    {
                        _ = HandleTranscriptAsync(te);
                    }
                };
                resultStream.StartProcessing();
                await Task.Delay(Timeout.Infinite, _cts.Token);
                return;
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Transcribe stream ended for session {SessionKey}; will retry if call continues.", _participantId);
                try
                {
                    var delay = Math.Min(5000, 250 * attempt);
                    await Task.Delay(delay, _cts.Token);
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

        string displayName;
        string participantIdForBroadcast;
        lock (_stateLock)
        {
            displayName = _displayName;
            participantIdForBroadcast = _participantId;
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

                var minPartialGap = Math.Clamp(_settings.TranscribePartialMinIntervalMilliseconds, 30, 500);
                if ((DateTime.UtcNow - _lastPartialUtc).TotalMilliseconds < minPartialGap)
                {
                    continue;
                }

                _lastPartial = text;
                _lastPartialUtc = DateTime.UtcNow;
                var partialEmitted = DateTime.UtcNow;
                var partialName = _participantManager.GetCanonicalDisplayName(participantIdForBroadcast) ?? displayName;
                await _transcriptAggregator.PublishAsync(new TranscriptFragment(
                    AudioTimestamp: (long)((result.StartTime ?? 0) * 10_000_000),
                    EmittedAtUtc: partialEmitted,
                    Kind: "Partial",
                    Text: text,
                    UserId: participantIdForBroadcast,
                    DisplayName: partialName));
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
            var finalEmitted = DateTime.UtcNow;
            var finalName = _participantManager.GetCanonicalDisplayName(participantIdForBroadcast) ?? displayName;
            _logger.LogInformation("Transcript mapped to {ParticipantName}: {Text}", finalName, text);
            await _transcriptAggregator.PublishAsync(new TranscriptFragment(
                AudioTimestamp: (long)((result.StartTime ?? 0) * 10_000_000),
                EmittedAtUtc: finalEmitted,
                Kind: "Final",
                Text: text,
                UserId: participantIdForBroadcast,
                DisplayName: finalName));
        }
    }

    private async Task<IAudioStreamEvent> GetNextAudioEventAsync()
    {
        var chunkMs = Math.Clamp(_settings.TranscribeAudioChunkMilliseconds, 50, 500);
        var targetChunkBytes = 16_000 * 2 * chunkMs / 1000;
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
        _silenceKeepAliveTimer?.Dispose();
        _silenceKeepAliveTimer = null;
        _cts.Cancel();
        if (_sessionTask is not null)
        {
            try
            {
                await _sessionTask;
            }
            catch
            {
                // faulted task on shutdown is ok
            }
        }

        _cts.Dispose();
        _audioSignal.Dispose();
    }
}
