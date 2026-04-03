using System.Collections.Concurrent;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using Amazon;
using Amazon.TranscribeStreaming;
using Amazon.TranscribeStreaming.Model;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

public sealed class AwsTranscribeService : IAsyncDisposable
{
    private static readonly Regex AwsSpeakerToken = new(@"spk_(\d+)", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private readonly ILogger<AwsTranscribeService> _logger;
    private readonly TranscriptBroadcaster _transcriptBroadcaster;
    private readonly MeetingParticipantService _meetingParticipants;
    private readonly bool _broadcastPartials;
    private readonly ConcurrentQueue<byte[]> _audioQueue = new();
    private readonly SemaphoreSlim _audioSignal = new(0);
    private readonly CancellationTokenSource _cts = new();
    private readonly AmazonTranscribeStreamingClient _client;

    private Task? _streamingTask;
    private TaskCompletionSource<bool>? _sessionReady;
    private StartStreamTranscriptionResponse? _activeResponse;
    private int _eventReceivedLogBudget = 40;
    private long _chunksPublishedToSdk;
    private long _chunksEnqueuedFromTeams;
    private int _transcriptEventCount;
    private int _emptyTranscriptStreak;
    private int _partialLogCounter;
    private int _initialResponseReceived;

    public AwsTranscribeService(
        ILogger<AwsTranscribeService> logger,
        TranscriptBroadcaster transcriptBroadcaster,
        MeetingParticipantService meetingParticipants,
        BotSettings settings)
    {
        _logger = logger;
        _transcriptBroadcaster = transcriptBroadcaster;
        _meetingParticipants = meetingParticipants;
        _broadcastPartials = settings.TranscriptBroadcastPartials;
        var regionName = !string.IsNullOrWhiteSpace(settings.AwsRegion)
            ? settings.AwsRegion
            : (Environment.GetEnvironmentVariable("AWS_REGION") ?? "us-east-1");
        _logger.LogInformation("AWS Transcribe Streaming client region: {Region}", regionName);
        _logger.LogInformation(
            "AWS credential hints: AWS_ACCESS_KEY_ID set={HasKey}, AWS_PROFILE={Profile}, AWS_SHARED_CREDENTIALS_FILE={CredsFile}",
            !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID")),
            Environment.GetEnvironmentVariable("AWS_PROFILE") ?? "(default)",
            Environment.GetEnvironmentVariable("AWS_SHARED_CREDENTIALS_FILE") ?? "(default path)");

        // Default client uses the same credential chain as AWS CLI (env, shared file, instance profile).
        _client = new AmazonTranscribeStreamingClient(RegionEndpoint.GetBySystemName(regionName));
    }

    /// <summary>
    /// Starts a single long-lived Transcribe session. Awaits until the stream is accepted or fails (credentials, region, network).
    /// </summary>
    public async Task StartStreamingAsync(CancellationToken cancellationToken = default)
    {
        if (_streamingTask is not null)
        {
            _logger.LogWarning(
                "StartStreamingAsync skipped: a Transcribe session already exists for this process. Audio still enqueues to the same stream; for a fresh AWS session restart the bot.");
            return;
        }

        _sessionReady = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var request = new StartStreamTranscriptionRequest
        {
            LanguageCode = LanguageCode.EnUS,
            MediaEncoding = MediaEncoding.Pcm,
            MediaSampleRateHertz = 16000,
            ShowSpeakerLabel = true,
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

            Interlocked.Exchange(ref _initialResponseReceived, 0);
            LogStartStreamTranscriptionOutcome(_activeResponse);

            Interlocked.Exchange(ref _eventReceivedLogBudget, 40);
            Interlocked.Exchange(ref _chunksPublishedToSdk, 0);
            Interlocked.Exchange(ref _chunksEnqueuedFromTeams, 0);
            Interlocked.Exchange(ref _transcriptEventCount, 0);
            Interlocked.Exchange(ref _emptyTranscriptStreak, 0);
            Interlocked.Exchange(ref _partialLogCounter, 0);

            var stream = _activeResponse.TranscriptResultStream;

            // Wire handlers synchronously immediately after the call returns (before any other await in this method).
            stream.ExceptionReceived += (_, e) =>
            {
                _logger.LogError(e.EventStreamException, "AWS Transcribe stream exception");
            };

            stream.InitialResponseReceived += (_, _) =>
            {
                Interlocked.Exchange(ref _initialResponseReceived, 1);
                _logger.LogInformation("AWS Transcribe initial response received (session active).");
            };

            // Some SDK builds deliver transcripts only on TranscriptEventReceived; others on EventReceived.
            // Do not also handle TranscriptEvent on EventReceived — the SDK raises both for the same payload (duplicate UI lines).
            stream.TranscriptEventReceived += (_, e) =>
            {
                try
                {
                    if (e.EventStreamEvent is TranscriptEvent te)
                    {
                        HandleTranscriptResult(te, "TranscriptEventReceived");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error handling Transcribe TranscriptEventReceived");
                }
            };

            stream.EventReceived += (_, e) =>
            {
                try
                {
                    if (e.EventStreamEvent is TranscriptEvent)
                    {
                        return;
                    }

                    if (Interlocked.Decrement(ref _eventReceivedLogBudget) >= 0)
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

            // Event handlers alone do not read the HTTP/2 response body; the SDK requires an explicit start (see
            // Amazon.Runtime.EventStreams.EventOutputStream / aws-sdk-net#3364). Without this, IsProcessing stays false
            // and InitialResponseReceived / TranscriptEvent never fire.
            stream.StartProcessing();
            _logger.LogInformation("AWS Transcribe TranscriptResultStream.StartProcessing() invoked (response reader active).");

            LogTranscriptStreamDiagnostics(stream);

            _ = WarnIfInitialResponseMissingAsync(linkedCts.Token);

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

    private void LogStartStreamTranscriptionOutcome(StartStreamTranscriptionResponse response)
    {
        try
        {
            var status = response.HttpStatusCode;
            _logger.LogInformation(
                "StartStreamTranscriptionAsync completed: HttpStatus={HttpStatus}, RequestId={RequestId}, SessionId={SessionId}, TranscriptResultStream={StreamPresent}.",
                status,
                response.RequestId ?? "(null)",
                response.SessionId ?? "(null)",
                response.TranscriptResultStream is not null);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not log StartStreamTranscription response metadata.");
        }
    }

    private async Task WarnIfInitialResponseMissingAsync(CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(20), cancellationToken);
            if (Volatile.Read(ref _initialResponseReceived) == 0)
            {
                _logger.LogWarning(
                    "AWS Transcribe: InitialResponseReceived did not fire within 20s after StartStreamTranscriptionAsync. " +
                    "Upload may still work; check outbound HTTPS/2 to transcribestreaming in this region, proxy/TLS inspection, and IAM transcribe:StartStreamTranscription. " +
                    "If HttpStatus was 200 and events still never arrive, try upgrading AWSSDK.TranscribeStreaming or open a support ticket with RequestId logs.");
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }
    }

    public void SendAudioChunk(byte[] data)
    {
        if (data.Length == 0)
        {
            return;
        }

        var enq = Interlocked.Increment(ref _chunksEnqueuedFromTeams);
        if (enq % 1000 == 0)
        {
            var pub = Interlocked.Read(ref _chunksPublishedToSdk);
            _logger.LogInformation(
                "Transcribe queue: {Enqueued} chunks enqueued from Teams, {Published} chunks published to AWS SDK (gap indicates publisher backlog).",
                enq,
                pub);
        }

        _audioQueue.Enqueue(data);
        _audioSignal.Release();
    }

    public void HandleTranscriptResult(TranscriptEvent transcriptEvent, string source = "")
    {
        if (transcriptEvent.Transcript?.Results is null)
        {
            _logger.LogDebug("TranscriptEvent had null Transcript.Results.");
            return;
        }

        var c = Interlocked.Increment(ref _transcriptEventCount);
        if (c == 1 || c % 25 == 0)
        {
            _logger.LogInformation(
                "Transcribe TranscriptEvent ({Source}) #{Ordinal}: {ResultCount} result(s).",
                string.IsNullOrEmpty(source) ? "handler" : source,
                c,
                transcriptEvent.Transcript.Results.Count);
        }

        var anyText = false;
        foreach (var result in transcriptEvent.Transcript.Results)
        {
            if (result.Alternatives.Count == 0)
            {
                continue;
            }

            var alt = result.Alternatives[0];
            string text = alt.Transcript ?? string.Empty;
            if (string.IsNullOrWhiteSpace(text))
            {
                continue;
            }

            anyText = true;
            var awsSpeakerId = TryGetAwsSpeakerId(alt);
            var speaker = _meetingParticipants.TryResolveSpeaker(awsSpeakerId);
            var speakerLabel = BuildSpeakerLabel(speaker, awsSpeakerId);

            var kind = result.IsPartial == true ? "Partial" : "Final";
            if (kind == "Final")
            {
                _logger.LogInformation("Transcribe final ({Speaker}): {Text}", speakerLabel, text);
            }
            else
            {
                var p = Interlocked.Increment(ref _partialLogCounter);
                if (p <= 5 || p % 30 == 0)
                {
                    _logger.LogInformation("Transcribe partial ({Speaker}): {Text}", speakerLabel, text);
                }
                else
                {
                    _logger.LogDebug("Transcribe partial ({Speaker}): {Text}", speakerLabel, text);
                }
            }

            if (kind == "Partial" && !_broadcastPartials)
            {
                continue;
            }

            _ = _transcriptBroadcaster.BroadcastAsync(
                kind,
                text,
                awsSpeakerId,
                speakerLabel,
                speaker?.UserPrincipalName,
                speaker?.AzureAdObjectId);
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

    private static string? TryGetAwsSpeakerId(Alternative alt)
    {
        if (alt.Items is null || alt.Items.Count == 0)
        {
            return null;
        }

        foreach (var item in alt.Items)
        {
            if (!string.IsNullOrEmpty(item.Speaker))
            {
                return item.Speaker;
            }
        }

        return null;
    }

    private static string BuildSpeakerLabel(SpeakerResolution? speaker, string? awsSpeakerId)
    {
        if (speaker is { } s)
        {
            if (!string.IsNullOrWhiteSpace(s.DisplayName) && !string.IsNullOrWhiteSpace(s.UserPrincipalName))
            {
                return $"{s.DisplayName} ({s.UserPrincipalName})";
            }

            if (!string.IsNullOrWhiteSpace(s.DisplayName))
            {
                return s.DisplayName;
            }

            if (!string.IsNullOrWhiteSpace(s.AzureAdObjectId))
            {
                return s.AzureAdObjectId;
            }
        }

        return FormatAwsSpeakerFallback(awsSpeakerId);
    }

    /// <summary>Human-readable when Teams roster mapping is not available (spk_0 → Speaker 1).</summary>
    private static string FormatAwsSpeakerFallback(string? awsSpeakerId)
    {
        if (string.IsNullOrEmpty(awsSpeakerId))
        {
            return "Speaker";
        }

        var m = AwsSpeakerToken.Match(awsSpeakerId);
        if (m.Success && int.TryParse(m.Groups[1].Value, out var n))
        {
            return $"Speaker {n + 1}";
        }

        return awsSpeakerId;
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

    private void LogTranscriptStreamDiagnostics(TranscriptResultStream stream)
    {
        try
        {
            var t = stream.GetType();
            _logger.LogInformation("TranscriptResultStream runtime type: {TypeName}.", t.FullName ?? t.Name);
            var isProcessing = t.GetProperty(
                "IsProcessing",
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            if (isProcessing is not null)
            {
                _logger.LogInformation(
                    "TranscriptResultStream.IsProcessing = {IsProcessing} (if false, the SDK may not be reading the response stream yet).",
                    isProcessing.GetValue(stream));
            }
            else
            {
                _logger.LogInformation(
                    "TranscriptResultStream has no IsProcessing property on this SDK build; rely on InitialResponseReceived / transcript events.");
            }
        }
        catch (Exception ex)
        {
            _logger.LogInformation(ex, "Could not read TranscriptResultStream diagnostic properties.");
        }
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
