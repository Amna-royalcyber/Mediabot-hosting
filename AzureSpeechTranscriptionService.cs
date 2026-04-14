using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading;
using Microsoft.CognitiveServices.Speech;
using Microsoft.CognitiveServices.Speech.Audio;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

/// <summary>
/// Identity-first transcription: one Azure Speech continuous recognizer per Teams media stream id (<c>sourceId</c>).
/// Recognizers start only after Graph has bound that stream to a participant (via <see cref="MeetingParticipantService"/>).
/// </summary>
public sealed class AzureSpeechTranscriptionService : IAsyncDisposable
{
    private static readonly AudioStreamFormat Pcm16kMono = AudioStreamFormat.GetWaveFormatPCM(16000, 16, 1);

    /// <summary>~15s PCM16 @ 16kHz mono while waiting for roster/mediaStreams binding.</summary>
    private const int MaxPreIdentityBytes = 480_000;

    private readonly BotSettings _settings;
    private readonly MeetingParticipantService _meetingParticipants;
    private readonly TranscriptBroadcaster _broadcaster;
    private readonly IChunkManager _chunkManager;
    private readonly ILogger<AzureSpeechTranscriptionService> _logger;
    private readonly ConcurrentDictionary<uint, StreamSession> _sessions = new();
    private volatile bool _disposed;
    private int _loggedMissingAzureConfig;

    private sealed class StreamSession
    {
        public readonly SemaphoreSlim Serialize = new(1, 1);
        public readonly object Gate = new();
        public PushAudioInputStream? Push;
        public SpeechRecognizer? Recognizer;
        public bool Started;
        public int PreIdentityBytes;
        public readonly List<byte[]> PreBuffer = new();
        public string IntraId = "";
        public string ParticipantId = "";
        public string DisplayName = "";
    }

    public AzureSpeechTranscriptionService(
        BotSettings settings,
        MeetingParticipantService meetingParticipants,
        TranscriptBroadcaster broadcaster,
        IChunkManager chunkManager,
        ILogger<AzureSpeechTranscriptionService> logger)
    {
        _settings = settings;
        _meetingParticipants = meetingParticipants;
        _broadcaster = broadcaster;
        _chunkManager = chunkManager;
        _logger = logger;
    }

    public void NotifyParticipantIdentityResolved(uint sourceId)
    {
        if (_disposed)
        {
            return;
        }

        _ = TryStartAfterIdentityAsync(sourceId);
    }

    private async Task TryStartAfterIdentityAsync(uint sourceId)
    {
        try
        {
            if (!_sessions.TryGetValue(sourceId, out var session))
            {
                return;
            }

            await session.Serialize.WaitAsync().ConfigureAwait(false);
            try
            {
                var shouldStart = false;
                lock (session.Gate)
                {
                    if (session.Started)
                    {
                        return;
                    }

                    if (!_meetingParticipants.TryGetParticipantForMediaStream(
                            sourceId,
                            out var intraId,
                            out var participantId,
                            out var displayName))
                    {
                        return;
                    }

                    session.IntraId = intraId;
                    session.ParticipantId = participantId;
                    session.DisplayName = displayName;
                    shouldStart = true;
                }

                if (shouldStart)
                {
                    await StartRecognizerAsync(session, sourceId).ConfigureAwait(false);
                }
            }
            finally
            {
                session.Serialize.Release();
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Deferred recognizer start failed for stream {SourceId}.", sourceId);
        }
    }

    public async Task ProcessPcm16Async(uint sourceId, byte[] pcm16kMono, long timestampHns)
    {
        if (_disposed || pcm16kMono.Length == 0)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(_settings.AzureSpeechKey) ||
            string.IsNullOrWhiteSpace(_settings.AzureSpeechRegion))
        {
            if (Interlocked.Exchange(ref _loggedMissingAzureConfig, 1) == 0)
            {
                _logger.LogWarning(
                    "Azure Speech is not configured. Set Bot:AzureSpeechKey and Bot:AzureSpeechRegion (or env BOT_AZURE_SPEECH_KEY / BOT_AZURE_SPEECH_REGION).");
            }

            return;
        }

        var session = _sessions.GetOrAdd(sourceId, _ => new StreamSession());
        await session.Serialize.WaitAsync().ConfigureAwait(false);
        try
        {
        var shouldStart = false;
        lock (session.Gate)
        {
            if (!session.Started)
            {
                if (!_meetingParticipants.TryGetParticipantForMediaStream(
                        sourceId,
                        out var intraId,
                        out var participantId,
                        out var displayName))
                {
                    if (session.PreIdentityBytes + pcm16kMono.Length <= MaxPreIdentityBytes)
                    {
                        session.PreBuffer.Add(pcm16kMono);
                        session.PreIdentityBytes += pcm16kMono.Length;
                    }

                    return;
                }

                session.IntraId = intraId;
                session.ParticipantId = participantId;
                session.DisplayName = displayName;
                shouldStart = true;
            }
        }

        if (shouldStart)
        {
            await StartRecognizerAsync(session, sourceId).ConfigureAwait(false);
        }

        lock (session.Gate)
        {
            if (session.Push is not null)
            {
                session.Push.Write(pcm16kMono);
            }
        }
        }
        finally
        {
            session.Serialize.Release();
        }
    }

    private async Task StartRecognizerAsync(StreamSession session, uint sourceId)
    {
        lock (session.Gate)
        {
            if (session.Started)
            {
                return;
            }
        }

        try
        {
            var speechConfig = SpeechConfig.FromSubscription(_settings.AzureSpeechKey!, _settings.AzureSpeechRegion!);
            speechConfig.SpeechRecognitionLanguage = "en-US";

            var push = AudioInputStream.CreatePushStream(Pcm16kMono);
            var audioConfig = AudioConfig.FromStreamInput(push);
            var recognizer = new SpeechRecognizer(speechConfig, audioConfig);

            recognizer.Recognized += (_, e) =>
            {
                if (e.Result.Reason != ResultReason.RecognizedSpeech)
                {
                    return;
                }

                var text = e.Result.Text;
                if (string.IsNullOrWhiteSpace(text))
                {
                    return;
                }

                string intraId;
                string participantId;
                string displayName;
                if (!_meetingParticipants.TryGetParticipantForMediaStream(
                        sourceId,
                        out intraId!,
                        out participantId!,
                        out displayName!))
                {
                    lock (session.Gate)
                    {
                        intraId = session.IntraId;
                        participantId = session.ParticipantId;
                        displayName = session.DisplayName;
                    }
                }

                var conf = TryParseConfidence(e.Result);
                _ = EmitTranscriptAsync(sourceId, intraId, participantId, displayName, text, conf);
            };

            recognizer.Canceled += (_, e) =>
            {
                if (e.Reason == CancellationReason.Error)
                {
                    _logger.LogWarning("Azure Speech error on stream {SourceId}: {Details}", sourceId, e.ErrorDetails);
                }
            };

            await recognizer.StartContinuousRecognitionAsync().ConfigureAwait(false);

            lock (session.Gate)
            {
                session.Push = push;
                session.Recognizer = recognizer;
                session.Started = true;
                foreach (var chunk in session.PreBuffer)
                {
                    push.Write(chunk);
                }

                session.PreBuffer.Clear();
                session.PreIdentityBytes = 0;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Azure Speech recognizer failed for stream {SourceId}.", sourceId);
        }
    }

    private async Task EmitTranscriptAsync(
        uint sourceId,
        string intraId,
        string participantId,
        string displayName,
        string text,
        double? confidence)
    {
        try
        {
            var utc = DateTime.UtcNow;
            await _broadcaster.BroadcastStructuredTranscriptAsync(
                intraId,
                participantId,
                displayName,
                sourceId,
                text,
                confidence,
                utc).ConfigureAwait(false);

            var dedupeKey = $"{sourceId}|{utc.Ticks}|{text}";
            await _chunkManager.RecordFinalAsync(
                utc,
                participantId,
                displayName,
                text,
                dedupeKey,
                sourceId).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Emit transcript failed for stream {SourceId}.", sourceId);
        }
    }

    public async ValueTask DisposeAsync()
    {
        _disposed = true;
        foreach (var kv in _sessions.ToArray())
        {
            await DisposeSessionAsync(kv.Value).ConfigureAwait(false);
        }

        _sessions.Clear();
    }

    private static async Task DisposeSessionAsync(StreamSession session)
    {
        SpeechRecognizer? rec;
        lock (session.Gate)
        {
            rec = session.Recognizer;
            session.Recognizer = null;
        }

        if (rec is not null)
        {
            try
            {
                await rec.StopContinuousRecognitionAsync().ConfigureAwait(false);
            }
            catch
            {
                // ignore
            }

            rec.Dispose();
        }

        lock (session.Gate)
        {
            session.Push?.Close();
            session.Push = null;
        }

        session.Serialize.Dispose();
    }

    private static double? TryParseConfidence(SpeechRecognitionResult result)
    {
        try
        {
            var json = result.Properties.GetProperty(PropertyId.SpeechServiceResponse_JsonResult);
            if (string.IsNullOrWhiteSpace(json))
            {
                return null;
            }

            using var doc = JsonDocument.Parse(json);
            if (!doc.RootElement.TryGetProperty("NBest", out var nBest) || nBest.GetArrayLength() == 0)
            {
                return null;
            }

            var first = nBest[0];
            return first.TryGetProperty("Confidence", out var c) ? c.GetDouble() : null;
        }
        catch
        {
            return null;
        }
    }
}
