using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

public sealed class TranscriptItem
{
    public required DateTime Timestamp { get; init; }

    /// <summary>Entra object id (GUID) when resolved; otherwise synthetic e.g. <c>msi-pending-{sourceId}</c>.</summary>
    public required string EntraObjectId { get; init; }

    public required string ParticipantName { get; init; }
    public required string Text { get; init; }
    public uint? SourceStreamId { get; init; }
}

public sealed class TranscriptionChunk
{
    public required DateTime StartTime { get; init; }
    public required DateTime EndTime { get; init; }
    public required List<TranscriptItem> Items { get; init; }
}

public sealed class TimeWindowChunk
{
    public required DateTime StartTime { get; init; }
    public required DateTime EndTime { get; init; }
    public required List<TranscriptItem> Fragments { get; init; }
}

/// <summary>
/// Strict wall-clock 3-minute windows from call anchor. No cross-chunk duplication; each final transcript item once per dedupe key.
/// </summary>
public sealed class TranscriptionChunkManager : BackgroundService, IChunkManager
{
    private static readonly TimeSpan ChunkDuration = TimeSpan.FromMinutes(3);

    private const string AlbFlagLengthLimitReached = "0";
    private const string AlbFlagLongSilence = "1";
    private const string AlbFlagTimerUpdate = "2";

    private readonly BotSettings _settings;
    private readonly MeetingContextStore _meetingContext;
    private readonly IParticipantManager _participantManager;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<TranscriptionChunkManager> _logger;

    private readonly object _lock = new();
    private int _anchorOnce;
    private DateTime _meetingStartTimeUtc;
    private bool _hasAnchor;
    private TimeWindowChunk? _currentWindow;
    private readonly HashSet<string> _dedupeKeys = new(StringComparer.Ordinal);

    public TranscriptionChunkManager(
        BotSettings settings,
        MeetingContextStore meetingContext,
        IParticipantManager participantManager,
        IHttpClientFactory httpClientFactory,
        ILogger<TranscriptionChunkManager> logger)
    {
        _settings = settings;
        _meetingContext = meetingContext;
        _participantManager = participantManager;
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    /// <summary>Reset chunk state when starting a new join attempt (before call id exists).</summary>
    public void ResetForNewJoin()
    {
        Interlocked.Exchange(ref _anchorOnce, 0);
        lock (_lock)
        {
            _hasAnchor = false;
            _currentWindow = null;
            _dedupeKeys.Clear();
        }
    }

    /// <summary>Set wall-clock anchor once when the call is established (starts [0–3), [3–6), … windows).</summary>
    public void BeginMeeting(DateTime anchorUtc)
    {
        if (Interlocked.Exchange(ref _anchorOnce, 1) != 0)
        {
            return;
        }

        lock (_lock)
        {
            _meetingStartTimeUtc = anchorUtc.Kind == DateTimeKind.Utc ? anchorUtc : anchorUtc.ToUniversalTime();
            _hasAnchor = true;
            _currentWindow = CreateNewWindow(_meetingStartTimeUtc);
            _dedupeKeys.Clear();
            _logger.LogInformation("Transcription chunk anchor set to {AnchorUtc} (UTC).", _meetingStartTimeUtc);
        }
    }

    public void EndMeeting()
    {
        Interlocked.Exchange(ref _anchorOnce, 0);
        lock (_lock)
        {
            _hasAnchor = false;
            _currentWindow = null;
            _dedupeKeys.Clear();
        }
    }

    /// <summary>Record a final transcript line into the correct 3-minute chunk (may flush prior empty chunks).</summary>
    public async Task RecordFinalAsync(
        DateTime utteranceUtc,
        string participantId,
        string speakerName,
        string text,
        string dedupeKey,
        uint? sourceStreamId = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(_settings.TranscriptAlbEndpoint))
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(text))
        {
            return;
        }

        var utc = utteranceUtc.Kind == DateTimeKind.Utc ? utteranceUtc : utteranceUtc.ToUniversalTime();

        List<TimeWindowChunk>? windowsToFlush = null;
        lock (_lock)
        {
            if (!_hasAnchor || _currentWindow is null)
            {
                return;
            }

            if (utc < _meetingStartTimeUtc)
            {
                utc = _meetingStartTimeUtc;
            }

            while (utc >= _currentWindow.EndTime)
            {
                windowsToFlush ??= new List<TimeWindowChunk>();
                windowsToFlush.Add(_currentWindow);
                _currentWindow = CreateNewWindow(_currentWindow.EndTime);
                _dedupeKeys.Clear();
            }

            if (!_dedupeKeys.Add(dedupeKey))
            {
                return;
            }

            var entraForAlb = sourceStreamId is uint sid
                ? _participantManager.GetEntraOidForTranscript(sid)
                : _participantManager.GetEntraObjectIdForTranscriptPayload(participantId);

            _currentWindow.Fragments.Add(new TranscriptItem
            {
                Timestamp = utc,
                EntraObjectId = entraForAlb,
                ParticipantName = speakerName.Trim(),
                Text = text.Trim(),
                SourceStreamId = sourceStreamId
            });
        }

        if (windowsToFlush is not null)
        {
            foreach (var window in windowsToFlush)
            {
                await FlushWindowAsync(window, flag: null, cancellationToken);
            }
        }
    }

    /// <summary>Timer-driven: close chunks when wall clock passes chunk end (handles silence with empty payloads).</summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        while (!stoppingToken.IsCancellationRequested && await timer.WaitForNextTickAsync(stoppingToken))
        {
            if (string.IsNullOrWhiteSpace(_settings.TranscriptAlbEndpoint))
            {
                continue;
            }

            while (true)
            {
                TimeWindowChunk? windowToFlush = null;
                lock (_lock)
                {
                    if (!_hasAnchor || _currentWindow is null)
                    {
                        break;
                    }

                    var now = DateTime.UtcNow;
                    if (now < _currentWindow.EndTime)
                    {
                        break;
                    }

                    windowToFlush = _currentWindow;
                    _currentWindow = CreateNewWindow(_currentWindow.EndTime);
                    _dedupeKeys.Clear();
                }

                if (windowToFlush is null)
                {
                    break;
                }

                await FlushWindowAsync(windowToFlush, flag: null, stoppingToken);
            }
        }
    }

    private static TimeWindowChunk CreateNewWindow(DateTime startUtc)
    {
        return new TimeWindowChunk
        {
            StartTime = startUtc,
            EndTime = startUtc.Add(ChunkDuration),
            Fragments = new List<TranscriptItem>()
        };
    }

    private async Task FlushWindowAsync(TimeWindowChunk window, string? flag, CancellationToken cancellationToken)
    {
        var endpoint = _settings.TranscriptAlbEndpoint;
        if (string.IsNullOrWhiteSpace(endpoint))
        {
            return;
        }

        var ordered = window.Fragments.OrderBy(i => i.Timestamp).ToList();
        var transcriptList = new List<AlbTranscriptLine>();
        foreach (var fragment in ordered)
        {
            if (string.IsNullOrWhiteSpace(fragment.Text))
            {
                continue;
            }

            var resolvedName = fragment.ParticipantName;
            if (fragment.SourceStreamId is uint sid)
            {
                var latestName = _participantManager.GetTranscriptSpeakerLabel(sid);
                if (!string.IsNullOrWhiteSpace(latestName))
                {
                    resolvedName = latestName;
                }
            }

            transcriptList.Add(new AlbTranscriptLine
            {
                Speaker = resolvedName,
                Text = fragment.Text.Trim(),
                Timestamp = fragment.Timestamp
            });
        }

        var payload = new AlbChunkPayload
        {
            MeetingId = _meetingContext.CurrentMeetingId,
            Transcript = transcriptList,
            Flag = flag ?? ResolveAlbFlag(transcriptList.Count)
        };

        try
        {
            var client = _httpClientFactory.CreateClient("AlbTranscriptSender");
            var jsonOptions = new JsonSerializerOptions
            {
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
            };

            using var request = new HttpRequestMessage(HttpMethod.Post, endpoint)
            {
                Content = JsonContent.Create(payload, options: jsonOptions)
            };

            using var response = await client.SendAsync(request, cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning(
                    "ALB chunk post failed. Status={Status}, MeetingId={MeetingId}, Flag={Flag}, Start={Start}, End={End}, Lines={Count}.",
                    (int)response.StatusCode,
                    payload.MeetingId,
                    payload.Flag,
                    window.StartTime,
                    window.EndTime,
                    window.Fragments.Count);
                return;
            }

            _logger.LogInformation(
                "Posted transcript chunk to ALB. MeetingId={MeetingId}, Flag={Flag}, Start={Start}, End={End}, Lines={Count}.",
                payload.MeetingId,
                payload.Flag,
                window.StartTime,
                window.EndTime,
                window.Fragments.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ALB chunk post error for window {Start}–{End}.", window.StartTime, window.EndTime);
        }
        finally
        {
            window.Fragments.Clear();
        }
    }

    private static string ResolveAlbFlag(int transcriptCount)
    {
        return transcriptCount == 0 ? AlbFlagLongSilence : AlbFlagLengthLimitReached;
    }

    private sealed class AlbChunkPayload
    {
        [JsonPropertyName("meeting_id")]
        public string MeetingId { get; set; } = string.Empty;

        [JsonPropertyName("transcript")]
        public List<AlbTranscriptLine> Transcript { get; set; } = new();

        [JsonPropertyName("flag")]
        public string Flag { get; set; } = string.Empty;
    }

    private sealed class AlbTranscriptLine
    {
        [JsonPropertyName("speaker")]
        public string Speaker { get; set; } = string.Empty;

        [JsonPropertyName("text")]
        public string Text { get; set; } = string.Empty;

        [JsonPropertyName("timestamp")]
        public DateTime Timestamp { get; set; }
    }
}
