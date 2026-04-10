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
}

public sealed class TranscriptionChunk
{
    public required DateTime StartTime { get; init; }
    public required DateTime EndTime { get; init; }
    public required List<TranscriptItem> Items { get; init; }
}

/// <summary>
/// Strict wall-clock 3-minute windows from call anchor. No cross-chunk duplication; each final transcript item once per dedupe key.
/// </summary>
public sealed class TranscriptionChunkManager : BackgroundService
{
    private static readonly TimeSpan ChunkDuration = TimeSpan.FromMinutes(3);

    /// <summary>3-minute ALB chunk: <c>length_limit_reached - 0</c> if any transcript text; <c>long_times_of_silence - 1</c> if none.</summary>
    private const string AlbFlagLengthLimitReached = "length_limit_reached - 0";
    private const string AlbFlagLongSilence = "long_times_of_silence - 1";

    private readonly BotSettings _settings;
    private readonly MeetingContextStore _meetingContext;
    private readonly ParticipantManager _participantManager;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<TranscriptionChunkManager> _logger;

    private readonly object _lock = new();
    private int _anchorOnce;
    private DateTime _anchorUtc;
    private bool _hasAnchor;
    private int _activeChunkIndex;
    private readonly List<TranscriptItem> _buffer = new();
    private readonly HashSet<string> _dedupeKeys = new(StringComparer.Ordinal);

    public TranscriptionChunkManager(
        BotSettings settings,
        MeetingContextStore meetingContext,
        ParticipantManager participantManager,
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
            _activeChunkIndex = 0;
            _buffer.Clear();
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
            _anchorUtc = anchorUtc.Kind == DateTimeKind.Utc ? anchorUtc : anchorUtc.ToUniversalTime();
            _hasAnchor = true;
            _activeChunkIndex = 0;
            _buffer.Clear();
            _dedupeKeys.Clear();
            _logger.LogInformation("Transcription chunk anchor set to {AnchorUtc} (UTC).", _anchorUtc);
        }
    }

    public void EndMeeting()
    {
        Interlocked.Exchange(ref _anchorOnce, 0);
        lock (_lock)
        {
            _hasAnchor = false;
            _buffer.Clear();
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

        List<TranscriptionChunk>? toSend = null;
        lock (_lock)
        {
            if (!_hasAnchor)
            {
                return;
            }

            if (utc < _anchorUtc)
            {
                utc = _anchorUtc;
            }

            var idx = (int)Math.Floor((utc - _anchorUtc).TotalMilliseconds / ChunkDuration.TotalMilliseconds);
            if (idx < 0)
            {
                idx = 0;
            }

            if (idx < _activeChunkIndex)
            {
                _logger.LogDebug(
                    "Dropping late transcript for chunk {ChunkIndex} (active={Active}); dedupe={Key}.",
                    idx,
                    _activeChunkIndex,
                    dedupeKey);
                return;
            }

            while (_activeChunkIndex < idx)
            {
                toSend ??= new List<TranscriptionChunk>();
                toSend.Add(BuildChunkToSend(_activeChunkIndex, takeBuffer: true));
                _activeChunkIndex++;
                _buffer.Clear();
                _dedupeKeys.Clear();
            }

            if (!_dedupeKeys.Add(dedupeKey))
            {
                return;
            }

            _buffer.Add(new TranscriptItem
            {
                Timestamp = utc,
                EntraObjectId = _participantManager.GetEntraObjectIdForTranscriptPayload(participantId),
                ParticipantName = speakerName.Trim(),
                Text = text.Trim()
            });
        }

        if (toSend is not null)
        {
            foreach (var chunk in toSend)
            {
                await PostChunkAsync(chunk, cancellationToken);
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
                TranscriptionChunk? chunk = null;
                lock (_lock)
                {
                    if (!_hasAnchor)
                    {
                        break;
                    }

                    var now = DateTime.UtcNow;
                    var wallIdx = (int)Math.Floor((now - _anchorUtc).TotalMilliseconds / ChunkDuration.TotalMilliseconds);
                    if (wallIdx < 0)
                    {
                        wallIdx = 0;
                    }

                    if (wallIdx <= _activeChunkIndex)
                    {
                        break;
                    }

                    chunk = BuildChunkToSend(_activeChunkIndex, takeBuffer: true);
                    _activeChunkIndex++;
                    _buffer.Clear();
                    _dedupeKeys.Clear();
                }

                if (chunk is null)
                {
                    break;
                }

                await PostChunkAsync(chunk, stoppingToken);
            }
        }
    }

    private TranscriptionChunk BuildChunkToSend(int chunkIndex, bool takeBuffer)
    {
        var start = _anchorUtc + TimeSpan.FromTicks(ChunkDuration.Ticks * chunkIndex);
        var end = start + ChunkDuration;
        var items = takeBuffer ? new List<TranscriptItem>(_buffer) : new List<TranscriptItem>();
        return new TranscriptionChunk
        {
            StartTime = start,
            EndTime = end,
            Items = items
        };
    }

    private async Task PostChunkAsync(TranscriptionChunk chunk, CancellationToken cancellationToken)
    {
        var endpoint = _settings.TranscriptAlbEndpoint;
        if (string.IsNullOrWhiteSpace(endpoint))
        {
            return;
        }

        var ordered = chunk.Items.OrderBy(i => i.Timestamp).ToList();
        var payload = new AlbChunkPayload
        {
            MeetingId = _meetingContext.CurrentMeetingId,
            Transcript = ordered
                .Select(i => new AlbTranscriptEntry
                {
                    EntraId = i.EntraObjectId,
                    Name = i.ParticipantName,
                    Text = i.Text
                })
                .ToList(),
            Flag = ResolveAlbFlag(ordered)
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
                    chunk.StartTime,
                    chunk.EndTime,
                    chunk.Items.Count);
                return;
            }

            _logger.LogInformation(
                "Posted transcript chunk to ALB. MeetingId={MeetingId}, Flag={Flag}, Start={Start}, End={End}, Lines={Count}.",
                payload.MeetingId,
                payload.Flag,
                chunk.StartTime,
                chunk.EndTime,
                chunk.Items.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ALB chunk post error for window {Start}–{End}.", chunk.StartTime, chunk.EndTime);
        }
    }

    private static string ResolveAlbFlag(IReadOnlyList<TranscriptItem> items)
    {
        var hasText = items.Any(i => !string.IsNullOrWhiteSpace(i.Text));
        return hasText ? AlbFlagLengthLimitReached : AlbFlagLongSilence;
    }

    private sealed class AlbChunkPayload
    {
        [JsonPropertyName("meeting_id")]
        public string MeetingId { get; set; } = string.Empty;

        [JsonPropertyName("transcript")]
        public List<AlbTranscriptEntry> Transcript { get; set; } = new();

        [JsonPropertyName("flag")]
        public string Flag { get; set; } = string.Empty;
    }

    private sealed class AlbTranscriptEntry
    {
        [JsonPropertyName("entra_id")]
        public string EntraId { get; set; } = string.Empty;

        [JsonPropertyName("name")]
        public string Name { get; set; } = string.Empty;

        [JsonPropertyName("text")]
        public string Text { get; set; } = string.Empty;
    }
}
