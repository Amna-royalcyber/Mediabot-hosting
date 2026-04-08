using System.Collections.Concurrent;
using System.Net.Http.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

public sealed class TranscriptAlbSender : BackgroundService
{
    private static readonly TimeSpan FlushInterval = TimeSpan.FromMinutes(3);
    private readonly BotSettings _settings;
    private readonly MeetingContextStore _meetingContext;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<TranscriptAlbSender> _logger;
    private readonly ConcurrentQueue<TranscriptLine> _pending = new();

    public TranscriptAlbSender(
        BotSettings settings,
        MeetingContextStore meetingContext,
        IHttpClientFactory httpClientFactory,
        ILogger<TranscriptAlbSender> logger)
    {
        _settings = settings;
        _meetingContext = meetingContext;
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    public void Enqueue(TranscriptFragment fragment)
    {
        // Keep payload aligned with UI behavior (final-only view).
        if (!string.Equals(fragment.Kind, "Final", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(fragment.DisplayName) || string.IsNullOrWhiteSpace(fragment.Text))
        {
            return;
        }

        _pending.Enqueue(new TranscriptLine(fragment.DisplayName.Trim(), fragment.Text.Trim()));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (string.IsNullOrWhiteSpace(_settings.TranscriptAlbEndpoint))
        {
            _logger.LogInformation("Transcript ALB endpoint is not configured; periodic JSON sender is disabled.");
            return;
        }

        using var timer = new PeriodicTimer(FlushInterval);
        while (!stoppingToken.IsCancellationRequested && await timer.WaitForNextTickAsync(stoppingToken))
        {
            await FlushAsync(stoppingToken);
        }
    }

    private async Task FlushAsync(CancellationToken cancellationToken)
    {
        var lines = new List<Dictionary<string, string>>();
        while (_pending.TryDequeue(out var line))
        {
            lines.Add(new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [line.Name] = line.Text
            });
        }

        var hasTranscript = lines.Count > 0;
        var payload = new TranscriptAlbPayload(
            meeting_id: _meetingContext.CurrentMeetingId,
            transcript: lines,
            flag: hasTranscript ? "length_limit_reached - 0" : "long_times_of_silence - 1");

        try
        {
            var client = _httpClientFactory.CreateClient("AlbTranscriptSender");
            using var request = new HttpRequestMessage(HttpMethod.Post, _settings.TranscriptAlbEndpoint)
            {
                Content = JsonContent.Create(payload)
            };
            using var response = await client.SendAsync(request, cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning(
                    "ALB transcript post failed. Status={StatusCode}, MeetingId={MeetingId}, TranscriptCount={Count}.",
                    (int)response.StatusCode,
                    payload.meeting_id,
                    lines.Count);
                return;
            }

            _logger.LogInformation(
                "Posted transcript batch to ALB. MeetingId={MeetingId}, TranscriptCount={Count}, Flag={Flag}.",
                payload.meeting_id,
                lines.Count,
                payload.flag);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ALB transcript post error for MeetingId={MeetingId}.", payload.meeting_id);
        }
    }

    private sealed record TranscriptLine(string Name, string Text);

    private sealed record TranscriptAlbPayload(
        string meeting_id,
        List<Dictionary<string, string>> transcript,
        string flag);
}
