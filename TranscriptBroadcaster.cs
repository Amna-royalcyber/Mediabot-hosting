using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

public sealed class TranscriptBroadcaster
{
    private readonly IHubContext<TranscriptHub> _hubContext;
    private readonly ILogger<TranscriptBroadcaster> _logger;

    public TranscriptBroadcaster(IHubContext<TranscriptHub> hubContext, ILogger<TranscriptBroadcaster> logger)
    {
        _hubContext = hubContext;
        _logger = logger;
    }

    public async Task BroadcastAsync(
        string kind,
        string text,
        string? awsSpeakerId = null,
        string? speakerLabel = null)
    {
        try
        {
            await _hubContext.Clients.All.SendAsync("transcript", new
            {
                kind,
                text,
                awsSpeakerId,
                speakerLabel,
                timestamp = DateTimeOffset.UtcNow
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SignalR broadcast failed for transcript kind={Kind}.", kind);
        }
    }

    public async Task BroadcastRosterAsync(IReadOnlyList<(string Id, string DisplayName)> participants)
    {
        try
        {
            await _hubContext.Clients.All.SendAsync("roster", new
            {
                participants = participants.Select(p => new { id = p.Id, displayName = p.DisplayName }).ToList(),
                timestamp = DateTimeOffset.UtcNow
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SignalR roster broadcast failed.");
        }
    }
}
