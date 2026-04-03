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

    public async Task BroadcastAsync(string kind, string text)
    {
        try
        {
            await _hubContext.Clients.All.SendAsync("transcript", new
            {
                kind,
                text,
                timestamp = DateTimeOffset.UtcNow
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SignalR broadcast failed for transcript kind={Kind}.", kind);
        }
    }
}
