using Microsoft.AspNetCore.SignalR;

namespace TeamsMediaBot;

public sealed class TranscriptBroadcaster
{
    private readonly IHubContext<TranscriptHub> _hubContext;

    public TranscriptBroadcaster(IHubContext<TranscriptHub> hubContext)
    {
        _hubContext = hubContext;
    }

    public Task BroadcastAsync(string kind, string text)
    {
        return _hubContext.Clients.All.SendAsync("transcript", new
        {
            kind,
            text,
            timestamp = DateTimeOffset.UtcNow
        });
    }
}
