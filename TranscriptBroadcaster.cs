using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

public sealed class TranscriptBroadcaster
{
    private readonly IHubContext<TranscriptHub> _hubContext;
    private readonly IParticipantManager _participantManager;
    private readonly ILogger<TranscriptBroadcaster> _logger;

    public TranscriptBroadcaster(
        IHubContext<TranscriptHub> hubContext,
        IParticipantManager participantManager,
        ILogger<TranscriptBroadcaster> logger)
    {
        _hubContext = hubContext;
        _participantManager = participantManager;
        _logger = logger;
    }

    /// <summary>Identity-first final transcript from Azure Speech (per media stream id).</summary>
    public async Task BroadcastStructuredTranscriptAsync(
        string intraId,
        string participantId,
        string displayName,
        uint ssrc,
        string text,
        double? confidence,
        DateTime utteranceUtc)
    {
        var resolvedParticipantId = _participantManager.GetEntraObjectIdForTranscriptPayload(participantId);
        var entraForClients = !string.IsNullOrWhiteSpace(resolvedParticipantId) &&
                              !ParticipantManager.IsSyntheticParticipantId(resolvedParticipantId)
            ? resolvedParticipantId
            : null;

        try
        {
            await _hubContext.Clients.All.SendAsync(
                "transcript",
                new
                {
                    kind = "Final",
                    intraId,
                    participantId = entraForClients ?? participantId,
                    displayName,
                    ssrc,
                    text,
                    confidence,
                    timestamp = new DateTimeOffset(utteranceUtc, TimeSpan.Zero),
                    azureAdObjectId = entraForClients,
                    tempLabel = false
                });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SignalR structured transcript broadcast failed for ssrc={Ssrc}.", ssrc);
        }
    }

    /// <summary>Clients should patch prior lines for <paramref name="sourceId"/> when display name / Entra resolves.</summary>
    public async Task BroadcastTranscriptIdentityUpdateAsync(uint sourceId, string? displayName, string? entraOid)
    {
        var resolvedEntra = string.IsNullOrWhiteSpace(entraOid)
            ? null
            : _participantManager.GetEntraObjectIdForTranscriptPayload(entraOid);
        var entraForClients = !string.IsNullOrWhiteSpace(resolvedEntra) &&
                              ParticipantManager.IsSyntheticParticipantId(resolvedEntra)
            ? null
            : resolvedEntra;

        try
        {
            await _hubContext.Clients.All.SendAsync("transcript-update", new
            {
                type = "transcript-update",
                sourceId,
                displayName,
                azureAdObjectId = entraForClients,
                timestamp = DateTimeOffset.UtcNow
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SignalR transcript-update failed for sourceId={SourceId}.", sourceId);
        }
    }

    public async Task BroadcastRosterAsync(IReadOnlyList<RosterParticipantDto> participants)
    {
        try
        {
            await _hubContext.Clients.All.SendAsync("roster", new
            {
                participants = participants.Select(p => new
                {
                    id = p.CallParticipantId,
                    displayName = p.DisplayName,
                    azureAdObjectId = p.AzureAdObjectId,
                    userPrincipalName = p.UserPrincipalName
                }).ToList(),
                timestamp = DateTimeOffset.UtcNow
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SignalR roster broadcast failed.");
        }
    }
}
