using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

public sealed class TranscriptBroadcaster
{
    private readonly IHubContext<TranscriptHub> _hubContext;
    private readonly TranscriptionChunkManager _chunkManager;
    private readonly ParticipantManager _participantManager;
    private readonly ILogger<TranscriptBroadcaster> _logger;

    public TranscriptBroadcaster(
        IHubContext<TranscriptHub> hubContext,
        TranscriptionChunkManager chunkManager,
        ParticipantManager participantManager,
        ILogger<TranscriptBroadcaster> logger)
    {
        _hubContext = hubContext;
        _chunkManager = chunkManager;
        _participantManager = participantManager;
        _logger = logger;
    }

    public async Task BroadcastAsync(
        string kind,
        string text,
        DateTime utteranceUtc,
        long audioTimestampHns,
        string? awsSpeakerId = null,
        string? speakerLabel = null,
        string? userPrincipalName = null,
        string? azureAdObjectId = null)
    {
        var entraForClients = string.IsNullOrWhiteSpace(azureAdObjectId)
            ? azureAdObjectId
            : _participantManager.GetEntraObjectIdForTranscriptPayload(azureAdObjectId);
        try
        {
            await _hubContext.Clients.All.SendAsync("transcript", new
            {
                kind,
                text,
                awsSpeakerId,
                speakerLabel,
                userPrincipalName,
                azureAdObjectId = entraForClients,
                timestamp = DateTimeOffset.UtcNow
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SignalR broadcast failed for transcript kind={Kind}.", kind);
        }

        if (!string.Equals(kind, "Final", StringComparison.OrdinalIgnoreCase) ||
            string.IsNullOrWhiteSpace(text) ||
            string.IsNullOrWhiteSpace(speakerLabel) ||
            string.IsNullOrWhiteSpace(azureAdObjectId))
        {
            return;
        }

        var dedupeKey = $"{azureAdObjectId}|{audioTimestampHns.ToString(System.Globalization.CultureInfo.InvariantCulture)}|{text}";
        try
        {
            await _chunkManager.RecordFinalAsync(
                utteranceUtc,
                azureAdObjectId,
                speakerLabel,
                text,
                dedupeKey);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Transcription chunk manager failed for final transcript.");
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
