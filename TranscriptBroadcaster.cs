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
        string? azureAdObjectId = null,
        uint? sourceStreamId = null)
    {
        var resolvedFromResolver = string.IsNullOrWhiteSpace(azureAdObjectId)
            ? null
            : _participantManager.GetEntraObjectIdForTranscriptPayload(azureAdObjectId);
        var resolvedEntraForClients = !string.IsNullOrWhiteSpace(resolvedFromResolver)
            ? resolvedFromResolver
            : (sourceStreamId is uint sid ? _participantManager.GetEntraOidForTranscript(sid) : null);
        var entraForClients = !string.IsNullOrWhiteSpace(resolvedEntraForClients) &&
                              (ParticipantManager.IsSyntheticParticipantId(resolvedEntraForClients) ||
                               string.Equals(resolvedEntraForClients, AwsTranscribeService.UnknownMixedUserId, StringComparison.OrdinalIgnoreCase))
            ? null
            : resolvedEntraForClients;
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
            (string.IsNullOrWhiteSpace(azureAdObjectId) && sourceStreamId is null))
        {
            return;
        }

        var dedupeKey = $"{sourceStreamId?.ToString() ?? azureAdObjectId}|{audioTimestampHns.ToString(System.Globalization.CultureInfo.InvariantCulture)}|{text}";
        try
        {
            await _chunkManager.RecordFinalAsync(
                utteranceUtc,
                !string.IsNullOrWhiteSpace(resolvedEntraForClients)
                    ? resolvedEntraForClients
                    : (sourceStreamId is uint sourceSid ? _participantManager.GetEntraOidForTranscript(sourceSid) : (azureAdObjectId ?? string.Empty)),
                speakerLabel,
                text,
                dedupeKey,
                sourceStreamId);
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
