using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

public sealed class TranscriptBroadcaster
{
    private readonly IHubContext<TranscriptHub> _hubContext;
    private readonly IChunkManager _chunkManager;
    private readonly IParticipantManager _participantManager;
    private readonly ILogger<TranscriptBroadcaster> _logger;

    public TranscriptBroadcaster(
        IHubContext<TranscriptHub> hubContext,
        IChunkManager chunkManager,
        IParticipantManager participantManager,
        ILogger<TranscriptBroadcaster> logger)
    {
        _hubContext = hubContext;
        _chunkManager = chunkManager;
        _participantManager = participantManager;
        _logger = logger;
    }

    /// <summary>Live transcript with resolved speaker (SignalR + optional ALB chunk).</summary>
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
                sourceId = sourceStreamId,
                tempLabel = false,
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

        await RecordAlbFinalCoreAsync(
            utteranceUtc,
            audioTimestampHns,
            text,
            resolvedEntraForClients,
            azureAdObjectId,
            speakerLabel,
            sourceStreamId);
    }

    /// <summary>Final transcript before Entra/display name is known — no resolved speaker label, no ALB chunk yet.</summary>
    public async Task BroadcastTempFinalAsync(
        string kind,
        string text,
        DateTime utteranceUtc,
        long audioTimestampHns,
        uint sourceId)
    {
        try
        {
            await _hubContext.Clients.All.SendAsync("transcript", new
            {
                kind,
                text,
                sourceId,
                userId = (string?)null,
                tempLabel = true,
                timestamp = DateTimeOffset.UtcNow
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SignalR temp transcript broadcast failed for kind={Kind}.", kind);
        }
    }

    /// <summary>Clients should patch all prior lines for <paramref name="sourceId"/> with the resolved display name.</summary>
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

    /// <summary>ALB chunk only (used when a final was first emitted as temp, then identity resolved).</summary>
    public async Task RecordAlbFinalChunkAsync(
        string kind,
        string text,
        DateTime utteranceUtc,
        long audioTimestampHns,
        string? azureAdObjectId,
        string? speakerLabel,
        uint? sourceStreamId)
    {
        if (!string.Equals(kind, "Final", StringComparison.OrdinalIgnoreCase) ||
            string.IsNullOrWhiteSpace(text) ||
            string.IsNullOrWhiteSpace(speakerLabel) ||
            (string.IsNullOrWhiteSpace(azureAdObjectId) && sourceStreamId is null))
        {
            return;
        }

        var resolvedFromResolver = string.IsNullOrWhiteSpace(azureAdObjectId)
            ? null
            : _participantManager.GetEntraObjectIdForTranscriptPayload(azureAdObjectId);
        var resolvedEntraForClients = !string.IsNullOrWhiteSpace(resolvedFromResolver)
            ? resolvedFromResolver
            : (sourceStreamId is uint sid ? _participantManager.GetEntraOidForTranscript(sid) : null);

        await RecordAlbFinalCoreAsync(
            utteranceUtc,
            audioTimestampHns,
            text,
            resolvedEntraForClients,
            azureAdObjectId,
            speakerLabel,
            sourceStreamId);
    }

    private async Task RecordAlbFinalCoreAsync(
        DateTime utteranceUtc,
        long audioTimestampHns,
        string text,
        string? resolvedEntraForClients,
        string? azureAdObjectId,
        string speakerLabel,
        uint? sourceStreamId)
    {
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
