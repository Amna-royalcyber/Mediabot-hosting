using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

/// <summary>
/// Periodically reconciles unresolved source streams with roster identities when Graph stream mapping is delayed.
/// </summary>
public sealed class IdentityBackfillService : BackgroundService
{
    private readonly ParticipantManager _participantManager;
    private readonly MeetingParticipantService _meetingParticipants;
    private readonly ILogger<IdentityBackfillService> _logger;

    public IdentityBackfillService(
        ParticipantManager participantManager,
        MeetingParticipantService meetingParticipants,
        ILogger<IdentityBackfillService> logger)
    {
        _participantManager = participantManager;
        _meetingParticipants = meetingParticipants;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(2));
        while (!stoppingToken.IsCancellationRequested && await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                ReconcileOnce();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Identity backfill reconciliation tick failed.");
            }
        }
    }

    private void ReconcileOnce()
    {
        var roster = _meetingParticipants.GetRosterSnapshot()
            .Where(r => !string.IsNullOrWhiteSpace(r.AzureAdObjectId))
            .GroupBy(r => r.AzureAdObjectId.Trim(), StringComparer.OrdinalIgnoreCase)
            .Select(g =>
            {
                var first = g.First();
                var oid = g.Key;
                var name = g
                    .Select(x => string.IsNullOrWhiteSpace(x.DisplayName) ? oid : x.DisplayName.Trim())
                    .FirstOrDefault(n => !string.IsNullOrWhiteSpace(n)) ?? oid;
                return (Oid: oid, DisplayName: name);
            })
            .ToList();

        if (roster.Count == 0)
        {
            return;
        }

        // Refresh display names for already-resolved identities.
        foreach (var entry in roster)
        {
            if (_participantManager.TryGetSourceIdForIdentity(entry.Oid, out var resolvedSourceId))
            {
                _participantManager.TryBindAudioSource(resolvedSourceId, entry.Oid, entry.DisplayName, "GraphBackfillRefresh");
            }
        }

        var unresolvedSources = _participantManager.GetUnresolvedSourceIds().Distinct().ToList();
        if (unresolvedSources.Count == 0)
        {
            return;
        }

        var assigned = _participantManager.GetParticipantIdsWithAudioSourceBindings();
        var unassignedRoster = roster
            .Where(r => !assigned.Contains(r.Oid))
            .ToList();

        // Deterministic delayed bind only when one pending stream and one unassigned identity remain.
        if (unresolvedSources.Count == 1 && unassignedRoster.Count == 1)
        {
            var sid = unresolvedSources[0];
            var target = unassignedRoster[0];
            _participantManager.TryBindAudioSource(sid, target.Oid, target.DisplayName, "DelayedBackfill");
            _logger.LogInformation(
                "Delayed identity backfill applied: sourceId {SourceId} -> {DisplayName} ({EntraOid}).",
                sid,
                target.DisplayName,
                target.Oid);
        }
    }
}
