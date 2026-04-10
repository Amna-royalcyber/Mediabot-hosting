namespace TeamsMediaBot;

/// <summary>
/// Maps <c>msi-pending-{sourceId}</c> placeholders to Microsoft Entra object ids and display names using
/// <see cref="ParticipantManager"/> bindings and roster <c>mediaStreams</c> correlation.
/// </summary>
public sealed class TranscriptIdentityResolver
{
    private readonly ParticipantManager _participantManager;
    private readonly MeetingParticipantService _meetingParticipants;

    public TranscriptIdentityResolver(
        ParticipantManager participantManager,
        MeetingParticipantService meetingParticipants)
    {
        _participantManager = participantManager;
        _meetingParticipants = meetingParticipants;
    }

    /// <summary>Returns Entra object id and display name suitable for SignalR and ALB payloads.</summary>
    public (string UserId, string DisplayName) Resolve(string? userId, string? displayName)
    {
        var uid = userId?.Trim() ?? "";
        var dn = displayName?.Trim() ?? "";

        if (string.IsNullOrEmpty(uid))
        {
            return (uid, dn);
        }

        if (!ParticipantManager.IsSyntheticParticipantId(uid))
        {
            return (uid, _participantManager.GetCanonicalDisplayName(uid) ?? dn);
        }

        if (!TryParseSyntheticSourceId(uid, out var sourceId))
        {
            return (uid, _participantManager.GetCanonicalDisplayName(uid) ?? dn);
        }

        if (_participantManager.TryResolveAudioSource(sourceId, out var boundId, out var boundDn) &&
            !ParticipantManager.IsSyntheticParticipantId(boundId))
        {
            return (boundId, _participantManager.GetCanonicalDisplayName(boundId) ?? boundDn);
        }

        if (_meetingParticipants.TryResolveAudioSourceToEntra(sourceId, out var entraOid, out var rosterName))
        {
            return (entraOid, rosterName);
        }

        return (uid, _participantManager.GetCanonicalDisplayName(uid) ?? dn);
    }

    private static bool TryParseSyntheticSourceId(string uid, out uint sourceId)
    {
        sourceId = 0;
        if (!ParticipantManager.IsSyntheticParticipantId(uid))
        {
            return false;
        }

        var suffix = uid.Substring(ParticipantManager.SyntheticIdPrefix.Length);
        return uint.TryParse(suffix, out sourceId);
    }
}
