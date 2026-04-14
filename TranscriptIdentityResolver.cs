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
    public (string UserId, string DisplayName) Resolve(string? userId, string? displayName, uint? sourceStreamId = null)
    {
        if (sourceStreamId is uint sid)
        {
            return ResolveFromSourceStreamId(sid, displayName);
        }

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

        return ResolveFromSourceStreamId(sourceId, dn);
    }

    private (string UserId, string DisplayName) ResolveFromSourceStreamId(uint sourceId, string? displayNameFallback)
    {
        var dn = displayNameFallback?.Trim() ?? "";

        if (_participantManager.TryGetBinding(sourceId, out var binding) && binding is not null)
        {
            var uid = !string.IsNullOrWhiteSpace(binding.EntraOid)
                ? binding.EntraOid.Trim()
                : ParticipantManager.SyntheticParticipantId(sourceId);

            var name = _participantManager.GetTranscriptSpeakerLabel(sourceId);
            if (string.IsNullOrWhiteSpace(name))
            {
                name = dn;
            }

            return (uid, name);
        }

        if (_meetingParticipants.TryResolveAudioSourceToEntra(sourceId, out var entraOid, out var rosterName))
        {
            return (entraOid, rosterName);
        }

        return (ParticipantManager.SyntheticParticipantId(sourceId), _participantManager.GetCanonicalDisplayName(ParticipantManager.SyntheticParticipantId(sourceId)) ?? dn);
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
