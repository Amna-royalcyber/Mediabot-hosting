using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

/// <summary>
/// Stable Teams/Entra identity for the lifetime of a call. Source-id (audio stream) bindings are immutable once set.
/// </summary>
public sealed class ParticipantInfo
{
    public required string ParticipantId { get; init; }
    public required string DisplayName { get; init; }
    public DateTime JoinTimestampUtc { get; init; }
    /// <summary>Primary MSI/source id bound to this participant, if any.</summary>
    public uint? AudioStreamId { get; init; }
}

/// <summary>
/// Global participant registry and audio source-id → participant mapping for a meeting.
/// Placeholder ids (<see cref="IsSyntheticParticipantId"/>) may be upgraded to Entra ids when Graph sends mediaStreams.
/// </summary>
public sealed class ParticipantManager
{
    public const string SyntheticIdPrefix = "msi-pending-";

    public static string SyntheticParticipantId(uint sourceId) => $"{SyntheticIdPrefix}{sourceId}";

    public static bool IsSyntheticParticipantId(string? participantId) =>
        !string.IsNullOrEmpty(participantId) &&
        participantId.StartsWith(SyntheticIdPrefix, StringComparison.OrdinalIgnoreCase);

    private readonly ILogger<ParticipantManager> _logger;
    private readonly object _lifecycleLock = new();

    private readonly ConcurrentDictionary<string, ParticipantInfo> _participants =
        new(StringComparer.OrdinalIgnoreCase);

    /// <summary>MSI/sourceId → Entra object id. Never overwritten with a different user.</summary>
    private readonly ConcurrentDictionary<uint, string> _sourceIdToParticipantId = new();

    /// <summary>When MSI is still bound to <c>msi-pending-*</c>, roster Entra oid from join-order fallback (for ALB <c>entra_id</c>).</summary>
    private readonly ConcurrentDictionary<uint, string> _joinOrderEntraOidBySourceId = new();

    private string _meetingKey = string.Empty;

    public ParticipantManager(ILogger<ParticipantManager> logger)
    {
        _logger = logger;
    }

    /// <summary>Call when a new Graph call is attached so late-join and prior mappings do not bleed across calls.</summary>
    public void BeginNewMeeting(string? callOrMeetingId)
    {
        lock (_lifecycleLock)
        {
            _meetingKey = string.IsNullOrWhiteSpace(callOrMeetingId) ? Guid.NewGuid().ToString("N") : callOrMeetingId.Trim();
            _participants.Clear();
            _sourceIdToParticipantId.Clear();
            _joinOrderEntraOidBySourceId.Clear();
            _logger.LogInformation("ParticipantManager reset for meeting key {MeetingKey}.", _meetingKey);
        }
    }

    /// <summary>Register a human participant from Graph roster (first display name wins).</summary>
    public void RegisterParticipant(string participantId, string displayName, DateTime joinTimestampUtc)
    {
        if (string.IsNullOrWhiteSpace(participantId))
        {
            return;
        }

        displayName = string.IsNullOrWhiteSpace(displayName) ? participantId.Trim() : displayName.Trim();
        var pid = participantId.Trim();

        _participants.AddOrUpdate(
            pid,
            _ => new ParticipantInfo
            {
                ParticipantId = pid,
                DisplayName = displayName,
                JoinTimestampUtc = joinTimestampUtc,
                AudioStreamId = null
            },
            (_, existing) => existing);
    }

    /// <summary>
    /// Bind a Teams media source id to a participant id (Entra oid or synthetic placeholder).
    /// If the source was bound to a <see cref="IsSyntheticParticipantId"/> placeholder and the new id is a real Entra user, the binding is upgraded.
    /// </summary>
    /// <returns>Previous synthetic participant id whose Transcribe session should be removed after a successful Graph upgrade; otherwise null.</returns>
    public string? TryBindAudioSource(uint sourceId, string participantId, string displayName, string reason)
    {
        if (string.IsNullOrWhiteSpace(participantId))
        {
            return null;
        }

        var pid = participantId.Trim();
        displayName = string.IsNullOrWhiteSpace(displayName) ? pid : displayName.Trim();

        RegisterParticipant(pid, displayName, DateTime.UtcNow);

        if (_sourceIdToParticipantId.TryGetValue(sourceId, out var existingPid))
        {
            if (string.Equals(existingPid, pid, StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }

            if (IsSyntheticParticipantId(existingPid) && !IsSyntheticParticipantId(pid))
            {
                if (_sourceIdToParticipantId.TryUpdate(sourceId, pid, existingPid))
                {
                    _joinOrderEntraOidBySourceId.TryRemove(sourceId, out _);
                    RegisterParticipant(pid, displayName, DateTime.UtcNow);
                    _logger.LogInformation(
                        "Upgraded sourceId {SourceId} from placeholder {OldParticipantId} to Entra user {NewParticipantId} ({Reason}).",
                        sourceId,
                        existingPid,
                        pid,
                        reason);
                    return existingPid;
                }

                return null;
            }

            _logger.LogWarning(
                "Ignoring {Reason} bind for sourceId {SourceId} → {NewParticipantId}; already bound to {ExistingParticipantId}.",
                reason,
                sourceId,
                pid,
                existingPid);

            return null;
        }

        if (!_sourceIdToParticipantId.TryAdd(sourceId, pid))
        {
            return null;
        }

        _logger.LogInformation(
            "Bound audio sourceId {SourceId} → {DisplayName} ({ParticipantId}) [{Reason}].",
            sourceId,
            GetCanonicalDisplayName(pid) ?? displayName,
            pid,
            reason);

        return null;
    }

    public bool TryResolveAudioSource(uint sourceId, out string participantId, out string displayName)
    {
        participantId = string.Empty;
        displayName = string.Empty;
        if (!_sourceIdToParticipantId.TryGetValue(sourceId, out var pid))
        {
            return false;
        }

        participantId = pid;
        displayName = GetCanonicalDisplayName(pid) ?? pid;
        return true;
    }

    /// <summary>Records roster Entra oid for join-order display fallback (MSI still synthetic until Graph upgrades).</summary>
    public void SetJoinOrderEntraHint(uint sourceId, string entraObjectId)
    {
        if (string.IsNullOrWhiteSpace(entraObjectId))
        {
            return;
        }

        _joinOrderEntraOidBySourceId[sourceId] = entraObjectId.Trim();
    }

    /// <summary>
    /// Entra object id for transcript payloads: real oid when bound or join-order hint; otherwise synthetic id string.
    /// </summary>
    public string GetEntraObjectIdForTranscriptPayload(string participantId)
    {
        if (string.IsNullOrWhiteSpace(participantId))
        {
            return string.Empty;
        }

        var p = participantId.Trim();
        if (!IsSyntheticParticipantId(p))
        {
            return p;
        }

        if (!TryParseSourceIdFromSynthetic(p, out var sid))
        {
            return p;
        }

        return _joinOrderEntraOidBySourceId.TryGetValue(sid, out var oid) && !string.IsNullOrWhiteSpace(oid)
            ? oid.Trim()
            : p;
    }

    public static bool TryParseSourceIdFromSynthetic(string? participantId, out uint sourceId)
    {
        sourceId = 0;
        if (!IsSyntheticParticipantId(participantId) || participantId is null)
        {
            return false;
        }

        var suffix = participantId.Substring(SyntheticIdPrefix.Length);
        return uint.TryParse(suffix, out sourceId);
    }

    /// <summary>Canonical display name for transcripts (Teams/Entra only; first registered wins).</summary>
    public string? GetCanonicalDisplayName(string participantId)
    {
        if (string.IsNullOrWhiteSpace(participantId))
        {
            return null;
        }

        return _participants.TryGetValue(participantId.Trim(), out var info) ? info.DisplayName : null;
    }

    public bool HasParticipant(string participantId) =>
        !string.IsNullOrWhiteSpace(participantId) &&
        _participants.ContainsKey(participantId.Trim());

    /// <summary>Entra user ids that already have at least one MSI/sourceId bound (used for inference).</summary>
    public HashSet<string> GetParticipantIdsWithAudioSourceBindings()
    {
        return new HashSet<string>(_sourceIdToParticipantId.Values, StringComparer.OrdinalIgnoreCase);
    }
}
