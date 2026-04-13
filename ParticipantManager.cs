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
    public uint? AudioStreamId { get; init; }
}

/// <summary>
/// One Teams audio MSI (<see cref="SourceId"/>) → one stream identity. <see cref="StreamParticipantId"/> never changes.
/// Graph/roster only enrich <see cref="EntraOid"/> / <see cref="DisplayName"/>; never reassign streams.
/// </summary>
public sealed class ParticipantBinding
{
    public uint SourceId { get; init; }

    /// <summary>Stable internal/AWS session key: <c>msi-pending-{SourceId}</c>. Never reassigned.</summary>
    public string StreamParticipantId { get; init; } = "";

    /// <summary>Microsoft Entra object id when known (Graph or hint). May start null.</summary>
    public string? EntraOid { get; set; }

    public string DisplayName { get; set; } = "";

    /// <summary>True when Graph has confirmed <see cref="EntraOid"/> (or equivalent authoritative bind).</summary>
    public bool IsFinal { get; set; }
}

/// <summary>
/// Global registry: <b>sourceId is the single source of truth</b>. Bind once; only enrich metadata; never reassign a stream to another user.
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

    private readonly ConcurrentDictionary<uint, ParticipantBinding> _bindings = new();

    private string _meetingKey = string.Empty;

    public ParticipantManager(ILogger<ParticipantManager> logger)
    {
        _logger = logger;
    }

    public void BeginNewMeeting(string? callOrMeetingId)
    {
        lock (_lifecycleLock)
        {
            _meetingKey = string.IsNullOrWhiteSpace(callOrMeetingId) ? Guid.NewGuid().ToString("N") : callOrMeetingId.Trim();
            _participants.Clear();
            _bindings.Clear();
            _logger.LogInformation("ParticipantManager reset for meeting key {MeetingKey}.", _meetingKey);
        }
    }

    public bool HasBinding(uint sourceId) => _bindings.ContainsKey(sourceId);

    public bool TryGetBinding(uint sourceId, out ParticipantBinding? binding) =>
        _bindings.TryGetValue(sourceId, out binding);

    /// <summary>Entra OID for ALB/SignalR: confirmed OID, else hint, else stable stream id string.</summary>
    public string GetEntraOidForTranscript(uint sourceId)
    {
        if (!_bindings.TryGetValue(sourceId, out var b))
        {
            return SyntheticParticipantId(sourceId);
        }

        if (!string.IsNullOrWhiteSpace(b.EntraOid))
        {
            return b.EntraOid.Trim();
        }

        return b.StreamParticipantId;
    }

    /// <summary>Legacy path: resolve synthetic session id to Entra/stream for payloads.</summary>
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

        return !TryParseSourceIdFromSynthetic(p, out var sid)
            ? p
            : GetEntraOidForTranscript(sid);
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

    /// <summary>Register a human participant from Graph roster (display cache).</summary>
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
    /// Creates or enriches the binding for <paramref name="sourceId"/> only. Never reassigns
    /// <see cref="ParticipantBinding.StreamParticipantId"/> after creation. Returns null always (no session swap).
    /// </summary>
    public string? TryBindAudioSource(uint sourceId, string? participantIdOrEntraFromGraph, string displayName, string reason)
    {
        displayName = string.IsNullOrWhiteSpace(displayName) ? $"Speaker ({sourceId})" : displayName.Trim();
        var graphOrAuthoritative =
            string.Equals(reason, "Graph", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(reason, "RosterMediaStreamsMap", StringComparison.OrdinalIgnoreCase);

        if (_bindings.TryGetValue(sourceId, out var existing))
        {
            if (existing.IsFinal && !graphOrAuthoritative)
            {
                return null;
            }

            if (graphOrAuthoritative)
            {
                var incomingOid = string.IsNullOrWhiteSpace(participantIdOrEntraFromGraph)
                    ? null
                    : participantIdOrEntraFromGraph.Trim();

                if (!existing.IsFinal)
                {
                    if (!string.IsNullOrWhiteSpace(incomingOid))
                    {
                        existing.EntraOid = incomingOid;
                        RegisterParticipant(incomingOid, displayName, DateTime.UtcNow);
                        existing.IsFinal = true;
                    }

                    if (!string.IsNullOrWhiteSpace(displayName))
                    {
                        existing.DisplayName = displayName;
                    }

                    _logger.LogInformation(
                        "Enriched non-final sourceId {SourceId} from Graph/roster: EntraOid={Entra}, DisplayName={Name} [{Reason}].",
                        sourceId,
                        existing.EntraOid,
                        existing.DisplayName,
                        reason);
                    return null;
                }

                if (!string.IsNullOrWhiteSpace(incomingOid) &&
                    !string.IsNullOrWhiteSpace(existing.EntraOid) &&
                    !string.Equals(existing.EntraOid, incomingOid, StringComparison.OrdinalIgnoreCase))
                {
                    _logger.LogWarning(
                        "Conflicting Graph mapping for sourceId {SourceId}. Existing: {Existing}, Incoming: {Incoming}",
                        sourceId,
                        existing.EntraOid,
                        incomingOid);
                    return null;
                }

                if (string.IsNullOrWhiteSpace(existing.DisplayName) && !string.IsNullOrWhiteSpace(displayName))
                {
                    existing.DisplayName = displayName;
                }
                return null;
            }

            if (!existing.IsFinal &&
                (string.Equals(reason, "JoinOrderDisplayFallback", StringComparison.OrdinalIgnoreCase) ||
                 string.Equals(reason, "JoinOrderDisplayFallbackMixed", StringComparison.OrdinalIgnoreCase)))
            {
                if (!string.IsNullOrWhiteSpace(participantIdOrEntraFromGraph))
                {
                    existing.EntraOid = participantIdOrEntraFromGraph.Trim();
                }

                existing.DisplayName = displayName;
                _logger.LogDebug("Join-order hint applied to existing non-final binding for sourceId {SourceId}.", sourceId);
                return null;
            }

            _logger.LogDebug(
                "Ignoring {Reason} for sourceId {SourceId}; binding already exists (IsFinal={IsFinal}).",
                reason,
                sourceId,
                existing.IsFinal);
            return null;
        }

        // First bind only — hard block reassignment is implicit (key did not exist).
        var streamPid = SyntheticParticipantId(sourceId);
        var binding = new ParticipantBinding
        {
            SourceId = sourceId,
            StreamParticipantId = streamPid,
            DisplayName = displayName
        };

        var initialGraphOid = string.IsNullOrWhiteSpace(participantIdOrEntraFromGraph)
            ? null
            : participantIdOrEntraFromGraph.Trim();
        if (graphOrAuthoritative && !string.IsNullOrWhiteSpace(initialGraphOid))
        {
            binding.EntraOid = initialGraphOid;
            RegisterParticipant(binding.EntraOid, displayName, DateTime.UtcNow);
            binding.IsFinal = true;
        }
        else if (string.Equals(reason, "JoinOrderDisplayFallback", StringComparison.OrdinalIgnoreCase) ||
                 string.Equals(reason, "JoinOrderDisplayFallbackMixed", StringComparison.OrdinalIgnoreCase))
        {
            if (!string.IsNullOrWhiteSpace(participantIdOrEntraFromGraph))
            {
                binding.EntraOid = participantIdOrEntraFromGraph.Trim();
            }

            binding.IsFinal = false;
        }
        else
        {
            binding.IsFinal = false;
        }

        RegisterParticipant(streamPid, binding.DisplayName, DateTime.UtcNow);
        _bindings[sourceId] = binding;

        _logger.LogInformation(
            "Created binding sourceId {SourceId} → stream {StreamId}; EntraOid={Entra}; IsFinal={Final} [{Reason}].",
            sourceId,
            streamPid,
            binding.EntraOid,
            binding.IsFinal,
            reason);

        return null;
    }

    public bool TryResolveAudioSource(uint sourceId, out string participantId, out string displayName)
    {
        participantId = string.Empty;
        displayName = string.Empty;
        if (!_bindings.TryGetValue(sourceId, out var b))
        {
            return false;
        }

        participantId = b.StreamParticipantId;
        displayName = string.IsNullOrWhiteSpace(b.DisplayName) ? participantId : b.DisplayName;
        return true;
    }

    public string? GetCanonicalDisplayName(string participantId)
    {
        if (string.IsNullOrWhiteSpace(participantId))
        {
            return null;
        }

        var p = participantId.Trim();
        if (TryParseSourceIdFromSynthetic(p, out var sid) &&
            _bindings.TryGetValue(sid, out var b) &&
            !string.IsNullOrWhiteSpace(b.DisplayName))
        {
            return b.DisplayName;
        }

        return _participants.TryGetValue(p, out var info) ? info.DisplayName : null;
    }

    public bool HasParticipant(string participantId) =>
        !string.IsNullOrWhiteSpace(participantId) &&
        _participants.ContainsKey(participantId.Trim());

    public HashSet<string> GetParticipantIdsWithAudioSourceBindings()
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var b in _bindings.Values)
        {
            if (!string.IsNullOrWhiteSpace(b.EntraOid))
            {
                set.Add(b.EntraOid);
            }
        }

        return set;
    }

    public void SetJoinOrderEntraHint(uint sourceId, string entraObjectId)
    {
        if (string.IsNullOrWhiteSpace(entraObjectId) || !_bindings.TryGetValue(sourceId, out var b) || b.IsFinal)
        {
            return;
        }

        b.EntraOid = entraObjectId.Trim();
    }
}
