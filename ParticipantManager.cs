using System.Collections.Concurrent;
using System.Threading;
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

    /// <summary>Stable human-facing label for this stream for the meeting (e.g. <c>Speaker 1</c>) when Entra name unknown.</summary>
    public string StableSpeakerLabel { get; set; } = "";

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

    private int _speakerCounter;

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
            Interlocked.Exchange(ref _speakerCounter, 0);
            _logger.LogInformation("ParticipantManager reset for meeting key {MeetingKey}.", _meetingKey);
        }
    }

    public bool HasBinding(uint sourceId) => _bindings.ContainsKey(sourceId);

    public bool TryGetBinding(uint sourceId, out ParticipantBinding? binding) =>
        _bindings.TryGetValue(sourceId, out binding);

    /// <summary>
    /// Human-facing transcript label: <b>always</b> prefers Entra/Graph display name when <see cref="ParticipantBinding.EntraOid"/> is set;
    /// otherwise stable <see cref="ParticipantBinding.StableSpeakerLabel"/> (e.g. Speaker 1). Never returns <c>Speaker ({sourceId})</c>.
    /// </summary>
    public string GetTranscriptSpeakerLabel(uint sourceId)
    {
        if (!_bindings.TryGetValue(sourceId, out var b))
        {
            return string.Empty;
        }

        if (!string.IsNullOrWhiteSpace(b.EntraOid))
        {
            var fromEntra = GetCanonicalDisplayName(b.EntraOid.Trim());
            if (!string.IsNullOrWhiteSpace(fromEntra))
            {
                return fromEntra;
            }

            if (!string.IsNullOrWhiteSpace(b.DisplayName))
            {
                return b.DisplayName.Trim();
            }
        }
        else if (!string.IsNullOrWhiteSpace(b.DisplayName))
        {
            return b.DisplayName.Trim();
        }

        return string.IsNullOrWhiteSpace(b.StableSpeakerLabel) ? string.Empty : b.StableSpeakerLabel.Trim();
    }

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
        var inputDisplayName = string.IsNullOrWhiteSpace(displayName) ? string.Empty : displayName.Trim();
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

                if (!string.IsNullOrWhiteSpace(incomingOid))
                {
                    existing.EntraOid = incomingOid;
                    existing.IsFinal = true;
                }

                if (!string.IsNullOrWhiteSpace(inputDisplayName))
                {
                    existing.DisplayName = inputDisplayName;
                }

                if (!string.IsNullOrWhiteSpace(existing.EntraOid))
                {
                    var resolved = string.IsNullOrWhiteSpace(existing.DisplayName) ? existing.EntraOid : existing.DisplayName;
                    RegisterParticipant(existing.EntraOid, resolved, DateTime.UtcNow);
                    _logger.LogInformation(
                        "Graph mapped sourceId {SourceId} → Entra {DisplayName}",
                        sourceId,
                        resolved);
                }

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
        var stableLabel = $"Speaker {Interlocked.Increment(ref _speakerCounter)}";
        var binding = new ParticipantBinding
        {
            SourceId = sourceId,
            StreamParticipantId = streamPid,
            StableSpeakerLabel = stableLabel
        };

        var initialGraphOid = string.IsNullOrWhiteSpace(participantIdOrEntraFromGraph)
            ? null
            : participantIdOrEntraFromGraph.Trim();
        if (graphOrAuthoritative && !string.IsNullOrWhiteSpace(initialGraphOid))
        {
            binding.EntraOid = initialGraphOid;
            binding.DisplayName = !string.IsNullOrWhiteSpace(inputDisplayName) ? inputDisplayName : stableLabel;
            RegisterParticipant(binding.EntraOid, binding.DisplayName, DateTime.UtcNow);
            binding.IsFinal = true;
        }
        else
        {
            binding.IsFinal = false;
            binding.DisplayName = stableLabel;
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
        displayName = GetTranscriptSpeakerLabel(sourceId);
        if (string.IsNullOrWhiteSpace(displayName))
        {
            displayName = string.IsNullOrWhiteSpace(b.StableSpeakerLabel) ? participantId : b.StableSpeakerLabel;
        }

        return true;
    }

    public string? GetCanonicalDisplayName(string participantId)
    {
        if (string.IsNullOrWhiteSpace(participantId))
        {
            return null;
        }

        var p = participantId.Trim();
        if (TryParseSourceIdFromSynthetic(p, out var sid))
        {
            var label = GetTranscriptSpeakerLabel(sid);
            if (!string.IsNullOrWhiteSpace(label))
            {
                return label;
            }
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
}
