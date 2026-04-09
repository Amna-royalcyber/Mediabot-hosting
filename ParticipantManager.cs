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
/// Global participant registry and immutable audio source-id → Entra user mapping for a meeting.
/// </summary>
public sealed class ParticipantManager
{
    private readonly ILogger<ParticipantManager> _logger;
    private readonly object _lifecycleLock = new();

    private readonly ConcurrentDictionary<string, ParticipantInfo> _participants =
        new(StringComparer.OrdinalIgnoreCase);

    /// <summary>MSI/sourceId → Entra object id. Never overwritten with a different user.</summary>
    private readonly ConcurrentDictionary<uint, string> _sourceIdToParticipantId = new();

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
    /// Bind a Teams media source id to an Entra user. If already bound to another user, the existing binding wins.
    /// </summary>
    public bool TryBindAudioSource(uint sourceId, string participantId, string displayName, string reason)
    {
        if (string.IsNullOrWhiteSpace(participantId))
        {
            return false;
        }

        var pid = participantId.Trim();
        displayName = string.IsNullOrWhiteSpace(displayName) ? pid : displayName.Trim();

        RegisterParticipant(pid, displayName, DateTime.UtcNow);

        if (_sourceIdToParticipantId.TryGetValue(sourceId, out var existingPid))
        {
            if (!string.Equals(existingPid, pid, StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning(
                    "Ignoring {Reason} bind for sourceId {SourceId} → {NewParticipantId}; already bound to {ExistingParticipantId}.",
                    reason,
                    sourceId,
                    pid,
                    existingPid);
            }

            return true;
        }

        if (!_sourceIdToParticipantId.TryAdd(sourceId, pid))
        {
            return true;
        }

        _logger.LogInformation(
            "Bound audio sourceId {SourceId} → {DisplayName} ({ParticipantId}) [{Reason}].",
            sourceId,
            GetCanonicalDisplayName(pid) ?? displayName,
            pid,
            reason);

        return true;
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
