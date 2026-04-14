using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Graph.Communications.Calls;
using Microsoft.Graph.Communications.Resources;
using Microsoft.Graph.Models;

namespace TeamsMediaBot;

/// <summary>
/// Tracks Teams meeting participants from Graph Communications roster updates and maps
/// AWS Transcribe speaker ids (spk_0, spk_1, …) to Entra profiles by <b>first-seen user order</b>
/// (excluding the bot application). AWS diarization may not match speaking order — labels are best-effort.
/// </summary>
public sealed class MeetingParticipantService
{
    /// <summary>AWS may send <c>spk_0</c> or a bare digit <c>0</c> depending on SDK/version.</summary>
    private static readonly Regex AwsSpeakerIndex = new(@"^(?:spk_)?(\d+)$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private readonly TranscriptBroadcaster _broadcaster;
    private readonly EntraUserResolver _entra;
    private readonly IParticipantManager _participantManager;
    private readonly ILogger<MeetingParticipantService> _logger;
    private readonly object _lock = new();
    private readonly ConcurrentDictionary<string, DateTime> _noSourceLogThrottle = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Call participant resource ids (for removals).</summary>
    private readonly Dictionary<string, string> _callParticipantIdToAzureUserId = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Stable order of human participants for spk_N → row N mapping.</summary>
    private readonly List<RosterEntry> _rosterOrder = new();

    /// <summary>Teams audio <c>sourceId</c> → Entra object id from roster <c>mediaStreams</c> (links placeholders to real users).</summary>
    private readonly ConcurrentDictionary<uint, string> _audioSourceIdToAzureObjectId = new();

    public MeetingParticipantService(
        TranscriptBroadcaster broadcaster,
        EntraUserResolver entra,
        IParticipantManager participantManager,
        ILogger<MeetingParticipantService> logger)
    {
        _broadcaster = broadcaster;
        _entra = entra;
        _participantManager = participantManager;
        _logger = logger;
    }

    public void AttachToCall(ICall call, string botAzureAdApplicationClientId)
    {
        _audioSourceIdToAzureObjectId.Clear();
        lock (_lock)
        {
            _callParticipantIdToAzureUserId.Clear();
            _rosterOrder.Clear();
        }

        var participants = call.Participants;
        participants.OnUpdated += (_, args) =>
        {
            try
            {
                foreach (var p in args.AddedResources)
                {
                    IngestParticipant(p, botAzureAdApplicationClientId);
                }

                foreach (var p in args.UpdatedResources)
                {
                    IngestParticipant(p, botAzureAdApplicationClientId);
                }

                foreach (var p in args.RemovedResources)
                {
                    RemoveParticipant(p);
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Participant roster handler failed.");
            }
        };

        try
        {
            foreach (var p in participants)
            {
                IngestParticipant(p, botAzureAdApplicationClientId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not ingest existing call participants into roster.");
        }

        _logger.LogInformation("Subscribed to call participant roster updates; Entra profiles resolved via Microsoft Graph when needed.");
    }

    /// <summary>
    /// Re-ingests every participant resource (late <c>mediaStreams</c> / <c>sourceId</c>). Pair with <see cref="ParticipantAudioRouter"/> periodic rescan for demos.
    /// </summary>
    public void ResyncParticipantMediaStreamsFromCall(ICall call, string botAzureAdApplicationClientId)
    {
        try
        {
            foreach (var p in call.Participants)
            {
                IngestParticipant(p, botAzureAdApplicationClientId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Resync participant mediaStreams from call failed.");
        }
    }

    /// <summary>Maps spk_N to the Nth human participant row (first-seen Azure AD user order).</summary>
    public SpeakerResolution? TryResolveSpeaker(string? awsSpeakerId)
    {
        if (string.IsNullOrEmpty(awsSpeakerId))
        {
            return null;
        }

        var m = AwsSpeakerIndex.Match(awsSpeakerId.Trim());
        if (!m.Success || !int.TryParse(m.Groups[1].Value, out var idx) || idx < 0)
        {
            return null;
        }

        lock (_lock)
        {
            if (idx < _rosterOrder.Count)
            {
                var e = _rosterOrder[idx];
                return new SpeakerResolution(
                    e.DisplayName,
                    e.UserPrincipalName,
                    e.AzureAdObjectId);
            }
        }

        return null;
    }

    /// <summary>Resolve Teams MSI/source id to Entra user using roster mediaStreams (when Graph lists source ids before audio bind upgrades).</summary>
    public bool TryResolveAudioSourceToEntra(uint sourceId, out string azureAdObjectId, out string displayName)
    {
        azureAdObjectId = string.Empty;
        displayName = string.Empty;
        if (!_audioSourceIdToAzureObjectId.TryGetValue(sourceId, out var oid) || string.IsNullOrWhiteSpace(oid))
        {
            return false;
        }

        azureAdObjectId = oid.Trim();
        lock (_lock)
        {
            foreach (var e in _rosterOrder)
            {
                if (string.Equals(e.AzureAdObjectId, azureAdObjectId, StringComparison.OrdinalIgnoreCase))
                {
                    displayName = string.IsNullOrWhiteSpace(e.DisplayName) ? azureAdObjectId : e.DisplayName.Trim();
                    return true;
                }
            }
        }

        displayName = azureAdObjectId;
        return true;
    }

    public IReadOnlyList<RosterParticipantDto> GetRosterSnapshot()
    {
        lock (_lock)
        {
            return _rosterOrder
                .Select(e => new RosterParticipantDto(
                    e.CallParticipantId,
                    e.DisplayName,
                    e.AzureAdObjectId,
                    e.UserPrincipalName))
                .ToList();
        }
    }

    private void IngestParticipant(IParticipant participant, string botClientId)
    {
        var resource = participant.Resource;
        if (resource is null)
        {
            return;
        }

        if (IsOurBot(resource, botClientId))
        {
            return;
        }

        var azureUserId = resource.Info?.Identity?.User?.Id;
        if (string.IsNullOrWhiteSpace(azureUserId))
        {
            return;
        }

        azureUserId = azureUserId.Trim();
        var callPartId = resource.Id;
        if (string.IsNullOrWhiteSpace(callPartId))
        {
            return;
        }

        var fromCall = resource.Info!.Identity!.User!.DisplayName?.Trim();
        var displayName = string.IsNullOrWhiteSpace(fromCall) ? null : fromCall;

        var needsGraph = string.IsNullOrWhiteSpace(displayName);
        lock (_lock)
        {
            _callParticipantIdToAzureUserId[callPartId] = azureUserId;

            var existingIdx = -1;
            for (var i = 0; i < _rosterOrder.Count; i++)
            {
                if (string.Equals(_rosterOrder[i].AzureAdObjectId, azureUserId, StringComparison.OrdinalIgnoreCase))
                {
                    existingIdx = i;
                    break;
                }
            }

            if (existingIdx >= 0)
            {
                var cur = _rosterOrder[existingIdx];
                if (!string.IsNullOrWhiteSpace(displayName))
                {
                    _rosterOrder[existingIdx] = cur with { DisplayName = displayName };
                }
            }
            else
            {
                _rosterOrder.Add(new RosterEntry(
                    callPartId,
                    azureUserId,
                    displayName ?? azureUserId,
                    UserPrincipalName: null));
            }
        }

        // Roster-first identity shell: participant identity exists before any audio/sourceId is seen.
        _participantManager.RegisterParticipant(azureUserId, displayName ?? azureUserId, DateTime.UtcNow);

        var sourceIds = GraphParticipantMediaStreams.ExtractSourceIds(resource);
        foreach (var sid in sourceIds)
        {
            _audioSourceIdToAzureObjectId[sid] = azureUserId;
            _logger.LogInformation(
                "Authoritative stream map discovered: sourceId {SourceId} -> {DisplayName} ({AzureAdObjectId}).",
                sid,
                displayName ?? azureUserId,
                azureUserId);
        }

        if (sourceIds.Count == 0)
        {
            var throttleKey = $"{azureUserId}:no-source";
            if (_noSourceLogThrottle.TryGetValue(throttleKey, out var last) && (DateTime.UtcNow - last) < TimeSpan.FromSeconds(30))
            {
                _ = PublishRosterAsync();
                if (needsGraph)
                {
                    _ = EnrichFromGraphAsync(azureUserId);
                }
                return;
            }
            _noSourceLogThrottle[throttleKey] = DateTime.UtcNow;

            var keys = resource.AdditionalData is null
                ? "<none>"
                : string.Join(", ", resource.AdditionalData.Keys);
            var anonymized = ReadAdditionalDataBool(resource.AdditionalData, "isIdentityAnonymized");
            var voiceConsent = ReadAdditionalDataString(resource.AdditionalData, "aiVoiceConsent");
            _logger.LogInformation(
                "Participant update has no sourceId yet for {DisplayName} ({AzureAdObjectId}). AdditionalData keys: {Keys}; isIdentityAnonymized={IsIdentityAnonymized}; aiVoiceConsent={AiVoiceConsent}",
                displayName ?? azureUserId,
                azureUserId,
                keys,
                anonymized.HasValue ? anonymized.Value.ToString() : "<missing>",
                string.IsNullOrWhiteSpace(voiceConsent) ? "<missing>" : voiceConsent);
        }

        _ = PublishRosterAsync();

        if (needsGraph)
        {
            _ = EnrichFromGraphAsync(azureUserId);
        }
    }

    private async Task EnrichFromGraphAsync(string azureUserId)
    {
        try
        {
            var profile = await _entra.GetUserAsync(azureUserId).ConfigureAwait(false);
            if (profile is null)
            {
                return;
            }

            var dn = string.IsNullOrWhiteSpace(profile.DisplayName) ? profile.Id : profile.DisplayName.Trim();
            var upn = profile.UserPrincipalName;

            lock (_lock)
            {
                for (var i = 0; i < _rosterOrder.Count; i++)
                {
                    if (!string.Equals(_rosterOrder[i].AzureAdObjectId, azureUserId, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    _rosterOrder[i] = _rosterOrder[i] with
                    {
                        DisplayName = dn,
                        UserPrincipalName = string.IsNullOrWhiteSpace(upn) ? _rosterOrder[i].UserPrincipalName : upn.Trim()
                    };
                }
            }

            await PublishRosterAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Graph enrichment failed for {UserId}.", azureUserId);
        }
    }

    private void RemoveParticipant(IParticipant participant)
    {
        var callPartId = participant.Resource?.Id;
        if (string.IsNullOrWhiteSpace(callPartId))
        {
            return;
        }

        string? removedAzureId = null;
        lock (_lock)
        {
            if (!_callParticipantIdToAzureUserId.TryGetValue(callPartId, out removedAzureId))
            {
                return;
            }

            _callParticipantIdToAzureUserId.Remove(callPartId);
            _rosterOrder.RemoveAll(e => string.Equals(e.CallParticipantId, callPartId, StringComparison.OrdinalIgnoreCase));
        }

        if (!string.IsNullOrWhiteSpace(removedAzureId))
        {
            foreach (var kv in _audioSourceIdToAzureObjectId.ToArray())
            {
                if (string.Equals(kv.Value, removedAzureId, StringComparison.OrdinalIgnoreCase))
                {
                    _audioSourceIdToAzureObjectId.TryRemove(kv.Key, out _);
                }
            }
        }

        _ = PublishRosterAsync();
    }

    private async Task PublishRosterAsync()
    {
        List<RosterParticipantDto> snapshot;
        lock (_lock)
        {
            snapshot = _rosterOrder
                .Select(e => new RosterParticipantDto(
                    e.CallParticipantId,
                    e.DisplayName,
                    e.AzureAdObjectId,
                    e.UserPrincipalName))
                .ToList();
        }

        await _broadcaster.BroadcastRosterAsync(snapshot).ConfigureAwait(false);
    }

    private static bool IsOurBot(Participant resource, string botClientId)
    {
        var appId = resource.Info?.Identity?.Application?.Id;
        return !string.IsNullOrEmpty(appId) &&
               string.Equals(appId.Trim(), botClientId.Trim(), StringComparison.OrdinalIgnoreCase);
    }

    private static bool? ReadAdditionalDataBool(IDictionary<string, object>? additionalData, string key)
    {
        if (additionalData is null)
        {
            return null;
        }

        foreach (var kv in additionalData)
        {
            if (!string.Equals(kv.Key, key, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var value = kv.Value;
            if (value is null)
            {
                return null;
            }

            if (value is bool b)
            {
                return b;
            }

            if (value is JsonElement je)
            {
                if (je.ValueKind is JsonValueKind.True or JsonValueKind.False)
                {
                    return je.GetBoolean();
                }

                if (je.ValueKind == JsonValueKind.String && bool.TryParse(je.GetString(), out var jb))
                {
                    return jb;
                }
            }

            if (bool.TryParse(Convert.ToString(value), out var parsed))
            {
                return parsed;
            }

            return null;
        }

        return null;
    }

    private static string? ReadAdditionalDataString(IDictionary<string, object>? additionalData, string key)
    {
        if (additionalData is null)
        {
            return null;
        }

        foreach (var kv in additionalData)
        {
            if (!string.Equals(kv.Key, key, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var value = kv.Value;
            if (value is null)
            {
                return null;
            }

            if (value is JsonElement je)
            {
                if (je.ValueKind == JsonValueKind.String)
                {
                    return je.GetString();
                }

                return je.GetRawText();
            }

            return Convert.ToString(value);
        }

        return null;
    }

    private readonly record struct RosterEntry(
        string CallParticipantId,
        string AzureAdObjectId,
        string DisplayName,
        string? UserPrincipalName);
}

public readonly record struct SpeakerResolution(string DisplayName, string? UserPrincipalName, string? AzureAdObjectId);

public sealed record RosterParticipantDto(
    string CallParticipantId,
    string DisplayName,
    string AzureAdObjectId,
    string? UserPrincipalName);
