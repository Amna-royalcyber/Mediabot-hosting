using System.Text.RegularExpressions;
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
    private readonly ILogger<MeetingParticipantService> _logger;
    private readonly object _lock = new();

    /// <summary>Call participant resource ids (for removals).</summary>
    private readonly Dictionary<string, string> _callParticipantIdToAzureUserId = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Stable order of human participants for spk_N → row N mapping.</summary>
    private readonly List<RosterEntry> _rosterOrder = new();

    public MeetingParticipantService(
        TranscriptBroadcaster broadcaster,
        EntraUserResolver entra,
        ILogger<MeetingParticipantService> logger)
    {
        _broadcaster = broadcaster;
        _entra = entra;
        _logger = logger;
    }

    public void AttachToCall(ICall call, string botAzureAdApplicationClientId)
    {
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

        lock (_lock)
        {
            if (!_callParticipantIdToAzureUserId.ContainsKey(callPartId))
            {
                return;
            }

            _callParticipantIdToAzureUserId.Remove(callPartId);
            _rosterOrder.RemoveAll(e => string.Equals(e.CallParticipantId, callPartId, StringComparison.OrdinalIgnoreCase));
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
