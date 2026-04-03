using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Microsoft.Graph.Communications.Calls;
using Microsoft.Graph.Communications.Resources;
using Microsoft.Graph.Models;

namespace TeamsMediaBot;

/// <summary>
/// Tracks Teams meeting participants from Graph Communications roster updates and maps
/// AWS Transcribe speaker ids (spk_0, spk_1, …) to display names by <b>stable join order</b>
/// (excluding the bot application). AWS assigns spk_* by audio diarization, which may not match
/// speaking order or roster order — treat names as best-effort when multiple humans speak.
/// </summary>
public sealed class MeetingParticipantService
{
    private static readonly Regex AwsSpeakerIndex = new(@"^spk_(\d+)$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private readonly TranscriptBroadcaster _broadcaster;
    private readonly ILogger<MeetingParticipantService> _logger;
    private readonly object _lock = new();

    private readonly List<RosterEntry> _rosterOrder = new();
    private readonly Dictionary<string, string> _idToDisplayName = new(StringComparer.OrdinalIgnoreCase);

    public MeetingParticipantService(TranscriptBroadcaster broadcaster, ILogger<MeetingParticipantService> logger)
    {
        _broadcaster = broadcaster;
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

        _logger.LogInformation("Subscribed to call participant roster updates for speaker name hints.");
    }

    /// <summary>Best-effort: maps spk_N to the Nth non-bot participant in first-seen roster order.</summary>
    public string? TryResolveTeamsDisplayName(string? awsSpeakerId)
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
                return _rosterOrder[idx].DisplayName;
            }
        }

        return null;
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

        var displayName = ResolveDisplayName(resource);
        if (string.IsNullOrWhiteSpace(displayName))
        {
            return;
        }

        displayName = displayName.Trim();
        var id = resource.Id;
        if (string.IsNullOrWhiteSpace(id))
        {
            return;
        }

        lock (_lock)
        {
            if (_idToDisplayName.TryGetValue(id, out var existing))
            {
                if (!string.Equals(existing, displayName, StringComparison.Ordinal))
                {
                    _idToDisplayName[id] = displayName;
                    for (var i = 0; i < _rosterOrder.Count; i++)
                    {
                        if (string.Equals(_rosterOrder[i].Id, id, StringComparison.OrdinalIgnoreCase))
                        {
                            _rosterOrder[i] = new RosterEntry(id, displayName);
                            break;
                        }
                    }
                }
            }
            else
            {
                _idToDisplayName[id] = displayName;
                _rosterOrder.Add(new RosterEntry(id, displayName));
            }
        }

        _ = PublishRosterAsync();
    }

    private void RemoveParticipant(IParticipant participant)
    {
        var id = participant.Resource?.Id;
        if (string.IsNullOrWhiteSpace(id))
        {
            return;
        }

        lock (_lock)
        {
            _idToDisplayName.Remove(id);
            _rosterOrder.RemoveAll(e => string.Equals(e.Id, id, StringComparison.OrdinalIgnoreCase));
        }

        _ = PublishRosterAsync();
    }

    private async Task PublishRosterAsync()
    {
        List<RosterEntry> snapshot;
        lock (_lock)
        {
            snapshot = _rosterOrder.ToList();
        }

        await _broadcaster.BroadcastRosterAsync(
            snapshot.Select(e => (e.Id, e.DisplayName)).ToList());
    }

    private static bool IsOurBot(Participant resource, string botClientId)
    {
        var appId = resource.Info?.Identity?.Application?.Id;
        return !string.IsNullOrEmpty(appId) &&
               string.Equals(appId.Trim(), botClientId.Trim(), StringComparison.OrdinalIgnoreCase);
    }

    private static string? ResolveDisplayName(Participant resource)
    {
        var identity = resource.Info?.Identity;
        if (identity is null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(identity.User?.DisplayName))
        {
            return identity.User!.DisplayName;
        }

        if (!string.IsNullOrWhiteSpace(identity.User?.Id))
        {
            return identity.User!.Id;
        }

        return null;
    }

    private readonly record struct RosterEntry(string Id, string DisplayName);
}
