using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

/// <summary>
/// Central speaker identity keyed by Teams media <c>sourceId</c> (MSI). Synced from <see cref="ParticipantManager"/> bindings.
/// </summary>
public sealed class SpeakerIdentityStore
{
    private readonly TranscriptBroadcaster _broadcaster;
    private readonly ILogger<SpeakerIdentityStore> _logger;

    /// <summary>Media source id → participant identity (Entra may arrive late).</summary>
    public ConcurrentDictionary<uint, ParticipantIdentity> SourceToParticipant { get; } = new();

    /// <summary>Entra object id → media source id (inverse lookup).</summary>
    public ConcurrentDictionary<string, uint> EntraToSource { get; } = new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<uint, (string Entra, string Name)> _lastResolutionBroadcast = new();

    public SpeakerIdentityStore(
        TranscriptBroadcaster broadcaster,
        ILogger<SpeakerIdentityStore> logger)
    {
        _broadcaster = broadcaster;
        _logger = logger;
    }

    public void ResetForNewMeeting()
    {
        SourceToParticipant.Clear();
        EntraToSource.Clear();
        _lastResolutionBroadcast.Clear();
    }

    public static ParticipantIdentity UnknownParticipant(uint sourceId) =>
        new()
        {
            SourceId = sourceId,
            EntraUserId = null,
            DisplayName = null,
            IsResolved = false
        };

    public bool TryGet(uint sourceId, out ParticipantIdentity identity) =>
        SourceToParticipant.TryGetValue(sourceId, out identity!);

    /// <summary>Sync from <see cref="ParticipantManager"/> after any binding change.</summary>
    public void OnParticipantBindingUpdated(ParticipantBinding binding)
    {
        var sourceId = binding.SourceId;
        var entra = binding.EntraOid?.Trim();
        var resolved = binding.State == IdentityState.Resolved && !string.IsNullOrWhiteSpace(entra);

        UpsertCore(sourceId, entra, binding.DisplayName, resolved);

        if (!resolved)
        {
            return;
        }

        var displayName = binding.DisplayName?.Trim();
        if (string.IsNullOrWhiteSpace(displayName))
        {
            displayName = entra;
        }

        if (_lastResolutionBroadcast.TryGetValue(sourceId, out var prev) &&
            string.Equals(prev.Entra, entra, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(prev.Name, displayName, StringComparison.Ordinal))
        {
            return;
        }

        _lastResolutionBroadcast[sourceId] = (entra!, displayName!);
        _ = PublishResolvedAsync(sourceId, entra, displayName);
    }

    private void UpsertCore(uint sourceId, string? entraOid, string? displayName, bool isResolved)
    {
        SourceToParticipant.AddOrUpdate(
            sourceId,
            _ => new ParticipantIdentity
            {
                SourceId = sourceId,
                EntraUserId = entraOid,
                DisplayName = string.IsNullOrWhiteSpace(displayName) ? null : displayName.Trim(),
                IsResolved = isResolved
            },
            (_, existing) =>
            {
                if (!string.IsNullOrWhiteSpace(entraOid))
                {
                    existing.EntraUserId = entraOid.Trim();
                }

                if (!string.IsNullOrWhiteSpace(displayName))
                {
                    existing.DisplayName = displayName.Trim();
                }

                existing.IsResolved = isResolved;
                return existing;
            });

        if (!string.IsNullOrWhiteSpace(entraOid))
        {
            EntraToSource[entraOid.Trim()] = sourceId;
        }
    }

    private async Task PublishResolvedAsync(uint sourceId, string? entraOid, string? displayName)
    {
        try
        {
            await _broadcaster.BroadcastTranscriptIdentityUpdateAsync(sourceId, displayName, entraOid);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Identity broadcast failed for sourceId {SourceId}.", sourceId);
        }
    }
}

public sealed class ParticipantIdentity
{
    public uint SourceId { get; set; }
    public string? EntraUserId { get; set; }
    public string? DisplayName { get; set; }
    public bool IsResolved { get; set; }
}
