using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

/// <summary>
/// Central speaker identity keyed by Teams media <c>sourceId</c> (MSI). Used for deferred display names and transcript backfill.
/// </summary>
public sealed class SpeakerIdentityStore
{
    private readonly TranscriptBroadcaster _broadcaster;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<SpeakerIdentityStore> _logger;

    /// <summary>Media source id → participant identity (Entra may arrive late).</summary>
    public ConcurrentDictionary<uint, ParticipantIdentity> SourceToParticipant { get; } = new();

    /// <summary>Entra object id → media source id (inverse lookup).</summary>
    public ConcurrentDictionary<string, uint> EntraToSource { get; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Final lines emitted before identity resolution (diagnostics / optional line-level tooling).</summary>
    public ConcurrentDictionary<uint, List<SpeakerTranscriptRecord>> PendingTranscripts { get; } = new();

    private readonly ConcurrentDictionary<uint, (string Entra, string Name)> _lastResolutionBroadcast = new();

    public SpeakerIdentityStore(
        TranscriptBroadcaster broadcaster,
        IServiceScopeFactory scopeFactory,
        ILogger<SpeakerIdentityStore> logger)
    {
        _broadcaster = broadcaster;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    public void ResetForNewMeeting()
    {
        SourceToParticipant.Clear();
        EntraToSource.Clear();
        PendingTranscripts.Clear();
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

    public void RegisterPendingTranscript(uint sourceId, SpeakerTranscriptRecord transcriptEvent)
    {
        var list = PendingTranscripts.GetOrAdd(sourceId, _ => new List<SpeakerTranscriptRecord>());
        lock (list)
        {
            list.Add(transcriptEvent);
        }
    }

    public void ApplyFinalDisplayNameToPending(uint sourceId, string? finalName)
    {
        if (!PendingTranscripts.TryGetValue(sourceId, out var list))
        {
            return;
        }

        lock (list)
        {
            foreach (var e in list)
            {
                e.FinalDisplayName = finalName;
            }
        }
    }

    private async Task PublishResolvedAsync(uint sourceId, string? entraOid, string? displayName)
    {
        try
        {
            ApplyFinalDisplayNameToPending(sourceId, displayName);
            await _broadcaster.BroadcastTranscriptIdentityUpdateAsync(sourceId, displayName, entraOid);
            using var scope = _scopeFactory.CreateScope();
            var aggregator = scope.ServiceProvider.GetRequiredService<TranscriptAggregator>();
            await aggregator.ResolvePendingAsync(sourceId);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Identity backfill failed for sourceId {SourceId}.", sourceId);
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

/// <summary>
/// One final transcript line tied to a media source id before Entra/display name is known (not AWS SDK <c>TranscriptEvent</c>).
/// </summary>
public sealed class SpeakerTranscriptRecord
{
    public required string Text { get; set; }
    public required uint SourceId { get; set; }
    public required DateTime Timestamp { get; set; }
    public string? FinalDisplayName { get; set; }
}
