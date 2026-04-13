using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Graph.Communications.Calls;
using Microsoft.Graph.Communications.Resources;

namespace TeamsMediaBot;

/// <summary>
/// Coordinates per-participant Transcribe streams and source-id to participant mapping.
/// </summary>
public sealed class TranscriptionManager : IAsyncDisposable
{
    private readonly BotSettings _settings;
    private readonly TranscriptAggregator _aggregator;
    private readonly MeetingParticipantService _meetingParticipants;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<TranscriptionManager> _logger;
    private readonly ConcurrentDictionary<uint, TranscribeStreamService> _streamsBySourceId = new();
    private readonly ConcurrentDictionary<uint, ParticipantIdentity> _participantBySourceId = new();
    private readonly ConcurrentDictionary<string, List<uint>> _sourceIdsByUserId = new(StringComparer.OrdinalIgnoreCase);
    private ICall? _attachedCall;
    private string? _botClientId;

    public TranscriptionManager(
        BotSettings settings,
        TranscriptAggregator aggregator,
        MeetingParticipantService meetingParticipants,
        ILoggerFactory loggerFactory,
        ILogger<TranscriptionManager> logger)
    {
        _settings = settings;
        _aggregator = aggregator;
        _meetingParticipants = meetingParticipants;
        _loggerFactory = loggerFactory;
        _logger = logger;
    }

    public void AttachToCall(ICall call, string botClientId)
    {
        _attachedCall = call;
        _botClientId = botClientId;

        call.Participants.OnUpdated += (_, args) =>
        {
            foreach (var p in args.AddedResources)
            {
                UpsertParticipantMappings(p, botClientId);
            }
            foreach (var p in args.UpdatedResources)
            {
                UpsertParticipantMappings(p, botClientId);
            }
            foreach (var p in args.RemovedResources)
            {
                RemoveParticipantMappings(p);
            }
        };

        TryHydrateFromCurrentRoster();
    }

    public async Task ProcessParticipantAudioAsync(uint sourceId, byte[] pcmChunk, long timestamp)
    {
        if (pcmChunk.Length == 0)
        {
            return;
        }

        if (!_participantBySourceId.TryGetValue(sourceId, out var participant))
        {
            var syntheticId = ParticipantManager.SyntheticParticipantId(sourceId);
            var syn = new ParticipantIdentity(syntheticId, $"Speaker ({sourceId})");
            if (_participantBySourceId.TryAdd(sourceId, syn))
            {
                participant = syn;
                _logger.LogInformation(
                    "Placeholder mapping sourceId {SourceId} -> {DisplayName} until Graph provides mediaStreams (no roster-based Entra guess).",
                    sourceId,
                    participant.DisplayName);
            }
            else
            {
                _participantBySourceId.TryGetValue(sourceId, out participant!);
            }
        }

        var stream = _streamsBySourceId.GetOrAdd(sourceId, _ =>
        {
            var s = new TranscribeStreamService(
                _settings,
                _aggregator,
                sourceId,
                participant,
                _loggerFactory.CreateLogger<TranscribeStreamService>());
            return s;
        });

        stream.UpdateParticipant(participant);
        await stream.EnsureStartedAsync();
        stream.EnqueueAudio(pcmChunk, timestamp);
    }

    private void UpsertParticipantMappings(IParticipant participant, string botClientId)
    {
        var resource = participant.Resource;
        var identity = resource?.Info?.Identity;
        var appId = identity?.Application?.Id;
        if (!string.IsNullOrWhiteSpace(appId) &&
            string.Equals(appId.Trim(), botClientId, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        var userId = identity?.User?.Id;
        if (string.IsNullOrWhiteSpace(userId))
        {
            return;
        }

        var displayName = identity?.User?.DisplayName;
        if (string.IsNullOrWhiteSpace(displayName))
        {
            displayName = userId;
        }

        var identityRecord = new ParticipantIdentity(userId.Trim(), displayName.Trim());
        var sourceIds = TryExtractSourceIds(resource);
        if (sourceIds.Count == 0)
        {
            _logger.LogDebug(
                "Participant {UserId} ({DisplayName}) has no sourceId in media streams yet. AdditionalData: {AdditionalDataSummary}",
                identityRecord.UserId,
                identityRecord.DisplayName,
                DescribeAdditionalData(resource));
            return;
        }

        _sourceIdsByUserId[userId.Trim()] = sourceIds;
        foreach (var sourceId in sourceIds)
        {
            if (_participantBySourceId.TryGetValue(sourceId, out var existing) &&
                !string.Equals(existing.UserId, identityRecord.UserId, StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning(
                    "Ignoring Graph mapping that would reassign sourceId {SourceId} from {ExistingUser} to {NewUser}.",
                    sourceId,
                    existing.UserId,
                    identityRecord.UserId);
                continue;
            }

            _participantBySourceId[sourceId] = identityRecord;
            _logger.LogInformation(
                "Mapped sourceId {SourceId} -> {DisplayName} ({UserId}).",
                sourceId,
                identityRecord.DisplayName,
                identityRecord.UserId);
            if (_streamsBySourceId.TryGetValue(sourceId, out var stream))
            {
                stream.UpdateParticipant(identityRecord);
            }
        }
    }

    private void RemoveParticipantMappings(IParticipant participant)
    {
        var userId = participant.Resource?.Info?.Identity?.User?.Id;
        if (string.IsNullOrWhiteSpace(userId))
        {
            return;
        }

        // Drop user→source index only; keep per-sourceId identity and Transcribe streams (immutable for the call).
        _sourceIdsByUserId.TryRemove(userId.Trim(), out _);
    }

    private static List<uint> TryExtractSourceIds(Microsoft.Graph.Models.Participant? participant) =>
        GraphParticipantMediaStreams.ExtractSourceIds(participant);

    private static string DescribeAdditionalData(Microsoft.Graph.Models.Participant? participant)
    {
        if (participant?.AdditionalData is null || participant.AdditionalData.Count == 0)
        {
            return "<empty>";
        }

        var keys = string.Join(",", participant.AdditionalData.Keys.OrderBy(k => k, StringComparer.OrdinalIgnoreCase));
        if (!participant.AdditionalData.TryGetValue("mediaStreams", out var mediaStreams) &&
            !participant.AdditionalData.TryGetValue("MediaStreams", out mediaStreams))
        {
            return $"keys=[{keys}]";
        }

        var mediaText = mediaStreams switch
        {
            JsonElement je => je.ToString(),
            null => "<null>",
            _ => mediaStreams.ToString() ?? "<unknown>"
        };
        if (mediaText.Length > 400)
        {
            mediaText = mediaText[..400] + "...";
        }

        return $"keys=[{keys}], mediaStreams={mediaText}";
    }

    private void TryHydrateFromCurrentRoster()
    {
        var call = _attachedCall;
        var botClientId = _botClientId;
        if (call is null || string.IsNullOrWhiteSpace(botClientId))
        {
            return;
        }

        try
        {
            foreach (var participant in call.Participants)
            {
                UpsertParticipantMappings(participant, botClientId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to hydrate participant mappings from current roster.");
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var stream in _streamsBySourceId.Values)
        {
            await stream.DisposeAsync();
        }

        _streamsBySourceId.Clear();
    }
}
