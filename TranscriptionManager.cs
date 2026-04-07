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
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<TranscriptionManager> _logger;
    private readonly ConcurrentDictionary<uint, TranscribeStreamService> _streamsBySourceId = new();
    private readonly ConcurrentDictionary<uint, ParticipantIdentity> _participantBySourceId = new();
    private readonly ConcurrentDictionary<string, List<uint>> _sourceIdsByUserId = new(StringComparer.OrdinalIgnoreCase);

    public TranscriptionManager(
        BotSettings settings,
        TranscriptAggregator aggregator,
        ILoggerFactory loggerFactory,
        ILogger<TranscriptionManager> logger)
    {
        _settings = settings;
        _aggregator = aggregator;
        _loggerFactory = loggerFactory;
        _logger = logger;
    }

    public void AttachToCall(ICall call, string botClientId)
    {
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
    }

    public async Task ProcessParticipantAudioAsync(uint sourceId, byte[] pcmChunk, long timestamp)
    {
        if (pcmChunk.Length == 0)
        {
            return;
        }

        var participant = _participantBySourceId.TryGetValue(sourceId, out var mapped)
            ? mapped
            : new ParticipantIdentity($"source:{sourceId}", $"Speaker {sourceId}");

        var stream = _streamsBySourceId.GetOrAdd(sourceId, _ =>
        {
            var s = new TranscribeStreamService(
                _settings,
                _aggregator,
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
            return;
        }

        _sourceIdsByUserId[userId.Trim()] = sourceIds;
        foreach (var sourceId in sourceIds)
        {
            _participantBySourceId[sourceId] = identityRecord;
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

        if (!_sourceIdsByUserId.TryRemove(userId.Trim(), out var sourceIds))
        {
            return;
        }

        foreach (var sourceId in sourceIds)
        {
            _participantBySourceId.TryRemove(sourceId, out _);
            if (_streamsBySourceId.TryRemove(sourceId, out var stream))
            {
                _ = stream.DisposeAsync();
            }
        }
    }

    private static List<uint> TryExtractSourceIds(Microsoft.Graph.Models.Participant? participant)
    {
        var list = new List<uint>();
        if (participant?.AdditionalData is null)
        {
            return list;
        }

        if (!participant.AdditionalData.TryGetValue("mediaStreams", out var msObj) || msObj is null)
        {
            return list;
        }

        if (msObj is JsonElement je && je.ValueKind == JsonValueKind.Array)
        {
            foreach (var stream in je.EnumerateArray())
            {
                if (stream.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }

                if (stream.TryGetProperty("sourceId", out var src))
                {
                    if (src.ValueKind == JsonValueKind.Number && src.TryGetUInt32(out var n))
                    {
                        list.Add(n);
                    }
                    else if (src.ValueKind == JsonValueKind.String &&
                             uint.TryParse(src.GetString(), out var s))
                    {
                        list.Add(s);
                    }
                }
            }
        }

        return list;
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
