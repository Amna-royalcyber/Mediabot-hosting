using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Graph.Communications.Calls;
using Microsoft.Graph.Communications.Resources;
using Microsoft.Skype.Bots.Media;

namespace TeamsMediaBot;

public sealed class ParticipantAudioRouter
{
    private readonly AudioProcessor _audioProcessor;
    private readonly AwsTranscribeService _awsTranscribeService;
    private readonly MeetingParticipantService _meetingParticipants;
    private readonly ILogger<ParticipantAudioRouter> _logger;
    private readonly ConcurrentDictionary<uint, ParticipantAudioBinding> _bindingBySourceId = new();

    public ParticipantAudioRouter(
        AudioProcessor audioProcessor,
        AwsTranscribeService awsTranscribeService,
        MeetingParticipantService meetingParticipants,
        ILogger<ParticipantAudioRouter> logger)
    {
        _audioProcessor = audioProcessor;
        _awsTranscribeService = awsTranscribeService;
        _meetingParticipants = meetingParticipants;
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

        // Roster may already contain participants before delta events; hydrate bindings immediately.
        TryHydrateFromCurrentRoster(call, botClientId);
    }

    private void TryHydrateFromCurrentRoster(ICall call, string botClientId)
    {
        try
        {
            foreach (var p in call.Participants)
            {
                UpsertParticipantMappings(p, botClientId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not hydrate participant source bindings from current roster.");
        }
    }

    public async Task HandleAudioAsync(AudioMediaReceivedEventArgs args)
    {
        var unmixed = args.Buffer.UnmixedAudioBuffers;
        if (unmixed is null || !unmixed.Any())
        {
            return;
        }

        foreach (var ub in unmixed)
        {
            var sourceId = Convert.ToUInt32(ub.ActiveSpeakerId);
            if (sourceId == (uint)DominantSpeakerChangedEventArgs.None)
            {
                continue;
            }

            if (!_bindingBySourceId.TryGetValue(sourceId, out var binding))
            {
                if (TryBindSingleRosterParticipant(sourceId, out var fallback) && fallback is not null)
                {
                    binding = fallback;
                    _logger.LogWarning(
                        "Mapped unmapped sourceId {SourceId} to sole roster participant {DisplayName} ({ParticipantId}).",
                        sourceId,
                        binding.DisplayName,
                        binding.ParticipantId);
                }
                else
                {
                    _logger.LogWarning(
                        "No participant binding for sourceId {SourceId}. Ensure Graph participant mediaStreams includes sourceId, or add more participants to roster.",
                        sourceId);
                    continue;
                }
            }

            var payload = CopyUnmixedBuffer(ub.Data, ub.Length);
            if (payload.Length == 0)
            {
                continue;
            }

            var pcm = _audioProcessor.ConvertToPcm(new AudioFrame(
                Data: payload,
                Timestamp: ub.OriginalSenderTimestamp,
                Length: (int)ub.Length,
                Format: AudioFormat.Pcm16K));

            if (pcm.Length == 0)
            {
                continue;
            }

            _logger.LogDebug("Audio received from {ParticipantName} ({ParticipantId}).", binding.DisplayName, binding.ParticipantId);
            await _awsTranscribeService.SendAudioChunkAsync(
                binding.ParticipantId,
                binding.DisplayName,
                pcm,
                ub.OriginalSenderTimestamp);
        }
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

        var participantId = identity?.User?.Id;
        if (string.IsNullOrWhiteSpace(participantId))
        {
            return;
        }

        var displayName = identity?.User?.DisplayName;
        if (string.IsNullOrWhiteSpace(displayName))
        {
            displayName = participantId;
        }

        foreach (var sourceId in TryExtractSourceIds(resource))
        {
            var binding = new ParticipantAudioBinding(participantId.Trim(), displayName.Trim());
            _bindingBySourceId[sourceId] = binding;
            _awsTranscribeService.UpsertParticipant(binding.ParticipantId, binding.DisplayName);
            _logger.LogInformation("Bound sourceId {SourceId} -> {DisplayName} ({ParticipantId}).", sourceId, binding.DisplayName, binding.ParticipantId);
        }
    }

    private void RemoveParticipantMappings(IParticipant participant)
    {
        var participantId = participant.Resource?.Info?.Identity?.User?.Id;
        if (string.IsNullOrWhiteSpace(participantId))
        {
            return;
        }

        foreach (var kvp in _bindingBySourceId.Where(k => string.Equals(k.Value.ParticipantId, participantId.Trim(), StringComparison.OrdinalIgnoreCase)).ToList())
        {
            _bindingBySourceId.TryRemove(kvp.Key, out _);
        }

        _awsTranscribeService.RemoveParticipant(participantId.Trim());
    }

    /// <summary>
    /// When Teams does not expose mediaStreams.sourceId in callbacks, unmixed audio still uses a source id.
    /// If exactly one human is in the roster, bind that speaker (typical 1:1 test call).
    /// </summary>
    private bool TryBindSingleRosterParticipant(uint sourceId, out ParticipantAudioBinding? binding)
    {
        binding = null;
        var roster = _meetingParticipants.GetRosterSnapshot();
        if (roster.Count != 1)
        {
            return false;
        }

        var single = roster[0];
        binding = new ParticipantAudioBinding(single.AzureAdObjectId, single.DisplayName);
        _bindingBySourceId[sourceId] = binding;
        _awsTranscribeService.UpsertParticipant(binding.ParticipantId, binding.DisplayName);
        return true;
    }

    private static List<uint> TryExtractSourceIds(Microsoft.Graph.Models.Participant? participant)
    {
        var list = new List<uint>();
        if (participant?.AdditionalData is null)
        {
            return list;
        }

        object? msObj = null;
        foreach (var kvp in participant.AdditionalData)
        {
            if (string.Equals(kvp.Key, "mediaStreams", StringComparison.OrdinalIgnoreCase))
            {
                msObj = kvp.Value;
                break;
            }
        }

        if (msObj is null)
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
        else if (msObj is JsonElement js && js.ValueKind == JsonValueKind.String)
        {
            var raw = js.GetString();
            if (!string.IsNullOrWhiteSpace(raw) && TryParseFromJson(raw, list))
            {
                return list;
            }
        }
        else if (msObj is string str && TryParseFromJson(str, list))
        {
            return list;
        }

        return list;
    }

    private static bool TryParseFromJson(string json, List<uint> list)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind != JsonValueKind.Array)
            {
                return false;
            }

            foreach (var stream in doc.RootElement.EnumerateArray())
            {
                if (stream.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }

                if (!stream.TryGetProperty("sourceId", out var src))
                {
                    continue;
                }

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

            return list.Count > 0;
        }
        catch
        {
            return false;
        }
    }

    private static byte[] CopyUnmixedBuffer(IntPtr ptr, long length)
    {
        if (ptr == IntPtr.Zero || length <= 0 || length > int.MaxValue)
        {
            return Array.Empty<byte>();
        }

        var bytes = new byte[(int)length];
        Marshal.Copy(ptr, bytes, 0, (int)length);
        return bytes;
    }

    private sealed record ParticipantAudioBinding(string ParticipantId, string DisplayName);
}
