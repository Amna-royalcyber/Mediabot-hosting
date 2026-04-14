namespace TeamsMediaBot;

public interface IParticipantManager
{
    bool HasBinding(uint sourceId);
    bool HasEntraOidForSource(uint sourceId);
    bool TryGetBinding(uint sourceId, out ParticipantBinding? binding);
    bool TryResolveUserFromAudioStream(uint sourceId, out string userId);
    string GetTranscriptSpeakerLabel(uint sourceId);
    string GetEntraOidForTranscript(uint sourceId);
    string GetEntraObjectIdForTranscriptPayload(string participantId);
    string? GetCanonicalDisplayName(string participantId);
    void RegisterParticipant(string participantId, string displayName, DateTime joinTimestampUtc);
    string? TryBindAudioSource(uint sourceId, string? participantIdOrEntraFromGraph, string displayName, string reason);
    IReadOnlyList<uint> GetUnresolvedSourceIds();
    bool TryGetSourceIdForIdentity(string entraOid, out uint sourceId);
    HashSet<string> GetParticipantIdsWithAudioSourceBindings();
}

public interface IChunkManager
{
    Task RecordFinalAsync(
        DateTime utteranceUtc,
        string participantId,
        string speakerName,
        string text,
        string dedupeKey,
        uint? sourceStreamId = null,
        CancellationToken cancellationToken = default);
}
