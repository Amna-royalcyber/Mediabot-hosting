using System.Collections.Concurrent;

namespace TeamsMediaBot;

public sealed class TranscriptBuffer
{
    private readonly ConcurrentDictionary<uint, ConcurrentQueue<TranscriptFragment>> _pendingBySource = new();

    public void Buffer(TranscriptFragment fragment)
    {
        if (fragment.SourceStreamId is not uint sid)
        {
            return;
        }

        var queue = _pendingBySource.GetOrAdd(sid, _ => new ConcurrentQueue<TranscriptFragment>());
        queue.Enqueue(fragment);
    }

    public IReadOnlyList<TranscriptFragment> DrainResolved(IParticipantManager participantManager)
    {
        var flushed = new List<TranscriptFragment>();
        foreach (var kv in _pendingBySource.ToArray())
        {
            var sid = kv.Key;
            if (!participantManager.TryGetBinding(sid, out var binding) || binding is null || binding.State != IdentityState.Resolved)
            {
                continue;
            }

            if (_pendingBySource.TryRemove(sid, out var queue))
            {
                while (queue.TryDequeue(out var item))
                {
                    flushed.Add(item);
                }
            }
        }

        return flushed;
    }

    public IReadOnlyList<TranscriptFragment> ResolvePending(uint sourceId, IParticipantManager participantManager)
    {
        var flushed = new List<TranscriptFragment>();
        if (!participantManager.TryGetBinding(sourceId, out var binding) || binding is null || binding.State != IdentityState.Resolved)
        {
            return flushed;
        }

        if (_pendingBySource.TryRemove(sourceId, out var queue))
        {
            while (queue.TryDequeue(out var item))
            {
                flushed.Add(item);
            }
        }

        return flushed;
    }
}
