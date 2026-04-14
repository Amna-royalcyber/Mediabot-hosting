using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace TeamsMediaBot;

public sealed class TranscriptDeduplicator
{
    private readonly ConcurrentDictionary<string, DateTime> _recent = new(StringComparer.Ordinal);
    private static readonly TimeSpan Window = TimeSpan.FromSeconds(5);

    public bool ShouldPass(uint? sourceId, string text, DateTime emittedAtUtc)
    {
        var normalized = text.Trim();
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return false;
        }

        var sourceKey = sourceId?.ToString() ?? "mixed";
        var key = $"{sourceKey}:{ComputeHash(normalized)}";
        if (_recent.TryGetValue(key, out var seenAt) && (emittedAtUtc - seenAt) <= Window)
        {
            return false;
        }

        _recent[key] = emittedAtUtc;
        Cleanup(emittedAtUtc);
        return true;
    }

    private void Cleanup(DateTime nowUtc)
    {
        foreach (var kv in _recent.ToArray())
        {
            if ((nowUtc - kv.Value) > Window.Add(Window))
            {
                _recent.TryRemove(kv.Key, out _);
            }
        }
    }

    private static string ComputeHash(string text)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(text));
        return Convert.ToHexString(bytes);
    }
}
