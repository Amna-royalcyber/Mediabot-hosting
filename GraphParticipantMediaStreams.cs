using System.Globalization;
using System.Text.Json;
using Microsoft.Graph.Models;

namespace TeamsMediaBot;

/// <summary>Parses Teams/Graph <c>participant.resource.additionalData["mediaStreams"]</c> source ids (shared by router + roster).</summary>
internal static class GraphParticipantMediaStreams
{
    public static List<uint> ExtractSourceIds(Participant? participant)
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

        msObj = msObj switch
        {
            JsonDocument d => d.RootElement,
            _ => msObj
        };

        if (msObj is JsonElement je)
        {
            AddSourceIdsFromJsonElement(je, list);
            if (list.Count > 0)
            {
                return list;
            }

            // Sometimes the element stringifies to a JSON array Teams sent with PascalCase keys.
            if (je.ValueKind is JsonValueKind.Array or JsonValueKind.Object or JsonValueKind.String)
            {
                var raw = je.GetRawText();
                if (!string.IsNullOrWhiteSpace(raw))
                {
                    TryParseFromJson(raw, list);
                }
            }

            return list;
        }

        if (msObj is string str && TryParseFromJson(str, list))
        {
            return list;
        }

        var fallback = Convert.ToString(msObj, CultureInfo.InvariantCulture);
        if (!string.IsNullOrWhiteSpace(fallback))
        {
            var t = fallback.Trim();
            if (t.Length > 0 && (t[0] == '[' || t[0] == '{'))
            {
                TryParseFromJson(t, list);
            }
        }

        return list;
    }

    private static void AddSourceIdsFromJsonElement(JsonElement je, List<uint> list)
    {
        switch (je.ValueKind)
        {
            case JsonValueKind.Array:
                foreach (var stream in je.EnumerateArray())
                {
                    TryAddSourceIdFromStreamObject(stream, list);
                }

                return;
            case JsonValueKind.Object:
                TryAddSourceIdFromStreamObject(je, list);
                return;
            case JsonValueKind.String:
                var raw = je.GetString();
                if (!string.IsNullOrWhiteSpace(raw))
                {
                    TryParseFromJson(raw, list);
                }

                return;
            default:
                return;
        }
    }

    /// <summary>Teams may send <c>sourceId</c>, <c>SourceId</c>, or other casings.</summary>
    private static void TryAddSourceIdFromStreamObject(JsonElement stream, List<uint> list)
    {
        if (stream.ValueKind != JsonValueKind.Object)
        {
            return;
        }

        foreach (var prop in stream.EnumerateObject())
        {
            if (!prop.Name.Equals("sourceId", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var v = prop.Value;
            if (v.ValueKind == JsonValueKind.Number && v.TryGetUInt32(out var n))
            {
                list.Add(n);
            }
            else if (v.ValueKind == JsonValueKind.String && uint.TryParse(v.GetString(), out var s))
            {
                list.Add(s);
            }

            return;
        }
    }

    private static bool TryParseFromJson(string json, List<uint> list)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            if (root.ValueKind == JsonValueKind.Array)
            {
                foreach (var stream in root.EnumerateArray())
                {
                    TryAddSourceIdFromStreamObject(stream, list);
                }

                return list.Count > 0;
            }

            if (root.ValueKind == JsonValueKind.Object)
            {
                TryAddSourceIdFromStreamObject(root, list);
                return list.Count > 0;
            }

            return false;
        }
        catch
        {
            return false;
        }
    }
}
