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
                return list.Distinct().ToList();
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

            if (list.Count > 0)
            {
                return list.Distinct().ToList();
            }

            // Ultimate fallback: recursively search all AdditionalData for keys named sourceId.
            ScanAdditionalDataForSourceIds(participant.AdditionalData, list);
            return list.Distinct().ToList();
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

        if (list.Count == 0)
        {
            ScanAdditionalDataForSourceIds(participant.AdditionalData, list);
        }

        return list.Distinct().ToList();
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

    private static void ScanAdditionalDataForSourceIds(IDictionary<string, object> data, List<uint> list)
    {
        foreach (var kv in data)
        {
            if (string.Equals(kv.Key, "sourceId", StringComparison.OrdinalIgnoreCase))
            {
                TryAddSourceIdFromUnknownValue(kv.Value, list);
            }

            ScanUnknownForSourceIds(kv.Value, list);
        }
    }

    private static void ScanUnknownForSourceIds(object? value, List<uint> list)
    {
        if (value is null)
        {
            return;
        }

        if (value is JsonDocument jd)
        {
            ScanJsonElementForSourceIds(jd.RootElement, list);
            return;
        }

        if (value is JsonElement je)
        {
            ScanJsonElementForSourceIds(je, list);
            return;
        }

        if (value is IDictionary<string, object> dict)
        {
            ScanAdditionalDataForSourceIds(dict, list);
            return;
        }

        if (value is IEnumerable<object> enumerable)
        {
            foreach (var item in enumerable)
            {
                ScanUnknownForSourceIds(item, list);
            }

            return;
        }

        if (value is string s)
        {
            var t = s.Trim();
            if (t.Length > 0 && (t[0] == '[' || t[0] == '{'))
            {
                TryParseFromJson(t, list);
            }
        }
    }

    private static void ScanJsonElementForSourceIds(JsonElement element, List<uint> list)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var prop in element.EnumerateObject())
                {
                    if (string.Equals(prop.Name, "sourceId", StringComparison.OrdinalIgnoreCase))
                    {
                        TryAddSourceIdFromJsonValue(prop.Value, list);
                    }

                    ScanJsonElementForSourceIds(prop.Value, list);
                }

                return;
            case JsonValueKind.Array:
                foreach (var item in element.EnumerateArray())
                {
                    ScanJsonElementForSourceIds(item, list);
                }

                return;
            case JsonValueKind.String:
                var raw = element.GetString();
                if (!string.IsNullOrWhiteSpace(raw))
                {
                    var t = raw.Trim();
                    if (t.Length > 0 && (t[0] == '[' || t[0] == '{'))
                    {
                        TryParseFromJson(t, list);
                    }
                }

                return;
            default:
                return;
        }
    }

    private static void TryAddSourceIdFromUnknownValue(object? value, List<uint> list)
    {
        if (value is null)
        {
            return;
        }

        switch (value)
        {
            case uint u:
                list.Add(u);
                return;
            case int i when i > 0:
                list.Add((uint)i);
                return;
            case long l when l > 0 && l <= uint.MaxValue:
                list.Add((uint)l);
                return;
            case JsonElement je:
                TryAddSourceIdFromJsonValue(je, list);
                return;
            default:
                if (uint.TryParse(Convert.ToString(value, CultureInfo.InvariantCulture), out var parsed))
                {
                    list.Add(parsed);
                }

                return;
        }
    }

    private static void TryAddSourceIdFromJsonValue(JsonElement value, List<uint> list)
    {
        if (value.ValueKind == JsonValueKind.Number && value.TryGetUInt32(out var n))
        {
            list.Add(n);
            return;
        }

        if (value.ValueKind == JsonValueKind.String && uint.TryParse(value.GetString(), out var s))
        {
            list.Add(s);
        }
    }
}
