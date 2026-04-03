using System.Text.RegularExpressions;

namespace TeamsMediaBot;

/// <summary>
/// Lightweight join URL parsing aligned with transcriber-style APIs (thread id + optional passcode from query).
/// Full Graph join coordinates still come from <see cref="CallHandler"/> when using <c>MeetingJoinUrl</c>.
/// </summary>
public static class MeetingJoinParser
{
    private static readonly Regex MeetupJoinPath = new(
        @"meetup-join/([^/?#]+)/([^/?#]+)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>
    /// Extracts meeting thread id from a meetup-join path and optional meeting passcode (<c>p</c> / <c>pwd</c> query).
    /// </summary>
    public static MeetingJoinUrlParts ParseJoinUrl(string? joinUrl)
    {
        if (string.IsNullOrWhiteSpace(joinUrl))
        {
            return new MeetingJoinUrlParts(null, null);
        }

        var trimmed = joinUrl.Trim();
        string? threadId = null;
        var pathMatch = MeetupJoinPath.Match(trimmed);
        if (pathMatch.Success)
        {
            threadId = FullyUnescape(pathMatch.Groups[1].Value);
        }

        string? passcode = null;
        if (Uri.TryCreate(trimmed, UriKind.Absolute, out var uri))
        {
            passcode = GetQueryParameter(uri.Query, "p")
                ?? GetQueryParameter(uri.Query, "pwd")
                ?? GetQueryParameter(uri.Query, "password");
        }

        return new MeetingJoinUrlParts(threadId, passcode);
    }

    private static string? GetQueryParameter(string query, string key)
    {
        if (string.IsNullOrEmpty(query))
        {
            return null;
        }

        query = query.TrimStart('?');
        foreach (var part in query.Split('&'))
        {
            var eq = part.IndexOf('=');
            if (eq <= 0)
            {
                continue;
            }

            var name = part[..eq];
            if (!name.Equals(key, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            try
            {
                return Uri.UnescapeDataString(part[(eq + 1)..]);
            }
            catch
            {
                return part[(eq + 1)..];
            }
        }

        return null;
    }

    private static string FullyUnescape(string value)
    {
        var current = value;
        for (var i = 0; i < 3; i++)
        {
            var decoded = Uri.UnescapeDataString(current);
            if (decoded == current)
            {
                break;
            }

            current = decoded;
        }

        return current;
    }
}

public readonly record struct MeetingJoinUrlParts(string? JoinMeetingId, string? Passcode);
