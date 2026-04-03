namespace TeamsMediaBot;

/// <summary>
/// Join request body compatible with transcriber-style <c>POST /api/meetings/join</c> controllers.
/// </summary>
public sealed class JoinMeetingRequest
{
    /// <summary>Optional correlation id for transcripts (this bot does not resolve Graph meetings by id alone).</summary>
    public string? MeetingId { get; set; }

    /// <summary>Teams meetup-join or meeting link (preferred).</summary>
    public string? MeetingJoinUrl { get; set; }

    public string? Passcode { get; set; }

    /// <summary>When not using a full join URL: meeting thread id (e.g. <c>19:meeting_...@thread.v2</c>).</summary>
    public string? ChatThreadId { get; set; }

    public string? ChatMessageId { get; set; }

    /// <summary>Required with <see cref="ChatThreadId"/> when <see cref="MeetingJoinUrl"/> is omitted.</summary>
    public string? OrganizerObjectId { get; set; }

    /// <summary>Meeting tenant; defaults to bot <c>AzureAd:TenantId</c> when omitted.</summary>
    public string? MeetingTenantId { get; set; }
}
