namespace TeamsMediaBot;

public sealed class MeetingContextStore
{
    private readonly object _lock = new();
    private string _meetingId = "unknown";
    private DateTime? _callEstablishedUtc;

    public string CurrentMeetingId
    {
        get
        {
            lock (_lock)
            {
                return _meetingId;
            }
        }
    }

    /// <summary>Wall-clock time when Graph reports the call established (used for 3-minute transcript windows).</summary>
    public DateTime? CallEstablishedUtc
    {
        get
        {
            lock (_lock)
            {
                return _callEstablishedUtc;
            }
        }
    }

    public void SetMeetingId(string? meetingId)
    {
        if (string.IsNullOrWhiteSpace(meetingId))
        {
            return;
        }

        lock (_lock)
        {
            _meetingId = meetingId.Trim();
        }
    }

    public void SetCallEstablishedUtc(DateTime utc)
    {
        var normalized = utc.Kind == DateTimeKind.Utc ? utc : utc.ToUniversalTime();
        lock (_lock)
        {
            _callEstablishedUtc = normalized;
        }
    }

    public void ResetMeetingContext()
    {
        lock (_lock)
        {
            _meetingId = "unknown";
            _callEstablishedUtc = null;
        }
    }
}
