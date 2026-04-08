namespace TeamsMediaBot;

public sealed class MeetingContextStore
{
    private readonly object _lock = new();
    private string _meetingId = "unknown";

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
}
