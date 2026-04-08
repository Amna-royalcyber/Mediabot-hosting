using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Microsoft.Graph.Communications.Calls;
using Microsoft.Graph.Contracts;
using Microsoft.Graph.Communications.Calls.Media;
using Microsoft.Graph.Communications.Client;
using Microsoft.Graph.Communications.Resources;
using Microsoft.Graph.Models;

namespace TeamsMediaBot;

public sealed class CallHandler
{
    private readonly BotSettings _settings;
    private readonly MeetingParticipantService _meetingParticipants;
    private readonly TranscriptionManager _transcriptionManager;
    private readonly ParticipantAudioRouter _participantAudioRouter;
    private readonly MeetingContextStore _meetingContext;
    private readonly ILogger<CallHandler> _logger;
    private ICommunicationsClient? _communicationsClient;

    public CallHandler(
        BotSettings settings,
        MeetingParticipantService meetingParticipants,
        TranscriptionManager transcriptionManager,
        ParticipantAudioRouter participantAudioRouter,
        MeetingContextStore meetingContext,
        ILogger<CallHandler> logger)
    {
        _settings = settings;
        _meetingParticipants = meetingParticipants;
        _transcriptionManager = transcriptionManager;
        _participantAudioRouter = participantAudioRouter;
        _meetingContext = meetingContext;
        _logger = logger;
    }

    public void Initialize(ICommunicationsClient communicationsClient)
    {
        _communicationsClient = communicationsClient;
        _communicationsClient.Calls().OnIncoming += OnIncomingCall;
    }

    public async Task<ICall> JoinMeetingByUrlAsync(string joinUrl, MediaHandler mediaHandler)
    {
        var (chatInfo, meetingInfo, normalizedUrl, organizerObjectId, meetingContextTenantId) =
            CreateJoinInfoFromUrl(joinUrl, _logger, _settings.TenantId);

        var joinTenantId = ResolveMeetingTenantId(meetingContextTenantId, _settings.TenantId, _logger);
        return await SubmitJoinAsync(
            chatInfo,
            meetingInfo,
            joinTenantId,
            organizerObjectId,
            normalizedUrl,
            mediaHandler);
    }

    /// <summary>
    /// Join when you already have thread id + organizer (e.g. from calendar/Graph) without a meetup-join URL.
    /// </summary>
    public async Task<ICall> JoinMeetingByCoordinatesAsync(
        string chatThreadId,
        string chatMessageId,
        string organizerObjectId,
        string meetingTenantId,
        MediaHandler mediaHandler,
        string? replyChainMessageId = null)
    {
        if (string.IsNullOrWhiteSpace(chatThreadId))
        {
            throw new ArgumentException("Chat thread id is required.", nameof(chatThreadId));
        }

        if (string.IsNullOrWhiteSpace(organizerObjectId))
        {
            throw new ArgumentException("Organizer object id is required.", nameof(organizerObjectId));
        }

        var chatInfo = new ChatInfo
        {
            ThreadId = chatThreadId.Trim(),
            MessageId = string.IsNullOrWhiteSpace(chatMessageId) ? "0" : chatMessageId.Trim()
        };

        if (!string.IsNullOrWhiteSpace(replyChainMessageId))
        {
            chatInfo.ReplyChainMessageId = replyChainMessageId.Trim();
        }

        var meetingInfo = BuildOrganizerMeetingInfo(organizerObjectId.Trim(), meetingTenantId.Trim());
        var joinTenantId = ResolveMeetingTenantId(meetingTenantId, _settings.TenantId, _logger);

        _logger.LogInformation(
            "Join by coordinates: threadId={ThreadId}, messageId={MessageId}, organizerOid={OrganizerOid}, joinTenantId={JoinTenantId}",
            chatInfo.ThreadId,
            chatInfo.MessageId,
            organizerObjectId,
            joinTenantId);

        return await SubmitJoinAsync(
            chatInfo,
            meetingInfo,
            joinTenantId,
            organizerObjectId,
            normalizedUrl: null,
            mediaHandler);
    }

    private async Task<ICall> SubmitJoinAsync(
        ChatInfo chatInfo,
        OrganizerMeetingInfo meetingInfo,
        string joinTenantId,
        string organizerObjectId,
        string? normalizedUrl,
        MediaHandler mediaHandler)
    {
        if (_communicationsClient is null)
        {
            throw new InvalidOperationException("Communications client has not been initialized.");
        }

        _logger.LogInformation("Joining with TenantId={TenantId}, Organizer={Organizer}", joinTenantId, organizerObjectId);

        // Align with Microsoft Graph comms samples (e.g. HueBot JoinCallAsync): 3-arg ctor + explicit scenario id.
        var mediaSession = mediaHandler.CreateMediaSession(_communicationsClient);
        var scenarioId = Guid.NewGuid();
        var joinParams = new JoinMeetingParameters(chatInfo, meetingInfo, mediaSession)
        {
            TenantId = joinTenantId,
            IsInteractiveRosterEnabled = true,
            IsParticipantInfoUpdatesEnabled = true,
            OptIntoDeltaRoster = true,
            AllowGuestToBypassLobby = true,
        };

        if (!string.IsNullOrWhiteSpace(_settings.JoinMeetingSubject))
        {
            joinParams.Subject = _settings.JoinMeetingSubject.Trim();
        }

        ICall call;
        try
        {
            call = await _communicationsClient.Calls().AddAsync(joinParams, scenarioId);
        }
        catch (Microsoft.Graph.Communications.Core.Exceptions.ServiceException ex)
        {
            var detail =
                $"ThreadId={chatInfo.ThreadId}; MessageId={chatInfo.MessageId}; ReplyChainMessageId={chatInfo.ReplyChainMessageId}; OrganizerOid={organizerObjectId}; NormalizedUrl={normalizedUrl}";
            if (ex.Message.Contains("source identity", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    $"Graph rejected the bot as a calling identity (403). {detail}; JoinTenantId={joinTenantId}; BotClientId={_settings.ClientId}. " +
                    "Teams: ensure Application access policies include this ClientId for the meeting organizer's user: " +
                    "resolve OrganizerOid in Entra, then Get-CsOnlineUser -Identity <organizer UPN> | Select ApplicationAccessPolicy; " +
                    "if a Tag: policy is shown, add this ClientId to that policy's AppIds (Set-CsApplicationAccessPolicy -AppIds @{Add='...'}). " +
                    "Also verify Global/TeamsBotPolicy and Grant-CsApplicationAccessPolicy -Global as needed.",
                    ex);
            }

            throw new InvalidOperationException(
                $"Graph join failed (likely meeting/thread not resolvable). {detail}",
                ex);
        }

        call.OnUpdated += (_, args) =>
        {
            var r = args.NewResource;
            var ri = r?.ResultInfo;
            _logger.LogInformation(
                "Call updated. State={State}, Direction={Direction}, ResultCode={Code}, Subcode={Subcode}, Message={Message}",
                r?.State,
                r?.Direction,
                ri?.Code,
                ri?.Subcode,
                ri?.Message);

            if (r?.State?.ToString() == "Established")
            {
                _logger.LogInformation(
                    "Call established. MediaHandler is receiving unmixed participant audio; speaking participants should produce per-source audio frames.");
            }
        };

        _meetingParticipants.AttachToCall(call, _settings.ClientId);
        _transcriptionManager.AttachToCall(call, _settings.ClientId);
        _participantAudioRouter.AttachToCall(call, _settings.ClientId);
        _meetingContext.SetMeetingId(call.Id);

        _logger.LogInformation("Join request submitted. Call ID: {CallId}, ScenarioId={ScenarioId}", call.Id, scenarioId);
        return call;
    }

    private void OnIncomingCall(ICallCollection _, CollectionEventArgs<ICall> args)
    {
        foreach (ICall incomingCall in args.AddedResources)
        {
            _logger.LogInformation("Incoming call received. Call ID: {CallId}", incomingCall.Id);
        }
    }

    /// <summary>
    /// Parses a Teams meeting join URL (meetup-join) into ChatInfo / MeetingInfo.
    /// ThreadId must be the thread segment (e.g. 19:meeting_...@thread.v2), not the full URL — otherwise Graph returns 404 NotFound.
    /// </summary>
    private static string ResolveMeetingTenantId(string? meetingContextTenantId, string configuredTenantId, ILogger logger)
    {
        if (string.IsNullOrWhiteSpace(meetingContextTenantId))
        {
            logger.LogInformation("Join: no context Tid in URL; using configured tenant {TenantId}.", configuredTenantId);
            return configuredTenantId;
        }

        if (!Guid.TryParse(meetingContextTenantId, out _))
        {
            logger.LogWarning("Join: context Tid is not a valid GUID; using configured tenant.");
            return configuredTenantId;
        }

        if (!string.Equals(meetingContextTenantId, configuredTenantId, StringComparison.OrdinalIgnoreCase))
        {
            logger.LogWarning(
                "Join: meeting context Tid {MeetingTid} differs from configured tenant {ConfiguredTenantId}. Using meeting Tid for JoinMeetingParameters.",
                meetingContextTenantId,
                configuredTenantId);
        }

        return meetingContextTenantId;
    }

    private static OrganizerMeetingInfo BuildOrganizerMeetingInfo(string organizerObjectId, string? tenantIdForOrganizer)
    {
        var user = new Identity { Id = organizerObjectId };
        if (!string.IsNullOrWhiteSpace(tenantIdForOrganizer))
        {
            // Same as Sample.Common JoinInfo.ParseJoinURL — binds organizer to the meeting tenant for Graph.
            user.SetTenantId(tenantIdForOrganizer.Trim());
        }

        var meetingInfo = new OrganizerMeetingInfo
        {
            Organizer = new IdentitySet { User = user }
        };

        meetingInfo.AdditionalData = new Dictionary<string, object>
        {
            ["allowConversationWithoutHost"] = true
        };

        return meetingInfo;
    }

    private static (ChatInfo ChatInfo, OrganizerMeetingInfo MeetingInfo, string NormalizedUrl, string OrganizerObjectId, string? MeetingContextTenantId) CreateJoinInfoFromUrl(
        string joinUrl,
        ILogger logger,
        string tenantId)
    {
        if (string.IsNullOrWhiteSpace(joinUrl))
        {
            throw new ArgumentException("Join URL is empty.", nameof(joinUrl));
        }

        var normalized = NormalizeTeamsJoinUrl(joinUrl.Trim(), logger);
        var uri = new Uri(normalized);

        if (!TryExtractTeamsThreadAndMessage(uri, out var threadId, out var messageId))
        {
            throw new ArgumentException(
                "Could not parse this Teams link. Use one of: " +
                "(1) Meet now / calendar join: …/l/meetup-join/19%3A…/0?… " +
                "(2) Meeting chat link: …/l/chat/19:meeting_…@thread.v2/conversations?… " +
                "Launcher links (launcher.html?url=…) are unwrapped automatically.",
                nameof(joinUrl));
        }

        var chatInfo = new ChatInfo
        {
            ThreadId = threadId,
            MessageId = messageId
        };

        TryParseTeamsJoinContext(uri, out var contextTid, out var organizerObjectId, out var replyChainMessageId);

        if (string.IsNullOrWhiteSpace(organizerObjectId))
        {
            throw new ArgumentException(
                "This link does not include the meeting organizer id (Oid). " +
                "Use the calendar join link: open the meeting in Outlook or Teams → \"Copy join link\" → paste the full URL " +
                "(it must be a …/meetup-join/… URL whose query string contains context=… with Oid). " +
                "Meeting chat links (…/l/chat/…/conversations) usually cannot be used to join via the Calling API.",
                nameof(joinUrl));
        }

        if (!string.IsNullOrWhiteSpace(replyChainMessageId))
        {
            chatInfo.ReplyChainMessageId = replyChainMessageId;
        }

        var meetingInfo = BuildOrganizerMeetingInfo(organizerObjectId, contextTid);

        logger.LogInformation(
            "Join parsed: normalizedUrl={NormalizedUrl}, threadId={ThreadId}, messageId={MessageId}, replyChainMessageId={ReplyChainMessageId}, organizerOid={OrganizerOid}, contextTid={ContextTid}, appTenantId={AppTenantId}",
            normalized,
            chatInfo.ThreadId,
            chatInfo.MessageId,
            chatInfo.ReplyChainMessageId,
            organizerObjectId,
            contextTid,
            tenantId);

        return (chatInfo, meetingInfo, normalized, organizerObjectId, contextTid);
    }

    /// <summary>
    /// Parses <c>?context=</c> JSON (Tid, Oid, optional MessageId for reply chain) per Microsoft Graph comms JoinInfo sample.
    /// </summary>
    private static void TryParseTeamsJoinContext(
        Uri uri,
        out string? contextTid,
        out string? organizerObjectId,
        out string? replyChainMessageId)
    {
        contextTid = null;
        organizerObjectId = null;
        replyChainMessageId = null;

        var raw = GetQueryParameter(uri.Query, "context");
        if (string.IsNullOrEmpty(raw))
        {
            return;
        }

        var current = raw;
        for (var i = 0; i < 3; i++)
        {
            string decoded;
            try
            {
                decoded = i == 0 ? current : Uri.UnescapeDataString(current);
            }
            catch
            {
                return;
            }

            try
            {
                using var doc = JsonDocument.Parse(decoded);
                var root = doc.RootElement;
                if (root.TryGetProperty("Tid", out var tid) && tid.ValueKind == JsonValueKind.String)
                {
                    contextTid = tid.GetString();
                }
                else if (root.TryGetProperty("tid", out var tidLower) && tidLower.ValueKind == JsonValueKind.String)
                {
                    contextTid = tidLower.GetString();
                }

                if (root.TryGetProperty("Oid", out var oid) && oid.ValueKind == JsonValueKind.String)
                {
                    organizerObjectId = oid.GetString();
                }
                else if (root.TryGetProperty("oid", out var oidLower) && oidLower.ValueKind == JsonValueKind.String)
                {
                    organizerObjectId = oidLower.GetString();
                }

                if (root.TryGetProperty("MessageId", out var mid) && mid.ValueKind == JsonValueKind.String)
                {
                    replyChainMessageId = mid.GetString();
                }
                else if (root.TryGetProperty("messageId", out var mid2) && mid2.ValueKind == JsonValueKind.String)
                {
                    replyChainMessageId = mid2.GetString();
                }

                return;
            }
            catch (JsonException)
            {
                current = decoded;
            }
        }
    }

    /// <summary>
    /// Unwraps Outlook Safe Links (<c>*.safelinks.protection.outlook.com?url=...</c>) and Teams
    /// <c>launcher.html?url=...</c> so we get a direct <c>teams.microsoft.com/.../meetup-join/...</c> URL with <c>context=</c>.
    /// </summary>
    private static string NormalizeTeamsJoinUrl(string joinUrl, ILogger logger)
    {
        if (!Uri.TryCreate(joinUrl, UriKind.Absolute, out var uri))
        {
            throw new ArgumentException("Join URL must be a valid absolute https URL.", nameof(joinUrl));
        }

        if (uri.Scheme != Uri.UriSchemeHttps)
        {
            throw new ArgumentException("Join URL must use https.", nameof(joinUrl));
        }

        var current = joinUrl;
        for (var i = 0; i < 5; i++)
        {
            if (!Uri.TryCreate(current, UriKind.Absolute, out uri))
            {
                break;
            }

            var innerEncoded = GetQueryParameter(uri.Query, "url");
            if (string.IsNullOrEmpty(innerEncoded))
            {
                break;
            }

            var isWrapper =
                uri.Host.Contains("safelinks", StringComparison.OrdinalIgnoreCase) ||
                uri.AbsolutePath.Contains("launcher", StringComparison.OrdinalIgnoreCase);
            if (!isWrapper)
            {
                break;
            }

            var inner = Uri.UnescapeDataString(innerEncoded);
            if (inner.StartsWith('/'))
            {
                current = $"{uri.Scheme}://{uri.Host}{inner}";
            }
            else if (inner.StartsWith("https://", StringComparison.OrdinalIgnoreCase) ||
                     inner.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
            {
                current = inner.Replace("http://", "https://", StringComparison.OrdinalIgnoreCase);
            }
            else
            {
                current = $"{uri.Scheme}://{uri.Host}/{inner.TrimStart('/')}";
            }

            logger.LogInformation("Unwrapped wrapper URL (Outlook Safe Links or Teams launcher) to Teams meeting link.");
        }

        return current;
    }

    private static string? GetQueryParameter(string query, string key)
    {
        if (string.IsNullOrEmpty(query) || query[0] == '?')
        {
            query = query.TrimStart('?');
        }

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

            return part[(eq + 1)..];
        }

        return null;
    }

    /// <summary>
    /// Supports meetup-join links and meeting chat links (/l/chat/…/conversations).
    /// </summary>
    private static bool TryExtractTeamsThreadAndMessage(Uri uri, out string threadId, out string messageId)
    {
        threadId = null!;
        messageId = null!;

        // Standard join: …/meetup-join/{thread}/{messageId}/…
        var meetupMatch = MeetupJoinRegex.Match(uri.AbsolutePath);
        if (meetupMatch.Success)
        {
            threadId = FullyUnescape(meetupMatch.Groups[1].Value);
            messageId = FullyUnescape(meetupMatch.Groups[2].Value);
            return true;
        }

        // Meeting chat thread: …/l/chat/19:meeting_…@thread.v2/conversations — use message "0" for join.
        var chatMatch = ChatMeetingRegex.Match(uri.AbsolutePath);
        if (chatMatch.Success)
        {
            threadId = FullyUnescape(chatMatch.Groups[1].Value);
            messageId = "0";
            return true;
        }

        var segments = uri.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var meetupIdx = Array.FindIndex(
            segments,
            s => s.Equals("meetup-join", StringComparison.OrdinalIgnoreCase));

        if (meetupIdx >= 0 && meetupIdx + 2 < segments.Length)
        {
            threadId = FullyUnescape(segments[meetupIdx + 1]);
            messageId = FullyUnescape(segments[meetupIdx + 2]);
            return true;
        }

        var chatIdx = Array.FindIndex(
            segments,
            s => s.Equals("chat", StringComparison.OrdinalIgnoreCase));
        if (chatIdx >= 0 && chatIdx + 2 < segments.Length &&
            segments[chatIdx + 2].Equals("conversations", StringComparison.OrdinalIgnoreCase))
        {
            threadId = FullyUnescape(segments[chatIdx + 1]);
            messageId = "0";
            return true;
        }

        return false;
    }

    private static string FullyUnescape(string value)
    {
        // Some wrapper links double-encode path segments (e.g. %253a => %3a).
        // Unescape multiple times so the final output matches what Graph expects.
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

    private static readonly Regex MeetupJoinRegex = new(
        @"meetup-join/([^/?#]+)/([^/?#]+)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly Regex ChatMeetingRegex = new(
        @"chat/([^/]+)/conversations",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
}
