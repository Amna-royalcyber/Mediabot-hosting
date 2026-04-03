using Azure.Core;
using Azure.Identity;
using Microsoft.Extensions.Logging;
using Microsoft.Graph.Communications.Calls;
using Microsoft.Graph.Communications.Calls.Media;
using Microsoft.Graph.Communications.Client;
using Microsoft.Graph.Communications.Client.Authentication;
using Microsoft.Graph.Communications.Common.Telemetry;
using Microsoft.Skype.Bots.Media;
using System.Net;
using System.Net.Http.Headers;

namespace TeamsMediaBot;

public sealed class BotSettings
{
    public required string TenantId { get; init; }
    public required string ClientId { get; init; }
    public required string ClientSecret { get; init; }
    public required string ServiceBaseUrl { get; init; }
    public required string AwsRegion { get; init; }

    /// <summary>Thumbprint of a TLS cert in Windows cert store (LocalMachine\My) used for Teams media mTLS.</summary>
    public required string MediaCertificateThumbprint { get; init; }

    /// <summary>VM public IPv4 address reachable by Microsoft Teams media edge.</summary>
    public required string MediaPublicIp { get; init; }

    public int MediaInstanceInternalPort { get; init; } = 8445;
    public int MediaInstancePublicPort { get; init; } = 8445;

    /// <summary>Optional; defaults to host from Bot callback URL.</summary>
    public string? MediaServiceFqdn { get; init; }

    /// <summary>Optional. Sets <see cref="JoinMeetingParameters.Subject"/> so the participant can show a clearer name in the roster (Azure Bot display name is also used by Teams).</summary>
    public string? JoinMeetingSubject { get; init; }
}

public sealed class BotService
{
    private readonly BotSettings _settings;
    private readonly CallHandler _callHandler;
    private readonly MediaHandler _mediaHandler;
    private readonly AwsTranscribeService _awsTranscribeService;
    private readonly ILogger<BotService> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IGraphLogger _graphLogger;
    private ICommunicationsClient? _communicationsClient;
    private bool _isInitialized;

    public BotService(
        BotSettings settings,
        CallHandler callHandler,
        MediaHandler mediaHandler,
        AwsTranscribeService awsTranscribeService,
        ILoggerFactory loggerFactory,
        ILogger<BotService> logger)
    {
        _settings = settings;
        _callHandler = callHandler;
        _mediaHandler = mediaHandler;
        _awsTranscribeService = awsTranscribeService;
        _loggerFactory = loggerFactory;
        _logger = logger;
        _graphLogger = new GraphLogger(_settings.ClientId);
    }

    public Task JoinMeetingAsync(string meetingJoinUrl) =>
        JoinMeetingAsync(new JoinMeetingRequest { MeetingJoinUrl = meetingJoinUrl });

    /// <summary>
    /// Join using the same request shape as a typical transcriber <c>POST /api/meetings/join</c> controller.
    /// </summary>
    public async Task JoinMeetingAsync(JoinMeetingRequest request)
    {
        EnsureInitialized();
        if (_communicationsClient is null)
        {
            throw new InvalidOperationException("Communications client is not initialized.");
        }

        _logger.LogInformation("Joining Teams meeting (Graph Communications).");
        await _awsTranscribeService.StartStreaming();

        if (!string.IsNullOrWhiteSpace(request.MeetingJoinUrl))
        {
            await _callHandler.JoinMeetingByUrlAsync(request.MeetingJoinUrl.Trim(), _mediaHandler);
        }
        else if (!string.IsNullOrWhiteSpace(request.ChatThreadId) && !string.IsNullOrWhiteSpace(request.OrganizerObjectId))
        {
            var meetingTid = string.IsNullOrWhiteSpace(request.MeetingTenantId)
                ? _settings.TenantId
                : request.MeetingTenantId.Trim();
            var messageId = string.IsNullOrWhiteSpace(request.ChatMessageId) ? "0" : request.ChatMessageId.Trim();
            await _callHandler.JoinMeetingByCoordinatesAsync(
                request.ChatThreadId.Trim(),
                messageId,
                request.OrganizerObjectId.Trim(),
                meetingTid,
                _mediaHandler);
        }
        else
        {
            throw new ArgumentException(
                "Provide MeetingJoinUrl, or ChatThreadId and OrganizerObjectId (and optional MeetingTenantId, ChatMessageId). " +
                "MeetingId alone cannot join a call via Graph Communications in this service.");
        }

        _logger.LogInformation("Join request submitted to Graph.");
    }

    public Task<HttpResponseMessage> ProcessNotificationAsync(HttpRequestMessage request)
    {
        EnsureInitialized();
        if (_communicationsClient is null)
        {
            throw new InvalidOperationException("Communications client is not initialized.");
        }

        return _communicationsClient.ProcessNotificationAsync(request);
    }

    private void EnsureInitialized()
    {
        if (_isInitialized)
        {
            return;
        }

        _communicationsClient = CreateCommunicationsClient();
        _callHandler.Initialize(_communicationsClient);
        _isInitialized = true;
    }

    private ICommunicationsClient CreateCommunicationsClient()
    {
        _logger.LogInformation(
            "Graph Communications identity: ClientId={ClientId}, TenantId={TenantId} (BOT_CLIENT_ID/BOT_TENANT_ID override appsettings when set).",
            _settings.ClientId,
            _settings.TenantId);

        var credential = new ClientSecretCredential(_settings.TenantId, _settings.ClientId, _settings.ClientSecret);
        var authLogger = _loggerFactory.CreateLogger<ClientCredentialsAuthenticationProvider>();
        var authProvider = new ClientCredentialsAuthenticationProvider(credential, _settings.TenantId, authLogger);

        // SDK requires BOTH: service base (origin) and notification (callback) URLs.
        // Bot:CallbackUrl / BOT_SERVICE_BASE_URL should be the full HTTPS callback, e.g. https://host/callback
        var notificationUrl = _settings.ServiceBaseUrl.Trim();
        if (string.IsNullOrWhiteSpace(notificationUrl))
        {
            throw new InvalidOperationException(
                "Callback URL is empty. Set Bot:CallbackUrl or BOT_SERVICE_BASE_URL to your public HTTPS callback (e.g. https://bot.example.com/callback).");
        }

        var notificationUri = new Uri(notificationUrl, UriKind.Absolute);
        if (notificationUri.Scheme != Uri.UriSchemeHttps)
        {
            throw new InvalidOperationException("Callback URL must use HTTPS.");
        }

        // Graph endpoint for place-call/join (SDK calls this "service base URL").
        var serviceBaseUri = new Uri("https://graph.microsoft.com/v1.0", UriKind.Absolute);

        var fqdn = string.IsNullOrWhiteSpace(_settings.MediaServiceFqdn)
            ? notificationUri.Host
            : _settings.MediaServiceFqdn.Trim();

        _logger.LogInformation(
            "Media Platform ServiceFqdn={MediaFqdn} (must match the TLS cert CN/SAN on this VM). Callback host={CallbackHost}. If these differ, set Media:ServiceFqdn or BOT_MEDIA_SERVICE_FQDN to the cert name.",
            fqdn,
            notificationUri.Host);

        if (!IPAddress.TryParse(_settings.MediaPublicIp.Trim(), out var publicIp))
        {
            throw new InvalidOperationException(
                "Media:PublicIp / BOT_MEDIA_PUBLIC_IP must be the VM public IPv4 address (e.g. 203.0.113.10).");
        }

        var mediaPlatformSettings = new MediaPlatformSettings
        {
            ApplicationId = _settings.ClientId,
            MediaPlatformInstanceSettings = new MediaPlatformInstanceSettings
            {
                CertificateThumbprint = _settings.MediaCertificateThumbprint.Trim(),
                InstanceInternalPort = _settings.MediaInstanceInternalPort,
                InstancePublicPort = _settings.MediaInstancePublicPort,
                InstancePublicIPAddress = publicIp,
                ServiceFqdn = fqdn
            }
        };

        return new CommunicationsClientBuilder(
                _settings.ClientId,
                _settings.ClientId,
                _graphLogger)
            .SetAuthenticationProvider(authProvider)
            .SetServiceBaseUrl(serviceBaseUri)
            .SetNotificationUrl(notificationUri)
            .SetMediaPlatformSettings(mediaPlatformSettings)
            .Build();
    }
}

public sealed class ClientCredentialsAuthenticationProvider : IRequestAuthenticationProvider
{
    private static readonly string[] GraphScopes = { "https://graph.microsoft.com/.default" };
    private readonly TokenCredential _credential;
    private readonly string _tenantId;
    private readonly ILogger<ClientCredentialsAuthenticationProvider> _logger;

    public ClientCredentialsAuthenticationProvider(
        TokenCredential credential,
        string tenantId,
        ILogger<ClientCredentialsAuthenticationProvider> logger)
    {
        _credential = credential;
        _tenantId = tenantId;
        _logger = logger;
    }

    public async Task AuthenticateOutboundRequestAsync(HttpRequestMessage request, string tenant)
    {
        // SDK passes tenant from the join/call context (see Graph comms samples). Using only the
        // credential's default tenant without this can contribute to "Call source identity invalid".
        var tenantForToken = string.IsNullOrWhiteSpace(tenant) ? _tenantId : tenant.Trim();
        var context = new TokenRequestContext(GraphScopes, tenantId: tenantForToken);
        AccessToken token = await _credential.GetTokenAsync(context, default);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Token);

        // Full JWT is a secret. Set PRINT_GRAPH_ACCESS_TOKEN=1 only for local debugging; disable afterward.
        var printToken = Environment.GetEnvironmentVariable("PRINT_GRAPH_ACCESS_TOKEN");
        if (string.Equals(printToken, "1", StringComparison.Ordinal) ||
            string.Equals(printToken, "true", StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogWarning(
                "PRINT_GRAPH_ACCESS_TOKEN enabled — Graph access token (tenant used for token: {TenantForToken}): {AccessToken}",
                tenantForToken,
                token.Token);
        }
    }

    public Task<RequestValidationResult> ValidateInboundRequestAsync(HttpRequestMessage request)
    {
        // Demo-only: validate inbound token/signature for production webhooks.
        return Task.FromResult(new RequestValidationResult
        {
            IsValid = true,
            TenantId = _tenantId
        });
    }
}
