using Microsoft.AspNetCore.Builder;
using System.Text;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

public static class Program
{
    public static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        // Kestrel URL must differ from the Media Platform's internal HTTP listener (see BotService + Media:HttpControlPort).
        // nginx should proxy to this port (default 5080). Override with BOT_HTTP_LISTEN_URL or Bot:ListenUrl.
        var appListenUrl = Environment.GetEnvironmentVariable("BOT_HTTP_LISTEN_URL");
        if (string.IsNullOrWhiteSpace(appListenUrl))
        {
            appListenUrl = builder.Configuration["Bot:ListenUrl"];
        }
        if (string.IsNullOrWhiteSpace(appListenUrl))
        {
            appListenUrl = "http://127.0.0.1:5080";
        }
        builder.WebHost.UseUrls(appListenUrl.Trim());

        builder.Logging.ClearProviders();
        builder.Logging.AddConsole();

        builder.Services.Configure<ForwardedHeadersOptions>(options =>
        {
            options.ForwardedHeaders = ForwardedHeaders.XForwardedFor | ForwardedHeaders.XForwardedProto;
            options.KnownNetworks.Clear();
            options.KnownProxies.Clear();
        });

        builder.Services.AddSignalR();
        builder.Services.AddSingleton(new BotSettings
        {
            TenantId = GetConfig(builder.Configuration, "BOT_TENANT_ID", "AzureAd:TenantId"),
            ClientId = GetConfig(builder.Configuration, "BOT_CLIENT_ID", "AzureAd:ClientId"),
            ClientSecret = GetConfig(builder.Configuration, "BOT_CLIENT_SECRET", "AzureAd:ClientSecret"),
            ServiceBaseUrl = GetConfig(builder.Configuration, "BOT_SERVICE_BASE_URL", "Bot:CallbackUrl"),
            AwsRegion = GetConfig(builder.Configuration, "AWS_REGION", "AWS:Region"),
            MediaCertificateThumbprint = GetConfig(builder.Configuration, "BOT_MEDIA_CERT_THUMBPRINT", "Media:CertificateThumbprint"),
            MediaPublicIp = GetConfig(builder.Configuration, "BOT_MEDIA_PUBLIC_IP", "Media:PublicIp"),
            MediaInstanceInternalPort = ReadInt(builder.Configuration, "BOT_MEDIA_INSTANCE_INTERNAL_PORT", "Media:InstanceInternalPort", 8445),
            MediaInstancePublicPort = ReadInt(builder.Configuration, "BOT_MEDIA_INSTANCE_PUBLIC_PORT", "Media:InstancePublicPort", 8445),
            MediaHttpControlPort = ReadInt(builder.Configuration, "BOT_MEDIA_HTTP_CONTROL_PORT", "Media:HttpControlPort", 5000),
            MediaServiceFqdn = ReadOptional(builder.Configuration, "BOT_MEDIA_SERVICE_FQDN", "Media:ServiceFqdn"),
            JoinMeetingSubject = ReadOptional(builder.Configuration, "BOT_JOIN_MEETING_SUBJECT", "Bot:JoinMeetingSubject")
        });

        builder.Services.AddSingleton<TranscriptBroadcaster>();
        builder.Services.AddSingleton<AudioProcessor>();
        builder.Services.AddSingleton<AwsTranscribeService>();
        builder.Services.AddSingleton<MediaHandler>();
        builder.Services.AddSingleton<CallHandler>();
        builder.Services.AddSingleton<BotService>();

        var app = builder.Build();

        // Must run first so SignalR and HTTPS URLs see correct scheme/host behind nginx.
        app.UseForwardedHeaders();

        app.UseDefaultFiles();
        app.UseStaticFiles();

        app.MapHub<TranscriptHub>("/hubs/transcripts");

        static async Task<IResult> HandleGraphCallback(HttpContext ctx, BotService botService, ILogger log)
        {
            // Convert ASP.NET request into HttpRequestMessage for the comms SDK.
            var req = ctx.Request;
            var uri = new Uri($"{req.Scheme}://{req.Host}{req.Path}{req.QueryString}");
            var msg = new HttpRequestMessage(new HttpMethod(req.Method), uri);

            foreach (var header in req.Headers)
            {
                if (!msg.Headers.TryAddWithoutValidation(header.Key, header.Value.ToArray()))
                {
                    msg.Content ??= new StreamContent(req.Body);
                    msg.Content.Headers.TryAddWithoutValidation(header.Key, header.Value.ToArray());
                }
            }

            if (msg.Content is null)
            {
                msg.Content = new StreamContent(req.Body);
            }

            var sdkResponse = await botService.ProcessNotificationAsync(msg);

            ctx.Response.StatusCode = (int)sdkResponse.StatusCode;
            foreach (var header in sdkResponse.Headers)
            {
                ctx.Response.Headers[header.Key] = header.Value.ToArray();
            }
            if (sdkResponse.Content is not null)
            {
                foreach (var header in sdkResponse.Content.Headers)
                {
                    ctx.Response.Headers[header.Key] = header.Value.ToArray();
                }
                // ASP.NET Core sets these automatically for some responses
                ctx.Response.Headers.Remove("transfer-encoding");
                await sdkResponse.Content.CopyToAsync(ctx.Response.Body);
            }

            log.LogInformation("Processed Graph comms callback. Status={Status}", (int)sdkResponse.StatusCode);
            return Results.Empty;
        }

        // Graph Communications notifications endpoint(s).
        app.MapPost("/communications/calls", (HttpContext ctx, BotService botService, ILoggerFactory loggerFactory) =>
            HandleGraphCallback(ctx, botService, loggerFactory.CreateLogger("GraphCommsNotifications")));

        app.MapPost("/callback", (HttpContext ctx, BotService botService, ILoggerFactory loggerFactory) =>
            HandleGraphCallback(ctx, botService, loggerFactory.CreateLogger("GraphCommsCallback")));

        static async Task<IResult> HandleMeetingsApiJoin(
            HttpContext ctx,
            JoinMeetingRequest request,
            BotService botService,
            ILogger log)
        {
            if (string.IsNullOrWhiteSpace(request.MeetingId) &&
                string.IsNullOrWhiteSpace(request.MeetingJoinUrl) &&
                string.IsNullOrWhiteSpace(request.ChatThreadId))
            {
                return Results.BadRequest(new { message = "Provide MeetingId, MeetingJoinUrl, or ChatThreadId (+ OrganizerObjectId)." });
            }

            if (string.IsNullOrWhiteSpace(request.MeetingJoinUrl) &&
                string.IsNullOrWhiteSpace(request.ChatThreadId))
            {
                return Results.BadRequest(new
                {
                    message = "This bot needs MeetingJoinUrl, or ChatThreadId with OrganizerObjectId, to join via Graph Communications.",
                    meetingId = request.MeetingId
                });
            }

            if (!string.IsNullOrWhiteSpace(request.ChatThreadId) &&
                string.IsNullOrWhiteSpace(request.OrganizerObjectId))
            {
                return Results.BadRequest(new { message = "When using ChatThreadId, OrganizerObjectId is required (and MeetingTenantId if not the bot home tenant)." });
            }

            var parsed = MeetingJoinParser.ParseJoinUrl(request.MeetingJoinUrl);
            var transcriptKey = !string.IsNullOrWhiteSpace(request.MeetingId)
                ? request.MeetingId!.Trim()
                : (!string.IsNullOrWhiteSpace(parsed.JoinMeetingId)
                    ? parsed.JoinMeetingId
                    : (!string.IsNullOrWhiteSpace(request.ChatThreadId)
                        ? request.ChatThreadId.Trim()
                        : request.MeetingJoinUrl?.Trim())) ?? "unknown";

            try
            {
                await botService.JoinMeetingAsync(request);
                return Results.Accepted(
                    uri: null,
                    value: new
                    {
                        transcriptKey,
                        joinMeetingId = parsed.JoinMeetingId,
                        chatThreadId = request.ChatThreadId,
                        message = "Join request submitted."
                    });
            }
            catch (ArgumentException ex)
            {
                log.LogWarning(ex, "Invalid join request.");
                return Results.BadRequest(new { message = ex.Message });
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Join meeting failed.");
                return Results.Json(
                    new
                    {
                        message = "Join meeting failed.",
                        error = ex.Message,
                        inner = ex.InnerException?.Message,
                        type = ex.GetType().FullName,
                        traceId = ctx.TraceIdentifier
                    },
                    statusCode: StatusCodes.Status500InternalServerError);
            }
        }

        app.MapPost("/api/meetings/join", async (HttpContext ctx, JoinMeetingRequest request, BotService botService, ILoggerFactory loggerFactory) =>
            await HandleMeetingsApiJoin(ctx, request, botService, loggerFactory.CreateLogger("Join")));

        app.MapPost("/api/bot/join", async (HttpContext ctx, JoinMeetingRequest request, BotService botService, ILoggerFactory loggerFactory) =>
        {
            var log = loggerFactory.CreateLogger("Join");
            if (string.IsNullOrWhiteSpace(request.MeetingId) &&
                string.IsNullOrWhiteSpace(request.MeetingJoinUrl) &&
                string.IsNullOrWhiteSpace(request.ChatThreadId))
            {
                return Results.BadRequest(new { message = "Provide MeetingJoinUrl, or MeetingId with MeetingJoinUrl, or ChatThreadId + OrganizerObjectId." });
            }

            if (string.IsNullOrWhiteSpace(request.MeetingJoinUrl) &&
                string.IsNullOrWhiteSpace(request.ChatThreadId))
            {
                return Results.BadRequest(new { message = "MeetingJoinUrl (or ChatThreadId + OrganizerObjectId) is required." });
            }

            if (!string.IsNullOrWhiteSpace(request.ChatThreadId) &&
                string.IsNullOrWhiteSpace(request.OrganizerObjectId))
            {
                return Results.BadRequest(new { message = "When using ChatThreadId, OrganizerObjectId is required." });
            }

            try
            {
                await botService.JoinMeetingAsync(request);
                return Results.Ok(new { message = "Join request submitted." });
            }
            catch (ArgumentException ex)
            {
                log.LogWarning(ex, "Invalid join request.");
                return Results.BadRequest(new { message = ex.Message });
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Join meeting failed.");
                return Results.Json(
                    new
                    {
                        message = "Join meeting failed.",
                        error = ex.Message,
                        inner = ex.InnerException?.Message,
                        type = ex.GetType().FullName,
                        traceId = ctx.TraceIdentifier
                    },
                    statusCode: StatusCodes.Status500InternalServerError);
            }
        });

        await app.RunAsync();
    }

    private static string GetConfig(IConfiguration configuration, string envKey, string configKey)
    {
        var env = Environment.GetEnvironmentVariable(envKey);
        if (!string.IsNullOrWhiteSpace(env))
        {
            return env.Trim();
        }

        var fromConfig = configuration[configKey];
        if (!string.IsNullOrWhiteSpace(fromConfig))
        {
            return fromConfig.Trim();
        }

        throw new InvalidOperationException($"Missing configuration: {envKey} or {configKey}");
    }

    private static string? ReadOptional(IConfiguration configuration, string envKey, string configKey)
    {
        var env = Environment.GetEnvironmentVariable(envKey);
        if (!string.IsNullOrWhiteSpace(env))
        {
            return env.Trim();
        }

        var fromConfig = configuration[configKey];
        return string.IsNullOrWhiteSpace(fromConfig) ? null : fromConfig.Trim();
    }

    private static int ReadInt(IConfiguration configuration, string envKey, string configKey, int defaultValue)
    {
        var env = Environment.GetEnvironmentVariable(envKey);
        if (!string.IsNullOrWhiteSpace(env) && int.TryParse(env.Trim(), out var fromEnv))
        {
            return fromEnv;
        }

        var fromConfig = configuration[configKey];
        if (!string.IsNullOrWhiteSpace(fromConfig) && int.TryParse(fromConfig.Trim(), out var fromFile))
        {
            return fromFile;
        }

        return defaultValue;
    }
}
