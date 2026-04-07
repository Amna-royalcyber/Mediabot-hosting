# Teams Media Bot (VM-hosted)

ASP.NET Core app that joins Microsoft Teams meetings using the **Microsoft Graph Communications SDK** with **application-hosted media**: audio is received on your Windows VM, processed, and streamed to **Amazon Transcribe** (streaming). Transcripts can be pushed to browsers via **SignalR**.

---

## Prerequisites

### Runtime and platform

| Requirement | Notes |
|-------------|--------|
| **Windows x64** | Skype / Teams media stack (`Microsoft.Skype.Bots.Media`) is **x64-only** (see `TeamsMediaBot.csproj`). |
| **.NET 8 SDK** | `TargetFramework` is `net8.0`. |
| **TLS certificate** | Installed in **Local Machine → Personal (My)**; thumbprint configured in `Media:CertificateThumbprint`. Used for **media** TLS; **CN/SAN must match** `Media:ServiceFqdn` (see Configuration). |
| **Public IPv4** | VM must expose the media port (default **8445**) to Microsoft Teams media edge; `Media:PublicIp` must be that address. |
| **DNS** | Hostname used for **signaling** (HTTPS callback) and the name on the **certificate** for **media** must resolve and reach this VM / load balancer as required by your network design. |

### Microsoft 365 / Azure

| Requirement | Notes |
|-------------|--------|
| **Entra ID app registration** | Application (client) ID, tenant ID, client secret; **application** permissions for calling (e.g. `Calls.JoinGroupCall.All`, `Calls.AccessMedia.All`, etc.) with **admin consent**. |
| **Azure Bot + Teams channel** | Bot registered in Azure; **Microsoft Teams** channel enabled; **Messaging endpoint** / callback must match your public HTTPS URL. |
| **Teams application access policy** | Your app’s client ID must be allowed in a **Teams application access policy** and assigned appropriately (`New-CsApplicationAccessPolicy` / `Grant-CsApplicationAccessPolicy`). Without this, Graph often returns **403** *Call source identity invalid*. |
| **Meeting join link** | Prefer a full **meetup-join** URL that includes `context` with **Tid** and **Oid** (organizer). |

### AWS

| Requirement | Notes |
|-------------|--------|
| **Credentials on the VM** | Default SDK chain (e.g. instance profile, env vars, or shared credentials) for **Transcribe Streaming**. |
| **IAM** | Permission to call **Amazon Transcribe streaming** in the chosen region (`AWS_REGION` / `AWS:Region`). |

### Optional

| Requirement | Notes |
|-------------|--------|
| **Reverse proxy** | e.g. nginx terminating HTTPS for `/callback` and app routes; app uses **forwarded headers** (`Program.cs`). |
| **PRINT_GRAPH_ACCESS_TOKEN** | Set to `1` or `true` only for **short-lived debugging** (logs full Graph token — remove after use). |

---

## HTTP routes

| Method | Path | Purpose |
|--------|------|--------|
| `POST` | `/api/bot/join` | Trigger join; returns **200** with JSON on success. |
| `POST` | `/api/meetings/join` | Same join logic; returns **202 Accepted** (transcriber-style response body). |
| `POST` | `/callback` | Graph Communications **notification** endpoint (must match bot callback URL). |
| `POST` | `/communications/calls` | Alternate Graph Communications notification path (same handler as `/callback`). |
| SignalR | `/hubs/transcripts` | Pushes transcript updates to connected clients. |
| Static | `/` | Serves `wwwroot` (e.g. `index.html`). |

**Join request body** (`JoinMeetingRequest` in `BotApiModels.cs`): at minimum **`meetingJoinUrl`**, or **`chatThreadId` + `organizerObjectId`** (+ optional **`meetingTenantId`**, **`chatMessageId`**). See that file for all properties.

---

## Configuration

Values are read from **environment variables first**, then **`appsettings.json`** (see `Program.cs`).

### `appsettings.json` sections

| Section | Purpose |
|---------|--------|
| `AzureAd` | `TenantId`, `ClientId`, `ClientSecret` for client-credentials Graph token. The **Graph Communications** logger and client builder use **`ClientId`** as the application identifier (no separate display name). **Do not commit production secrets**; use env vars or a secret store. |
| `Bot` | `CallbackUrl` — full HTTPS URL to **`/callback`** (or the path your Azure Bot uses). |
| `AWS` | `Region` for Transcribe (overridable by `AWS_REGION`). |
| `Media` | Certificate thumbprint, public IP, internal/public ports (**8445**), **`ServiceFqdn`** (must match cert **CN/SAN** even if callback host differs). |

### Environment variables (override config)

| Variable | Maps to |
|----------|---------|
| `BOT_TENANT_ID` | `AzureAd:TenantId` |
| `BOT_CLIENT_ID` | `AzureAd:ClientId` |
| `BOT_CLIENT_SECRET` | `AzureAd:ClientSecret` |
| `BOT_SERVICE_BASE_URL` | `Bot:CallbackUrl` |
| `BOT_MEDIA_CERT_THUMBPRINT` | `Media:CertificateThumbprint` |
| `BOT_MEDIA_PUBLIC_IP` | `Media:PublicIp` |
| `BOT_MEDIA_INSTANCE_INTERNAL_PORT` | `Media:InstanceInternalPort` |
| `BOT_MEDIA_INSTANCE_PUBLIC_PORT` | `Media:InstancePublicPort` |
| `BOT_MEDIA_SERVICE_FQDN` | `Media:ServiceFqdn` (**use when callback host ≠ cert name**) |
| `AWS_REGION` | AWS region for Transcribe |

---

## Project layout — what each file does

| Path | Role |
|------|------|
| **`TeamsMediaBot.csproj`** | SDK project; **x64**, NuGet packages (Graph Communications, Skype Bots Media, AWS Transcribe). Copies native media DLLs on build/publish. |
| **`Program.cs`** | App entry: DI, **`BotSettings`**, forwarded headers, SignalR, static files, **all HTTP routes** (join APIs, Graph callbacks). |
| **`BotService.cs`** | Builds **`ICommunicationsClient`** (auth, callback URL, **media platform settings**, Graph base URL). **`JoinMeetingAsync`**, **`ProcessNotificationAsync`**. |
| **`BotSettings`** (in `BotService.cs`) | Strongly typed configuration injected from `Program`. |
| **`ClientCredentialsAuthenticationProvider`** (in `BotService.cs`) | Acquires **app-only** Graph tokens for outbound SDK requests. |
| **`CallHandler.cs`** | Parses Teams **join URLs** (and optional **coordinates**), builds **`JoinMeetingParameters`**, calls **`Calls().AddAsync`**. Sets organizer **tenant** (`SetTenantId`) and **reply-chain** message id when present in URL context. |
| **`MeetingJoinParser.cs`** | Extracts thread id / optional passcode from a URL for API responses and correlation. |
| **`BotApiModels.cs`** | **`JoinMeetingRequest`** JSON model for join endpoints. |
| **`MediaHandler.cs`** | Creates **app-hosted** **`IMediaSession`** with **unmixed meeting audio**; subscribes to **`AudioMediaReceived`** and routes per-source buffers to **`ParticipantAudioStreamHandler`**. |
| **`ParticipantAudioStreamHandler.cs`** | Reads **`UnmixedAudioBuffers`**, converts to PCM, and calls **`TranscriptionManager.ProcessParticipantAudioAsync`** per **`ActiveSpeakerId`**. |
| **`AudioProcessor.cs`** | Converts/buffers raw audio frames for streaming. |
| **`TranscriptionManager.cs`** | Maps **Graph participant `mediaStreams[].sourceId`** to **Entra user id + display name**; one **`TranscribeStreamService`** per source id; lifecycle on join/leave. |
| **`TranscribeStreamService.cs`** | **Amazon Transcribe streaming** per participant (**`ShowSpeakerLabel = false`**); publishes **`TranscriptFragment`** with identity from Teams, not AWS labels. |
| **`TranscriptAggregator.cs`** | Background merge/broadcast of fragments to SignalR with correct display names. |
| **`TranscriptBroadcaster.cs`** | Sends transcript payloads to **SignalR** clients. |
| **`TranscriptHub.cs`** | SignalR hub for `/hubs/transcripts`. |
| **`appsettings.json`** | Default configuration (replace secrets via env in production). |
| **`wwwroot/index.html`** | Optional simple static UI. |
| **`scripts/Teams-ApplicationAccessPolicy.ps1`** | Example Teams PowerShell to add the bot app id to an **application access policy** (tenant admin). |

---

## Build and run

```powershell
dotnet restore
dotnet build -c Release
dotnet run -c Release
```

Publish (example):

```powershell
dotnet publish -c Release -r win-x64 --self-contained false
```

Ensure the **Skype native DLLs** are present in the output folder (the project runs a target to copy them after restore).

---

## Troubleshooting (quick)

| Symptom | Likely cause |
|---------|----------------|
| **403** *Call source identity invalid* | Teams **application access policy** missing your **ClientId**, or organizer’s user policy doesn’t include it. |
| **Media platform failed to initialize** / FQDN vs cert mismatch | Set **`Media:ServiceFqdn`** or **`BOT_MEDIA_SERVICE_FQDN`** to the **exact hostname on the certificate** (may differ from callback host). |
| No audio / no transcripts | Join must **succeed** first; check Graph callbacks reach **`/callback`**; verify media **IP/port** and firewall. |

---

## References

- [Microsoft Graph Communications samples](https://github.com/microsoftgraph/microsoft-graph-comms-samples) (calling bots, join patterns, registration docs).
- [Register a calling bot](https://microsoftgraph.github.io/microsoft-graph-comms-samples/docs/articles/calls/register-calling-bot.html).
- Teams PowerShell: [`New-CsApplicationAccessPolicy`](https://learn.microsoft.com/powershell/module/microsoftteams/new-csapplicationaccesspolicy), [`Grant-CsApplicationAccessPolicy`](https://learn.microsoft.com/powershell/module/microsoftteams/grant-csapplicationaccesspolicy).
