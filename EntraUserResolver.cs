using System.Collections.Concurrent;
using Azure.Identity;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;

namespace TeamsMediaBot;

/// <summary>
/// Resolves Azure AD (Entra ID) user profiles via Microsoft Graph using the same app credentials as the bot.
/// Requires application permission <c>User.Read.All</c> (or <c>Directory.Read.All</c>) in Azure AD.
/// </summary>
public sealed class EntraUserResolver
{
    private static readonly string[] GraphScopes = { "https://graph.microsoft.com/.default" };

    private readonly ConcurrentDictionary<string, EntraUserProfile> _cache = new(StringComparer.OrdinalIgnoreCase);
    private readonly GraphServiceClient _graph;
    private readonly ILogger<EntraUserResolver> _logger;

    public EntraUserResolver(BotSettings settings, ILogger<EntraUserResolver> logger)
    {
        _logger = logger;
        var credential = new ClientSecretCredential(settings.TenantId, settings.ClientId, settings.ClientSecret);
        _graph = new GraphServiceClient(credential, GraphScopes);
    }

    public async Task<EntraUserProfile?> GetUserAsync(string azureAdObjectId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(azureAdObjectId))
        {
            return null;
        }

        var key = azureAdObjectId.Trim();
        if (_cache.TryGetValue(key, out var cached))
        {
            return cached;
        }

        try
        {
            var user = await _graph.Users[key].GetAsync(
                requestConfiguration =>
                {
                    requestConfiguration.QueryParameters.Select = new[]
                    {
                        "id",
                        "displayName",
                        "userPrincipalName",
                        "mail"
                    };
                },
                cancellationToken);

            if (user is null || string.IsNullOrWhiteSpace(user.Id))
            {
                return null;
            }

            var profile = new EntraUserProfile(
                user.Id,
                user.DisplayName?.Trim(),
                user.UserPrincipalName?.Trim(),
                user.Mail?.Trim());

            _cache[key] = profile;
            return profile;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Microsoft Graph user lookup failed for object id {UserId}. Grant User.Read.All app permission if missing.", key);
            return null;
        }
    }
}

public sealed record EntraUserProfile(string Id, string? DisplayName, string? UserPrincipalName, string? Mail);
