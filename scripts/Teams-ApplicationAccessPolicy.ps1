# Requires: Teams Administrator (or equivalent) and MicrosoftTeams PowerShell module.
# Fixes 403 "Call source identity invalid" for Graph Communications / Teams calling bots
# by allowing your Entra app (client id) in a Teams application access policy.
#
# Usage (run in elevated PowerShell if your org requires it):
#   .\Teams-ApplicationAccessPolicy.ps1 -BotAppId "8f992da5-20ea-42a1-bf11-0a09ba42b35c"
#
# If New-CsApplicationAccessPolicy fails because the identity already exists, the script
# adds the app id to the existing policy instead.

param(
    [Parameter(Mandatory = $false)]
    [string] $BotAppId = "8f992da5-20ea-42a1-bf11-0a09ba42b35c",

    [Parameter(Mandatory = $false)]
    [string] $PolicyIdentity = "GraphCallingApplicationAccessPolicy"
)

$ErrorActionPreference = "Stop"

if (-not (Get-Module -ListAvailable -Name MicrosoftTeams)) {
    Install-Module MicrosoftTeams -Scope CurrentUser -Force
}

Import-Module MicrosoftTeams

Write-Host "Connecting to Microsoft Teams (sign in as a Teams admin for the SAME tenant as AzureAd:TenantId / BOT_TENANT_ID)..." -ForegroundColor Cyan
Connect-MicrosoftTeams

$existing = $null
try {
    $existing = Get-CsApplicationAccessPolicy -Identity $PolicyIdentity -ErrorAction SilentlyContinue
} catch {
    $existing = $null
}

if (-not $existing) {
    Write-Host "Creating policy '$PolicyIdentity' with AppId $BotAppId ..." -ForegroundColor Cyan
    New-CsApplicationAccessPolicy -Identity $PolicyIdentity `
        -AppIds @($BotAppId) `
        -Description "Allow Graph Communications bot app to participate in Teams meetings/calls"
} else {
    Write-Host "Policy '$PolicyIdentity' exists; adding AppId $BotAppId if missing ..." -ForegroundColor Cyan
    Set-CsApplicationAccessPolicy -Identity $PolicyIdentity -AppIds @{ Add = $BotAppId }
}

Write-Host "Granting policy tenant-wide (-Global) ..." -ForegroundColor Cyan
Grant-CsApplicationAccessPolicy -PolicyName $PolicyIdentity -Global

Write-Host "`nVerification (policy should list your BotAppId):" -ForegroundColor Green
Get-CsApplicationAccessPolicy -Identity $PolicyIdentity | Format-List Identity, AppIds, Description

Write-Host "`nDone. Wait 15-60+ minutes for replication, then retry the bot join." -ForegroundColor Green
Write-Host "Docs: https://learn.microsoft.com/powershell/module/microsoftteams/new-csapplicationaccesspolicy" -ForegroundColor DarkGray
