param(
    [string]$RenderUrl = 'https://nba-betting-5qgf.onrender.com'
)

$ErrorActionPreference = 'Stop'

function Require-GHCLI {
    if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
        Write-Error "GitHub CLI 'gh' not found. Install from https://cli.github.com/ and login via 'gh auth login'."
    }
}

Require-GHCLI

Write-Host "Setting GitHub secrets: CRON_TOKEN and RENDER_URL=$RenderUrl"

# Prompt for CRON_TOKEN securely
$cron = Read-Host -AsSecureString -Prompt 'Enter CRON_TOKEN for Render cron auth'
$cronPlain = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($cron))

# Set secrets
& gh secret set CRON_TOKEN -b $cronPlain
& gh secret set RENDER_URL -b $RenderUrl

Write-Host "Secrets set. Verify in GitHub → Settings → Secrets and variables → Actions."
