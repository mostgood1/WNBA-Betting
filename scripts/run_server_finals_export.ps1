param(
  [string]$Since = '2025-10-21',
  [string]$Until = '2025-10-23',
  [string]$BaseUrl = 'https://nba-betting-5qgf.onrender.com',
  [string]$Token
)

$ErrorActionPreference = 'Stop'
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
Set-Location -Path $RepoRoot

# Try explicit -Token, then CRON_TOKEN from env, else from .env
$token = $Token
if (-not $token) { $token = $env:CRON_TOKEN }
if (-not $token) {
  $dotenv = Join-Path $RepoRoot '.env'
  if (Test-Path $dotenv) {
    $line = (Get-Content -Path $dotenv -Encoding UTF8 | Where-Object { $_ -match '^CRON_TOKEN=' } | Select-Object -First 1)
    if ($line) {
      $token = ($line -replace '^CRON_TOKEN=','').Trim().Trim('"').Trim("'")
    }
  }
}

$uri = "$BaseUrl/api/finals/export?since=$Since&until=$Until&push=1"
if ($token) {
  $uri = "$uri&token=$token"
}

try {
  $r = Invoke-WebRequest -UseBasicParsing -Uri $uri -TimeoutSec 180
  Write-Host "Status:" $r.StatusCode
  Write-Host $r.Content
} catch {
  Write-Host "ERR:" $_.Exception.Message
  exit 1
}
