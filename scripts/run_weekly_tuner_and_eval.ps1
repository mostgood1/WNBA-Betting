param(
  [int]$Days = 14,
  [switch]$Push
)

$ErrorActionPreference = 'Stop'
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
Set-Location -Path $RepoRoot

# 1) Tune per-player calibration over trailing window
$runTuner = Join-Path $ScriptDir 'run_tuner_trailing.ps1'
if (-not (Test-Path $runTuner)) { throw "Missing tuner runner: $runTuner" }
& powershell -NoProfile -ExecutionPolicy Bypass -File $runTuner -Days $Days -GitPush:$Push 2>&1 | Out-Host

# 2) Run trailing evaluator and push its artifacts
$runEval = Join-Path $ScriptDir 'run_eval_trailing.ps1'
if (Test-Path $runEval) {
  & powershell -NoProfile -ExecutionPolicy Bypass -File $runEval -Days $Days -GitPush:$Push 2>&1 | Out-Host
}

Write-Host ("Weekly tuner+eval complete (Days={0}, Push={1})" -f $Days, $Push.IsPresent)
