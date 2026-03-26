$ErrorActionPreference = 'Stop'

$workspace = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $workspace '.venv\Scripts\python.exe'

$procs = Get-CimInstance Win32_Process | Where-Object {
    $_.CommandLine -match 'waitress' -and $_.CommandLine -match '5051'
}

if ($procs) {
    $procs | ForEach-Object {
        Stop-Process -Id $_.ProcessId -Force
    }
}

& $pythonExe -m waitress --listen=127.0.0.1:5051 app:app