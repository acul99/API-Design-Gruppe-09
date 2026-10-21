param(
    [ValidateSet('run', 'check', 'invalid', 'processing', 'crash', 'priority', 'ttl', 'all')]
    [string]$Mode = 'run'
)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location -LiteralPath $projectRoot
try {
    $projectPython = Join-Path $projectRoot '.venv\Scripts\python.exe'
    if (-not (Test-Path -LiteralPath $projectPython)) {
        throw 'Bitte zuerst eine .venv anlegen und python -m pip install -e . ausfuehren (siehe README).'
    }
    if ($Mode -eq 'run' -or $Mode -eq 'check') {
        & $projectPython -m iot_analysis $Mode
    } else {
        & $projectPython -m iot_analysis demo --scenario $Mode
    }
    if ($LASTEXITCODE -ne 0) { throw "Projektstart fehlgeschlagen (Exit $LASTEXITCODE)." }
} finally {
    Pop-Location
}
