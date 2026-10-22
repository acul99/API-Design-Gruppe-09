# Loescht ALLE Queues im RabbitMQ (Exchanges bleiben bestehen).
# Vorher alle Consumer und den Producer beenden, sonst deklarieren sie Queues neu.
#
# Aufruf: .\delete_queues.ps1
# Falls Skripte blockiert sind: powershell -ExecutionPolicy Bypass -File .\delete_queues.ps1

# rabbitmqctl suchen: erst im PATH, sonst im Standard-Installationsordner
$ctl = (Get-Command rabbitmqctl.bat -ErrorAction SilentlyContinue).Source
if (-not $ctl) {
    $roots = @($env:ProgramFiles, ${env:ProgramFiles(x86)}) | Where-Object { $_ }
    $ctl = $roots |
        ForEach-Object { Get-ChildItem "$_\RabbitMQ Server\rabbitmq_server-*\sbin\rabbitmqctl.bat" -ErrorAction SilentlyContinue } |
        Sort-Object FullName | Select-Object -Last 1 -ExpandProperty FullName
}
if (-not $ctl) {
    Write-Host "rabbitmqctl.bat nicht gefunden"
    exit 1
}

$output = & $ctl list_queues name --quiet --no-table-headers 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "RabbitMQ laeuft nicht oder ist nicht erreichbar"
    exit 1
}

$queues = @($output | ForEach-Object { "$_".Trim() } | Where-Object { $_ })
if ($queues.Count -eq 0) {
    Write-Host "Keine Queues vorhanden"
    exit 0
}

foreach ($q in $queues) {
    & $ctl delete_queue $q *> $null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "geloescht: $q"
    } else {
        Write-Host "FEHLER beim Loeschen: $q"
    }
}
