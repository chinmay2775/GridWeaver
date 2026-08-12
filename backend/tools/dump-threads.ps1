$srv = (Get-NetTCPConnection -LocalPort 9099 -State Listen -ErrorAction SilentlyContinue).OwningProcess | Select-Object -First 1
if (-not $srv) { Write-Error "Nothing listening on 9099 - is the server running?"; exit 1 }

New-Item -ItemType Directory -Force -Path C:\temp | Out-Null
jcmd $srv Thread.dump_to_file -format=json -overwrite C:\temp\vt.json | Out-Null

$raw     = Get-Content C:\temp\vt.json -Raw
$conns   = ([regex]::Matches($raw, 'ingest-conn')).Count
$carrier = ([regex]::Matches($raw, 'ForkJoinPool-1-worker')).Count
$rssMb   = [math]::Round((Get-Process -Id $srv).WorkingSet64 / 1MB, 1)

Write-Host ""
Write-Host "server pid       : $srv"
Write-Host "virtual threads  : $conns"
Write-Host "carrier threads  : $carrier"
Write-Host "process RSS (MB) : $rssMb"
Write-Host ""
Invoke-RestMethod http://localhost:8080/debug/stats | ConvertTo-Json