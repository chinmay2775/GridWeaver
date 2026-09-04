$out = "final-review-$(Get-Date -Format 'yyyyMMdd-HHmmss').txt"

function Section($name, $block) {
    "`n=== $name ===" | Add-Content $out
    & $block | ConvertTo-Json -Depth 5 | Add-Content $out
}

"GridWeaver final review  $(Get-Date)" | Set-Content $out

Section "threads"   { Invoke-RestMethod http://localhost:8080/debug/threads }
Section "stats"     { Invoke-RestMethod http://localhost:8080/debug/stats }
Section "zones"     { Invoke-RestMethod http://localhost:8080/debug/zones }
Section "states"    { Invoke-RestMethod http://localhost:8080/debug/states }
Section "balance"   { Invoke-RestMethod http://localhost:8080/debug/balance }
Section "transfers" { Invoke-RestMethod http://localhost:8080/debug/transfers }
Section "lag"       { Invoke-RestMethod http://localhost:8080/debug/lag }
Section "events"    { Invoke-RestMethod "http://localhost:8080/debug/events?limit=15" }
Section "health"    { Invoke-RestMethod http://localhost:8080/actuator/health }

# Thread dump: the JSON format is the only one that shows virtual threads
$srv = (Get-NetTCPConnection -LocalPort 9099 -State Listen).OwningProcess | Select-Object -First 1
New-Item -ItemType Directory -Force -Path C:\temp | Out-Null
jcmd $srv Thread.dump_to_file -format=json -overwrite C:\temp\final.json | Out-Null
$raw = Get-Content C:\temp\final.json -Raw

"`n=== thread dump ===" | Add-Content $out
"virtual threads (ingest-conn) : $(([regex]::Matches($raw,'ingest-conn')).Count)"        | Add-Content $out
"carrier threads (ForkJoinPool): $(([regex]::Matches($raw,'ForkJoinPool-1-worker')).Count)" | Add-Content $out
"scheduler threads (gw-sched)  : $(([regex]::Matches($raw,'gw-sched')).Count)"           | Add-Content $out
"process RSS (MB)              : $([math]::Round((Get-Process -Id $srv).WorkingSet64/1MB,1))" | Add-Content $out

Get-Content $out
Write-Host "`nWrote $out" -ForegroundColor Green