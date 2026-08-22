param(
    [int]$Connections = 10000,
    [string]$Mode = "virtual",
    [int]$SampleSeconds = 60
)

$ErrorActionPreference = "Continue"
$outFile = "audit-$Mode-$Connections.csv"

Write-Host "=== GridWeaver audit: $Mode mode, target $Connections connections ===" -ForegroundColor Cyan

$srv = (Get-NetTCPConnection -LocalPort 9099 -State Listen -ErrorAction SilentlyContinue).OwningProcess |
       Select-Object -First 1
if (-not $srv) { Write-Error "Nothing listening on 9099"; exit 1 }

"elapsed_s,connections,platform_threads,rss_mb,heap_used_mb" | Set-Content $outFile

$t0 = Get-Date
for ($i = 0; $i -lt $SampleSeconds; $i++) {
    try {
        $th  = Invoke-RestMethod "http://localhost:8080/debug/threads" -TimeoutSec 5
        $rss = [math]::Round((Get-Process -Id $srv).WorkingSet64 / 1MB, 1)
        $el  = [math]::Round(((Get-Date) - $t0).TotalSeconds, 1)

        "$el,$($th.activeConnections),$($th.platformThreadsLive),$rss,$($th.heapUsedMb)" |
            Add-Content $outFile

        Write-Host ("{0,6}s  conn {1,6}  platThreads {2,6}  RSS {3,7} MB  heap {4,5} MB" -f `
            $el, $th.activeConnections, $th.platformThreadsLive, $rss, $th.heapUsedMb)
    } catch {
        Write-Host "sample failed: $_" -ForegroundColor Yellow
    }
    Start-Sleep -Seconds 1
}

Write-Host "`nWrote $outFile" -ForegroundColor Green