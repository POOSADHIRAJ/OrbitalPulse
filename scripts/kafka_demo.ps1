# Kafka mode demo. Terminal 1: .\scripts\kafka_start.ps1   Terminal 2: .\scripts\run_demo.ps1 (dashboard)
# Terminal 3 (this script): start the consumer, then publish synthetic events.
param([string[]]$Runs = @("RUN-001-CLEAN","RUN-002-DQ-DEFECTS"), [double]$Rate = 300, [double]$Redeliver = 0.02)
Set-Location (Split-Path $PSScriptRoot -Parent)
$py = ".\.venv\Scripts\python.exe"
$consumer = Start-Process $py -ArgumentList "-m","orbitalpulse.streaming.kafka_consumer" -PassThru -NoNewWindow
Start-Sleep 3
& $py -m orbitalpulse.ingestion.kafka_producer --runs $Runs --rate $Rate --redeliver $Redeliver
Write-Host "Published. The consumer keeps running (PID $($consumer.Id)); stop it with: Stop-Process -Id $($consumer.Id)"
