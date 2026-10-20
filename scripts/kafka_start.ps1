# Start a local single-node Kafka (KRaft) broker. Keep this window open.
# Uses $env:KAFKA_HOME, else .\kafka_2.13-4.1.2, else C:\k\kafka. Requires Java (JDK 17+; you have 21).
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
$py = ".\.venv\Scripts\python.exe"
$props = (& $py scripts\kafka_prepare.py | Select-Object -Last 1)
$kafka = if ($env:KAFKA_HOME) { $env:KAFKA_HOME } elseif (Test-Path .\kafka_2.13-4.1.2\libs) { (Resolve-Path .\kafka_2.13-4.1.2).Path } else { "C:\k\kafka" }
& "$kafka\bin\windows\kafka-server-start.bat" $props
