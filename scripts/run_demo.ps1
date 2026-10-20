# Generate + import the synthetic demo campaign, then start the dashboard (no Kafka needed).
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
$py = ".\.venv\Scripts\python.exe"
& $py scripts\op.py import-demo
Write-Host "Open http://127.0.0.1:8000  (API docs: http://127.0.0.1:8000/docs)"
& $py scripts\op.py serve
