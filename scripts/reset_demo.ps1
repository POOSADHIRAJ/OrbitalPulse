# Delete ALL stored data and re-import the demo campaign.
Set-Location (Split-Path $PSScriptRoot -Parent)
& .\.venv\Scripts\python.exe scripts\op.py reset
& .\.venv\Scripts\python.exe scripts\op.py import-demo
