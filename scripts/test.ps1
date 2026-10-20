Set-Location (Split-Path $PSScriptRoot -Parent)
& .\.venv\Scripts\python.exe -m pytest -rs
& .\.venv\Scripts\python.exe -m ruff check orbitalpulse tests scripts
