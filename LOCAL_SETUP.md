# Local Setup Guide

This guide explains how to configure the environment and run the project locally on Windows PowerShell.

## 1. Prerequisites

- Python `3.12` (recommended; avoid `3.14` for this project dependency set)
- MySQL `8.x` running locally
- JCDecaux API key
- OpenWeather API key (One Call API access)

## 2. Clone and enter project

```powershell
cd C:\dev
git clone <your-repo-url> dublin_bike_frontend_backend
cd dublin_bike_frontend_backend
```

## 3. Create virtual environment

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## 4. Configure environment variables

1. Copy `.env.example` to `.env` in project root:

```powershell
Copy-Item .env.example .env -Force
```

2. Fill required values in `.env`:
- `MYSQL_HOST`
- `MYSQL_PORT`
- `MYSQL_USER`
- `MYSQL_PASSWORD`
- `MYSQL_DB`
- `JCDECAUX_API_KEY`
- `JCDECAUX_CONTRACT` (usually `dublin`)
- `OPENWEATHER_API_KEY`
- `OWM_LAT`, `OWM_LON`, `OWM_UNITS`
- `API_PORT` (optional, default `5000`)

3. Also copy to `backend/.env` (used by `run_weather_ingest.py`):

```powershell
Copy-Item .env backend\.env -Force
```

## 5. Prepare database

Make sure MySQL service is running and target DB user has create/alter/insert/select privileges.

If you previously hit partition errors during local testing, reset bike tables:

```sql
USE bike_db;
DROP TABLE IF EXISTS bike_history;
DROP TABLE IF EXISTS bike_current;
```

## 6. Load `.env` into current shell

From project root:

```powershell
Get-Content .env | ForEach-Object {
  if ($_ -match '^\s*#' -or $_ -match '^\s*$') { return }
  $name, $value = $_ -split '=', 2
  Set-Item -Path "Env:$name" -Value $value
}
```

## 7. Run ingestion and analytics jobs

Use module mode from `backend` directory:

```powershell
cd backend
python -m jobs.run_bike_ingest
python -m jobs.run_weather_ingest
python -m jobs.run_analytics
```

## 8. Start backend API (and optionally frontend)

### API only

```powershell
cd backend
python -m api.app
```

API health check:
- `http://127.0.0.1:5000/api/health`

### API + frontend from Flask

```powershell
cd backend
$env:FRONTEND_DIR = (Resolve-Path ..\frontend).Path
python -m api.app
```

Open:
- `http://127.0.0.1:5000`

## 9. Common issues

### `ModuleNotFoundError: No module named 'api'`

Cause: running `python jobs/run_xxx.py` directly.

Fix: run as module from `backend`:

```powershell
python -m jobs.run_bike_ingest
```

### `VALUES LESS THAN value must be strictly increasing for each partition`

Cause: existing partition boundaries conflict in `bike_history`.

Fix: drop and recreate bike tables (see Section 5), then rerun bike ingest.

### NumPy/Pandas import error mentioning `cp312` vs `cpython-314`

Cause: broken mixed virtual environment.

Fix: recreate virtual environment with Python 3.12:

```powershell
deactivate
Remove-Item -Recurse -Force .venv
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```
