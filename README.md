# Dublin Bikes Dashboard (Frontend + Flask API)

This package adds a **frontend page** (Leaflet map + Chart.js charts) and a **Flask API layer** so the frontend **never connects to MySQL directly**.

## What you get

- Map markers for all stations, showing **available bikes / capacity**
- Click station → **24-hour bike availability forecast** line chart
- Weather card showing **current weather**; click → **72-hour weather forecast** chart
- Flask API endpoints:
  - `GET /api/bikes/current`
  - `GET /api/bikes/forecast?station_id=XX&hours=24`
  - `GET /api/weather/current`
  - `GET /api/weather/forecast?hours=72`
  - `GET /api/health`

## Folder structure

```
backend/
  api/
    app.py            # Flask API (read-only)
    bike.py           # your ingest module (JCDecaux)
    weather.py        # your ingest module (OpenWeather)
    analytics.py      # your model + forecast writer
  jobs/
    run_bike_ingest.py
    run_weather_ingest.py
    run_analytics.py
  requirements.txt
frontend/
  index.html
  styles.css
  app.js
.env.example
```

## 1) Set environment variables

Copy `.env.example` to `.env` and fill your values.

Key variables:
- MySQL: `MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB`
- JCDecaux: `JCDECAUX_API_KEY, JCDECAUX_CONTRACT=dublin`
- OpenWeather: `OPENWEATHER_API_KEY, OWM_LAT, OWM_LON`

## 2) Install backend dependencies

```bash
cd backend
python -m venv .venv
# Windows: .venv\\Scripts\\activate
# Mac/Linux: source .venv/bin/activate
pip install -r requirements.txt
```

## 3) Run the ingestion + model jobs

You can run them manually first:

```bash
# from backend/
python jobs/run_bike_ingest.py
python jobs/run_weather_ingest.py
python jobs/run_analytics.py
```

Scheduling (recommended):
- `run_bike_ingest.py` every **5 minutes**
- `run_weather_ingest.py` every **1 hour**
- `run_analytics.py` every **1 hour** (or 30 minutes)

Use **Windows Task Scheduler** or a cron job.

## 4) Run the Flask API

```bash
# from backend/
python -m api.app
```

Default port: `5000` (set `API_PORT` to change).

## 5) Run the frontend

Option A (recommended): use VSCode Live Server and open `frontend/index.html`.

Option B: serve frontend via Flask (one-command demo)

```bash
# from backend/
set FRONTEND_DIR=../frontend   # Windows (PowerShell: $env:FRONTEND_DIR="../frontend")
python -m api.app
# then open http://127.0.0.1:5000
```

If you serve frontend separately, API calls go to the same origin by default.
If you need a different backend URL, edit `frontend/app.js`:

```js
const API_BASE = 'http://127.0.0.1:5000';
```

## Notes
- DB timestamps are stored/queried in **UTC**. The browser renders them in local time.
- If `bike_forecast` is empty, run `jobs/run_analytics.py` after you have enough history + weather data.


