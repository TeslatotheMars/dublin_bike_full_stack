# Cityflow Dublin Bikes Dashboard

Cityflow is a Dublin Bikes web dashboard with a Flask API, MySQL data store, static frontend, and a machine-learning pipeline for station availability forecasts.

The app shows live Dublin Bikes station availability on an interactive map, 24-hour station-level bike availability forecasts, current and forecast weather for Dublin, simple account login/register flows, a journey planner, and a small built-in help assistant.

## Features

- Live Dublin Bikes station map with availability status markers.
- 24-hour bike availability forecast per station.
- Current Dublin weather and 72-hour weather forecast chart.
- Journey planner between selected bike stations.
- Login, registration, and profile API backed by MySQL and JWT.
- Bike/weather ingestion jobs using JCDecaux and OpenWeather APIs.
- Analytics job that trains a model and writes bike forecasts.
- Demo-data fallback for local development when real historical data is not available.

## Tech Stack

- Backend: Python, Flask, Flask-CORS, Flask-JWT-Extended, PyMySQL
- Data: MySQL
- ML/data: pandas, scikit-learn, joblib
- Frontend: HTML, CSS, JavaScript
- Maps/charts: Leaflet, Leaflet Routing Machine, Chart.js
- External APIs: JCDecaux Dublin Bikes, OpenWeather

## Repository Structure

```text
backend/
  api/
    app.py              # Flask app and API routes
    auth.py             # Login, registration, profile routes
    bike.py             # JCDecaux ingestion repository
    weather.py          # OpenWeather ingestion repository
    analytics.py        # Model training and forecast writer
    chat_api.py         # Help assistant routes
    db.py               # MySQL connection helper
  jobs/
    run_bike_ingest.py
    run_weather_ingest.py
    run_analytics.py
  data/
    data_check.py       # Checks real data availability
    demo_data.py        # Synthetic fallback data
  tests/
frontend/
  index.html
  styles.css
  app.js
.env.example
requirements.txt
LOCAL_SETUP.md
QUICK_START.md
```

## Prerequisites

- Python 3.12 recommended.
- MySQL 8.x.
- JCDecaux API key.
- OpenWeather API key with forecast access.

This project has been tested around Python 3.12 dependency versions. If you hit NumPy or pandas binary import errors, recreate the virtual environment with Python 3.12.

## Setup

From the repository root:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install -r backend\requirements.txt
```

Create your environment file:

```powershell
Copy-Item .env.example .env -Force
Copy-Item .env backend\.env -Force
```

Update `.env` with your local database and API credentials:

```env
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=
MYSQL_DB=bike_db

JCDECAUX_API_KEY=YOUR_JCDECAUX_KEY
JCDECAUX_CONTRACT=dublin

OPENWEATHER_API_KEY=YOUR_OPENWEATHER_KEY
OWM_LAT=53.3498
OWM_LON=-6.2603
OWM_UNITS=metric

API_PORT=5000
```

Before running commands in a new PowerShell session, load the environment variables:

```powershell
Get-Content .env | ForEach-Object {
  if ($_ -match '^\s*#' -or $_ -match '^\s*$') { return }
  $name, $value = $_ -split '=', 2
  Set-Item -Path "Env:$name" -Value $value
}
```

## Database

Create the MySQL database if it does not already exist:

```sql
CREATE DATABASE bike_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

The bike, weather, stats, and forecast tables are created by the ingestion and analytics repositories when the jobs run.

For account support, create the `users` table. The current auth code expects `email` and `balance` columns:

```sql
CREATE TABLE users (
  id INT AUTO_INCREMENT PRIMARY KEY,
  username VARCHAR(50) NOT NULL UNIQUE,
  email VARCHAR(255),
  password_hash VARCHAR(255) NOT NULL,
  balance DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Running Locally

Run the data jobs from the `backend` directory using module mode:

```powershell
cd backend
python -m jobs.run_bike_ingest
python -m jobs.run_weather_ingest
python -m jobs.run_analytics
```

Start the API:

```powershell
cd backend
python -m api.app
```

Health check:

```text
http://127.0.0.1:5000/api/health
```

To serve the frontend through Flask as well:

```powershell
cd backend
$env:FRONTEND_DIR = (Resolve-Path ..\frontend).Path
python -m api.app
```

Then open:

```text
http://127.0.0.1:5000
```

The frontend JavaScript calls the API at `http://127.0.0.1:5000`.

## API Endpoints

| Method | Endpoint | Description |
| --- | --- | --- |
| `GET` | `/api/health` | Check API and database connectivity |
| `GET` | `/api/bikes/current` | Get current station availability |
| `GET` | `/api/bikes/forecast?station_id=1&hours=24` | Get station bike forecast |
| `GET` | `/api/weather/current` | Get current Dublin weather |
| `GET` | `/api/weather/forecast?hours=72` | Get weather forecast |
| `POST` | `/api/auth/signup` | Register a user |
| `POST` | `/api/auth/login` | Login and receive a JWT |
| `GET` | `/api/auth/profile` | Get profile for authenticated user |
| `GET` | `/api/chat/categories` | Get assistant categories |
| `POST` | `/api/chat/answer` | Get assistant answer by question ID |

## Analytics and Demo Data

The analytics job trains a bike availability model and stores forecasts in MySQL:

```powershell
cd backend
python -m jobs.run_analytics
```

By default, the analytics repository can fall back to generated demo data when there is not enough real historical bike data. This keeps local development usable before the ingest jobs have accumulated enough history.

Disable fallback:

```powershell
$env:USE_DEMO_FALLBACK = "0"
python -m jobs.run_analytics
```

More detail is available in `QUICK_START.md`, `backend/DEMO_DATA_STRATEGY.md`, and `backend/INTEGRATION_GUIDE.md`.

## Tests

Run the backend tests from the repository root:

```powershell
pytest
```

Most route tests mock database access, so they can run without a fully populated local database.

## Troubleshooting

If imports fail with `ModuleNotFoundError: No module named 'api'`, run backend jobs from the `backend` directory with `python -m ...`.

If database-backed routes fail, check that MySQL is running, `.env` is loaded, the database exists, and the expected tables have been created.

If bike or weather data is empty, run the ingestion jobs first and confirm your JCDecaux and OpenWeather keys are valid.

If analytics uses demo data unexpectedly, inspect recent `bike_history` rows and set `USE_DEMO_FALLBACK=0` when you want the job to fail instead of falling back.

See `LOCAL_SETUP.md` for a more detailed Windows setup guide.







