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
frontend/
  index.html
  styles.css
  app.js
.env.example
requirements.txt
```

<img width="1882" height="826" alt="image" src="https://github.com/user-attachments/assets/b02018a8-73eb-49d8-bd62-3a2269c41ccf" />
<img width="1251" height="662" alt="image" src="https://github.com/user-attachments/assets/a6ed1cec-fe5b-4522-b9ec-fca54e346810" />








