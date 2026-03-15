// ---------- Config ----------
// If you serve frontend separately (e.g. VSCode Live Server), set API_BASE to your Flask URL.
const API_BASE = 'http://127.0.0.1:5000';

const statusEl = document.getElementById('status');

// ---------- Map ----------
const map = L.map('map', { zoomControl: true }).setView([53.3498, -6.2603], 13);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 19,
  attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);

let markersById = new Map();

function availabilityClass(available, capacity, status) {
  if (!status || String(status).toUpperCase() !== 'OPEN') return 'off';
  if (!capacity || capacity <= 0) return 'off';
  const ratio = available / capacity;
  if (ratio >= 0.5) return 'ok';
  if (ratio >= 0.2) return 'mid';
  return 'low';
}

function markerStyle(cls) {
  // Use simple circle markers with color via CSS-like mapping
  const color = ({ ok: '#4ade80', mid: '#fbbf24', low: '#f87171', off: '#94a3b8' })[cls] || '#94a3b8';
  return {
    radius: 8,
    color,
    weight: 2,
    fillColor: color,
    fillOpacity: 0.85,
  };
}

async function fetchJSON(path) {
  const res = await fetch(API_BASE + path);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }
  return res.json();
}

// ---------- Station Forecast Modal ----------
const stationModal = document.getElementById('stationModal');
const stationTitle = document.getElementById('stationTitle');
const stationSub = document.getElementById('stationSub');
const closeStation = document.getElementById('closeStation');
let stationChart;

function openModal(modal) {
  modal.setAttribute('aria-hidden', 'false');
}
function closeModal(modal) {
  modal.setAttribute('aria-hidden', 'true');
}

stationModal.addEventListener('click', (e) => {
  if (e.target?.dataset?.close) closeModal(stationModal);
});
closeStation.addEventListener('click', () => closeModal(stationModal));

function renderStationChart(labels, values) {
  const ctx = document.getElementById('stationChart');
  if (stationChart) stationChart.destroy();
  stationChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: 'Predicted available bikes',
        data: values,
        tension: 0.25,
        pointRadius: 0,
        borderWidth: 2
      }]
    },
    options: {
      responsive: true,
      plugins: {
        legend: { display: true },
        tooltip: { mode: 'index', intersect: false }
      },
      interaction: { mode: 'index', intersect: false },
      scales: {
        x: { ticks: { maxTicksLimit: 10 } },
        y: { beginAtZero: true }
      }
    }
  });
}

async function showStationForecast(station) {
  stationTitle.textContent = station.name || `Station ${station.station_id}`;
  stationSub.textContent = `${station.available_bikes} bikes / ${station.capacity} capacity • ${station.address || ''}`;
  openModal(stationModal);

  try {
    const rows = await fetchJSON(`/api/bikes/forecast?station_id=${station.station_id}&hours=24`);
    if (!rows.length) {
      renderStationChart(['No data'], [0]);
      return;
    }
    const labels = rows.map(r => {
      const d = new Date(r.forecast_time);
      return d.toLocaleString(undefined, { hour: '2-digit', minute: '2-digit', month: 'short', day: '2-digit' });
    });
    const values = rows.map(r => Math.round(Number(r.available_bike) * 10) / 10);
    renderStationChart(labels, values);
  } catch (err) {
    renderStationChart(['Error'], [0]);
    console.error(err);
  }
}

// ---------- Weather ----------
const weatherCard = document.getElementById('weatherCard');
const weatherMain = document.getElementById('weatherMain');
const wTemp = document.getElementById('wTemp');
const wWind = document.getElementById('wWind');
const wHum = document.getElementById('wHum');
const wTime = document.getElementById('wTime');

const weatherModal = document.getElementById('weatherModal');
const closeWeather = document.getElementById('closeWeather');
const weatherSub = document.getElementById('weatherSub');
let weatherChart;

weatherModal.addEventListener('click', (e) => {
  if (e.target?.dataset?.close) closeModal(weatherModal);
});
closeWeather.addEventListener('click', () => closeModal(weatherModal));

function renderWeatherChart(labels, temps, winds) {
  const ctx = document.getElementById('weatherChart');
  if (weatherChart) weatherChart.destroy();
  weatherChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [
        { label: 'Temp (°C)', data: temps, tension: 0.25, pointRadius: 0, borderWidth: 2 },
        { label: 'Wind (m/s)', data: winds, tension: 0.25, pointRadius: 0, borderWidth: 2 }
      ]
    },
    options: {
      responsive: true,
      plugins: { tooltip: { mode: 'index', intersect: false } },
      interaction: { mode: 'index', intersect: false },
      scales: { x: { ticks: { maxTicksLimit: 10 } }, y: { beginAtZero: true } }
    }
  });
}

async function loadWeatherCard() {
  try {
    const w = await fetchJSON('/api/weather/current');
    weatherMain.textContent = `${w.weather_main || ''} — ${w.weather_description || ''}`;
    wTemp.textContent = `${Math.round(Number(w.temperature) * 10) / 10}°C`;
    wWind.textContent = `${Math.round(Number(w.wind_speed) * 10) / 10} m/s`;
    wHum.textContent = `${w.humidity}%`;
    const d = new Date(w.observed_at);
    wTime.textContent = d.toLocaleString();
  } catch (err) {
    weatherMain.textContent = 'No data';
    console.error(err);
  }
}

async function showWeatherForecast() {
  openModal(weatherModal);
  weatherSub.textContent = 'Loading…';
  try {
    const rows = await fetchJSON('/api/weather/forecast?hours=72');
    weatherSub.textContent = rows.length ? `Points: ${rows.length}` : 'No forecast data';
    if (!rows.length) {
      renderWeatherChart(['No data'], [0], [0]);
      return;
    }
    const labels = rows.map(r => {
      const d = new Date(r.forecast_time);
      return d.toLocaleString(undefined, { weekday: 'short', hour: '2-digit' });
    });
    const temps = rows.map(r => Math.round(Number(r.temperature) * 10) / 10);
    const winds = rows.map(r => Math.round(Number(r.wind_speed) * 10) / 10);
    renderWeatherChart(labels, temps, winds);
  } catch (err) {
    weatherSub.textContent = 'Error loading forecast';
    renderWeatherChart(['Error'], [0], [0]);
    console.error(err);
  }
}

weatherCard.addEventListener('click', showWeatherForecast);
weatherCard.addEventListener('keydown', (e) => {
  if (e.key === 'Enter' || e.key === ' ') showWeatherForecast();
});

// ---------- Data refresh loop ----------
async function refreshBikes() {
  const rows = await fetchJSON('/api/bikes/current');

  const seen = new Set();
  for (const s of rows) {
    seen.add(s.station_id);
    const cls = availabilityClass(s.available_bikes, s.capacity, s.status);

    if (!markersById.has(s.station_id)) {
      const marker = L.circleMarker([s.latitude, s.longitude], markerStyle(cls));
      marker.addTo(map);
      marker.on('click', () => handleMarkerClick(s));
      markersById.set(s.station_id, { marker, station: s, cls });
    } else {
      const entry = markersById.get(s.station_id);
      entry.station = s;
      if (entry.cls !== cls) {
        entry.marker.setStyle(markerStyle(cls));
        entry.cls = cls;
      }
    }

    const popupHtml = `
      <div style="min-width: 220px">
        <div style="font-weight:700;margin-bottom:4px">${escapeHtml(s.name || 'Station ' + s.station_id)}</div>
        <div style="color:#444; font-size:12px">${escapeHtml(s.address || '')}</div>
        <div style="margin-top:8px">
          <b>${s.available_bikes}</b> bikes / <b>${s.capacity}</b> capacity
        </div>
        <div style="margin-top:6px; font-size:12px; color:#444">Status: ${escapeHtml(String(s.status || ''))}</div>
      </div>
    `;
    markersById.get(s.station_id).marker.bindPopup(popupHtml);
  }

  // remove vanished stations
  for (const [id, entry] of markersById.entries()) {
    if (!seen.has(id)) {
      map.removeLayer(entry.marker);
      markersById.delete(id);
    }
  }

  statusEl.textContent = `Stations: ${rows.length} • Updated: ${new Date().toLocaleTimeString()}`;
}

function escapeHtml(str) {
  return String(str)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

async function boot() {
  try {
    await fetchJSON('/api/health');
    statusEl.textContent = 'Connected';
  } catch {
    statusEl.textContent = 'API not reachable (check backend)';
  }

  await loadWeatherCard();
  await refreshBikes();

  // Bikes: refresh every 60s (backend updates every 5 min)
  setInterval(() => refreshBikes().catch(console.error), 60 * 1000);

  // Weather: refresh every 10 min (backend updates hourly)
  setInterval(() => loadWeatherCard().catch(console.error), 10 * 60 * 1000);
}

boot().catch(console.error);


// ---------- K-06: Journey Planner Logic ----------
let startPoint = null;
let endPoint = null;
let routingControl = null;

const startInput = document.getElementById('startStation');
const endInput = document.getElementById('endStation');


function handleMarkerClick(station) {
    showStationForecast(station);
    if (!startPoint) {
        startPoint = { lat: station.latitude, lng: station.longitude, name: station.name };
        startInput.value = `From: ${station.name}`;
    } else if (!endPoint) {
        endPoint = { lat: station.latitude, lng: station.longitude, name: station.name };
        endInput.value = `To: ${station.name}`;
    } else {
        
        startPoint = { lat: station.latitude, lng: station.longitude, name: station.name };
        endPoint = null;
        startInput.value = `From: ${station.name}`;
        endInput.value = "";
    }
}


function calculateRoute() {
    if (!startPoint || !endPoint) {
        alert("Please click two stations on the map first!");
        return;
    }

    if (routingControl) map.removeControl(routingControl);

    routingControl = L.Routing.control({
        waypoints: [
            L.latLng(startPoint.lat, startPoint.lng),
            L.latLng(endPoint.lat, endPoint.lng)
        ],
        routeWhileDragging: false,
        addWaypoints: false,
        showAlternatives: true,
        altLineOptions: { styles: [{ color: 'black', opacity: 0.15, weight: 9 }] }
    }).addTo(map);

    document.getElementById('status').innerText = 'Route Found';
}


function clearRoute() {
    if (routingControl) map.removeControl(routingControl);
    startPoint = null;
    endPoint = null;
    startInput.value = "";
    endInput.value = "";
    document.getElementById('routeInstructions').innerHTML = "";
}

document.getElementById('btnPlan').addEventListener('click', calculateRoute);
document.getElementById('btnClearRoute').addEventListener('click', clearRoute);
