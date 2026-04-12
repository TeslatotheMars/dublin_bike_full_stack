/* =============================================
   Cityflow · app.js
   Backend: http://127.0.0.1:5000
   ============================================= */

const API = 'http://127.0.0.1:5000';

// ── Screen router ────────────────────────────
function goTo(id) {
  document.querySelectorAll('.screen').forEach(s => {
    s.classList.remove('active');
    s.style.display = 'none';
  });
  const el = document.getElementById(id);
  if (!el) return;
  el.style.display = 'flex';
  el.classList.add('active');
  if (id === 'screen-map' && !mapReady) bootMap();
}

document.getElementById('homeSignupBtn').addEventListener('click', () => { switchTab('register'); goTo('screen-auth'); });
document.getElementById('homeLoginBtn').addEventListener('click',  () => { switchTab('login');    goTo('screen-auth'); });
document.getElementById('authLogoBack').addEventListener('click', () => goTo('screen-home'));
document.getElementById('skipToMap').addEventListener('click', (e) => { e.preventDefault(); goTo('screen-map'); });
document.getElementById('mapBackHome').addEventListener('click',    () => goTo('screen-home'));
document.getElementById('mapLoginBtn').addEventListener('click',    () => { switchTab('login');    goTo('screen-auth'); });
document.getElementById('mapRegisterBtn').addEventListener('click', () => { switchTab('register'); goTo('screen-auth'); });

// ── Tab switcher ─────────────────────────────
function switchTab(tab) {
  const isLogin = tab === 'login';
  document.getElementById('tabLogin').classList.toggle('active', isLogin);
  document.getElementById('tabRegister').classList.toggle('active', !isLogin);
  document.getElementById('panel-login').style.display    = isLogin ? 'block' : 'none';
  document.getElementById('panel-register').style.display = isLogin ? 'none'  : 'block';
  clearAuthMessages();
}
window.switchTab = switchTab;

function clearAuthMessages() {
  document.getElementById('authError').style.display   = 'none';
  document.getElementById('authSuccess').style.display = 'none';
}
function showErr(msg) { const el = document.getElementById('authError');   el.textContent = msg; el.style.display = 'block'; }
function showOk(msg)  { const el = document.getElementById('authSuccess'); el.textContent = msg; el.style.display = 'block'; }

function togglePw(inputId, btn) {
  const input = document.getElementById(inputId);
  const show  = input.type === 'password';
  input.type  = show ? 'text' : 'password';
  btn.style.opacity = show ? '1' : '.5';
}
window.togglePw = togglePw;

// ── Helpers ───────────────────────────────────
async function api(path, opts) {
  const res = await fetch(API + path, opts);
  if (!res.ok) { const d = await res.json().catch(() => ({})); throw new Error(d.error || res.statusText); }
  return res.json();
}
function esc(s) {
  return String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}
function wxEmoji(main = '') {
  const m = main.toLowerCase();
  if (m.includes('clear'))   return '☀️';
  if (m.includes('cloud'))   return '☁️';
  if (m.includes('rain'))    return '🌧️';
  if (m.includes('drizzle')) return '🌦️';
  if (m.includes('snow'))    return '❄️';
  if (m.includes('thunder')) return '⛈️';
  if (m.includes('mist') || m.includes('fog')) return '🌫️';
  return '🌤️';
}

// ── Login ─────────────────────────────────────
document.getElementById('loginSubmitBtn').addEventListener('click', async () => {
  clearAuthMessages();
  const username = document.getElementById('loginUsername').value.trim();
  const password = document.getElementById('loginPassword').value;
  if (!username || !password) { showErr('Please enter your username and password.'); return; }
  const btn = document.getElementById('loginSubmitBtn');
  btn.disabled = true; btn.textContent = 'Signing in…';
  try {
    const data = await api('/api/auth/login', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password })
    });
    if (data.access_token) localStorage.setItem('cf_token', data.access_token);
    showOk('✓ Signed in successfully!');
    setTimeout(() => goTo('screen-map'), 700);
  } catch (err) {
    showErr(err.message || 'Invalid credentials.');
  } finally { btn.disabled = false; btn.textContent = 'Sign In'; }
});
['loginUsername','loginPassword'].forEach(id =>
  document.getElementById(id)?.addEventListener('keydown', e => {
    if (e.key === 'Enter') document.getElementById('loginSubmitBtn').click();
  })
);

// ── Register ──────────────────────────────────
document.getElementById('registerSubmitBtn').addEventListener('click', async () => {
  clearAuthMessages();
  const username = document.getElementById('registerUsername').value.trim();
  const password = document.getElementById('registerPassword').value;
  const confirm  = document.getElementById('registerConfirm').value;
  if (!username || !password)   { showErr('All fields are required.');           return; }
  if (password !== confirm)     { showErr('Passwords do not match.');             return; }
  if (password.length < 6)      { showErr('Password must be at least 6 chars.'); return; }
  const btn = document.getElementById('registerSubmitBtn');
  btn.disabled = true; btn.textContent = 'Creating account…';
  try {
    await api('/api/auth/signup', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password })
    });
    showOk('🎉 Account created! Signing you in…');
    const data = await api('/api/auth/login', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password })
    });
    if (data.access_token) localStorage.setItem('cf_token', data.access_token);
    setTimeout(() => goTo('screen-map'), 900);
  } catch (err) {
    showErr(err.message || 'Registration failed. Try a different username.');
  } finally { btn.disabled = false; btn.textContent = 'Create Account'; }
});

// ============================================================
//  PROFILE DROPDOWN
// ============================================================

async function loadProfileInNav() {
  const token = localStorage.getItem('cf_token');
  if (!token) return;
  try {
    const res = await fetch(`${API}/api/auth/profile`, {
      headers: { 'Authorization': `Bearer ${token}` }
    });
    if (!res.ok) { localStorage.removeItem('cf_token'); return; }
    const user = await res.json();
    const initials = user.username.substring(0, 2).toUpperCase();

    // Hide login/register, show avatar
    document.getElementById('mapLoginBtn').style.display    = 'none';
    document.getElementById('mapRegisterBtn').style.display = 'none';
    document.getElementById('profileWrapper').style.display = 'block';

    // Fill avatar
    document.getElementById('profileAvatar').textContent      = initials;
    document.getElementById('profileAvatarLarge').textContent = initials;

    // Fill modal data
    document.getElementById('profileName').textContent     = user.username;
    document.getElementById('profileSince').textContent    = 'Member since ' + user.member_since;
    document.getElementById('profileEmail').textContent    = user.email;
    document.getElementById('profileUsername').textContent = user.username;
    document.getElementById('profileBalance').textContent  = '€' + user.balance.toFixed(2);
  } catch(e) { console.warn('Profile load failed', e); }
}

window.toggleProfileModal = function() {
  const modal = document.getElementById('profileModal');
  if (!modal) return;
  const isOpen = modal.style.display === 'flex';
  modal.style.display = isOpen ? 'none' : 'flex';
};

window.signOut = function() {
  localStorage.removeItem('cf_token');
  document.getElementById('profileModal').style.display   = 'none';
  document.getElementById('profileWrapper').style.display = 'none';
  document.getElementById('mapLoginBtn').style.display    = '';
  document.getElementById('mapRegisterBtn').style.display = '';
  goTo('screen-home');
};

// Close modal when clicking the dark backdrop
document.addEventListener('click', e => {
  const modal = document.getElementById('profileModal');
  if (modal && e.target === modal) modal.style.display = 'none';
});

// ── MAP ───────────────────────────────────────
let map, markers = new Map(), mapReady = false;

function initMap() {
  if (map) return;
  map = L.map('map').setView([53.3498, -6.2603], 13);
  L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
    maxZoom: 19, attribution: '&copy; OpenStreetMap &copy; CARTO'
  }).addTo(map);
  markers = new Map();
}

function availClass(bikes, cap, status) {
  if (String(status || '').toUpperCase() !== 'OPEN' || !cap) return 'off';
  const r = bikes / cap;
  if (r >= 0.5) return 'ok';
  if (r >= 0.2) return 'mid';
  return 'low';
}
const DOT = { ok: '#3dad77', mid: '#f59e0b', low: '#ef4444', off: '#9ca3af' };
function mStyle(cls) {
  return { radius: 8, color: '#fff', weight: 2.5, fillColor: DOT[cls], fillOpacity: 0.9 };
}

async function refreshBikes() {
  const rows = await api('/api/bikes/current');
  const seen = new Set();
  for (const s of rows) {
    seen.add(s.station_id);
    const cls = availClass(s.available_bikes, s.capacity, s.status);
    if (!markers.has(s.station_id)) {
      const m = L.circleMarker([s.latitude, s.longitude], mStyle(cls));
      m.addTo(map);
      m.on('click', () => onMarkerClick(s));
      markers.set(s.station_id, { m, s, cls });
    } else {
      const e = markers.get(s.station_id);
      e.s = s;
      if (e.cls !== cls) { e.m.setStyle(mStyle(cls)); e.cls = cls; }
    }
    const popup = `
      <div style="font-family:Inter,sans-serif;min-width:190px">
        <div style="font-family:'Plus Jakarta Sans',sans-serif;font-weight:700;font-size:13px;margin-bottom:3px">${esc(s.name || 'Station ' + s.station_id)}</div>
        <div style="font-size:11px;color:#9ca3af;margin-bottom:7px">${esc(s.address || '')}</div>
        <div style="font-size:12px"><span style="color:#3dad77;font-weight:700">${s.available_bikes}</span> bikes available</div>
        <div style="font-size:11px;color:#9ca3af;margin-top:4px">Capacity: ${s.capacity} · ${esc(String(s.status || ''))}</div>
        <div style="font-size:11px;color:#3dad77;margin-top:5px">Click for 24h AI forecast →</div>
      </div>`;
    markers.get(s.station_id).m.bindPopup(popup);
  }
  for (const [id, e] of markers.entries()) {
    if (!seen.has(id)) { map.removeLayer(e.m); markers.delete(id); }
  }
  document.getElementById('status').textContent = `${rows.length} stations · ${new Date().toLocaleTimeString()}`;
}

// ── Station forecast modal ────────────────────
const stationModal = document.getElementById('stationModal');
let stationChart;
document.getElementById('closeStation').addEventListener('click', () => stationModal.setAttribute('aria-hidden','true'));
stationModal.addEventListener('click', e => { if (e.target?.dataset?.close) stationModal.setAttribute('aria-hidden','true'); });

function drawStationChart(labels, values) {
  const ctx = document.getElementById('stationChart');
  if (stationChart) stationChart.destroy();
  stationChart = new Chart(ctx, {
    type: 'line',
    data: { labels, datasets: [{ label: 'Available bikes (predicted)', data: values, borderColor: '#3dad77', backgroundColor: 'rgba(61,173,119,.08)', tension: 0.35, pointRadius: 0, borderWidth: 2.5, fill: true }] },
    options: { responsive: true, plugins: { legend: { labels: { color: '#9ca3af', font: { family: 'Inter', size: 11 } } }, tooltip: { mode: 'index', intersect: false } }, scales: { x: { ticks: { color: '#b0b7c3', maxTicksLimit: 10, font: { size: 11 } }, grid: { color: 'rgba(0,0,0,.05)' } }, y: { beginAtZero: true, ticks: { color: '#b0b7c3', font: { size: 11 } }, grid: { color: 'rgba(0,0,0,.05)' } } } }
  });
}

async function showStationForecast(s) {
  document.getElementById('stationTitle').textContent = s.name || `Station ${s.station_id}`;
  document.getElementById('stationSub').textContent   = `${s.available_bikes} bikes / ${s.capacity} capacity · ${s.address || ''}`;
  stationModal.setAttribute('aria-hidden', 'false');
  const hint = document.getElementById('mapHint');
  if (hint) hint.style.opacity = '0';
  try {
    const rows = await api(`/api/bikes/forecast?station_id=${s.station_id}&hours=24`);
    if (!rows.length) { drawStationChart(['No data'], [0]); return; }
    drawStationChart(
      rows.map(r => new Date(r.forecast_time).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })),
      rows.map(r => r.available_bike)
    );
  } catch { drawStationChart(['Error loading forecast'], [0]); }
}

// ── Weather ───────────────────────────────────
const weatherModal = document.getElementById('weatherModal');
let weatherChart;
document.getElementById('closeWeather').addEventListener('click', () => weatherModal.setAttribute('aria-hidden','true'));
weatherModal.addEventListener('click', e => { if (e.target?.dataset?.close) weatherModal.setAttribute('aria-hidden','true'); });
document.getElementById('weatherCard').addEventListener('click', showWeatherForecast);
document.getElementById('weatherCard').addEventListener('keydown', e => { if (e.key === 'Enter') showWeatherForecast(); });
document.getElementById('showWeatherForecastBtn').addEventListener('click', showWeatherForecast);

function drawWeatherChart(labels, temps, winds) {
  const ctx = document.getElementById('weatherChart');
  if (weatherChart) weatherChart.destroy();
  weatherChart = new Chart(ctx, {
    type: 'line',
    data: { labels, datasets: [
      { label: 'Temp (°C)',  data: temps, borderColor: '#f97316', backgroundColor: 'rgba(249,115,22,.07)', tension: 0.3, pointRadius: 0, borderWidth: 2, fill: true },
      { label: 'Wind (m/s)', data: winds, borderColor: '#3b82f6', backgroundColor: 'rgba(59,130,246,.05)', tension: 0.3, pointRadius: 0, borderWidth: 2, fill: true }
    ]},
    options: { responsive: true, plugins: { legend: { labels: { color: '#9ca3af', font: { family: 'Inter', size: 11 } } }, tooltip: { mode: 'index', intersect: false } }, scales: { x: { ticks: { color: '#b0b7c3', maxTicksLimit: 10, font: { size: 11 } }, grid: { color: 'rgba(0,0,0,.05)' } }, y: { beginAtZero: true, ticks: { color: '#b0b7c3', font: { size: 11 } }, grid: { color: 'rgba(0,0,0,.05)' } } } }
  });
}

async function showWeatherForecast() {
  weatherModal.setAttribute('aria-hidden', 'false');
  document.getElementById('weatherSub').textContent = 'Loading…';
  try {
    const rows = await api('/api/weather/forecast?hours=72');
    document.getElementById('weatherSub').textContent = `${rows.length} data points · Dublin`;
    if (!rows.length) { drawWeatherChart(['No data'], [0], [0]); return; }
    drawWeatherChart(
      rows.map(r => new Date(r.forecast_time).toLocaleString([], { weekday: 'short', hour: '2-digit' })),
      rows.map(r => Math.round(Number(r.temperature) * 10) / 10),
      rows.map(r => Math.round(Number(r.wind_speed) * 10) / 10)
    );
    const hasRain = rows.slice(0, 8).some(r => (r.weather_main || '').toLowerCase().includes('rain'));
    const banner = document.getElementById('rainBanner');
    if (banner) banner.style.display = hasRain ? 'block' : 'none';
  } catch {
    document.getElementById('weatherSub').textContent = 'Could not load forecast';
    drawWeatherChart(['Error'], [0], [0]);
  }
}

async function loadWeatherCard() {
  try {
    const w = await api('/api/weather/current');
    const temp  = Math.round(Number(w.temperature) * 10) / 10;
    const wind  = Math.round(Number(w.wind_speed)  * 10) / 10;
    const emoji = wxEmoji(w.weather_main);
    const desc  = w.weather_description || w.weather_main || '—';
    document.getElementById('wIcon').textContent       = emoji;
    document.getElementById('wTemp').textContent       = `${temp}°C`;
    document.getElementById('weatherMain').textContent = desc;
    document.getElementById('wEmoji').textContent      = emoji;
    document.getElementById('wTempBig').textContent    = `${temp}°C`;
    document.getElementById('wMainBig').textContent    = desc;
    document.getElementById('wWind').textContent       = `${wind} m/s`;
    document.getElementById('wHum').textContent        = `${w.humidity}%`;
    document.getElementById('wTime').textContent       = new Date(w.observed_at).toLocaleTimeString();
  } catch (e) { console.warn('Weather unavailable', e); }
}

// ── Journey planner ───────────────────────────
let startPt = null, endPt = null, routeCtrl = null;

function updatePlannerUI() {
  const stepA = document.getElementById('stepA');
  const stepB = document.getElementById('stepB');
  const startLabel = document.getElementById('startStationLabel');
  const endLabel   = document.getElementById('endStationLabel');
  const hint       = document.getElementById('routeInstructions');
  const summary    = document.getElementById('routeSummary');
  const btnPlan    = document.getElementById('btnPlan');

  if (!startPt) {
    // Nothing selected
    stepA.classList.remove('filled','active'); stepA.classList.add('active');
    stepB.classList.remove('filled','active');
    startLabel.textContent = 'Click a pin on the map';
    startLabel.classList.add('empty');
    endLabel.textContent = 'Click a pin on the map';
    endLabel.classList.add('empty');
    hint.innerHTML = 'Click any <strong>station pin</strong> on the map to set your start point';
    summary.style.display = 'none';
    btnPlan.disabled = true;
  } else if (!endPt) {
    // Start selected, waiting for end
    stepA.classList.remove('active'); stepA.classList.add('filled');
    stepB.classList.remove('filled','active'); stepB.classList.add('active');
    startLabel.textContent = startPt.name;
    startLabel.classList.remove('empty');
    endLabel.textContent = 'Click a pin on the map';
    endLabel.classList.add('empty');
    hint.innerHTML = '✅ Start set! Now click your <strong>destination station</strong>';
    summary.style.display = 'none';
    btnPlan.disabled = true;
  } else {
    // Both selected
    stepA.classList.remove('active'); stepA.classList.add('filled');
    stepB.classList.remove('active'); stepB.classList.add('filled');
    startLabel.textContent = startPt.name;
    startLabel.classList.remove('empty');
    endLabel.textContent = endPt.name;
    endLabel.classList.remove('empty');
    hint.innerHTML = '✅ Both stations set — click <strong>Find Cycling Route</strong>!';
    summary.style.display = 'none';
    btnPlan.disabled = false;
  }
}

function onMarkerClick(s) {
  showStationForecast(s);
  if (!startPt) {
    startPt = { lat: s.latitude, lng: s.longitude, name: s.name };
  } else if (!endPt) {
    endPt = { lat: s.latitude, lng: s.longitude, name: s.name };
  } else {
    // Reset and start over
    startPt = { lat: s.latitude, lng: s.longitude, name: s.name };
    endPt   = null;
    if (routeCtrl) { map.removeControl(routeCtrl); routeCtrl = null; }
  }
  updatePlannerUI();
}

document.getElementById('btnPlan').addEventListener('click', () => {
  if (!startPt || !endPt) return;
  if (routeCtrl) map.removeControl(routeCtrl);

  routeCtrl = L.Routing.control({
    waypoints: [L.latLng(startPt.lat, startPt.lng), L.latLng(endPt.lat, endPt.lng)],
    routeWhileDragging: false,
    addWaypoints: false,
    showAlternatives: false,
    lineOptions: { styles: [{ color: '#3dad77', weight: 5, opacity: 0.8 }] },
    createMarker: () => null,  // hide default markers, use our pins
  }).addTo(map);

  // Show summary once route is found
  routeCtrl.on('routesfound', function(e) {
    const route    = e.routes[0];
    const distKm   = (route.summary.totalDistance / 1000).toFixed(1);
    const mins     = Math.round(route.summary.totalTime / 60);
    const cycMins  = Math.max(Math.round(distKm / 0.25), 3); // ~15km/h cycling

    const summary = document.getElementById('routeSummary');
    document.getElementById('routeSummaryText').textContent =
      `${startPt.name} → ${endPt.name}`;
    document.getElementById('routeSummaryTime').textContent =
      `~${cycMins} min cycling · ${distKm} km`;
    summary.style.display = 'block';

    document.getElementById('routeInstructions').innerHTML =
      `🚲 Route found! Approx <strong>${cycMins} mins</strong> cycling`;
  });
});

document.getElementById('btnClearRoute').addEventListener('click', () => {
  if (routeCtrl) { map.removeControl(routeCtrl); routeCtrl = null; }
  startPt = null; endPt = null;
  updatePlannerUI();
});

// Initialise planner UI on load
updatePlannerUI();

// ── Boot map ──────────────────────────────────
async function bootMap() {
  mapReady = true;
  initMap();
  try { await api('/api/health'); document.getElementById('status').textContent = 'Connected'; }
  catch { document.getElementById('status').textContent = 'Backend not reachable'; }
  await loadWeatherCard().catch(console.warn);
  await refreshBikes().catch(console.warn);
  await loadProfileInNav().catch(console.warn);
  setInterval(() => refreshBikes().catch(console.warn),        60_000);
  setInterval(() => loadWeatherCard().catch(console.warn), 10 * 60_000);
  initChatbot();
}

document.addEventListener('keydown', e => {
  if (e.key !== 'Escape') return;
  stationModal.setAttribute('aria-hidden', 'true');
  weatherModal.setAttribute('aria-hidden', 'true');
});

goTo('screen-home');

// ============================================================
//  CHATBOT
// ============================================================
function initChatbot() {
  const msgsEl = document.getElementById('chatMsgs');
  if (!msgsEl) return;

  function addMsg(html, isUser) {
    const d = document.createElement('div');
    if (isUser) {
      d.className = 'cb-user-msg';
      d.innerHTML = `<div class="cb-user-bubble">${html}</div>`;
    } else {
      d.className = 'cb-bot-msg';
      d.innerHTML = `
        <div class="cb-bot-icon">
          <svg width="11" height="11" viewBox="0 0 26 26" fill="none">
            <circle cx="13" cy="13" r="10" stroke="white" stroke-width="2"/>
            <path d="M13 7v6l4 4" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
          </svg>
        </div>
        <div class="cb-bot-bubble">${html}</div>`;
    }
    msgsEl.appendChild(d);
    msgsEl.scrollTop = msgsEl.scrollHeight;
  }

  function showTyping() {
    const d = document.createElement('div');
    d.id = 'cb-typing'; d.className = 'cb-bot-msg';
    d.innerHTML = `
      <div class="cb-bot-icon">
        <svg width="11" height="11" viewBox="0 0 26 26" fill="none">
          <circle cx="13" cy="13" r="10" stroke="white" stroke-width="2"/>
          <path d="M13 7v6l4 4" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>
      </div>
      <div class="cb-bot-bubble"><div class="cb-typing"><span></span><span></span><span></span></div></div>`;
    msgsEl.appendChild(d); msgsEl.scrollTop = msgsEl.scrollHeight;
  }
  function removeTyping() { document.getElementById('cb-typing')?.remove(); }

  function catButtonsHTML() {
    return `<div style="display:flex;flex-direction:column;gap:0;margin-top:6px">
      <button class="cb-cat-btn" onclick="cbSelectCat('getting_started','🚲','Getting Started')">🚲 Getting Started</button>
      <button class="cb-cat-btn" onclick="cbSelectCat('pricing','💳','Pricing & Subscriptions')">💳 Pricing & Subscriptions</button>
      <button class="cb-cat-btn" onclick="cbSelectCat('stations','📍','Finding Stations')">📍 Finding Stations</button>
      <button class="cb-cat-btn" onclick="cbSelectCat('cycling_tips','🌦️','Cycling in Dublin')">🌦️ Cycling in Dublin</button>
      <button class="cb-cat-btn" onclick="cbSelectCat('cityflow','📊','About CityFlow')">📊 About CityFlow</button>
    </div>`;
  }

  addMsg("Hey! 👋 I'm the Cityflow assistant. Pick a topic and I'll answer your Dublin Bikes questions instantly." + catButtonsHTML(), false);

  window.cbSelectCat = function(catId, icon, label) {
    addMsg(icon + ' ' + label, true);
    showTyping();
    fetch(`${API}/api/chat/categories`)
      .then(r => r.json())
      .then(cats => {
        removeTyping();
        const cat = cats.find(c => c.id === catId);
        if (!cat) { addMsg('Could not load questions.', false); return; }
        const qHtml = `
          <button class="cb-back-btn" onclick="cbHome()">← Back to topics</button>
          <div style="font-size:11px;font-weight:600;margin-bottom:4px;opacity:.7">Choose a question:</div>
          ${cat.questions.map(q =>
            `<button class="cb-q-btn" onclick="cbAskQuestion('${q.id}','${q.question.replace(/'/g, "\\'")}')">${q.question}</button>`
          ).join('')}`;
        addMsg(qHtml, false);
      })
      .catch(() => { removeTyping(); addMsg('Could not load questions. Is the backend running?', false); });
  };

  window.cbAskQuestion = function(questionId, questionText) {
    addMsg(questionText, true);
    showTyping();
    fetch(`${API}/api/chat/answer`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question_id: questionId })
    })
      .then(r => r.json())
      .then(data => {
        removeTyping();
        addMsg(`<div class="cb-ans-text">${esc(data.answer)}</div>
          <div style="margin-top:8px;padding-top:6px;border-top:1px solid rgba(255,255,255,.1)">
            <button class="cb-back-btn" onclick="cbHome()">← Ask another question</button>
          </div>`, false);
      })
      .catch(() => { removeTyping(); addMsg('Could not load the answer.', false); });
  };

  window.cbHome = function() {
    showTyping();
    setTimeout(() => { removeTyping(); addMsg('What else can I help you with? 😊' + catButtonsHTML(), false); }, 400);
  };
}