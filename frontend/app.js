const API_BASE = `${window.location.origin}/api`;
const WS_PROTO = window.location.protocol === "https:" ? "wss" : "ws";
const WS_URL = `${WS_PROTO}://${window.location.host}/ws/alerts`;

const state = {
  autoRefresh: true,
  selectedPatientId: null,
  patients: [],
  alerts: [],
};

const el = {
  lastRefresh: document.getElementById("lastRefresh"),
  notifyStatus: document.getElementById("notifyStatus"),
  kpiMonitored: document.getElementById("kpiMonitored"),
  kpiCritical: document.getElementById("kpiCritical"),
  kpiHigh: document.getElementById("kpiHigh"),
  kpiAvgRisk: document.getElementById("kpiAvgRisk"),
  riskFilter: document.getElementById("riskFilter"),
  searchInput: document.getElementById("searchInput"),
  patientRows: document.getElementById("patientRows"),
  alertFeed: document.getElementById("alertFeed"),
  detailPane: document.getElementById("detailPane"),
  reloadBtn: document.getElementById("reloadBtn"),
  autorefreshBtn: document.getElementById("autorefreshBtn"),
};

function tierClass(tier) {
  return `risk-pill risk-${tier}`;
}

function fmt(value, suffix = "") {
  if (value === null || value === undefined || Number.isNaN(value)) return "--";
  return `${value}${suffix}`;
}

async function request(path, options = {}) {
  const res = await fetch(`${API_BASE}${path}`, options);
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}`);
  }
  return res.json();
}

function renderRows(items) {
  el.patientRows.innerHTML = items
    .map(
      (p) => `
      <tr data-id="${p.subject_id}">
        <td><strong>#${p.subject_id}</strong><br><small>${new Date(p.updated_at).toLocaleString()}</small></td>
        <td><span class="${tierClass(p.risk_tier)}">${p.risk_tier}</span><br>${Math.round(p.risk_probability * 100)}%</td>
        <td>
          HR ${fmt(p.heart_rate)} bpm<br>
          MAP ${fmt(p.bp_mean)} mmHg<br>
          SpO2 ${fmt(p.spo2, "%")}
        </td>
        <td>${fmt(p.heart_rate_trend)} bpm</td>
        <td>${p.risk_reasons.join(", ")}</td>
      </tr>`
    )
    .join("");

  document.querySelectorAll("#patientRows tr").forEach((row) => {
    row.addEventListener("click", () => selectPatient(Number(row.dataset.id)));
  });
}

function renderAlerts(items, searchTerm = "") {
  const token = (searchTerm || "").trim().toLowerCase();
  const filtered = !token
    ? items
    : items.filter(
        (a) =>
          String(a.subject_id).toLowerCase().includes(token) ||
          String(a.alert || "").toLowerCase().includes(token) ||
          String(a.charttime || "").toLowerCase().includes(token)
      );

  if (!filtered.length) {
    el.alertFeed.innerHTML = "<p class='muted'>No active alerts.</p>";
    return;
  }

  el.alertFeed.innerHTML = filtered
    .map(
      (a) => `
      <div class="alert-item ${a.risk_tier}">
        <strong>Patient #${a.subject_id}</strong>
        <div>${a.alert}</div>
        <small>At alert: HR ${fmt(a.alert_heart_rate)} bpm, MAP ${fmt(a.alert_bp_mean)} mmHg</small><br>
        <small>${a.charttime || "time unavailable"}</small>
      </div>`
    )
    .join("");

  document.querySelectorAll("#alertFeed .alert-item").forEach((node, idx) => {
    node.style.cursor = "pointer";
    node.addEventListener("click", () => {
      const alert = filtered[idx];
      if (alert && alert.subject_id) {
        selectPatient(Number(alert.subject_id));
      }
    });
  });
}

function sparkline(values) {
  const points = values.filter((v) => typeof v === "number");
  if (!points.length) {
    return "<p class='muted'>Insufficient telemetry points.</p>";
  }

  const min = Math.min(...points);
  const max = Math.max(...points);
  const span = max - min || 1;
  const width = 420;
  const height = 120;
  const mapped = points
    .map((v, i) => {
      const x = (i / Math.max(points.length - 1, 1)) * width;
      const y = height - ((v - min) / span) * (height - 10) - 5;
      return `${x},${y}`;
    })
    .join(" ");

  return `
    <svg class="sparkline" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
      <polyline fill="none" stroke="#1fd0ff" stroke-width="3" points="${mapped}" />
    </svg>`;
}

async function selectPatient(subjectId) {
  state.selectedPatientId = subjectId;
  try {
    const data = await request(`/patients/${subjectId}`);
    const p = data.patient;
    const timeline = data.timeline || [];
    const hrSeries = timeline.map((x) => x.heart_rate).filter((v) => v !== null);

    el.detailPane.innerHTML = `
      <h4>Patient #${p.subject_id}</h4>
      <p><span class="${tierClass(p.risk_tier)}">${p.risk_tier}</span> ${Math.round(p.risk_probability * 100)}% risk</p>
      <div class="metric-grid">
        <div class="metric"><span class="metric-label">Heart Rate</span><span class="metric-value">${fmt(p.heart_rate)} bpm</span></div>
        <div class="metric"><span class="metric-label">MAP</span><span class="metric-value">${fmt(p.bp_mean)} mmHg</span></div>
        <div class="metric"><span class="metric-label">SpO2</span><span class="metric-value">${fmt(p.spo2, "%")}</span></div>
        <div class="metric"><span class="metric-label">Temperature</span><span class="metric-value">${fmt(p.temp, " C")}</span></div>
      </div>
      <p>Primary flags: ${p.risk_reasons.join(", ")}</p>
      ${sparkline(hrSeries)}
    `;
  } catch (err) {
    el.detailPane.textContent = `Unable to load patient: ${err.message}`;
  }
}

async function loadDashboard() {
  const risk = el.riskFilter.value;
  const search = el.searchInput.value.trim();
  const query = new URLSearchParams();
  if (risk) query.set("risk", risk);
  if (search) query.set("search", search);
  query.set("limit", "150");

  const [summaryData, patientsData, alertsData, notifyData] = await Promise.all([
    request("/summary"),
    request(`/patients?${query.toString()}`),
    request("/alerts/live?limit=25"),
    request("/notifications/status"),
  ]);

  state.patients = patientsData.items;
  state.alerts = alertsData.items;
  el.lastRefresh.textContent = `Last refresh ${new Date(summaryData.last_refreshed).toLocaleString()}`;
  el.kpiMonitored.textContent = summaryData.summary.patients_monitored;
  el.kpiCritical.textContent = summaryData.summary.critical_count;
  el.kpiHigh.textContent = summaryData.summary.high_count;
  el.kpiAvgRisk.textContent = `${Math.round(summaryData.summary.average_risk * 100)}%`;
  el.notifyStatus.textContent = `Email alerts: ${notifyData.enabled ? "ON" : "OFF"} | sent ${notifyData.sent_count} | errors ${notifyData.error_count}`;

  renderRows(patientsData.items);
  renderAlerts(alertsData.items, search);

  if (state.selectedPatientId && state.patients.some((p) => p.subject_id === state.selectedPatientId)) {
    selectPatient(state.selectedPatientId);
  }

  if (!state.selectedPatientId && patientsData.items.length === 1) {
    selectPatient(Number(patientsData.items[0].subject_id));
  }
}

function setupRealtime() {
  try {
    const ws = new WebSocket(WS_URL);
    ws.onmessage = (event) => {
      const payload = JSON.parse(event.data);
      el.lastRefresh.textContent = `Live stream ${new Date(payload.timestamp).toLocaleTimeString()}`;
    };
  } catch (_) {
    // Non-blocking enhancement.
  }
}

el.reloadBtn.addEventListener("click", async () => {
  try {
    await request("/reload", { method: "POST" });
    await loadDashboard();
  } catch (err) {
    el.lastRefresh.textContent = `Reload failed: ${err.message}`;
  }
});

el.autorefreshBtn.addEventListener("click", () => {
  state.autoRefresh = !state.autoRefresh;
  el.autorefreshBtn.textContent = `Auto Refresh: ${state.autoRefresh ? "ON" : "OFF"}`;
});

el.riskFilter.addEventListener("change", () => loadDashboard().catch(console.error));
el.searchInput.addEventListener("input", () => loadDashboard().catch(console.error));

setInterval(() => {
  if (state.autoRefresh) {
    loadDashboard().catch((err) => {
      el.lastRefresh.textContent = `Connection issue: ${err.message}`;
    });
  }
}, 6000);

loadDashboard().catch((err) => {
  el.lastRefresh.textContent = `Backend unavailable: ${err.message}`;
});
setupRealtime();
