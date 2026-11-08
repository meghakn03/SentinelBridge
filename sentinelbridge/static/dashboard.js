"use strict";

const byId = (id) => document.getElementById(id);
const levels = ["critical", "high", "medium", "low", "info", "unknown"];
const runButton = byId("run");
const refreshButton = byId("refresh");
const runLabels = {
  running: ["In progress", "info"],
  succeeded: ["Succeeded", "low"],
  failed: ["Failed", "critical"],
  deferred: ["Deferred", "medium"],
  page_limit: ["Page limit reached", "medium"],
  interrupted: ["Interrupted", "high"],
};
let latestEvents = [];
let lastEventsJson = null;
let lastRunsJson = null;
let refreshTask = null;
let starting = false;
let running = false;
let connected = false;
let requestNotice = "";

function element(tag, value, className) {
  const node = document.createElement(tag);
  if (value !== undefined) node.textContent = value;
  if (className) node.className = className;
  return node;
}

function formatDate(date) {
  return date.toISOString().slice(0, 19).replace("T", " ");
}

function timestamp(value) {
  return formatDate(new Date(value * 1000));
}

function timeCell(row, date) {
  const time = element("time", formatDate(date));
  time.dateTime = date.toISOString();
  row.insertCell().append(time);
}

function emptyRow(body, columns, message) {
  const cell = body.insertRow().insertCell();
  cell.colSpan = columns;
  cell.className = "empty";
  cell.textContent = message;
}

function showDetail(event) {
  const dialog = byId("event-detail");
  byId("detail-title").textContent = event.source_event_id;
  byId("detail-message").textContent = event.message;
  const fields = [
    ["Severity", event.severity], ["Category", event.category], ["Action", event.action],
    ["Outcome", event.outcome], ["Actor", event.actor], ["Asset", event.asset],
    ["Source IP", event.source_ip], ["Schema version", event.schema_version],
    ["Event time (UTC)", formatDate(new Date(event.timestamp))],
  ];
  byId("detail-fields").replaceChildren(
    ...fields.flatMap(([label, value]) => [element("dt", label), element("dd", value || "—")]),
  );
  byId("detail-json").textContent = JSON.stringify(event, null, 2);
  dialog.querySelector("details").open = false;
  dialog.showModal();
}

function renderEvents() {
  const search = byId("search").value.trim().toLowerCase();
  const severity = byId("severity").value;
  const filtered = latestEvents.filter((event) =>
    (!severity || event.severity === severity)
    && (!search || Object.values(event).some((value) => String(value ?? "").toLowerCase().includes(search))),
  );
  const body = byId("event-rows");
  body.replaceChildren();
  byId("event-count").textContent = search || severity
    ? `${filtered.length} of ${latestEvents.length} events`
    : `${filtered.length} ${filtered.length === 1 ? "event" : "events"}`;
  if (!filtered.length) {
    emptyRow(body, 5, latestEvents.length ? "No events match these filters." : "Run ingestion to populate the event feed.");
  }
  for (const event of filtered) {
    const row = body.insertRow();
    const button = element("button", event.source_event_id, "event-link");
    button.type = "button";
    button.addEventListener("click", () => showDetail(event));
    row.insertCell().append(button, element("span", event.action.replaceAll("_", " "), "event-action"));
    row.insertCell().append(element("span", event.severity, `pill ${event.severity}`));
    row.insertCell().textContent = event.asset || "—";
    row.insertCell().textContent = event.outcome;
    timeCell(row, new Date(event.timestamp));
  }
}

function renderDistribution(events) {
  const counts = Object.fromEntries(levels.map((level) => [level, 0]));
  for (const event of events) counts[event.severity]++;
  const namespace = "http://www.w3.org/2000/svg";
  const svg = document.createElementNS(namespace, "svg");
  svg.setAttribute("viewBox", `0 0 ${events.length || 1} 10`);
  svg.setAttribute("preserveAspectRatio", "none");
  svg.setAttribute("aria-hidden", "true");
  let offset = 0;
  const legend = [];
  for (const level of levels) {
    const count = counts[level];
    if (count) {
      const rect = document.createElementNS(namespace, "rect");
      rect.setAttribute("x", offset);
      rect.setAttribute("width", count);
      rect.setAttribute("height", 10);
      rect.setAttribute("class", level);
      svg.append(rect);
      offset += count;
    }
    if (level !== "unknown" || count) {
      const item = element("li");
      const label = level[0].toUpperCase() + level.slice(1);
      item.append(element("i", "", level), element("span", `${label} ${count}`));
      legend.push(item);
    }
  }
  byId("distribution").replaceChildren(svg);
  byId("legend").replaceChildren(...legend);
  byId("severity-count").textContent = events.length;
}

function renderRuns(runs) {
  const body = byId("run-rows");
  body.replaceChildren();
  if (!runs.length) emptyRow(body, 6, "No ingestion runs yet.");
  for (const run of runs) {
    const row = body.insertRow();
    row.insertCell().textContent = `#${run.id} · ${run.trigger}`;
    const [label, tone] = runLabels[run.status];
    row.insertCell().append(element("span", label, `pill ${tone}`));
    timeCell(row, new Date(run.started * 1000));
    row.insertCell().textContent = run.delivered;
    row.insertCell().textContent = run.retries;
    row.insertCell().textContent = run.error || `${run.pages} pages · ${run.duplicates} duplicates`;
  }
}

function updateButton() {
  runButton.disabled = !connected || running || starting;
  runButton.querySelector("span").textContent = running || starting ? "Ingestion running…" : "Run ingestion";
}

function showNotice(message) {
  const notice = byId("notice");
  notice.textContent = message;
  notice.hidden = !message;
}

function renderStatus(status) {
  running = status.running;
  const lastRun = status.last_run;
  const last = lastRun?.status;
  const cooldowns = Object.entries(status.cooldowns).map(([key, value]) =>
    `${key === "source" ? "Source API" : "Splunk HEC"} resumes after ${timestamp(value)} UTC.`,
  );
  const labels = {
    succeeded: "Last run succeeded", failed: "Last run failed", deferred: cooldowns.length ? "Cooldown active" : "Last run deferred",
    page_limit: "Page limit reached", interrupted: "Last run interrupted",
  };
  const state = byId("state");
  state.querySelector("span").textContent = running ? "Ingestion running" : labels[last] || "Ready to ingest";
  state.dataset.tone = last === "failed" || last === "interrupted" ? "error" : cooldowns.length ? "warning" : "normal";
  byId("state-note").textContent = lastRun?.finished
    ? `Run #${lastRun.id} · ${timestamp(lastRun.finished).slice(11)} UTC`
    : status.poll_interval_seconds ? `Scheduled every ${status.poll_interval_seconds}s` : "Manual ingestion";
  byId("environment").textContent = status.mode === "demo" ? "Demo environment" : "Live environment";
  byId("mode").textContent = status.mode === "demo" ? "SIMULATED" : "LIVE";
  for (const key of ["delivered", "duplicates", "retries", "pages"]) {
    byId(key).textContent = status.totals[key].toLocaleString();
  }
  byId("limits").textContent = `${status.totals.rate_limits} rate-limit responses`;
  byId("checkpoint").textContent = status.checkpoint_at
    ? `Saved at ${timestamp(status.checkpoint_at).slice(11)} UTC`
    : "No checkpoint yet";
  showNotice([requestNotice, lastRun?.error, ...cooldowns].filter(Boolean).join(" "));
  updateButton();
}

async function api(path, options) {
  const response = await fetch(path, { ...options, cache: "no-store" });
  if (!response.ok) {
    if (response.status === 409) throw new Error("An ingestion run is already in progress.");
    throw new Error(`Connector API returned HTTP ${response.status}.`);
  }
  return response.json();
}

async function loadDashboard() {
  refreshButton.disabled = true;
  try {
    const [status, runs, events] = await Promise.all([
      api("/api/status"), api("/api/runs"), api("/api/events"),
    ]);
    connected = true;
    renderStatus(status);
    const eventsJson = JSON.stringify(events.items);
    if (eventsJson !== lastEventsJson) {
      latestEvents = events.items;
      renderEvents();
      renderDistribution(latestEvents);
      lastEventsJson = eventsJson;
    }
    const runsJson = JSON.stringify(runs.items);
    if (runsJson !== lastRunsJson) {
      renderRuns(runs.items);
      lastRunsJson = runsJson;
    }
    byId("updated").textContent = `Updated ${timestamp(Date.now() / 1000).slice(11)} UTC`;
  } catch {
    connected = false;
    byId("state").querySelector("span").textContent = "Connector unavailable";
    byId("state").dataset.tone = "error";
    byId("state-note").textContent = "Waiting for the API";
    showNotice("Could not refresh the dashboard. Check the connector service and try again.");
    updateButton();
  } finally {
    refreshButton.disabled = false;
  }
}

function refresh() {
  if (!refreshTask) {
    refreshTask = loadDashboard().finally(() => { refreshTask = null; });
  }
  return refreshTask;
}

runButton.addEventListener("click", async () => {
  starting = true;
  requestNotice = "";
  updateButton();
  try {
    await api("/api/runs", { method: "POST", headers: { "X-SentinelBridge-Action": "ingest" } });
  } catch (error) {
    requestNotice = error.message || "Could not start ingestion.";
  } finally {
    await refreshTask;
    starting = false;
    await refresh();
  }
});

refreshButton.addEventListener("click", () => {
  requestNotice = "";
  refresh();
});
byId("search").addEventListener("input", renderEvents);
byId("severity").addEventListener("change", renderEvents);

function updateLocation() {
  const hash = window.location.hash || "#overview";
  for (const link of document.querySelectorAll(".nav-link[href^='#']")) {
    const active = link.hash === hash;
    link.classList.toggle("current", active);
    if (active) {
      link.setAttribute("aria-current", "location");
      byId("location-label").textContent = link.textContent;
    } else {
      link.removeAttribute("aria-current");
    }
  }
}

window.addEventListener("hashchange", updateLocation);
updateLocation();
refresh();
setInterval(refresh, 2500);
