const healthCard = document.getElementById("healthCard");
const ingestorCard = document.getElementById("ingestorCard");
const tableTitle = document.getElementById("tableTitle");
const tableSelect = document.getElementById("tableSelect");
const schemaCard = document.getElementById("schemaCard");
const previewHead = document.getElementById("previewHead");
const previewBody = document.getElementById("previewBody");
const rowLimit = document.getElementById("rowLimit");
const refreshAllButton = document.getElementById("refreshAll");
const themeToggleButton = document.getElementById("themeToggle");
const analyticsRanges = document.getElementById("analyticsRanges");
const analyticsMeta = document.getElementById("analyticsMeta");
const tokensChartCanvas = document.getElementById("tokensChart");
const turnsChartCanvas = document.getElementById("turnsChart");
const concurrentSessionsChartCanvas = document.getElementById("concurrentSessionsChart");

let activeTable = null;
let analyticsRange = "24h";
let tokensChart = null;
let turnsChart = null;
let concurrentSessionsChart = null;
let lastAnalyticsPayload = null;
const THEME_STORAGE_KEY = "cortex-monitor-theme";
const MODEL_COLORS = [
  "#155e75",
  "#3b82f6",
  "#0f766e",
  "#b45309",
  "#7c3aed",
  "#e11d48",
  "#2563eb",
  "#059669",
  "#4f46e5",
  "#0891b2",
];

function readCssVar(name, fallback) {
  const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return value || fallback;
}

function chartTheme() {
  return {
    text: readCssVar("--chart-text", "#435568"),
    grid: readCssVar("--chart-grid", "#e8eff4"),
  };
}

function getStoredTheme() {
  try {
    const value = window.localStorage.getItem(THEME_STORAGE_KEY);
    if (value === "light" || value === "dark") return value;
  } catch (_) {}
  return null;
}

function getPreferredTheme() {
  const stored = getStoredTheme();
  if (stored) return stored;
  return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches
    ? "dark"
    : "light";
}

function applyTheme(theme) {
  document.documentElement.setAttribute("data-theme", theme);
  if (themeToggleButton) {
    themeToggleButton.textContent = theme === "dark" ? "Light" : "Dark";
    themeToggleButton.setAttribute("aria-label", theme === "dark" ? "Switch to light theme" : "Switch to dark theme");
  }
}

function persistTheme(theme) {
  try {
    window.localStorage.setItem(THEME_STORAGE_KEY, theme);
  } catch (_) {}
}

function setText(el, value) {
  el.textContent = value;
}

function isHttpUrl(value) {
  return typeof value === "string" && /^https?:\/\//i.test(value);
}

function searchQueryUrl(query) {
  if (!query || typeof query !== "string") return "";
  return `https://duckduckgo.com/?q=${encodeURIComponent(query)}`;
}

function escapeText(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function createStatBlock(label, value, stateClass = "") {
  const node = document.createElement("article");
  node.className = `card ${stateClass}`.trim();
  const labelNode = document.createElement("div");
  labelNode.className = "stat-label";
  labelNode.textContent = label;
  const valueNode = document.createElement("div");
  valueNode.className = "stat-value";
  valueNode.textContent = value;
  node.appendChild(labelNode);
  node.appendChild(valueNode);
  return node;
}

function formatBucketLabel(unixSeconds, rangeKey) {
  const date = new Date(unixSeconds * 1000);
  if (rangeKey === "30d") {
    return date.toLocaleString([], { month: "short", day: "numeric" });
  }
  if (rangeKey === "7d") {
    return date.toLocaleString([], { month: "short", day: "numeric", hour: "2-digit" });
  }
  if (rangeKey === "24h") {
    return date.toLocaleString([], { month: "short", day: "numeric", hour: "2-digit" });
  }
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function formatBucketSize(seconds) {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)}h`;
  return `${Math.round(seconds / 86400)}d`;
}

function formatCompactNumber(value) {
  const n = Number(value || 0);
  const abs = Math.abs(n);
  if (abs >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(abs >= 10_000_000_000 ? 0 : 1).replace(/\\.0$/, "")}B`;
  if (abs >= 1_000_000) return `${(n / 1_000_000).toFixed(abs >= 10_000_000 ? 0 : 1).replace(/\\.0$/, "")}M`;
  if (abs >= 1_000) return `${(n / 1_000).toFixed(abs >= 10_000 ? 0 : 1).replace(/\\.0$/, "")}K`;
  return `${n}`;
}

function buildBucketAxis(range) {
  const bucketSeconds = Number(range.bucket_seconds || 3600);
  const fromUnix = Number(range.from_unix || 0);
  const toUnix = Number(range.to_unix || 0);
  if (!fromUnix || !toUnix || bucketSeconds <= 0) return [];

  const start = Math.floor(fromUnix / bucketSeconds) * bucketSeconds;
  const end = Math.floor(toUnix / bucketSeconds) * bucketSeconds;
  const axis = [];
  for (let ts = start; ts <= end; ts += bucketSeconds) axis.push(ts);
  return axis;
}

function createOrUpdateChart(currentChart, canvas, type, labels, datasets, yTitle, options = {}) {
  if (!canvas || typeof Chart === "undefined") return currentChart;
  const palette = chartTheme();
  const stacked = options.stacked === true;
  const maxTicks = Number(options.maxTicks || 10);
  const yTickFormatter = typeof options.yTickFormatter === "function" ? options.yTickFormatter : null;

  if (!currentChart) {
    return new Chart(canvas.getContext("2d"), {
      type,
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        interaction: { mode: "nearest", intersect: false },
        scales: {
          x: { stacked, ticks: { maxTicksLimit: maxTicks, color: palette.text }, grid: { color: palette.grid } },
          y: {
            stacked,
            beginAtZero: true,
            ticks: {
              color: palette.text,
              callback: yTickFormatter
                ? (value) => yTickFormatter(value)
                : undefined,
            },
            grid: { color: palette.grid },
            title: { display: true, text: yTitle, color: palette.text },
          },
        },
        plugins: {
          legend: { position: "bottom", labels: { boxWidth: 10, boxHeight: 10, usePointStyle: true, color: palette.text } },
          tooltip: { enabled: true },
        },
      },
    });
  }

  currentChart.config.type = type;
  currentChart.data.labels = labels;
  currentChart.data.datasets = datasets;
  if (currentChart.options?.scales?.x) {
    currentChart.options.scales.x.stacked = stacked;
    if (currentChart.options.scales.x.ticks) {
      currentChart.options.scales.x.ticks.maxTicksLimit = maxTicks;
    }
  }
  if (currentChart.options?.scales?.y) {
    currentChart.options.scales.y.stacked = stacked;
  }
  if (currentChart.options?.scales?.x?.ticks) currentChart.options.scales.x.ticks.color = palette.text;
  if (currentChart.options?.scales?.x?.grid) currentChart.options.scales.x.grid.color = palette.grid;
  if (currentChart.options?.scales?.y?.ticks) currentChart.options.scales.y.ticks.color = palette.text;
  if (currentChart.options?.scales?.y?.ticks) {
    currentChart.options.scales.y.ticks.callback = yTickFormatter
      ? (value) => yTickFormatter(value)
      : undefined;
  }
  if (currentChart.options?.scales?.y?.grid) currentChart.options.scales.y.grid.color = palette.grid;
  if (currentChart.options?.scales?.y?.title) currentChart.options.scales.y.title.color = palette.text;
  if (currentChart.options?.plugins?.legend?.labels) currentChart.options.plugins.legend.labels.color = palette.text;
  currentChart.update();
  return currentChart;
}

function renderAnalytics(data) {
  lastAnalyticsPayload = data;
  if (!data?.ok) {
    analyticsMeta.textContent = data?.error || "Unable to load analytics";
    return;
  }

  const range = data.range || {};
  const bucketAxis = buildBucketAxis(range);
  const tokenPoints = data.series?.tokens || [];
  const turnPoints = data.series?.turns || [];
  const concurrentPoints = data.series?.concurrent_sessions || [];

  const tokenMap = new Map();
  const turnMap = new Map();
  const tokenTotals = new Map();

  tokenPoints.forEach((point) => {
    const model = point.model || "unknown";
    const bucket = Number(point.bucket_unix);
    const value = Number(point.tokens || 0);
    if (!tokenMap.has(model)) tokenMap.set(model, new Map());
    tokenMap.get(model).set(bucket, value);
    tokenTotals.set(model, (tokenTotals.get(model) || 0) + value);
  });

  turnPoints.forEach((point) => {
    const model = point.model || "unknown";
    const bucket = Number(point.bucket_unix);
    const value = Number(point.turns || 0);
    if (!turnMap.has(model)) turnMap.set(model, new Map());
    turnMap.get(model).set(bucket, value);
    if (!tokenTotals.has(model)) tokenTotals.set(model, 0);
  });

  const concurrentMap = new Map();
  concurrentPoints.forEach((point) => {
    const bucket = Number(point.bucket_unix);
    const value = Number(point.concurrent_sessions || 0);
    concurrentMap.set(bucket, value);
  });

  const models = Array.from(tokenTotals.entries())
    .sort((a, b) => b[1] - a[1])
    .map(([model]) => model)
    .slice(0, 8);

  const labels = bucketAxis.map((bucket) => formatBucketLabel(bucket, range.key));
  const tokenDatasets = models.map((model, index) => {
    const color = MODEL_COLORS[index % MODEL_COLORS.length];
    return {
      label: model,
      data: bucketAxis.map((bucket) => tokenMap.get(model)?.get(bucket) ?? 0),
      borderColor: color,
      backgroundColor: `${color}26`,
      borderWidth: 1,
      borderRadius: 3,
    };
  });
  const turnDatasets = models.map((model, index) => {
    const color = MODEL_COLORS[index % MODEL_COLORS.length];
    return {
      label: model,
      data: bucketAxis.map((bucket) => turnMap.get(model)?.get(bucket) ?? 0),
      borderColor: color,
      backgroundColor: `${color}B0`,
      borderWidth: 1,
      borderRadius: 4,
    };
  });

  tokensChart = createOrUpdateChart(
    tokensChart,
    tokensChartCanvas,
    "bar",
    labels,
    tokenDatasets,
    "Generation Tokens",
    {
      stacked: true,
      maxTicks: range.key === "7d" || range.key === "30d" ? 12 : 10,
      yTickFormatter: formatCompactNumber,
    },
  );
  turnsChart = createOrUpdateChart(
    turnsChart,
    turnsChartCanvas,
    "bar",
    labels,
    turnDatasets,
    "Turns",
    { stacked: false, maxTicks: range.key === "7d" || range.key === "30d" ? 12 : 10 },
  );
  concurrentSessionsChart = createOrUpdateChart(
    concurrentSessionsChart,
    concurrentSessionsChartCanvas,
    "line",
    labels,
    [
      {
        label: "Concurrent sessions",
        data: bucketAxis.map((bucket) => concurrentMap.get(bucket) ?? 0),
        borderColor: "#f59e0b",
        backgroundColor: "#f59e0b33",
        borderWidth: 2,
        tension: 0.2,
        pointRadius: 2,
        fill: true,
      },
    ],
    "Sessions",
    { stacked: false, maxTicks: range.key === "7d" || range.key === "30d" ? 12 : 10 },
  );

  const modelCount = models.length;
  analyticsMeta.textContent = `${range.label} • ${formatBucketSize(range.bucket_seconds)} buckets • ${modelCount} model${modelCount === 1 ? "" : "s"} • updated ${new Date().toLocaleTimeString()}`;
}

async function api(path) {
  const response = await fetch(path);
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error || "request failed");
  }
  return data;
}

function renderHealth(data) {
  healthCard.innerHTML = "";
  if (!data.ok) {
    healthCard.appendChild(createStatBlock("ClickHouse", "Unavailable", "bad"));
    healthCard.appendChild(createStatBlock("Reason", data.error || "not available", "bad"));
    const failedConnections = data.connections?.error ? "unavailable" : "unknown";
    healthCard.appendChild(createStatBlock("Connections", failedConnections, "warn"));
    return;
  }

  const pingValue = data.ping_ms === null || data.ping_ms === undefined
    ? "n/a"
    : `${Number(data.ping_ms).toFixed(2)} ms`;
  const connectionTotal = data.connections?.total;
  const connectionLabel = connectionTotal === null || connectionTotal === undefined
    ? "unknown"
    : `${Number(connectionTotal).toLocaleString()} total`;

  healthCard.appendChild(createStatBlock("ClickHouse", `${data.url}`, "ok"));
  healthCard.appendChild(createStatBlock("Database", data.database, "ok"));
  healthCard.appendChild(createStatBlock("Version", data.version || "n/a", "ok"));
  healthCard.appendChild(createStatBlock("Ping", pingValue, "ok"));
  healthCard.appendChild(createStatBlock("Connections", connectionLabel));
}

function renderIngestor(data) {
  ingestorCard.innerHTML = "";
  if (!data.ingestor) {
    ingestorCard.appendChild(createStatBlock("Ingestor", "Unknown", "warn"));
    return;
  }

  let status = "Unknown";
  let statusClass = "warn";
  if (!data.ingestor.present) {
    status = "Table missing";
  } else if (!data.ingestor.latest) {
    status = "No samples";
  } else if (data.ingestor.alive) {
    status = "Healthy";
    statusClass = "ok";
  } else {
    status = "Stale";
  }

  ingestorCard.appendChild(createStatBlock("Ingestor", status, statusClass));

  if (data.ingestor.latest) {
    const heartbeatAge = data.ingestor.age_seconds === null || data.ingestor.age_seconds === undefined
      ? "unknown"
      : `${data.ingestor.age_seconds}s`;
    const queueDepth = data.ingestor.latest.queue_depth;
    const queueDepthLabel = queueDepth === null || queueDepth === undefined ? "unknown" : `${queueDepth}`;
    const filesActive = data.ingestor.latest.files_active;
    const filesWatched = data.ingestor.latest.files_watched;

    ingestorCard.appendChild(createStatBlock("Heartbeat age", heartbeatAge, statusClass));
    ingestorCard.appendChild(
      createStatBlock(
        "Queue depth",
        queueDepthLabel,
      ),
    );
    ingestorCard.appendChild(
      createStatBlock(
        "Files",
        `${filesActive ?? "?"} active / ${filesWatched ?? "?"} watched`,
      ),
    );
  } else {
    ingestorCard.appendChild(
      createStatBlock("Heartbeat", data.ingestor.present ? "waiting for first row" : "table not found"),
    );
  }
}

function renderTableOptions(tables) {
  if (!tableSelect) return;
  const options = [
    ...tables.map((entry) => ({
      value: entry.name,
      label: `${entry.name} (${Number(entry.rows || 0).toLocaleString()} rows)`,
    })),
    { value: "web_searches", label: "web_searches (virtual)" },
  ];

  tableSelect.innerHTML = "";
  options.forEach((entry) => {
    const option = document.createElement("option");
    option.value = entry.value;
    option.textContent = entry.label;
    tableSelect.appendChild(option);
  });

  const available = new Set(options.map((entry) => entry.value));
  if (!activeTable || !available.has(activeTable)) {
    activeTable = options.length > 0 ? options[0].value : null;
  }

  if (activeTable) {
    tableSelect.value = activeTable;
  }
}

function renderTableRows(detail) {
  previewHead.innerHTML = "";
  previewBody.innerHTML = "";

  if (!detail.schema || !detail.schema.length) {
    previewHead.innerHTML = '<tr><th>No schema available</th></tr>';
    return;
  }

  const header = document.createElement("tr");
  detail.schema.forEach((column) => {
    const cell = document.createElement("th");
    cell.textContent = column.name;
    header.appendChild(cell);
  });
  previewHead.appendChild(header);

  const columns = detail.schema.map((entry) => entry.name);

  if (!detail.rows || !detail.rows.length) {
    const row = document.createElement("tr");
    const message = document.createElement("td");
    message.setAttribute("colspan", `${columns.length}`);
    message.textContent = "No rows found for this table.";
    row.appendChild(message);
    previewBody.appendChild(row);
    return;
  }

  detail.rows.forEach((entry) => {
    const row = document.createElement("tr");
    columns.forEach((column) => {
      const cell = document.createElement("td");
      const value = entry[column];
      if (value === null || value === undefined) {
        cell.textContent = "";
      } else if (activeTable === "web_searches" && column === "result_url" && isHttpUrl(value)) {
        const link = document.createElement("a");
        link.href = value;
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        link.textContent = value;
        cell.appendChild(link);
      } else if (activeTable === "web_searches" && column === "search_query" && String(value).trim().length > 0) {
        const link = document.createElement("a");
        link.href = searchQueryUrl(String(value));
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        link.textContent = value;
        cell.appendChild(link);
      } else if (typeof value === "object") {
        cell.textContent = JSON.stringify(value);
      } else {
        cell.textContent = value;
      }
      row.appendChild(cell);
    });
    previewBody.appendChild(row);
  });
}

function renderSchema(detail) {
  schemaCard.innerHTML = "";
  if (!detail.schema || !detail.schema.length) {
    setText(schemaCard, "No schema metadata returned");
    schemaCard.classList.remove("muted");
    return;
  }

  const html = detail.schema
    .map((column) => `${escapeText(column.name)}: ${escapeText(column.type)}`)
    .join(" • ");
  schemaCard.textContent = html;
  schemaCard.classList.remove("muted");
}

async function loadHealth() {
  const data = await api('/api/health');
  renderHealth(data);
}

async function loadStatus() {
  const data = await api('/api/status');
  renderIngestor(data);
  return data;
}

async function loadTables() {
  const data = await api('/api/tables');
  renderTableOptions(data.tables || []);

  if (activeTable) {
    await loadTable(activeTable);
  }
}

async function loadAnalytics() {
  const data = await api(`/api/analytics?range=${encodeURIComponent(analyticsRange)}`);
  renderAnalytics(data);
}

async function loadTable(name) {
  tableTitle.textContent = `Table: ${name}`;
  const limit = Number(rowLimit.value || 25);
  const path =
    name === "web_searches"
      ? `/api/web-searches?limit=${limit}`
      : `/api/tables/${encodeURIComponent(name)}?limit=${limit}`;
  const data = await api(path);

  renderSchema(data);
  renderTableRows(data);
}

async function hydrate() {
  try {
    await Promise.all([loadHealth(), loadStatus(), loadTables(), loadAnalytics()]);
  } catch (error) {
    healthCard.innerHTML = '';
    ingestorCard.innerHTML = '';
    if (tableSelect) tableSelect.innerHTML = "";
    schemaCard.textContent = error.message;
    tableTitle.textContent = 'Connection issue';
    analyticsMeta.textContent = `Analytics unavailable: ${error.message}`;
  }
}

refreshAllButton.addEventListener('click', hydrate);
rowLimit.addEventListener('change', () => {
  if (activeTable) {
    loadTable(activeTable);
  }
});

if (tableSelect) {
  tableSelect.addEventListener("change", () => {
    activeTable = tableSelect.value;
    if (activeTable) loadTable(activeTable);
  });
}

if (themeToggleButton) {
  themeToggleButton.addEventListener("click", () => {
    const current = document.documentElement.getAttribute("data-theme") === "dark" ? "dark" : "light";
    const next = current === "dark" ? "light" : "dark";
    applyTheme(next);
    persistTheme(next);
    if (lastAnalyticsPayload) renderAnalytics(lastAnalyticsPayload);
  });
}

if (analyticsRanges) {
  const buttons = Array.from(analyticsRanges.querySelectorAll("button"));
  buttons.forEach((button) => {
    button.addEventListener("click", async () => {
      const selected = button.dataset.range || "24h";
      analyticsRange = selected;
      buttons.forEach((candidate) => candidate.classList.remove("active"));
      button.classList.add("active");
      try {
        await loadAnalytics();
      } catch (error) {
        analyticsMeta.textContent = `Analytics unavailable: ${error.message}`;
      }
    });
  });
}

applyTheme(getPreferredTheme());
hydrate();
setInterval(hydrate, 10000);
