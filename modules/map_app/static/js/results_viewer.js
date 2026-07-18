// T-route results viewer: loads simulated output from the python server and
// colors the flowpaths layer per timestep, entirely in the maplibre style.
// Each feature's current value lives in maplibre feature-state; the paint
// expressions are set once per variable and interpolate that value to a color
// ramp, so a timestep change is just a batch of setFeatureState calls.
// Loaded at the end of <body>; uses the global `map` created by main.js, so
// all map wiring happens in the DOMContentLoaded handler at the bottom.

const RESULT_VARIABLES = {
  flow: { label: "Flow", units: "m³/s" },
  velocity: { label: "Velocity", units: "m/s" },
  depth: { label: "Depth", units: "m" },
};

// Sequential color ramps offered in the palette picker. "ice-fire" is the
// original ramp; the others are borrowed from kepler.gl.
const RESULT_PALETTES = {
  "ice-fire": ["#0077b6", "#00b4d8", "#90e0ef", "#ffba08", "#ff6b35", "#d00000"],
  "viridis": ["#440154", "#414487", "#2a788e", "#22a884", "#7ad151", "#fde725"],
  "warming": ["#5a1846", "#900c3f", "#c70039", "#e3611c", "#f1920e", "#ffc300"],
};
const RESULT_NO_DATA_COLOR = "rgba(128, 128, 128, 0.35)";

const resultsState = {
  outputDir: null,
  variable: "flow",
  cache: {}, // variable -> {time, values, min, max, file}
  timeIndex: 0,
  isPlaying: false,
  playInterval: null,
  playSpeed: 5,
  originalPaint: null, // flowpaths paint to restore on clear
  hoveredId: null, // flowpath under the cursor, for live tooltip updates
  scale: "linear", // "linear" | "log" color scale
  palette: "ice-fire",
};

function currentPalette() {
  return RESULT_PALETTES[resultsState.palette] || RESULT_PALETTES["ice-fire"];
}

let resultsHoverPopup;

function resultsData() {
  return resultsState.cache[resultsState.variable] || null;
}

function setResultsStatus(kind, message) {
  document.getElementById("results-status").hidden = !message;
  document.getElementById("results-status-dot").className =
    "status-dot" + (kind ? ` ${kind}` : "");
  document.getElementById("results-status-text").textContent = message;
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

// Decode the Arrow IPC stream returned by /troute_output into a compact,
// typed-array result. The whole feature x time matrix stays as one Float32Array
// (no per-value JS objects), which is what keeps large runs from blowing up
// browser memory the way the old JSON payload did.
function parseArrowResults(buffer) {
  const table = Arrow.tableFromIPC(new Uint8Array(buffer));
  const meta = table.schema.metadata;

  const idValues = table.getChild("feature_id").data[0].values; // BigInt64Array
  const featureIds = new Float64Array(idValues.length);
  const index = new Map();
  for (let f = 0; f < idValues.length; f++) {
    const id = Number(idValues[f]);
    featureIds[f] = id;
    index.set(id, f);
  }

  const time = JSON.parse(meta.get("time"));
  return {
    file: meta.get("file"),
    variable: meta.get("variable"),
    time,
    nTimes: time.length,
    min: parseFloat(meta.get("min")),
    max: parseFloat(meta.get("max")),
    bounds: JSON.parse(meta.get("bounds")),
    featureIds,
    index,
    matrix: table.getChild("values").data[0].children[0].values, // Float32Array, feature-major
  };
}

async function fetchResultsVariable(variable) {
  const response = await fetch("/troute_output", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ output_dir: resultsState.outputDir, variable }),
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    throw new Error(error.error || "Failed to load results");
  }
  return parseArrowResults(await response.arrayBuffer());
}

async function loadResults() {
  const outputDir = document.getElementById("output-dir").value.trim();
  if (!outputDir) {
    setResultsStatus("error", "Enter a run output directory first");
    return;
  }

  if (outputDir !== resultsState.outputDir) {
    resultsState.cache = {};
    resultsState.timeIndex = 0;
  }
  resultsState.outputDir = outputDir;

  const button = document.getElementById("load-results-button");
  button.disabled = true;
  setResultsStatus("loading", "Loading t-route output...");

  try {
    resultsState.cache[resultsState.variable] = await fetchResultsVariable(resultsState.variable);
    showResults();
    zoomToSelection();
  } catch (error) {
    setResultsStatus("error", error.message);
  } finally {
    button.disabled = false;
  }
}

// Offer known run directories as suggestions in the output directory input.
async function populateOutputDirOptions() {
  try {
    const dirs = await (await fetch("/output_dirs")).json();
    document.getElementById("output-dir-options").innerHTML = dirs
      .map((dir) => `<option value="${dir}"></option>`)
      .join("");
  } catch (error) {
    console.error("Failed to list output directories:", error);
  }
}

function showResults() {
  const data = resultsData();

  document.getElementById("results-controls").hidden = false;
  document.getElementById("results-feature-count").textContent = data.featureIds.length;
  document.getElementById("results-timestep-count").textContent = data.time.length;

  const slider = document.getElementById("results-time-slider");
  slider.max = Math.max(0, data.time.length - 1);
  resultsState.timeIndex = Math.min(resultsState.timeIndex, data.time.length - 1);
  slider.value = resultsState.timeIndex;

  updateResultsLegend();
  applyResultsPaint();
  scheduleFeatureStateUpdate();
  setResultsStatus("success", `Loaded ${data.file.split("/").pop()}`);
}

// ---------------------------------------------------------------------------
// Map painting
// ---------------------------------------------------------------------------

const FLOWPATH_FEATURE = { source: "flowpaths", sourceLayer: "flowpaths" };

// This feature's value at the current timestep; -9999 (the t-route fill value,
// also used when no state is set) marks missing data.
const RESULT_VALUE = ["coalesce", ["feature-state", "value"], -9999];

// Evenly spaced color stops across the data range, in linear or log10 space.
// In log mode values are floored to a small positive number before the log so
// zeros and the min don't blow up.
function resultColorStops(data) {
  const colors = currentPalette();
  const log = resultsState.scale === "log";

  let lo = data.min;
  let hi = data.max;
  if (log) {
    lo = Math.max(data.min, 1e-6);
    hi = Math.max(data.max, lo * 10);
  }
  if (hi <= lo) hi = lo + 1;

  const input = log ? ["log10", ["max", RESULT_VALUE, lo]] : RESULT_VALUE;
  const a = log ? Math.log10(lo) : lo;
  const b = log ? Math.log10(hi) : hi;

  const stops = colors.flatMap((color, i) => [
    a + ((b - a) * i) / (colors.length - 1),
    color,
  ]);
  return ["interpolate", ["linear"], input, ...stops];
}

function resultColorExpression(data) {
  return [
    "case",
    ["<=", RESULT_VALUE, -9998],
    RESULT_NO_DATA_COLOR,
    resultColorStops(data),
  ];
}

function resultWidthExpression(data) {
  const max = data.max > data.min ? data.max : data.min + 1;
  return [
    "case",
    ["<=", RESULT_VALUE, -9998],
    1,
    ["interpolate", ["linear"], RESULT_VALUE, data.min, 1.5, max, 7],
  ];
}

// Set once per loaded variable; timestep changes only touch feature-state.
function applyResultsPaint() {
  const data = resultsData();

  if (!resultsState.originalPaint) {
    resultsState.originalPaint = {
      "line-color": map.getPaintProperty("flowpaths", "line-color"),
      "line-width": map.getPaintProperty("flowpaths", "line-width"),
    };
  }

  map.setPaintProperty("flowpaths", "line-color", resultColorExpression(data));
  map.setPaintProperty("flowpaths", "line-width", resultWidthExpression(data));
}

function updateFeatureStates() {
  const data = resultsData();
  if (!data) return;

  const { featureIds, matrix, nTimes } = data;
  const t = resultsState.timeIndex;
  for (let f = 0; f < featureIds.length; f++) {
    map.setFeatureState(
      { ...FLOWPATH_FEATURE, id: featureIds[f] },
      { value: matrix[f * nTimes + t] }
    );
  }
  updateResultsTimeDisplay();
  refreshHoverPopup();
}

// Coalesce rapid slider/playback changes into at most one update per frame.
let featureStateUpdateQueued = false;
function scheduleFeatureStateUpdate() {
  if (featureStateUpdateQueued) return;
  featureStateUpdateQueued = true;
  requestAnimationFrame(() => {
    featureStateUpdateQueued = false;
    updateFeatureStates();
  });
}

function clearResults() {
  stopResultsPlayback();

  map.removeFeatureState(FLOWPATH_FEATURE);
  if (resultsState.originalPaint) {
    map.setPaintProperty("flowpaths", "line-color", resultsState.originalPaint["line-color"]);
    map.setPaintProperty("flowpaths", "line-width", resultsState.originalPaint["line-width"]);
    resultsState.originalPaint = null;
  }

  document.getElementById("results-controls").hidden = true;
  setResultsStatus("", "");
}

// ---------------------------------------------------------------------------
// Legend and time display
// ---------------------------------------------------------------------------

function updateResultsLegend() {
  const data = resultsData();
  const { label, units } = RESULT_VARIABLES[resultsState.variable];
  const scaleNote = resultsState.scale === "log" ? " · log" : "";

  document.getElementById("results-legend-title").textContent = `${label} (${units})${scaleNote}`;
  document.getElementById("results-legend-min").textContent = data.min.toFixed(2);
  document.getElementById("results-legend-max").textContent = data.max.toFixed(2);

  document.querySelector("#results-panel .legend-gradient").style.background =
    `linear-gradient(to right, ${currentPalette().join(", ")})`;
}

// Re-derive the flowpath paint and legend after a scale or palette change.
function refreshResultsStyle() {
  if (!resultsData()) return;
  applyResultsPaint();
  updateResultsLegend();
}

function formatResultTime(t) {
  // Numeric time is seconds since the run reference time; otherwise ISO dates.
  if (typeof t === "number") return `T+${Math.floor(t / 3600)}h`;
  return String(t).replace("T", " ");
}

function updateResultsTimeDisplay() {
  const data = resultsData();
  document.getElementById("results-current-time").textContent = formatResultTime(
    data.time[resultsState.timeIndex]
  );
  drawResultsOverview();
}

// ---------------------------------------------------------------------------
// Timeseries overview sparkline
// ---------------------------------------------------------------------------

// Sum across all flowpaths at each timestep, computed once per variable.
function resultTotals(data) {
  if (!data.totals) {
    const { matrix, featureIds, nTimes } = data;
    const totals = new Float64Array(nTimes);
    for (let f = 0; f < featureIds.length; f++) {
      const base = f * nTimes;
      for (let t = 0; t < nTimes; t++) {
        const v = matrix[base + t];
        if (v > -9998) totals[t] += v;
      }
    }
    data.totals = totals;
  }
  return data.totals;
}

// Draw the basin total under the slider (same width), with a marker at the
// current timestep, to make high-flow periods findable in long timeseries.
function drawResultsOverview() {
  const data = resultsData();
  const canvas = document.getElementById("results-overview");
  if (!data || canvas.clientWidth === 0) return;

  const totals = resultTotals(data);
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth;
  const h = canvas.clientHeight;
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  const ctx = canvas.getContext("2d");
  ctx.scale(dpr, dpr);

  let min = Infinity;
  let max = -Infinity;
  for (const v of totals) {
    if (v < min) min = v;
    if (v > max) max = v;
  }
  const range = max - min || 1;

  const accent =
    getComputedStyle(document.documentElement).getPropertyValue("--color-primary").trim() ||
    "#00d4ff";
  const pad = 3;
  const x = (t) => (totals.length > 1 ? pad + (t / (totals.length - 1)) * (w - 2 * pad) : w / 2);
  const y = (v) => h - pad - ((v - min) / range) * (h - 2 * pad);

  ctx.beginPath();
  ctx.moveTo(x(0), y(totals[0]));
  for (let t = 1; t < totals.length; t++) ctx.lineTo(x(t), y(totals[t]));
  ctx.strokeStyle = accent;
  ctx.lineWidth = 1;
  ctx.stroke();

  ctx.lineTo(x(totals.length - 1), h - pad);
  ctx.lineTo(x(0), h - pad);
  ctx.closePath();
  ctx.globalAlpha = 0.15;
  ctx.fillStyle = accent;
  ctx.fill();
  ctx.globalAlpha = 1;

  const markerX = x(resultsState.timeIndex);
  ctx.strokeStyle = "#ffba08";
  ctx.beginPath();
  ctx.moveTo(markerX, pad);
  ctx.lineTo(markerX, h - pad);
  ctx.stroke();
}

function seekFromOverview(e) {
  const data = resultsData();
  if (!data) return;
  const rect = e.currentTarget.getBoundingClientRect();
  const fraction = (e.clientX - rect.left) / rect.width;
  resultsState.timeIndex = Math.max(
    0,
    Math.min(data.time.length - 1, Math.round(fraction * (data.time.length - 1)))
  );
  document.getElementById("results-time-slider").value = resultsState.timeIndex;
  scheduleFeatureStateUpdate();
}

// ---------------------------------------------------------------------------
// Playback
// ---------------------------------------------------------------------------

function stepResults(direction) {
  const data = resultsData();
  if (!data) return;
  const steps = data.time.length;
  resultsState.timeIndex = (resultsState.timeIndex + direction + steps) % steps;
  document.getElementById("results-time-slider").value = resultsState.timeIndex;
  scheduleFeatureStateUpdate();
}

const RESULTS_PLAY_ICON =
  '<svg viewBox="0 0 24 24" fill="currentColor"><polygon points="5 3 19 12 5 21 5 3"/></svg>';
const RESULTS_PAUSE_ICON =
  '<svg viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="4" width="4" height="16"/><rect x="14" y="4" width="4" height="16"/></svg>';

function startResultsPlayback() {
  resultsState.isPlaying = true;
  const button = document.getElementById("results-play-button");
  button.classList.add("active");
  button.innerHTML = RESULTS_PAUSE_ICON;
  resultsState.playInterval = setInterval(() => stepResults(1), 2500 / resultsState.playSpeed);
}

function stopResultsPlayback() {
  resultsState.isPlaying = false;
  const button = document.getElementById("results-play-button");
  button.classList.remove("active");
  button.innerHTML = RESULTS_PLAY_ICON;
  clearInterval(resultsState.playInterval);
}

function toggleResultsPlayback() {
  if (resultsState.isPlaying) {
    stopResultsPlayback();
  } else {
    startResultsPlayback();
  }
}

// ---------------------------------------------------------------------------
// Hover tooltip
// ---------------------------------------------------------------------------

// Popup HTML for one flowpath at the current timestep.
function flowpathHoverHtml(id) {
  const data = resultsData();
  const row = data.index.get(id);
  const value = row === undefined ? undefined : data.matrix[row * data.nTimes + resultsState.timeIndex];
  const { label, units } = RESULT_VARIABLES[resultsState.variable];
  const text =
    value === undefined || value <= -9998 ? "no data" : `${value.toFixed(3)} ${units}`;
  return `wb-${id}<br>${label}: ${text}`;
}

function onFlowpathHover(e) {
  const data = resultsData();
  if (!data || !e.features?.length) return;

  const id = e.features[0].id;
  resultsState.hoveredId = id;
  resultsHoverPopup.setLngLat(e.lngLat).setHTML(flowpathHoverHtml(id)).addTo(map);
}

// Keep the open tooltip in sync when the value under it changes (playback,
// slider, variable switch) without needing the mouse to move.
function refreshHoverPopup() {
  if (resultsState.hoveredId == null || !resultsHoverPopup.isOpen()) return;
  resultsHoverPopup.setHTML(flowpathHoverHtml(resultsState.hoveredId));
}

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

async function selectResultsVariable(button) {
  document.querySelectorAll("#results-panel .var-btn").forEach((b) =>
    b.classList.remove("active")
  );
  button.classList.add("active");
  resultsState.variable = button.dataset.var;

  if (!resultsData()) {
    setResultsStatus("loading", `Loading ${resultsState.variable}...`);
    try {
      resultsState.cache[resultsState.variable] = await fetchResultsVariable(
        resultsState.variable
      );
    } catch (error) {
      setResultsStatus("error", error.message);
      return;
    }
  }
  showResults();
}

document.addEventListener("DOMContentLoaded", () => {
  populateOutputDirOptions();
  document.getElementById("load-results-button").addEventListener("click", loadResults);
  document.getElementById("clear-results-button").addEventListener("click", clearResults);
  document.getElementById("results-play-button").addEventListener("click", toggleResultsPlayback);
  document.getElementById("results-step-back").addEventListener("click", () => stepResults(-1));
  document.getElementById("results-step-forward").addEventListener("click", () => stepResults(1));

  document.getElementById("results-overview").addEventListener("click", seekFromOverview);

  document.getElementById("results-time-slider").addEventListener("input", (e) => {
    resultsState.timeIndex = parseInt(e.target.value, 10);
    scheduleFeatureStateUpdate();
  });

  document.getElementById("results-speed-slider").addEventListener("input", (e) => {
    resultsState.playSpeed = parseInt(e.target.value, 10);
    document.getElementById("results-speed-value").textContent = `${resultsState.playSpeed}x`;
    if (resultsState.isPlaying) {
      stopResultsPlayback();
      startResultsPlayback();
    }
  });

  document.querySelectorAll("#results-panel .var-btn").forEach((button) => {
    button.addEventListener("click", () => selectResultsVariable(button));
  });

  document.querySelectorAll("#results-panel .scale-btn").forEach((button) => {
    button.addEventListener("click", () => {
      document.querySelectorAll("#results-panel .scale-btn").forEach((b) =>
        b.classList.remove("active")
      );
      button.classList.add("active");
      resultsState.scale = button.dataset.scale;
      refreshResultsStyle();
    });
  });

  document.getElementById("results-palette").addEventListener("change", (e) => {
    resultsState.palette = e.target.value;
    refreshResultsStyle();
  });

  // main.js has created the map by now (its DOMContentLoaded handler runs first).
  resultsHoverPopup = new maplibregl.Popup({ closeButton: false, closeOnClick: false });
  // Bound to the invisible fat overlay so thin flowpaths are easy to hover.
  map.on("mousemove", "flowpaths-hover", onFlowpathHover);
  map.on("mouseleave", "flowpaths-hover", () => {
    resultsState.hoveredId = null;
    resultsHoverPopup.remove();
  });
});
