// Map and workflow-form wiring for the preprocessor UI.
// This script is loaded in <head>, so nothing here may touch the DOM at the
// top level — all lookups and listener registration happen inside the
// DOMContentLoaded handler at the bottom of the file.
// Depends on map_layers.js (loaded first) for updateIncomingStyle and HIDDEN_FILTER.

let map;
let gageHoverPopup;
let selectedGageMarker = null;

// Last divide clicked on the map (with its upstream index range and that of
// its outlet flowpath), kept so the upstream highlight can be recomputed when
// the subset type changes.
let lastClickedDivide = null;

// ---------------------------------------------------------------------------
// Workflow form
// ---------------------------------------------------------------------------

function isGageInput() {
  return document.getElementById("gage-checkbox").checked;
}

// Select the given input type (basin or gage) and fill in the id, as when a
// feature is picked on the map.
function setWorkflowInput(inputType, value) {
  document.getElementById("gage-checkbox").checked = inputType === "gage";
  document.getElementById("workflow-input").value = value;
  updateWorkflowInputPlaceholder();
}

function updateWorkflowInputPlaceholder() {
  document.getElementById("workflow-input").placeholder = isGageInput()
    ? "e.g. 01646500"
    : "e.g. cat-2739307";
  updateCliCommand();
}

function updateCliPrefix() {
  const usePip = document.getElementById("runcmd-toggle").checked;
  document.getElementById("cli-prefix").textContent = usePip
    ? "python -m ngiab_data_cli"
    : "uvx --from ngiab_data_preprocess cli";
}

// Rebuild the CLI command preview from the current form state.
function updateCliCommand() {
  const workflowInput = document.getElementById("workflow-input").value.trim();

  // The prefix starts hidden (opacity 0 in css) until there is a command.
  document.getElementById("cli-prefix").style.opacity = workflowInput ? 1 : 0;

  if (!workflowInput) {
    document.getElementById("cli-command").textContent = "";
    return;
  }

  const startDate = document.getElementById("start-time").value.split("T")[0];
  const endDate = document.getElementById("end-time").value.split("T")[0];
  const model = document.getElementById("model-select").value;

  let command = `-i ${workflowInput}`;
  if (isGageInput()) command += " --gage";
  command += ` -sfr --start ${startDate} --end ${endDate}`;
  if (model) command += ` --${model}`;
  command += " --run";

  document.getElementById("cli-command").textContent = command;
}

// Zoom to the gage id in the workflow input and drop a marker on it.
async function zoomToGage() {
  const gageId = document.getElementById("workflow-input").value.trim();
  if (!gageId) return;

  try {
    const response = await fetch("/gage_location", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ gage_id: gageId }),
    });
    const data = await response.json();

    if (!response.ok) {
      console.error(data.error);
      return;
    }

    const coordinates = [data.lon, data.lat];

    selectedGageMarker?.remove();
    selectedGageMarker = new maplibregl.Marker()
      .setLngLat(coordinates)
      .setPopup(new maplibregl.Popup().setHTML(`Gage ${data.gage_id}`))
      .addTo(map);

    map.flyTo({ center: coordinates, zoom: 10, essential: true });
  } catch (error) {
    console.error("Error zooming to gage:", error);
  }
}

// ---------------------------------------------------------------------------
// Map
// ---------------------------------------------------------------------------

function initMap() {
  const protocol = new pmtiles.Protocol({ metadata: true });
  maplibregl.addProtocol("pmtiles", protocol.tile);
  maplibregl.setWorkerCount(4);

  // Match the basemap to the browser color scheme.
  const colorScheme = window.matchMedia?.("(prefers-color-scheme: dark)").matches
    ? "dark"
    : "light";
  const styleUrl = `https://communityhydrofabric.com/map/styles/${colorScheme}-base.json`;

  map = new maplibregl.Map({
    container: "map",
    center: [-96, 40],
    zoom: 4,
    validateStyle: false,
  });
  // updateIncomingStyle merges the hydrofabric sources and layers into the basemap.
  map.setStyle(styleUrl, { transformStyle: updateIncomingStyle });

  gageHoverPopup = new maplibregl.Popup({ closeButton: false, closeOnClick: false });

  // Resolving a divide's outlet flowpath needs loaded tiles, so defer clicks
  // that land before the first full load.
  map.on("click", "divides", (e) => {
    if (e.target.loaded()) return onDivideClick(e);
    e.target.once("load", () => onDivideClick(e));
  });
  map.on("mouseenter", "divides", () => {
    map.getCanvas().style.cursor = "pointer";
  });
  map.on("mouseleave", "divides", () => {
    map.getCanvas().style.cursor = "";
  });
  map.on("click", "conus_gages", onGageClick);
  map.on("mouseenter", "conus_gages", onGageMouseEnter);
  map.on("mouseleave", "conus_gages", onGageMouseLeave);
}

// Find the flowpath feature with the given id in the loaded tiles.
function queryFlowpath(flowpathId) {
  return map.querySourceFeatures("flowpaths", {
    sourceLayer: "flowpaths",
    filter: ["==", ["id"], flowpathId],
  })[0];
}

function onDivideClick(e) {
  // Selection is disabled while t-route results are painted on the flowpaths.
  if (resultsState.originalPaint) return;
  if (!e.features?.length) return;
  const divide = e.features[0];

  // The divide's outlet flowpath (toid) carries the wider upstream range used
  // for nexus subsetting.
  const outletFlowpath = queryFlowpath(divide.properties.toid);

  lastClickedDivide = {
    catId: `cat-${divide.id}`,
    lngLat: e.lngLat,
    upstreamId: divide.properties.upstream_id,
    numUpstreams: divide.properties.num_upstreams,
    outlet: outletFlowpath && {
      upstreamId: outletFlowpath.properties.upstream_id,
      numUpstreams: outletFlowpath.properties.num_upstreams,
    },
  };

  setWorkflowInput("basin", lastClickedDivide.catId);
  updateUpstreamHighlight();
}

// Highlight the last clicked divide and everything upstream of it, entirely
// client-side: the tiles carry a preorder index where a feature's upstreams
// are exactly those with upstream_id in (upstream_id, upstream_id + num_upstreams].
function updateUpstreamHighlight() {
  if (!lastClickedDivide) return;
  const { upstreamId, numUpstreams, outlet, lngLat } = lastClickedDivide;

  // Nexus mode subsets from the divide's outlet flowpath, which also covers
  // sibling catchments draining to the same nexus.
  const byNexus = document.getElementById("radio-nexus").checked;
  if (byNexus && !outlet) {
    console.warn("Outlet flowpath not in loaded tiles; falling back to catchment subset");
  }
  const range = (byNexus && outlet) || { upstreamId, numUpstreams };

  map.setFilter("selected-divides", ["==", "upstream_id", upstreamId]);
  map.setFilter("upstream-divides", [
    "all",
    [">", "upstream_id", range.upstreamId],
    ["<=", "upstream_id", range.upstreamId + range.numUpstreams],
    ["!=", "upstream_id", upstreamId],
  ]);

  if (range.numUpstreams === 0) {
    new maplibregl.Popup().setLngLat(lngLat).setHTML("No upstreams").addTo(map);
  }
}

function onGageClick(e) {
  const gageId = e.features[0].properties.hl_link;
  setWorkflowInput("gage", gageId);

  const usgsUrl = `https://waterdata.usgs.gov/monitoring-location/${gageId}`;
  new maplibregl.Popup()
    .setLngLat(e.lngLat)
    .setHTML(
      `Selected gage ${gageId}<br>` +
      `<a href="${usgsUrl}" target="_blank" rel="noopener noreferrer">Open USGS monitoring location</a>`
    )
    .addTo(map);
}

function onGageMouseEnter(e) {
  map.getCanvas().style.cursor = "pointer";

  // If the map is zoomed out far enough to show multiple world copies, shift
  // the popup onto the copy being pointed at.
  const coordinates = e.features[0].geometry.coordinates.slice();
  while (Math.abs(e.lngLat.lng - coordinates[0]) > 180) {
    coordinates[0] += e.lngLat.lng > coordinates[0] ? 360 : -360;
  }

  gageHoverPopup
    .setLngLat(coordinates)
    .setHTML(`${e.features[0].properties.hl_uri}<br> click to select gage`)
    .addTo(map);
}

function onGageMouseLeave() {
  map.getCanvas().style.cursor = "";
  gageHoverPopup.remove();
}

// Zoom to the loaded results, the selected gage, or the last clicked divide.
function zoomToSelection() {
  const resultsBounds = resultsState.originalPaint && resultsData()?.bounds;
  if (resultsBounds) {
    const [minX, minY, maxX, maxY] = resultsBounds;
    map.fitBounds([[minX, minY], [maxX, maxY]], { padding: 60 });
  } else if (selectedGageMarker) {
    map.flyTo({ center: selectedGageMarker.getLngLat(), zoom: 10, essential: true });
  } else if (lastClickedDivide) {
    map.flyTo({ center: lastClickedDivide.lngLat, zoom: 10, essential: true });
  }
}

// ---------------------------------------------------------------------------
// DOM wiring
// ---------------------------------------------------------------------------

// Map-settings checkboxes and the layer each one shows/hides.
const LAYER_TOGGLES = [
  ["gages__input", "conus_gages"],
  ["camels__input", "camels"],
  ["nwm__input", "nwm_zarr_chunks"],
  ["aorc__input", "aorc_zarr_chunks"],
];

function initLayerToggles() {
  for (const [checkboxId, layerId] of LAYER_TOGGLES) {
    const checkbox = document.getElementById(checkboxId);
    checkbox.addEventListener("change", () => {
      map.setFilter(layerId, checkbox.checked ? null : HIDDEN_FILTER);
    });
  }
}

// Keep each toggle switch's sliding handle text in sync with its checked state.
function initToggleSwitches() {
  document.querySelectorAll(".toggle-switch").forEach((toggleSwitch) => {
    const input = toggleSwitch.querySelector(".toggle-input");
    const handle = toggleSwitch.querySelector(".toggle-handle");
    const leftText = toggleSwitch.querySelector(".toggle-text-left").textContent;
    const rightText = toggleSwitch.querySelector(".toggle-text-right").textContent;

    const updateHandle = () => {
      handle.textContent = input.checked ? rightText : leftText;
    };
    updateHandle();
    // Delay so the text changes mid-slide.
    input.addEventListener("change", () => setTimeout(updateHandle, 180));
  });
}

function initWorkflowForm() {
  const on = (id, event, handler) =>
    document.getElementById(id).addEventListener(event, handler);

  on("runcmd-toggle", "change", () => {
    updateCliPrefix();
    updateCliCommand();
  });

  on("start-time", "change", updateCliCommand);
  on("end-time", "change", updateCliCommand);
  on("model-select", "change", updateCliCommand);
  on("gage-checkbox", "change", updateWorkflowInputPlaceholder);

  on("workflow-input", "input", updateCliCommand);
  on("workflow-input", "change", () => {
    if (isGageInput()) zoomToGage();
  });

  // Recompute the upstream highlight when the subset type changes.
  on("radio-nexus", "change", updateUpstreamHighlight);
  on("radio-catchment", "change", updateUpstreamHighlight);

  on("zoom-selection", "click", zoomToSelection);
}

document.addEventListener("DOMContentLoaded", () => {
  initMap();
  initWorkflowForm();
  initLayerToggles();
  initToggleSwitches();
  updateCliPrefix();
  updateWorkflowInputPlaceholder();
});
