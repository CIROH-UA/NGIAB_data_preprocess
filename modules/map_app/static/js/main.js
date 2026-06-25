var colorDict = {
  selectedCatOutline: getComputedStyle(document.documentElement).getPropertyValue('--selected-cat-outline'),
  selectedCatFill: getComputedStyle(document.documentElement).getPropertyValue('--selected-cat-fill'),
  upstreamCatOutline: getComputedStyle(document.documentElement).getPropertyValue('--upstream-cat-outline'),
  upstreamCatFill: getComputedStyle(document.documentElement).getPropertyValue('--upstream-cat-fill'),
  flowlineToCatOutline: getComputedStyle(document.documentElement).getPropertyValue('--flowline-to-cat-outline'),
  flowlineToNexusOutline: getComputedStyle(document.documentElement).getPropertyValue('--flowline-to-nexus-outline'),
  nexusOutline: getComputedStyle(document.documentElement).getPropertyValue('--nexus-outline'),
  nexusFill: getComputedStyle(document.documentElement).getPropertyValue('--nexus-fill'),
  clearFill: getComputedStyle(document.documentElement).getPropertyValue('--clear-fill')
};

// Return the currently selected workflow input type (basin or gage).
function getWorkflowInputType() {
  return document.querySelector('input[name="input-type"]:checked').value;
}

// Update the workflow input placeholder and CLI preview when the input type changes.
function updateWorkflowInputPlaceholder() {
  const inputType = getWorkflowInputType();
  const workflowInput = document.getElementById("workflow-input");

  workflowInput.placeholder =
    inputType === "gage" ? "e.g. 01646500" : "e.g. cat-2739307";

  create_cli_command();
}
// Generate a CLI preview that reflects the current workflow configuration. 
function create_cli_command() {
  const cliPrefix = document.getElementById("cli-prefix");
  cliPrefix.style.opacity = 1;

  const inputType = getWorkflowInputType();
  const workflowInput = document.getElementById("workflow-input").value.trim();
  const startDate = document.getElementById("start-time").value.split("T")[0];
  const endDate = document.getElementById("end-time").value.split("T")[0];
  const outputRoot = document.getElementById("output-root").value.trim();
  const model = document.getElementById("model-select").value;
  const runNgiab = document.getElementById("run-ngiab").checked;

  if (!workflowInput) {
    $("#cli-command").text("");
    return;
  }

  let inputPart = `-i ${workflowInput}`;

  if (inputType === "gage") {
    inputPart += " --gage";
  }

  let command = `${inputPart} -sfr --start ${startDate} --end ${endDate}`;

  if (outputRoot) command += ` --output_root ${outputRoot}`;
  if (model) command += ` --${model}`;
  if (runNgiab) command += ` --run`;

  $("#cli-command").text(command);
}

function updateCommandPrefix() {
  const toggleInput = document.getElementById("runcmd-toggle");
  const cliPrefix = document.getElementById("cli-prefix");
  const uvxText = "uvx --from ngiab_data_preprocess cli";
  const pythonText = "python -m ngiab_data_cli";

  cliPrefix.textContent = toggleInput.checked ? pythonText : uvxText;
}

updateCommandPrefix();
updateWorkflowInputPlaceholder();

document.getElementById("runcmd-toggle").addEventListener("change", function () {
  updateCommandPrefix();
  create_cli_command();
});

document.getElementById("start-time").addEventListener("change", create_cli_command);
document.getElementById("end-time").addEventListener("change", create_cli_command);
document.getElementById("workflow-input").addEventListener("input", create_cli_command);

document.getElementById("workflow-input").addEventListener("change", function () {
  if (getWorkflowInputType() === "gage") {
    zoomToGage();
  }
});

document.querySelectorAll('input[name="input-type"]').forEach((radio) => {
  radio.addEventListener("change", updateWorkflowInputPlaceholder);
});

document.getElementById("output-root").addEventListener("input", create_cli_command);
document.getElementById("model-select").addEventListener("change", create_cli_command);
document.getElementById("run-ngiab").addEventListener("change", create_cli_command);

// Add the PMTiles plugin to the maplibregl global.
let protocol = new pmtiles.Protocol({ metadata: true });
maplibregl.addProtocol("pmtiles", protocol.tile);

// Select light-style if the browser is in light mode.
var style = 'https://communityhydrofabric.s3.us-east-1.amazonaws.com/map/styles/light-style.json';
var colorScheme = "light";

if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
  style = 'https://communityhydrofabric.s3.us-east-1.amazonaws.com/map/styles/dark-style.json';
  colorScheme = "dark";
}

var map = new maplibregl.Map({
  container: "map", // container id
  style: style, // style URL
  center: [-96, 40], //starting position [lng, lat]
  zoom: 4, //starting zoom
});

let selectedGageMarker = null;

// Zoom to the requested USGS gage and place a marker on the map.
async function zoomToGage() {
  const gageId = document.getElementById("workflow-input").value.trim();

  if (!gageId) {
    return;
  }

  try {
    const response = await fetch("/gage_location", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({gage_id: gageId}),
    });

    const data = await response.json();

    if (!response.ok) {
      console.error(data.error);
      return;
    }

    const coordinates = [data.lon, data.lat];

    if (selectedGageMarker) {
      selectedGageMarker.remove();
    }

    selectedGageMarker = new maplibregl.Marker()
      .setLngLat(coordinates)
      .setPopup(new maplibregl.Popup().setHTML(`Gage ${data.gage_id}`))
      .addTo(map);

    map.flyTo({
      center: coordinates,
      zoom: 10,
      essential: true,
    });
  } catch (error) {
    console.error("Error zooming to gage:", error);
  }
}

map.on("load", () => {
  map.addSource("camels_basins", {
    type: "vector",
    url: "pmtiles://https://communityhydrofabric.s3.us-east-1.amazonaws.com/map/camels.pmtiles",
  });

  map.addLayer({
    id: "camels",
    type: "line",
    source: "camels_basins",
    "source-layer": "camels_basins",
    layout: {},
    filter: ["any", ["==", "hru_id", ""]],
    paint: {
      "line-width": 1.5,
      "line-color": ["rgba", 134, 30, 232, 1],
    },
  });
});

let nwm_paint;
let aorc_paint;

if (colorScheme === "light") {
  nwm_paint = {
    "line-width": 1,
    "line-color": ["rgba", 0, 0, 0, 1],
  };
  aorc_paint = {
    "line-width": 1,
    "line-color": ["rgba", 71, 58, 222, 1],
  };
}

if (colorScheme === "dark") {
  nwm_paint = {
    "line-width": 1,
    "line-color": ["rgba", 255, 255, 255, 1],
  };
  aorc_paint = {
    "line-width": 1,
    "line-color": ["rgba", 242, 252, 126, 1],
  };
}

map.on("load", () => {
  map.addSource("nwm_zarr_chunks", {
    type: "vector",
    url: "pmtiles://https://communityhydrofabric.s3.us-east-1.amazonaws.com/map/forcing_chunks/nwm_retro_v3_zarr_chunks.pmtiles",
  });

  map.addSource("aorc_zarr_chunks", {
    type: "vector",
    url: "pmtiles://https://communityhydrofabric.s3.us-east-1.amazonaws.com/map/forcing_chunks/aorc_zarr_chunks.pmtiles",
  });

  map.addLayer({
    id: "nwm_zarr_chunks",
    type: "line",
    source: "nwm_zarr_chunks",
    "source-layer": "nwm_zarr_chunks",
    layout: {},
    filter: ["any"],
    paint: nwm_paint,
  });

  map.addLayer({
    id: "aorc_zarr_chunks",
    type: "line",
    source: "aorc_zarr_chunks",
    "source-layer": "aorc_zarr_chunks",
    layout: {},
    filter: ["any"],
    paint: aorc_paint,
  });
});

function update_map(cat_id, e) {
  map.setFilter('selected-catchments', ['any', ['in', 'divide_id', cat_id]]);
  map.setFilter('upstream-catchments', ['any', ['in', 'divide_id', ""]]);
  // get the position of the subset toggle
  // false means subset by nexus, true means subset by catchment

  var nexus_catchment = document.getElementById('radio-catchment').checked;
  var subset_type = nexus_catchment ? 'catchment' : 'nexus';

  if (subset_type === 'catchment') {
    fetch('/get_upstream_catids', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(cat_id),
    })
    .then(response => response.json())
    .then(data => {
      map.setFilter('upstream-catchments', ['any', ['in', 'divide_id', ...data]]);

      if (data.length === 0) {
        new maplibregl.Popup()
          .setLngLat(e.lngLat)
          .setHTML('No upstreams')
          .addTo(map);
      }
    });
  } else {
    fetch('/get_upstream_wbids', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(cat_id),
    })
    .then(response => response.json())
    .then(data => {
      map.setFilter('upstream-catchments', ['any', ['in', 'divide_id', ...data]]);

      if (data.length === 0) {
        new maplibregl.Popup()
          .setLngLat(e.lngLat)
          .setHTML('No upstreams')
          .addTo(map);
      }
    });
  }
}

let lastClickedLngLat = null;
let lastClickedCatId = null;

// Keep the workflow input synchronized with map selections.
map.on('click', 'catchments', (e) => {
  const cat_id = e.features[0].properties.divide_id;
  lastClickedCatId = cat_id;
  lastClickedLngLat = e.lngLat; // Store the last clicked location

  document.querySelector('input[name="input-type"][value="basin"]').checked = true;
  updateWorkflowInputPlaceholder();
  document.getElementById("workflow-input").value = cat_id;

  update_map(cat_id, e);
  create_cli_command();
});

document.getElementById("radio-catchment").addEventListener('change', function() {
  if (lastClickedCatId && lastClickedLngLat) {
    const fakeEvent = { lngLat: lastClickedLngLat };
    update_map(lastClickedCatId, fakeEvent);
  }
});

document.getElementById("radio-nexus").addEventListener('change', function() {
  if (lastClickedCatId && lastClickedLngLat) {
    const fakeEvent = { lngLat: lastClickedLngLat };
    update_map(lastClickedCatId, fakeEvent);
  }
});

// Create a popup, but don't add it to the map yet.
const popup = new maplibregl.Popup({
  closeButton: false,
  closeOnClick: false
});

map.on('mouseenter', 'conus_gages', (e) => {
  // Change the cursor style as a UI indicator.
  map.getCanvas().style.cursor = 'pointer';

  const coordinates = e.features[0].geometry.coordinates.slice();
  const description = e.features[0].properties.hl_uri + "<br> click to select gage";

  // Ensure that if the map is zoomed out such that multiple
  // copies of the feature are visible, the popup appears
  // over the copy being pointed to.
  while (Math.abs(e.lngLat.lng - coordinates[0]) > 180) {
    coordinates[0] += e.lngLat.lng > coordinates[0] ? 360 : -360;
  }

  // Populate the popup and set its coordinates
  // based on the feature found.
  popup.setLngLat(coordinates).setHTML(description).addTo(map);
});

map.on("mouseleave", "conus_gages", () => {
  map.getCanvas().style.cursor = "";
  popup.remove();
});

// Populate the workflow using the selected gage from the map.
map.on("click", "conus_gages", (e) => {
  const gageId = e.features[0].properties.hl_link;

  document.querySelector('input[name="input-type"][value="gage"]').checked = true;
  updateWorkflowInputPlaceholder();
  document.getElementById("workflow-input").value = gageId;

  create_cli_command();

  new maplibregl.Popup()
    .setLngLat(e.lngLat)
    .setHTML(`Selected gage ${gageId}`)
    .addTo(map);
});

// TOGGLE BUTTON LOGIC
function initializeToggleSwitches() {
  // Find all toggle switches
  const toggleSwitches = document.querySelectorAll(".toggle-switch");

  toggleSwitches.forEach((toggleSwitch) => {
    const toggleInput = toggleSwitch.querySelector(".toggle-input");
    const toggleHandle = toggleSwitch.querySelector(".toggle-handle");
    const leftText = toggleSwitch.querySelector(".toggle-text-left").textContent;
    const rightText = toggleSwitch.querySelector(".toggle-text-right").textContent;

    toggleHandle.textContent = toggleInput.checked ? rightText : leftText;

    toggleInput.addEventListener("change", function () {
      setTimeout(() => {
        toggleHandle.textContent = this.checked ? rightText : leftText;
      }, 180);
    });
  });
}

document.addEventListener("DOMContentLoaded", initializeToggleSwitches);

const toggleSwitchGages = document.querySelector("#gages__input");
toggleSwitchGages.addEventListener("change", function () {
  if (toggleSwitchGages.checked) {
    map.setFilter("conus_gages", null); //show gages
  } else {
    map.setFilter("conus_gages", ["any", ["==", "hl_uri", ""]]); //hide gages
  }
});

const toggleSwitchCamels = document.querySelector("#camels__input");
toggleSwitchCamels.addEventListener("change", function () {
  if (toggleSwitchCamels.checked) {
    map.setFilter("camels", null);
  } else {
    map.setFilter("camels", ["any", ["==", "hl_uri", ""]]);
  }
});

const toggleSwitchNwm = document.querySelector("#nwm__input");
toggleSwitchNwm.addEventListener("change", function () {
  if (toggleSwitchNwm.checked) {
    map.setFilter("nwm_zarr_chunks", null);
  } else {
    map.setFilter("nwm_zarr_chunks", ["any", ["==", "hl_uri", ""]]);
  }
});

const toggleSwitchAorc = document.querySelector("#aorc__input");
toggleSwitchAorc.addEventListener("change", function () {
  if (toggleSwitchAorc.checked) {
    map.setFilter("aorc_zarr_chunks", null);
  } else {
    map.setFilter("aorc_zarr_chunks", ["any", ["==", "hl_uri", ""]]);
  }
});