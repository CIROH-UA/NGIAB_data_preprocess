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

// A function that creates a cli command from the interface
function create_cli_command() {
  const cliPrefix = document.getElementById("cli-prefix");
  cliPrefix.style.opacity = 1;
  var selected_basins = $("#selected-basins").text();
  var start_time = document.getElementById("start-time").value.split("T")[0];
  var end_time = document.getElementById("end-time").value.split("T")[0];
  var command = `-i ${selected_basins} --subset --start ${start_time} --end ${end_time} --forcings --realization --run`;
  var command_all = `-i ${selected_basins} --start ${start_time} --end ${end_time} --all`;
  if (selected_basins != "None - get clicking!") {
    $("#cli-command").text(command);
  }
}

function updateCommandPrefix() {
  const toggleInput = document.getElementById("runcmd-toggle");
  const cliPrefix = document.getElementById("cli-prefix");
  const uvxText = "uvx --from ngiab_data_preprocess cli";
  const pythonText = "python -m ngiab_data_cli";
  // Set initial handle text based on the default state using data attribute
  cliPrefix.textContent = toggleInput.checked ? pythonText : uvxText;
}
document.getElementById("runcmd-toggle").addEventListener('change', updateCommandPrefix);

// These functions are exported by data_processing.js
document.getElementById('map').addEventListener('click', create_cli_command);
// Add keydown handler
document.getElementById('map').addEventListener('keydown', (e) => {
  if (e.key === 'Enter') {
    create_cli_command(e);
  }
});
document.getElementById('start-time').addEventListener('change', create_cli_command);
document.getElementById('end-time').addEventListener('change', create_cli_command);


// add the PMTiles plugin to the maplibregl global.
let protocol = new pmtiles.Protocol({ metadata: true });
maplibregl.addProtocol("pmtiles", protocol.tile);

// select light-style if the browser is in light mode
// select dark-style if the browser is in dark mode
var style = 'https://communityhydrofabric.s3.us-east-1.amazonaws.com/map/styles/light-style.json';
var colorScheme = "light";
if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
  style = 'https://communityhydrofabric.s3.us-east-1.amazonaws.com/map/styles/dark-style.json';
  colorScheme = "dark";
}
var map = new maplibregl.Map({
  container: "map", // container id
  style: style, // style URL
  center: [-96, 40], // starting position [lng, lat]
  zoom: 4, // starting zoom
});

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

if (colorScheme == "light") {
  nwm_paint = {
    "line-width": 1,
    "line-color": ["rgba", 0, 0, 0, 1],
  };
  aorc_paint = {
    "line-width": 1,
    "line-color": ["rgba", 71, 58, 222, 1],
  };
}
if (colorScheme == "dark") {    
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

// crosshair and gage selection box rules
const crosshair = document.getElementById('map-crosshair');
const box = document.getElementById('map-box');
let lastInteractionWasKeyboard = false;
// Detect keyboard interaction so just clicking on the map doesn't show the crosshair
document.addEventListener('keydown', (e) => {
  if (e.key === 'Tab' || e.key === 'Enter') {
    lastInteractionWasKeyboard = true;
  }
});
document.addEventListener('mousedown', () => {
  lastInteractionWasKeyboard = false;
});

document.getElementById('map').addEventListener('focusin', () => {
  if (!lastInteractionWasKeyboard) return; // Only show crosshair if last interaction was keyboard
  crosshair.style.display = 'block';
  if (showGages) {
    box.style.display = 'block';
  }
});
document.getElementById('map').addEventListener('focusout', () => {
  crosshair.style.display = 'none';
  box.style.display = 'none';
});

function update_map(cat_id, e) {
  $('#selected-basins').text(cat_id)
  map.setFilter('selected-catchments', ['any', ['in', 'divide_id', cat_id]]);
  map.setFilter('upstream-catchments', ['any', ['in', 'divide_id', ""]])
  // get the position of the subset toggle
  // false means subset by nexus, true means subset by catchment
  var nexus_catchment = document.getElementById('subset-toggle').checked;
  var subset_type = nexus_catchment ? 'catchment' : 'nexus';
  console.log('subset_type:', subset_type);

  if (subset_type == 'catchment') {
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
map.on('click', 'catchments', (e) => {
  cat_id = e.features[0].properties.divide_id;
  lastClickedLngLat = e.lngLat; // Store the last clicked location
  update_map(cat_id, e);
});

map.getCanvas().addEventListener('keydown', (e) => {
  if (e.key === 'Enter') {
    // Get the map canvas size
    const canvas = map.getCanvas();
    const rect = canvas.getBoundingClientRect();

    // Compute the center of the map in pixel coordinates
    const centerPoint = [rect.width / 2, rect.height / 2];

    // Query features at the center point
    const features = map.queryRenderedFeatures(centerPoint, { layers: ['catchments'] });

    if (features && features.length > 0) {
      const feature = features[0];
      const id = feature.properties.divide_id;
      const lngLat = map.getCenter(); // use the actual map center

      lastClickedLngLat = lngLat;
      update_map(id, { lngLat });
    }
  }
});


// When you want to use it (e.g., in your toggle handler):
document.getElementById("subset-toggle").addEventListener('change', function() {
  const cat_id = document.getElementById('selected-basins').textContent;
  if (cat_id && cat_id !== 'None - get clicking!' && lastClickedLngLat) {
    // Create a fake event with the last clicked location
    const fakeEvent = { lngLat: lastClickedLngLat };
    update_map(cat_id, fakeEvent);
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
  const description = e.features[0].properties.hl_uri + "<br> click for more info";

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

map.on("click", "conus_gages", (e) => {
  //  https://waterdata.usgs.gov/monitoring-location/02465000
  window.open(
    "https://waterdata.usgs.gov/monitoring-location/" +
    e.features[0].properties.hl_link,
    "_blank",
  );
});
show = false;

// TOGGLE BUTTON LOGIC
function initializeToggleSwitches() {
  // Find all toggle switches
  const toggleSwitches = document.querySelectorAll(".toggle-switch");
  // Process each toggle switch
  toggleSwitches.forEach((toggleSwitch) => {
    const toggleInput = toggleSwitch.querySelector(".toggle-input");
    const toggleHandle = toggleSwitch.querySelector(".toggle-handle");
    const leftText =
      toggleSwitch.querySelector(".toggle-text-left").textContent;
    const rightText =
      toggleSwitch.querySelector(".toggle-text-right").textContent;
    // Set initial handle text based on the default state using data attribute
    toggleHandle.textContent = toggleInput.checked ? rightText : leftText;
    // Add event listener
    toggleInput.addEventListener("change", function () {
      setTimeout(() => {
        if (this.checked) {
          toggleHandle.textContent = rightText;
        } else {
          toggleHandle.textContent = leftText;
        }
      }, 180);
    });
  });
}
const subsetToggle = document.querySelector("#subset-toggle");

subsetToggle.addEventListener("keydown", (e) => {
  if (e.code === "Space") {
    e.stopPropagation();
  }
});
        
document.addEventListener("DOMContentLoaded", initializeToggleSwitches);

showGages = false;
const toggleButtonGages = document.querySelector("#toggle-button-gages");
toggleButtonGages.addEventListener("click", () => {
  if (showGages) {
    map.setFilter("conus_gages", ["any", ["==", "hl_uri", ""]]);
    toggleButtonGages.innerText = "Show gages";
    showGages = false;
  } else {
    map.setFilter("conus_gages", null);
    toggleButtonGages.innerText = "Hide gages";
    showGages = true;
    if (!lastInteractionWasKeyboard) return;
    box.style.display = 'block';  
  }
});

function getBoxBounds() {
  const rect = box.getBoundingClientRect();
  console.log(rect)
  const mapRect = document.getElementById('map').getBoundingClientRect();

  const minX = rect.left - mapRect.left;
  const minY = rect.top - mapRect.top;
  const maxX = minX + rect.width;
  const maxY = minY + rect.height;

  return [[minX, minY], [maxX, maxY]];
}

function getFeaturesInBox(layers) {
  const bounds = getBoxBounds();
  const features = map.queryRenderedFeatures(bounds, { layers });
  return features;
}

document.getElementById('map').addEventListener('keydown', (e) => {
  if (e.code === 'Space') {
    e.preventDefault(); // prevents page from scrolling
    if (showGages && box.style.display === 'block') {
      const currentZoom = map.getZoom();
      const requiredZoom = 10; // adjust to whatever makes sense

      if (currentZoom >= requiredZoom) {
        const features = getFeaturesInBox(['conus_gages']);
        console.log('Features in box:', features);
        if (features.length > 0) {
          console.log('Selected features:', features);
          
          const coordinates = features[0].geometry.coordinates.slice();
          const description = features[0].properties.hl_uri + "<br> press enter for more info";
          const url = "https://waterdata.usgs.gov/monitoring-location/" +
            features[0].properties.hl_link;

          const popupContent = document.createElement('div');
          popupContent.setAttribute('tabindex', '0');
          popupContent.innerHTML = description

          // Add a keydown listener for the SPACE key
          popupContent.addEventListener('keydown', (evt) => {
            if (evt.code === 'Enter') {
              evt.preventDefault();
              window.open(url, '_blank');
            }
          });

          popup.setLngLat(coordinates).setDOMContent(popupContent).addTo(map);
        } else {
          console.log('No features inside the box.');
        }
    } else {
        console.log(`Zoom in further`);
      }
    }
  }
})

showCamels = false;
const toggleButtonCamels = document.querySelector("#toggle-button-camels");
toggleButtonCamels.addEventListener("click", () => {
  if (showCamels) {
    map.setFilter("camels", ["any", ["==", "hru_id", ""]]);
    toggleButtonCamels.innerText = "Show CAMELS basins";
    showCamels = false;
  } else {
    map.setFilter("camels", null);
    toggleButtonCamels.innerText = "Hide CAMELS basins";
    showCamels = true;
  }
});

showNwm = false;
const toggleButtonNwm = document.querySelector("#toggle-button-nwm");
toggleButtonNwm.addEventListener("click", () => {
  if (showNwm) {
    map.setFilter("nwm_zarr_chunks", ["any"]);
    toggleButtonNwm.innerText = "Overlay NWM chunks";
    showNwm = false;
  } else {
    map.setFilter("nwm_zarr_chunks", null);
    toggleButtonNwm.innerText = "Hide NWM chunks";
    showNwm = true;
  }
});

showAorc = false;
const toggleButtonAorc = document.querySelector("#toggle-button-aorc");
toggleButtonAorc.addEventListener("click", () => {
  if (showAorc) {
    map.setFilter("aorc_zarr_chunks", ["any"]);
    toggleButtonAorc.innerText = "Overlay AORC chunks";
    showAorc = false;
  } else {
    map.setFilter("aorc_zarr_chunks", null);
    toggleButtonAorc.innerText = "Hide AORC chunks";
    showAorc = true;
  }
});
