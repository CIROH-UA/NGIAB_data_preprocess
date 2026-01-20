// ============================================
// HYDRO SELECT - MapLibre + DeckGL Application
// Reverse-controlled mode: DeckGL handles interaction, MapLibre renders base map
// ============================================

const { Deck } = deck;
const { ScatterplotLayer, TripsLayer } = deck;

// HubbleGL for video export
const { DeckAdapter, DeckAnimation } = hubble;

// State management
const state = {
  selectedBasin: null,
  upstreamBasins: [],
  upstreamFlowpaths: [], // Store flowpath IDs for animation
  flowpathTrips: [], // GeoJSON features converted to trips format
  layers: {
    gages: false,
    camels: false,
    nwm: false,
    aorc: false,
  },
  subsetType: "nexus",
  dataSource: "nwm",
  commandType: "uvx",
  lastClickLngLat: null,
  viewState: {
    longitude: -96,
    latitude: 40,
    zoom: 4,
    pitch: 0,
    bearing: 0,
  },
  clickRings: [], // For click animations
  animationTime: 0, // Current animation time for trips
  tripsLoopLength: 1800, // Animation loop duration in "frames"
  animationSpeed: 10, // Speed multiplier
  // Recording state
  isRecording: false,
  recordingProgress: 0,
};

// Recording adapter
let deckAdapter = null;

// PMTiles sources
const pmtileSources = {
  gages:
    "pmtiles://https://communityhydrofabric.s3.us-east-1.amazonaws.com/map/conus_gages.pmtiles",
  camels:
    "pmtiles://https://communityhydrofabric.s3.us-east-1.amazonaws.com/map/camels.pmtiles",
  nwm: "pmtiles://https://communityhydrofabric.s3.us-east-1.amazonaws.com/map/forcing_chunks/nwm_retro_v3_zarr_chunks.pmtiles",
  aorc: "pmtiles://https://communityhydrofabric.s3.us-east-1.amazonaws.com/map/forcing_chunks/aorc_zarr_chunks.pmtiles",
};

// Map and deck instances
let map = null;
let deckgl = null;

// Initialize the application
function init() {
  initMap();
  initEventListeners();
  startLogPolling();
  startClickAnimation();
  startTripsAnimation();
}

// Initialize MapLibre and DeckGL
function initMap() {
  // Register PMTiles protocol
  const protocol = new pmtiles.Protocol();
  maplibregl.addProtocol("pmtiles", protocol.tile);

  // Detect color scheme
  const isDark =
    window.matchMedia &&
    window.matchMedia("(prefers-color-scheme: dark)").matches;
  const style = isDark
    ? "https://communityhydrofabric.s3.us-east-1.amazonaws.com/map/styles/dark-style.json"
    : "https://communityhydrofabric.s3.us-east-1.amazonaws.com/map/styles/light-style.json";

  // Create MapLibre map (non-interactive, controlled by DeckGL)
  map = new maplibregl.Map({
    container: "map",
    style: style,
    center: [state.viewState.longitude, state.viewState.latitude],
    zoom: state.viewState.zoom,
    interactive: false, // DeckGL handles all interactions
    preserveDrawingBuffer: true, // Required for recording
  });

  map.on("load", () => {
    addMapSources();
    addMapLayers();
  });

  // Create DeckGL overlay on top of the map
  deckgl = new Deck({
    parent: document.getElementById("map"),
    style: { position: "absolute", top: 0, left: 0, zIndex: 1 },
    controller: true,
    initialViewState: state.viewState,
    onViewStateChange: ({ viewState }) => {
      state.viewState = viewState;
      // Sync MapLibre camera with DeckGL
      map.jumpTo({
        center: [viewState.longitude, viewState.latitude],
        zoom: viewState.zoom,
        bearing: viewState.bearing,
        pitch: viewState.pitch,
      });
    },
    onClick: handleDeckClick,
    onHover: handleDeckHover,
    layers: getDeckLayers(),
    getCursor: () => "crosshair",
    glOptions: {
      preserveDrawingBuffer: true, // Required for recording
    },
  });

  // Initialize HubbleGL adapter
  deckAdapter = new DeckAdapter({});
}

// Add click ring for visual feedback
function addClickRing(longitude, latitude) {
  state.clickRings.push({
    position: [longitude, latitude],
    timestamp: Date.now(),
    radius: 100,
    opacity: 1,
  });
}

// Animate click rings
function startClickAnimation() {
  const animate = () => {
    const now = Date.now();
    const duration = 500; // ms

    // Update rings
    state.clickRings = state.clickRings.filter((ring) => {
      const elapsed = now - ring.timestamp;
      if (elapsed > duration) return false;

      const progress = elapsed / duration;
      // Radius grows based on zoom level
      const baseRadius = 30000 / Math.pow(2, state.viewState.zoom - 4);
      ring.radius = progress * baseRadius;
      ring.opacity = 1 - progress;
      return true;
    });

    // Update DeckGL layers
    if (deckgl && state.clickRings.length >= 0) {
      deckgl.setProps({ layers: getDeckLayers() });
    }

    requestAnimationFrame(animate);
  };
  animate();
}

// Get DeckGL layers (click rings + animated flowpaths)
function getDeckLayers() {
  const layers = [];

  // Animated flowpaths using TripsLayer
  if (state.flowpathTrips.length > 0) {
    layers.push(
      new TripsLayer({
        id: "flowpath-trips",
        data: state.flowpathTrips,
        getPath: (d) => d.path,
        getTimestamps: (d) => d.timestamps,
        getColor: [0, 212, 255],
        opacity: 0.9,
        widthMinPixels: 3,
        widthMaxPixels: 6,
        jointRounded: true,
        capRounded: true,
        trailLength: 300,
        currentTime: state.animationTime,
        shadowEnabled: false,
      }),
    );

    // Add a glow layer behind the trips
    layers.push(
      new TripsLayer({
        id: "flowpath-trips-glow",
        data: state.flowpathTrips,
        getPath: (d) => d.path,
        getTimestamps: (d) => d.timestamps,
        getColor: [0, 212, 255, 100],
        opacity: 0.5,
        widthMinPixels: 8,
        widthMaxPixels: 14,
        jointRounded: true,
        capRounded: true,
        trailLength: 500,
        currentTime: state.animationTime,
        shadowEnabled: false,
      }),
    );
  }

  // Click ring animation layer
  if (state.clickRings.length > 0) {
    layers.push(
      new ScatterplotLayer({
        id: "click-rings",
        data: state.clickRings,
        getPosition: (d) => d.position,
        getRadius: (d) => d.radius,
        getFillColor: [0, 0, 0, 0],
        getLineColor: (d) => [0, 212, 255, Math.floor(d.opacity * 255)],
        stroked: true,
        filled: false,
        lineWidthMinPixels: 2,
        lineWidthMaxPixels: 3,
        radiusUnits: "meters",
        pickable: false,
        updateTriggers: {
          getRadius: [state.clickRings.map((r) => r.radius)],
          getLineColor: [state.clickRings.map((r) => r.opacity)],
        },
      }),
    );
  }

  return layers;
}

// Start the trips animation loop
function startTripsAnimation() {
  const animate = () => {
    // Increment animation time
    state.animationTime =
      (state.animationTime + state.animationSpeed) % state.tripsLoopLength;

    // Update DeckGL layers
    if (deckgl && state.flowpathTrips.length > 0) {
      deckgl.setProps({ layers: getDeckLayers() });
    }

    requestAnimationFrame(animate);
  };
  animate();
}

// Convert flowpath coordinates to trips format
// Timestamps are assigned so water "flows" downstream (from end of array to start)
// function convertFlowpathsToTrips(features) {
//   return features.map((feature, index) => {
//     const coords = feature.geometry.coordinates;
//     const numPoints = coords.length;

//     // Reverse the path so animation flows downstream
//     // (flowpaths are typically digitized upstream to downstream)
//     const path = [...coords].reverse();

//     // Create timestamps - spread across the loop with offset per feature
//     // This creates staggered "pulses" of water
//     const featureOffset = (index * 50) % state.tripsLoopLength;
//     const timestamps = path.map((_, i) => {
//       return (
//         featureOffset + (i / (numPoints - 1)) * (state.tripsLoopLength * 0.6)
//       );
//     });

//     return {
//       path,
//       timestamps,
//     };
//   });
// }

function convertFlowpathsToTrips(features) {
  const toids = features.map((feature) =>
    parseInt(feature.properties.toid.split("-")[1]),
  );
  const ids = features.map((feature) =>
    parseInt(feature.properties.id.split("-")[1]),
  );
  const idToToid = new Map(ids.map((id, i) => [id, toids[i]]));

  // Build a map of id to feature for quick lookup
  const idToFeature = new Map(
    features.map((f) => [parseInt(f.properties.id.split("-")[1]), f]),
  );

  // Calculate "depth" - how many segments downstream to the outlet
  function getDepthToOutlet(id, memo = new Map()) {
    if (memo.has(id)) return memo.get(id);
    const nextId = idToToid.get(id);
    if (nextId === undefined || !idToFeature.has(nextId)) {
      memo.set(id, 0);
      return 0;
    }
    const depth = 1 + getDepthToOutlet(nextId, memo);
    memo.set(id, depth);
    return depth;
  }

  const memo = new Map();
  const timePerSegment =
    state.tripsLoopLength /
    (Math.max(...ids.map((id) => getDepthToOutlet(id, memo))) + 1);

  return features.map((feature) => {
    // console.log(feature);
    const coords = feature.geometry.coordinates;
    const id = parseInt(feature.properties.id.split("-")[1]);
    const depth = Math.min(getDepthToOutlet(id, memo), features.length);

    // This segment ends when the next one starts
    const endTime = state.tripsLoopLength - depth * timePerSegment;
    const startTime = endTime - timePerSegment;

    const path = coords;
    const timestamps = path.map((_, i) => {
      return startTime + (i / (path.length - 1)) * timePerSegment;
    });

    return {
      path,
      timestamps,
    };
  });
}

// Handle DeckGL click - query MapLibre for features
function handleDeckClick(info, event) {
  const { coordinate, x, y } = info;

  if (!coordinate || !map) return true;

  // Add visual click feedback
  addClickRing(coordinate[0], coordinate[1]);

  // Query MapLibre for catchment features at click point
  // Base style uses layer id 'catchments' with source-layer 'conus_divides'
  const point = map.project([coordinate[0], coordinate[1]]);

  // Query catchments
  const catchmentFeatures = map.queryRenderedFeatures(point, {
    layers: ["catchments"],
  });

  if (catchmentFeatures && catchmentFeatures.length > 0) {
    const feature = catchmentFeatures[0];
    const divideId = feature.properties.divide_id;
    if (divideId) {
      state.lastClickLngLat = { lng: coordinate[0], lat: coordinate[1] };
      selectBasin(divideId);
      return true;
    }
  }

  // Query gages (base style uses 'conus_gages' layer)
  if (state.layers.gages) {
    const gageFeatures = map.queryRenderedFeatures(point, {
      layers: ["conus_gages"],
    });

    if (gageFeatures && gageFeatures.length > 0) {
      const feature = gageFeatures[0];
      const hlLink = feature.properties.hl_link;
      if (hlLink) {
        window.open(
          `https://waterdata.usgs.gov/monitoring-location/${hlLink}`,
          "_blank",
        );
      }
    }
  }

  return true;
}

// Handle DeckGL hover - show tooltip
function handleDeckHover(info) {
  const tooltip = document.getElementById("tooltip");
  const { coordinate, x, y } = info;

  if (!coordinate || !map) {
    tooltip.classList.remove("visible");
    return;
  }

  const point = map.project([coordinate[0], coordinate[1]]);
  // Base style uses 'catchments' layer
  const features = map.queryRenderedFeatures(point, {
    layers: ["catchments"],
  });

  if (features && features.length > 0) {
    const divideId = features[0].properties.divide_id;
    if (divideId) {
      tooltip.textContent = divideId;
      tooltip.style.left = `${x + 15}px`;
      tooltip.style.top = `${y + 15}px`;
      tooltip.classList.add("visible");
      return;
    }
  }

  tooltip.classList.remove("visible");
}

// Add PMTiles sources to map (only ones not in base style)
function addMapSources() {
  // hydrofabric source with divides, flowpaths, gages is already in base style
  // Only add additional sources not in base style

  map.addSource("camels", {
    type: "vector",
    url: pmtileSources.camels,
  });

  map.addSource("nwm", {
    type: "vector",
    url: pmtileSources.nwm,
  });

  map.addSource("aorc", {
    type: "vector",
    url: pmtileSources.aorc,
  });
}

// Add MapLibre layers (only ones not already in base style)
function addMapLayers() {
  const isDark =
    window.matchMedia &&
    window.matchMedia("(prefers-color-scheme: dark)").matches;

  // Base style already has:
  // - 'catchments' (fill, conus_divides)
  // - 'flowpaths' (line, conus_flowpaths)
  // - 'selected-catchments' (fill, conus_divides, filter on divide_id)
  // - 'upstream-catchments' (fill, conus_divides, filter on divide_id)
  // - 'conus_gages' (circle, conus_gages, filter on hl_reference)

  // Add a subtle static highlight for upstream flowpaths (DeckGL TripsLayer will animate on top)
  map.addLayer({
    id: "upstream-flowpaths-highlight",
    type: "line",
    source: "hydrofabric",
    "source-layer": "conus_flowpaths",
    filter: ["in", "id", ""],
    paint: {
      "line-color": "#00d4ff",
      "line-width": 2,
      "line-opacity": 0.3,
    },
  });

  // CAMELS basins layer (hidden by default)
  map.addLayer({
    id: "camels",
    type: "line",
    source: "camels",
    "source-layer": "camels_basins",
    filter: ["==", "hru_id", "__hidden__"],
    paint: {
      "line-color": "#861ee8",
      "line-width": 2,
    },
  });

  // NWM chunks layer (hidden by default)
  map.addLayer({
    id: "nwm-chunks",
    type: "line",
    source: "nwm",
    "source-layer": "nwm_zarr_chunks",
    filter: ["==", "id", "__hidden__"],
    paint: {
      "line-color": isDark ? "#ffffff" : "#000000",
      "line-width": 1,
    },
  });

  // AORC chunks layer (hidden by default)
  map.addLayer({
    id: "aorc-chunks",
    type: "line",
    source: "aorc",
    "source-layer": "aorc_zarr_chunks",
    filter: ["==", "id", "__hidden__"],
    paint: {
      "line-color": isDark ? "#f2fc7e" : "#473ade",
      "line-width": 1,
    },
  });
}

// Select a basin and fetch upstreams
async function selectBasin(catId) {
  state.selectedBasin = catId;
  document.getElementById("selected-basin").textContent = catId;

  // Update filters on layers from base style
  // selected-catchments uses filter: ["any",["in", "divide_id", ""]]
  map.setFilter("selected-catchments", ["in", "divide_id", catId]);

  updateCliCommand();

  // Fetch upstream catchments
  const endpoint =
    state.subsetType === "catchment"
      ? "/get_upstream_catids"
      : "/get_upstream_wbids";

  try {
    const response = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(catId),
    });

    const upstreams = await response.json();
    state.upstreamBasins = upstreams;

    document.getElementById("upstream-count").textContent = upstreams.length;

    // Update upstream-catchments filter from base style
    if (upstreams.length > 0) {
      map.setFilter("upstream-catchments", ["in", "divide_id", ...upstreams]);

      // Convert cat-XXXXX to wb-XXXXX for flowpath IDs
      const flowpathIds = upstreams.map((id) => "wb-" + id.split("-")[1]);
      // Also add the selected basin's flowpath
      flowpathIds.push("wb-" + catId.split("-")[1]);

      state.upstreamFlowpaths = flowpathIds;

      // Update MapLibre highlight layer
      map.setFilter("upstream-flowpaths-highlight", [
        "in",
        "id",
        ...flowpathIds,
      ]);

      // Query the flowpath geometries from the map for TripsLayer
      queryFlowpathGeometries(flowpathIds);
    } else {
      map.setFilter("upstream-catchments", ["in", "divide_id", ""]);
      map.setFilter("upstream-flowpaths-highlight", ["in", "id", ""]);
      state.upstreamFlowpaths = [];
      state.flowpathTrips = [];
      deckgl.setProps({ layers: getDeckLayers() });
    }
  } catch (error) {
    console.error("Error fetching upstreams:", error);
    state.upstreamBasins = [];
    state.upstreamFlowpaths = [];
    state.flowpathTrips = [];
    document.getElementById("upstream-count").textContent = "0";
    map.setFilter("upstream-catchments", ["in", "divide_id", ""]);
    map.setFilter("upstream-flowpaths-highlight", ["in", "id", ""]);
    deckgl.setProps({ layers: getDeckLayers() });
  }
}

// Query flowpath geometries from MapLibre and convert to trips format
function queryFlowpathGeometries(flowpathIds) {
  // Query all rendered features from the flowpaths layer
  const features = map.queryRenderedFeatures("hydrofabric", {
    sourceLayer: "conus_flowpaths",
    filter: ["in", "id", ...flowpathIds],
  });

  // if (features.length > 0) {
  // Convert to trips format
  state.flowpathTrips = convertFlowpathsToTrips(features);
  // Reset animation time for fresh start
  state.animationTime = 0;
  deckgl.setProps({ layers: getDeckLayers() });
  // } //else {
  // If no features found (might not be loaded yet), try again after a delay
  // setTimeout(() => {
  //   const retryFeatures = map.queryRenderedFeatures("hydrofabric", {
  //     sourceLayer: "conus_flowpaths",
  //     filter: ["in", "id", ...flowpathIds],
  //   });

  //   if (retryFeatures.length > 0) {
  //     state.flowpathTrips = convertFlowpathsToTrips(retryFeatures);
  //     state.animationTime = 0;
  //     deckgl.setProps({ layers: getDeckLayers() });
  //   }
  // }, 500);
  // }
}

// Update CLI command display
function updateCliCommand() {
  if (!state.selectedBasin) return;

  const startTime = document.getElementById("start-time").value.split("T")[0];
  const endTime = document.getElementById("end-time").value.split("T")[0];

  const prefix =
    state.commandType === "uvx"
      ? "uvx --from ngiab_data_preprocess cli"
      : "python -m ngiab_data_cli";

  const command = `${prefix} -i ${state.selectedBasin} --subset --start ${startTime} --end ${endTime} --forcings --realization --run`;

  document.getElementById("cli-command").textContent = command;
}

// Toggle layer visibility
function toggleLayer(layerId, visible) {
  if (!map || !map.getLayer(layerId)) return;

  if (visible) {
    // Show layer by removing filter or setting to show all
    if (layerId === "conus_gages") {
      map.setFilter(layerId, null); // Show all gages
    } else {
      map.setFilter(layerId, null);
    }
  } else {
    // Hide layer with impossible filter
    const hiddenFilter =
      layerId === "conus_gages"
        ? ["==", "hl_reference", "__hidden__"]
        : layerId === "camels"
          ? ["==", "hru_id", "__hidden__"]
          : ["==", "id", "__hidden__"];
    map.setFilter(layerId, hiddenFilter);
  }
}

// Initialize event listeners
function initEventListeners() {
  // Layer toggles
  document.getElementById("toggle-gages").addEventListener("change", (e) => {
    state.layers.gages = e.target.checked;
    toggleLayer("conus_gages", e.target.checked);
  });

  document.getElementById("toggle-camels").addEventListener("change", (e) => {
    state.layers.camels = e.target.checked;
    toggleLayer("camels", e.target.checked);
  });

  document.getElementById("toggle-nwm").addEventListener("change", (e) => {
    state.layers.nwm = e.target.checked;
    toggleLayer("nwm-chunks", e.target.checked);
  });

  document.getElementById("toggle-aorc").addEventListener("change", (e) => {
    state.layers.aorc = e.target.checked;
    toggleLayer("aorc-chunks", e.target.checked);
  });

  // Subset type radio buttons
  document.querySelectorAll('input[name="subset-type"]').forEach((radio) => {
    radio.addEventListener("change", (e) => {
      state.subsetType = e.target.value;
      if (state.selectedBasin && state.lastClickLngLat) {
        selectBasin(state.selectedBasin);
      }
    });
  });

  // Data source toggle
  document.querySelectorAll(".source-btn").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      document
        .querySelectorAll(".source-btn")
        .forEach((b) => b.classList.remove("active"));
      e.target.classList.add("active");
      state.dataSource = e.target.dataset.source;
    });
  });

  // Command type toggle
  document.querySelectorAll(".cmd-btn").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      document
        .querySelectorAll(".cmd-btn")
        .forEach((b) => b.classList.remove("active"));
      e.target.classList.add("active");
      state.commandType = e.target.dataset.cmd;
      updateCliCommand();
    });
  });

  // Time inputs
  document
    .getElementById("start-time")
    .addEventListener("change", updateCliCommand);
  document
    .getElementById("end-time")
    .addEventListener("change", updateCliCommand);

  // Map controls
  document.getElementById("zoom-in").addEventListener("click", () => {
    state.viewState.zoom = Math.min(state.viewState.zoom + 1, 18);
    deckgl.setProps({ initialViewState: { ...state.viewState } });
  });

  document.getElementById("zoom-out").addEventListener("click", () => {
    state.viewState.zoom = Math.max(state.viewState.zoom - 1, 2);
    deckgl.setProps({ initialViewState: { ...state.viewState } });
  });

  document.getElementById("reset-view").addEventListener("click", () => {
    state.viewState = {
      longitude: -96,
      latitude: 40,
      zoom: 4,
      pitch: 0,
      bearing: 0,
    };
    deckgl.setProps({ initialViewState: { ...state.viewState } });
  });

  // Copy command
  document
    .getElementById("copy-command")
    .addEventListener("click", async () => {
      const command = document.getElementById("cli-command").textContent;
      try {
        await navigator.clipboard.writeText(command);
        const btn = document.getElementById("copy-command");
        btn.classList.add("copied");
        btn.querySelector("span").textContent = "Copied!";
        setTimeout(() => {
          btn.classList.remove("copied");
          btn.querySelector("span").textContent = "Copy";
        }, 2000);
      } catch (err) {
        console.error("Failed to copy:", err);
      }
    });

  // Console toggle
  document.getElementById("toggle-console").addEventListener("click", () => {
    document.getElementById("console").classList.toggle("expanded");
  });

  document.getElementById("console-header").addEventListener("click", () => {
    document.getElementById("console").classList.toggle("expanded");
  });

  // Action buttons
  document.getElementById("btn-subset").addEventListener("click", handleSubset);
  document
    .getElementById("btn-forcings")
    .addEventListener("click", handleForcings);
  document
    .getElementById("btn-realization")
    .addEventListener("click", handleRealization);

  // Recording button
  document.getElementById("btn-record").addEventListener("click", handleRecord);
}

// Handle subset action
async function handleSubset() {
  if (!state.selectedBasin) {
    alert("Please select a catchment first");
    return;
  }

  const catId = state.selectedBasin;

  const checkResponse = await fetch("/subset_check", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify([catId]),
  });

  if (checkResponse.status === 409) {
    const filename = await checkResponse.text();
    if (!confirm("A geopackage already exists. Overwrite?")) {
      showOutput(`Subset canceled. Geopackage at ${filename}`);
      return;
    }
  }

  showProgress("Creating subset...");

  const startTime = performance.now();

  try {
    const response = await fetch("/subset", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        cat_id: [catId],
        subset_type: state.subsetType,
      }),
    });

    const filename = await response.text();
    const duration = ((performance.now() - startTime) / 1000).toFixed(2);

    hideProgress();
    showOutput(`Done in ${duration}s. Subset saved to ${filename}`);
  } catch (error) {
    console.error("Subset error:", error);
    hideProgress();
    showOutput(`Error: ${error.message}`);
  }
}

// Handle forcings action
async function handleForcings() {
  if (!state.selectedBasin) {
    alert("Please select a catchment and create a subset first");
    return;
  }

  const checkResponse = await fetch("/subset_check", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify([state.selectedBasin]),
  });

  if (checkResponse.status !== 409) {
    alert("Please create a subset first");
    return;
  }

  const forcingDir = await checkResponse.text();
  const startTime = document.getElementById("start-time").value;
  const endTime = document.getElementById("end-time").value;

  showProgress("Generating forcings...");

  const progressResponse = await fetch("/make_forcings_progress_file", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(forcingDir),
  });

  const progressFile = await progressResponse.text();
  pollForcingsProgress(progressFile);

  try {
    await fetch("/forcings", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        forcing_dir: forcingDir,
        start_time: startTime,
        end_time: endTime,
        source: state.dataSource,
      }),
    });
  } catch (error) {
    console.error("Forcings error:", error);
    hideProgress();
    showOutput(`Error: ${error.message}`);
  }
}

// Poll forcings progress
function pollForcingsProgress(progressFile) {
  const interval = setInterval(async () => {
    try {
      const response = await fetch("/forcings_progress", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(progressFile),
      });

      const data = await response.text();

      if (data === "NaN") {
        updateProgress(0, "Downloading data...");
      } else {
        const percent = parseInt(data, 10);
        updateProgress(percent, "Calculating zonal statistics...");

        if (percent >= 100) {
          clearInterval(interval);
          hideProgress();
          showOutput("Forcings generated successfully");
        }
      }
    } catch (error) {
      console.error("Progress error:", error);
      clearInterval(interval);
    }
  }, 1000);
}

// Handle realization action
async function handleRealization() {
  if (!state.selectedBasin) {
    alert("Please select a catchment and create a subset first");
    return;
  }

  const checkResponse = await fetch("/subset_check", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify([state.selectedBasin]),
  });

  if (checkResponse.status !== 409) {
    alert("Please create a subset first");
    return;
  }

  const forcingDir = await checkResponse.text();
  const startTime = document.getElementById("start-time").value;
  const endTime = document.getElementById("end-time").value;

  showProgress("Creating realization...");

  try {
    await fetch("/realization", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        forcing_dir: forcingDir,
        start_time: startTime,
        end_time: endTime,
      }),
    });

    hideProgress();
    showOutput("Realization generated successfully");
  } catch (error) {
    console.error("Realization error:", error);
    hideProgress();
    showOutput(`Error: ${error.message}`);
  }
}

// Progress UI helpers
function showProgress(message) {
  const section = document.getElementById("progress-section");
  section.style.display = "block";
  document.getElementById("progress-status").textContent = message;
  document.getElementById("progress-percent").textContent = "0%";
  document.getElementById("progress-fill").style.width = "0%";
}

function updateProgress(percent, message) {
  document.getElementById("progress-percent").textContent = `${percent}%`;
  document.getElementById("progress-fill").style.width = `${percent}%`;
  if (message) {
    document.getElementById("progress-status").textContent = message;
  }
}

function hideProgress() {
  document.getElementById("progress-section").style.display = "none";
}

function showOutput(message) {
  const section = document.getElementById("output-section");
  section.style.display = "block";
  document.getElementById("output-path").textContent = message;
}

// Handle recording
async function handleRecord() {
  if (state.flowpathTrips.length === 0) {
    alert("Please select a catchment first to have something to animate");
    return;
  }

  if (state.isRecording) {
    return; // Already recording
  }

  const duration =
    parseInt(document.getElementById("record-duration").value) || 5;
  const fps = parseInt(document.getElementById("record-fps").value) || 30;

  state.isRecording = true;
  updateRecordingUI(true);

  try {
    await recordAnimation(duration, fps);
  } catch (error) {
    console.error("Recording error:", error);
    alert("Recording failed: " + error.message);
  }

  state.isRecording = false;
  updateRecordingUI(false);
}

// Update recording UI state
function updateRecordingUI(isRecording) {
  const statusEl = document.getElementById("recording-status");
  const statusText = statusEl.querySelector(".status-text");
  const recordBtn = document.getElementById("btn-record");

  if (isRecording) {
    statusEl.classList.add("recording");
    statusEl.classList.remove("ready");
    statusText.textContent = "Recording...";
    recordBtn.classList.add("recording");
    recordBtn.innerHTML = `
            <svg viewBox="0 0 24 24" fill="currentColor" stroke="none">
                <rect x="6" y="6" width="12" height="12"/>
            </svg>
            Recording...
        `;

    // Add overlay
    const overlay = document.createElement("div");
    overlay.className = "recording-overlay";
    overlay.id = "recording-overlay";
    overlay.innerHTML = '<span class="dot"></span> Recording';
    document.body.appendChild(overlay);
  } else {
    statusEl.classList.remove("recording");
    statusEl.classList.add("ready");
    statusText.textContent = "Ready to record";
    recordBtn.classList.remove("recording");
    recordBtn.innerHTML = `
            <svg viewBox="0 0 24 24" fill="currentColor" stroke="none">
                <circle cx="12" cy="12" r="8"/>
            </svg>
            Record Animation
        `;

    // Remove overlay
    const overlay = document.getElementById("recording-overlay");
    if (overlay) overlay.remove();
  }
}

// Record animation using canvas capture
async function recordAnimation(durationSec, fps) {
  const totalFrames = durationSec * fps;
  const frameDuration = 1000 / fps;
  const animationPerFrame = state.tripsLoopLength / totalFrames;

  // Get the map container for dimensions
  const mapContainer = document.getElementById("map");
  const width = mapContainer.offsetWidth;
  const height = mapContainer.offsetHeight;

  // Create a canvas to composite MapLibre + DeckGL
  const compositeCanvas = document.createElement("canvas");
  compositeCanvas.width = width;
  compositeCanvas.height = height;
  const ctx = compositeCanvas.getContext("2d");

  // Set up MediaRecorder
  const stream = compositeCanvas.captureStream(fps);
  const mediaRecorder = new MediaRecorder(stream, {
    mimeType: "video/webm;codecs=vp9",
    videoBitsPerSecond: 8000000,
  });

  const chunks = [];
  mediaRecorder.ondataavailable = (e) => {
    if (e.data.size > 0) chunks.push(e.data);
  };

  return new Promise((resolve, reject) => {
    mediaRecorder.onstop = () => {
      const blob = new Blob(chunks, { type: "video/webm" });
      downloadVideo(blob);
      resolve();
    };

    mediaRecorder.onerror = reject;
    mediaRecorder.start();

    // Store original animation time
    const originalTime = state.animationTime;
    let frameCount = 0;

    // Pause normal animation
    const wasAnimating = true;

    const captureFrame = () => {
      if (frameCount >= totalFrames) {
        mediaRecorder.stop();
        // Restore animation
        state.animationTime = originalTime;
        return;
      }

      // Set animation time for this frame
      state.animationTime =
        (frameCount * animationPerFrame) % state.tripsLoopLength;

      // Update deck layers
      deckgl.setProps({ layers: getDeckLayers() });

      // Wait for render then capture
      requestAnimationFrame(() => {
        setTimeout(() => {
          // Get MapLibre canvas
          const mapCanvas = map.getCanvas();

          // Get DeckGL canvas
          const deckCanvas = deckgl.getCanvas();

          // Composite both canvases
          ctx.clearRect(0, 0, width, height);
          ctx.drawImage(mapCanvas, 0, 0);
          if (deckCanvas) {
            ctx.drawImage(deckCanvas, 0, 0);
          }

          frameCount++;

          // Update progress
          const progress = Math.round((frameCount / totalFrames) * 100);
          const statusText = document.querySelector(
            "#recording-status .status-text",
          );
          if (statusText) {
            statusText.textContent = `Recording... ${progress}%`;
          }

          // Capture next frame
          setTimeout(captureFrame, frameDuration / 2);
        }, frameDuration / 2);
      });
    };

    // Start capturing
    captureFrame();
  });
}

// Download the recorded video
function downloadVideo(blob) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `flowpath-animation-${state.selectedBasin || "map"}-${Date.now()}.webm`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// Log polling
function startLogPolling() {
  setInterval(async () => {
    try {
      const response = await fetch("/logs");
      const data = await response.json();

      const logOutput = document.getElementById("log-output");
      logOutput.innerHTML = data.logs
        .map((line) => `<div>${line}</div>`)
        .join("");
    } catch (error) {
      // Silently fail
    }
  }, 1000);
}

// Initialize on DOM ready
document.addEventListener("DOMContentLoaded", init);
