// Hydrofabric sources and layers merged into the incoming basemap style.
// Loaded before main.js; exposes updateIncomingStyle and HIDDEN_FILTER globals.

// Legacy filter that matches nothing: an empty "any" is always false.
const HIDDEN_FILTER = ["any"];

function updateIncomingStyle(previousStyle, nextStyle) {
  // Resolve theme colors from the page so map layers match the UI palette.
  const computedStyle = getComputedStyle(document.documentElement);
  const cssColor = (name, fallback) =>
    computedStyle.getPropertyValue(name).trim() || fallback;

  const upstream_index_url = "https://communityhydrofabric.com/map/only_geometry/upstream_index/";
  const s3_url = "https://communityhydrofabric.s3.us-east-1.amazonaws.com/map/";

  const hydrofabric_map_data = {
    sources: {
      "flowpaths": {
        type: "vector",
        url: "pmtiles://" + upstream_index_url + "flowpaths.pmtiles",
      },
      "divides": {
        type: "vector",
        url: "pmtiles://" + upstream_index_url + "divides.pmtiles",
      },
      // merged.pmtiles still provides the gage locations (hl_* properties)
      "hydrofabric": {
        type: "vector",
        url: "pmtiles://" + s3_url + "merged.pmtiles",
      },
      "camels_basins": {
        type: "vector",
        url: "pmtiles://" + s3_url + "camels.pmtiles",
      },
      "nwm_zarr_chunks": {
        type: "vector",
        url: "pmtiles://" + s3_url + "forcing_chunks/nwm_retro_v3_zarr_chunks.pmtiles",
      },
      "aorc_zarr_chunks": {
        type: "vector",
        url: "pmtiles://" + s3_url + "forcing_chunks/aorc_zarr_chunks.pmtiles",
      },
    },
    layers: [
      {
        id: "flowpaths",
        type: "line",
        source: "flowpaths",
        "source-layer": "flowpaths",
        layout: {
          "line-cap": "round",
        },
        paint: {
          "line-width": [
            "interpolate",
            ["exponential", 1.6],
            ["get", "order"],
            1,
            1,
            8,
            6,
          ],
          "line-color": [
            "interpolate",
            ["linear"],
            ["zoom"],
            1.3,
            "rgba(0, 119, 187, 0)",
            5,
            "rgba(0, 119, 187, 1)",
          ],
        },
      },
      {
        // Invisible fat overlay of the flowpaths, so hovering thin lines is
        // forgiving. Mouse events for the results tooltip bind to this layer.
        id: "flowpaths-hover",
        type: "line",
        source: "flowpaths",
        "source-layer": "flowpaths",
        layout: {
          "line-cap": "round",
        },
        paint: {
          "line-width": 14,
          "line-color": "#000000",
          "line-opacity": 0,
        },
      },
      {
        id: "divides",
        type: "fill",
        source: "divides",
        "source-layer": "divides",
        paint: {
          "fill-color": "rgba(0, 0, 0, 0)",
          "fill-outline-color": [
            "interpolate",
            ["linear"],
            ["zoom"],
            6,
            "rgba(1, 1, 1, 0)",
            7,
            "rgba(1, 1, 1, 0.5)",
          ],
        },
      },
      {
        id: "selected-divides",
        type: "fill",
        source: "divides",
        "source-layer": "divides",
        paint: {
          "fill-color": "rgba(238, 51, 119, 0.316)",
          "fill-outline-color": "rgba(238, 51, 119, 0.7)",
        },
        filter: HIDDEN_FILTER,
      },
      {
        id: "upstream-divides",
        type: "fill",
        source: "divides",
        "source-layer": "divides",
        paint: {
          "fill-color": "rgba(238, 119, 51, 0.278)",
          "fill-outline-color": "rgba(238, 119, 51, 0.7)",
        },
        filter: HIDDEN_FILTER,
      },
      {
        id: "camels",
        type: "line",
        source: "camels_basins",
        "source-layer": "camels_basins",
        filter: HIDDEN_FILTER,
        paint: {
          "line-width": 1.5,
          "line-color": "rgba(134, 30, 232, 1)",
        },
      },
      {
        id: "nwm_zarr_chunks",
        type: "line",
        source: "nwm_zarr_chunks",
        "source-layer": "nwm_zarr_chunks",
        filter: HIDDEN_FILTER,
        paint: {
          "line-width": 1,
          "line-color": cssColor("--color-base-content", "#888888"),
        },
      },
      {
        id: "aorc_zarr_chunks",
        type: "line",
        source: "aorc_zarr_chunks",
        "source-layer": "aorc_zarr_chunks",
        filter: HIDDEN_FILTER,
        paint: {
          "line-width": 1,
          "line-color": cssColor("--color-warning", "#ffaa00"),
        },
      },
      {
        id: "conus_gages",
        type: "circle",
        source: "hydrofabric",
        "source-layer": "conus_gages",
        filter: HIDDEN_FILTER,
        paint: {
          "circle-radius": {
            stops: [[3, 2], [11, 5]],
          },
          "circle-color": cssColor("--color-base-content", "#c8c8c8"),
          "circle-opacity": {
            stops: [[3, 0], [9, 1]],
          },
        },
      },
    ]
  };
  const boostTextHalo = (layer) => ({ ...layer, paint: { ...layer.paint, "text-halo-width": 3, "text-halo-blur": 3 }, });

  return {
    ...nextStyle,
    sources: {
      ...nextStyle.sources,
      ...hydrofabric_map_data.sources,
    },
    layers: [
      // base layers, then our layers, then symbol layers (icons stripped, halos boosted)
      ...nextStyle.layers.filter((layer) => layer.type !== "symbol"),
      ...hydrofabric_map_data.layers,
      ...nextStyle.layers
        .filter((layer) => layer.type === "symbol" && !layer.paint?.["text-halo-width"] && !layer.layout?.["icon-image"]),
      ...nextStyle.layers
        .filter((layer) => layer.type === "symbol" && layer.paint?.["text-halo-width"])
        .map(boostTextHalo),
    ],
  };
}
