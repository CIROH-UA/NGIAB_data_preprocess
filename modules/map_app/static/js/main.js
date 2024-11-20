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
    var selected_basins = $('#selected-basins').text();
    var start_time = document.getElementById('start-time').value.split('T')[0];
    var end_time = document.getElementById('end-time').value.split('T')[0];
    var command = `python -m ngiab_data_cli -i ${selected_basins} --subset --start ${start_time} --end ${end_time} --forcings --realization --run`;
    var command_all = `python -m ngiab_data_cli -i ${selected_basins} --start ${start_time} --end ${end_time} --all`;
    if (selected_basins != "None - get clicking!") {
        $('#cli-command').text(command);
    }
}

// These functions are exported by data_processing.js
document.getElementById('map').addEventListener('click', create_cli_command);
document.getElementById('start-time').addEventListener('change', create_cli_command);
document.getElementById('end-time').addEventListener('change', create_cli_command);


// add the PMTiles plugin to the maplibregl global.
let protocol = new pmtiles.Protocol({metadata: true});
maplibregl.addProtocol("pmtiles", protocol.tile);

// select light-style if the browser is in light mode
// select dark-style if the browser is in dark mode
var style = 'static/resources/light-style.json';
if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
    style = 'static/resources/dark-style.json';
}
var map = new maplibregl.Map({
    container: 'map', // container id
    style: style, // style URL
    center: [-96, 40], // starting position [lng, lat]
    zoom: 4 // starting zoom
});
function update_map(cat_id, e) {
    $('#selected-basins').text(cat_id)
    map.setFilter('selected-catchments', ['any', ['in', 'divide_id', cat_id]]);
    map.setFilter('upstream-catchments', ['any', ['in', 'divide_id', ""]])
    
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
}
map.on('click', 'catchments', (e) => {
    cat_id = e.features[0].properties.divide_id;    
    update_map(cat_id, e);
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

map.on('mouseleave', 'conus_gages', () => {
    map.getCanvas().style.cursor = '';
    popup.remove();
});

map.on('click', 'conus_gages', (e) => {
    //  https://waterdata.usgs.gov/monitoring-location/02465000 
    window.open("https://waterdata.usgs.gov/monitoring-location/" + e.features[0].properties.hl_link, '_blank');
}
);
show = false;
const toggleButton = document.querySelector('#toggle-button');
toggleButton.addEventListener('click', () => {
    if (show) {
        map.setFilter('conus_gages', ['any', ['==', 'hl_uri', ""]])
        toggleButton.innerText = 'Show gages';
        show = false;
    } else {
        map.setFilter('conus_gages', null)
        toggleButton.innerText = 'Hide gages';
        show = true;
    }
});

