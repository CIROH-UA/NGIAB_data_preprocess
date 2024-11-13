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

var map = new maplibregl.Map({
    container: 'map', // container id
    style: 'static/resources/style.json', // style URL
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
