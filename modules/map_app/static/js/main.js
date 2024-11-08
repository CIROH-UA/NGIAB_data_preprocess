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

// add the PMTiles plugin to the maplibregl global.
let protocol = new pmtiles.Protocol({metadata: true});
maplibregl.addProtocol("pmtiles", protocol.tile);

var map = new maplibregl.Map({
    container: 'map', // container id
    style: 'static/resources/style.json', // style URL
    center: [-96, 40], // starting position [lng, lat]
    zoom: 4 // starting zoom
});
map.on('load', () => {
    map.addSource('conus', {
        type: 'vector',
        url: 'pmtiles://https://communityhydrofabric.s3.us-east-1.amazonaws.com/conus.pmtiles',
    });
    map.addLayer({
        'id': 'vpu',
        'type': 'line',
        'source': 'conus',
        'source-layer': 'vpu',
        'layout': {},
        'paint': {
            'line-width': 2,
            'line-color': colorDict.flowlineToCatOutline,
        }
    });
    map.addLayer({
        'id': 'flowpaths',
        'type': 'line',
        'source': 'conus',
        'source-layer': 'flowpaths',
        'layout': {},
        'paint': {
            'line-color': colorDict.flowlineToNexusOutline,
            'line-width': { "stops": [[7, 1], [10, 2]] },
            'line-opacity': { "stops": [[7, 0], [11, 1]] }
        }
    });
    map.addLayer({
        'id': 'catchments',
        'type': 'fill',
        'source': 'conus',
        'source-layer': 'catchments',
        'layout': {},
        'paint': {
            'fill-color': colorDict.clearFill,
            'fill-outline-color': colorDict.nexusOutline,
            'fill-opacity': { "stops": [[7, 0], [11, 1]] }
        }
    });
    map.addLayer({
        'id': 'selected-flowpaths',
        'type': 'line',
        'source': 'conus',
        'source-layer': 'flowpaths',
        'layout': {},
        'paint': {
            'line-color': colorDict.flowlineToNexusOutline,
            'line-width': { "stops": [[7, 1], [10, 2]] },
            'line-opacity': { "stops": [[7, 0], [11, 1]] }
        },
        "filter": ["any",["in", "id", ""],]
    });
    map.addLayer({
        'id': 'selected-catchments',
        'type': 'fill',
        'source': 'conus',
        'source-layer': 'catchments',
        'layout': {},
        'paint': {
            'fill-color': colorDict.selectedCatFill,
            'fill-outline-color': colorDict.selectedCatOutline,
            'fill-opacity': { "stops": [[7, 0], [11, 1]] }
        },
        "filter": ["any",["in", "divide_id", ""],]
    });
    map.addLayer({
        'id': 'upstream-catchments',
        'type': 'fill',
        'source': 'conus',
        'source-layer': 'catchments',
        'layout': {},
        'paint': {
            'fill-color': colorDict.upstreamCatFill,
            'fill-outline-color': colorDict.upstreamCatFill,
            'fill-opacity': { "stops": [[7, 0], [11, 1]] }
        },
        "filter": ["any",["in", "divide_id", ""],]
    });
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


// function clear_selection() {
//     cat_id_dict = {};
//     synchronizeUpdates();
//     $('#selected-basins').text(Object.keys(cat_id_dict));
// }

// $('#clear-button').click(clear_selection);

// function select_by_id() {
//     cat_id = $('#cat_id_input').val();
//     update_map(cat_id);
//     // get the lat long for zoom
//     fetch('/get_cat_xy', {
//         method: 'POST',
//         headers: { 'Content-Type': 'application/json' },
//         body: JSON.stringify(cat_id),
//     })
//     .then(response => response.json())
//     .then(data => {
//         map.flyTo({
//             // These options control the ending camera position: centered at
//             // the target, at zoom level 9, and north up.
//             center: data,
//             zoom: 9,
//             bearing: 0,
    
//             // These options control the flight curve, making it move
//             // slowly and zoom out almost completely before starting
//             // to pan.
//             speed: 0.2, // make the flying slow
//             curve: 1, // change the speed at which it zooms out
    
//             // This can be any easing function: it takes a number between
//             // 0 and 1 and returns another number between 0 and 1.
//             easing(t) {
//                 return t;
//             },
    
//             // this animation is considered essential with respect to prefers-reduced-motion
//             essential: true
//         });
//     });
// }

// $('#select-button').click(select_by_id);

// function select_by_gage_id() {
//     gage_ids = $('#gage_id_input').val();
//     fetch('/get_upstream_catids', {
//         method: 'POST',
//         headers: { 'Content-Type': 'application/json' },
//         body: JSON.stringify(cat_id),
//     })
//     .then(response => response.json())
//     .then(data => {
//         map.setFilter('upstream-catchments', ['any', ['in', 'divide_id', ...data]]);
//         if (data.length === 0) { 
//             new maplibregl.Popup()
//             .setLngLat(e.lngLat)
//             .setHTML('No upstreams')
//             .addTo(map);
//         }
//     });

// }

// $('#select-gage-button').click(select_by_gage_id);
