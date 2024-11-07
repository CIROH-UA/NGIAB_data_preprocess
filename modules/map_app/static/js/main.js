var cat_id_dict = {};
var selected_cat_layer = null;
var upstream_maps = {};
var flowline_layers = {};

var registered_layers = {}


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


async function update_selected() {
    console.log('updating selected');
    if (!(Object.keys(cat_id_dict).length === 0)) {
        return fetch('/get_geojson_from_catids', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(cat_id_dict),
        })
            .then(response => response.json())
            .then(data => {
                // if the cat_id is already in the dict, remove the key
                // remove the old layer
                // layer = map.getLayer('selected')
                map.getSource('selected').setData(data);
                console.log(data);
            })
            .catch(error => {
                console.error('Error:', error);
            });
    } else {
        if (selected_cat_layer) {
            map.removeLayer('selected');
        }
        return Promise.resolve();
    }
}

async function populate_upstream() {
    console.log('populating upstream selected');
    // drop any key that is not in the cat_id_dict
    for (const [key, value] of Object.entries(upstream_maps)) {
        if (!(key in cat_id_dict)) {
            map.removeLayer(value);
            delete upstream_maps[key];
        }
    }
    // add any key that is in the cat_id_dict but not in the upstream_maps
    for (const [key, value] of Object.entries(cat_id_dict)) {
        if (!(key in upstream_maps)) {
            upstream_maps[key] = null;
        }
    }
    if (Object.keys(upstream_maps).length === 0) {
        return Promise.resolve();
    }

    const fetchPromises = Object.entries(upstream_maps).map(([key, value]) => {
        if (value === null) {
            return fetch('/get_upstream_geojson_from_catids', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(key),
            })
                .then(response => response.json())
                .then(data => {
                    // if the cat_id is already in the dict, remove the key
                    // remove the old layer
                    if (upstream_maps[key]) {
                        map.removeLayer(upstream_maps[key]);
                    }
                    console.log(data);
                    // add the new layer if the downstream cat's still selected
                    if (key in cat_id_dict) {
                        layer_group = map.addLayer({
                            'id': 'upstream',
                            'type': 'fill',
                            'source': { 'type': 'geojson', 'data': data },
                            'layout': {},
                            'paint': {
                                'fill-color': '#088',
                                'fill-opacity': 0.8
                            }
                        });
                        upstream_maps[key] = layer_group;
                        layer_group.eachLayer(function (layer) {
                            if (layer._path) {
                                layer._path.classList.add('upstream-cat-layer');
                            }
                        });
                    }
                })
                .catch(error => {
                    console.error('Error:', error);
                });
        }
    });
    return fetchPromises;

}

async function populate_flowlines() {
    console.log('populating flowlines');
    // drop any key that is not in the cat_id_dict
    for (const [key, value] of Object.entries(flowline_layers)) {
        if (!(key in cat_id_dict)) {
            for (i of flowline_layers[key]) {
                map.removeLayer(i);
                delete flowline_layers[key];
            }
        }
    }
    // add any key that is in the cat_id_dict but not in the flowline_layers
    for (const [key, value] of Object.entries(cat_id_dict)) {
        if (!(key in flowline_layers)) {
            flowline_layers[key] = null;
        }
    }
    if (Object.keys(flowline_layers).length === 0) {
        return Promise.resolve();
    }

    const fetchPromises = Object.entries(flowline_layers).map(([key, value]) => {
        if (value === null) {
            return fetch('/get_flowlines_from_catids', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(key),
            })
                .then(response => response.json())
                .then(data => {
                    // if the cat_id is already in the dict, remove the key
                    // remove the old layer
                    if (flowline_layers[key]) {
                        for (i of flowline_layers[key]) {
                            map.removeLayer(i);
                        }
                    }
                    // loud!
                    // console.log(data);
                    to_cat = JSON.parse(data['to_cat']);
                    to_nexus = JSON.parse(data['to_nexus']);
                    nexus = JSON.parse(data['nexus']);
                    // add the new layer if the downstream cat's still selected
                    if (key in cat_id_dict) {
                        to_cat_layer = maplibregl.geoJSON(to_cat).addTo(map);
                        to_nexus_layer = maplibregl.geoJSON(to_nexus).addTo(map);
                        nexus_layer = maplibregl.geoJSON(nexus).addTo(map);
                        // hack to add css classes to the flowline layers
                        // using eachLayer as it waits for layer to be done updating
                        // directly accessing the _layers keys may not always work
                        to_cat_layer.eachLayer(function (layer) {
                            if (layer._path) {
                                layer._path.classList.add('flowline-to-cat-layer');
                            }
                        });
                        to_nexus_layer.eachLayer(function (layer) {
                            if (layer._path) {
                                layer._path.classList.add('flowline-to-nexus-layer');
                            }
                        });
                    }
                    flowline_layers[key] = [to_cat_layer, to_nexus_layer, nexus_layer];
                })

                .catch(error => {
                    console.error('Error:', error);
                });
        }
    });
    return fetchPromises;

}

async function synchronizeUpdates() {
    console.log('Starting updates');

    // wait for all promises
    const selectedPromise = await update_selected();
    // const upstreamPromises = await populate_upstream();
    // const flowlinePromises = await populate_flowlines();
    await Promise.any([selectedPromise, ...upstreamPromises, ...flowlinePromises]).then(() => {
        // This block executes after all promises from populate_upstream and populate_flowlines have resolved
        console.log('All updates are complete');
    }).catch(error => {
        console.error('An error occurred:', error);
    });
}

function onMapClick(event) {
    console.log(event);
    // Extract the clicked coordinates
    var lat = event.lngLat.lat;
    var lng = event.lngLat.lng;

    // Send an AJAX request to the Flask backend
    fetch('/handle_map_interaction', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            coordinates: { lat: lat, lng: lng }
        }),
    })
        .then(response => response.json())
        .then(data => {

            // if the cat_id is already in the dict, remove the key
            if (data['cat_id'] in cat_id_dict) {
                delete cat_id_dict[data['cat_id']];
            }
            else {
                // temporary fix to only allow one basin to be selected
                cat_id_dict = {};
                // uncomment above line to allow multiple basins to be selected
                cat_id_dict[data['cat_id']] = [lat, lng];
            }
            console.log('clicked on cat_id: ' + data['cat_id'] + ' coords :' + lat + ', ' + lng);


            synchronizeUpdates();
            //$('#selected-basins').text(Object.keys(cat_id_dict).join(', '));
            // revert this line too
            $('#selected-basins').text(Object.keys(cat_id_dict));

        })
        .catch(error => {
            console.error('Error:', error);
        });

}

function select_by_lat_lon() {
    lat = $('#lat_input').val();
    lon = $('#lon_input').val();
    fetch('/handle_map_interaction', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            coordinates: { lat: lat, lng: lon }
        }),
    })
        .then(response => response.json())
        .then(data => {
            cat_id = data['cat_id'];
            cat_id_dict[cat_id] = [lat, lon];
            synchronizeUpdates();
            $('#selected-basins').text(Object.keys(cat_id_dict));
        })
        .catch(error => {
            console.error('Error:', error);
        });
}

$('#select-lat-lon-button').click(select_by_lat_lon);

function select_by_id() {
    cat_id_dict = {};
    cat_ids = $('#cat_id_input').val();

    cat_ids = cat_ids.split(',');
    for (cat_id of cat_ids) {
        cat_id_dict[cat_id] = [0, 0];
    }
    synchronizeUpdates();
    $('#selected-basins').text(Object.keys(cat_id_dict));
}

$('#select-button').click(select_by_id);

function clear_selection() {
    cat_id_dict = {};
    synchronizeUpdates();
    $('#selected-basins').text(Object.keys(cat_id_dict));
}

$('#clear-button').click(clear_selection);

function get_catid_from_gage_id(gage_id) {
    return fetch('/get_catid_from_gage_id', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            gage_id: gage_id
        }),
    })
        .then(response => response.json())
        .then(data => {
            return data['cat_ids'];
        })
        .catch(error => {
            console.error('Error:', error);
        });
}

function select_by_gage_id() {
    gage_ids = $('#gage_id_input').val();
    gage_ids = gage_ids.split(',');
    for (gage_id of gage_ids) {
        cat_ids = get_catid_from_gage_id(gage_id);
        cat_ids.then(function (result) {
            for (result of result) {
                cat_id_dict[result] = [0, 0];
            }
            $('#selected-basins').text(Object.keys(cat_id_dict));
        });
    }
    synchronizeUpdates();
}

$('#select-gage-button').click(select_by_gage_id);

// // Initialize the map
// var map = maplibregl.map('map', { crs: maplibregl.CRS.EPSG3857 }).setView([40, -96], 5);

// //Create in-map Legend / Control Panel
// var legend = maplibregl.control({ position: 'bottomright' });
// // load in html template for the legend
// legend.onAdd = function (map) {
//     legend_div = maplibregl.DomUtil.create('div', 'custom_legend');
//     return legend_div
// };
// legend.addTo(map);


// add the PMTiles plugin to the maplibregl global.
let protocol = new pmtiles.Protocol({metadata: true});
maplibregl.addProtocol("pmtiles", protocol.tile);

var map = new maplibregl.Map({
    container: 'map', // container id
    style: 'https://demotiles.maplibre.org/style.json', // style URL
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
            // 'fill-color': colorDict.clearFill,
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
            //  'fill-outline-color': colorDict.flowlineToNexusOutline,
        }
    });
    map.addLayer({
        'id': 'catchment',
        'type': 'fill',
        'source': 'conus',
        'source-layer': 'catchments',
        'layout': {},
        'paint': {
            'fill-color': colorDict.clearFill,
            'fill-outline-color': colorDict.nexusOutline,
        }
    });
});
    // map.addLayer({
    //     'id': 'selected',
    //     'type': 'fill',
    //     'source': 'conus',
    //     'source-layer': 'catchments',
    //     'layout': {},
    //     'paint': {
    //         'fill-color': colorDict.selectedCatFill,
    //         'fill-outline-color': colorDict.selectedCatOutline,
    //     }
    // });
    // map.addLayer({
    //     'id': 'upstream',
    //     'type': 'fill',
    //     'source': { 'type': 'geojson', 'data': null },
    //     'layout': {},
    //     'paint': {
    //         'fill-color': colorDict.selectedCatFill,
    //         'fill-outline-color': colorDict.selectedCatOutline,
    //     }
    // });
    // map.addLayer({
    //     'id': 'flowlines',
    //     'type': 'fill',
    //     'source': { 'type': 'geojson', 'data': null },
    //     'layout': {},
    //     'paint': {
    //         'fill-color': colorDict.selectedCatFill,
    //         'fill-outline-color': colorDict.selectedCatOutline,
    //     }
    // });

// map.on('load', () => {
//     map.addSource('contours', {
//         type: 'geojson',
//         data: 'static/tiles/conus.geojson',
//     });
//     map.addLayer({
//         'id': 'cats',
//         'type': 'line',
//         'source': 'contours',
//         // 'source-layer': 'contour',
//         // 'layout': {
//         //     'line-join': 'round',
//         //     'line-cap': 'round'
//         // },
//         // 'paint': {
//         //     'line-color': '#ff69b4',
//         //     'line-width': 1
//         // }
//     });
// });
map.on('click', onMapClick);

