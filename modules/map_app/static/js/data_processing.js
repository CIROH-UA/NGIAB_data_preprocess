async function subset() {
    var cat_id = $('#selected-basins').text()
    if (cat_id == 'None - get clicking!') {
        alert('Please select at least one basin in the map before subsetting');
        return;
    }
    fetch('/subset_check', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify([cat_id]),
    })
    .then(async response => {
    // 409 if that subset gpkg path already exists
        if (response.status == 409) {
            const filename = await response.text();
            console.log("check response")
            if (!confirm('A geopackage already exists with that catchment name. Overwrite?')) {
                alert("Subset canceled.");
                document.getElementById('output-path').innerHTML = "Subset canceled. Geopackage located at " + filename;
                return;
            }
        } 
        const startTime = performance.now(); // Start the timer
        fetch('/subset', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify([cat_id]),
            })
                .then(response => response.text())
                .then(filename => {
                    console.log(filename);
                    const endTime = performance.now(); // Stop the timer
                    const duration = endTime - startTime; // Calculate the duration in milliseconds
                    console.log('Request took ' + duration / 1000 + ' milliseconds');
                    document.getElementById('output-path').innerHTML = "Done in " + (duration / 1000).toFixed(2) + "s, subset to <a href='file://" + filename + "'>" + filename + "</a>";
                })
                .catch(error => {
                    console.error('Error:', error);
                }).finally(() => {
                    document.getElementById('subset-button').disabled = false;
                    document.getElementById('subset-loading').style.visibility = "hidden";
                });
    });
}

async function forcings() {
    fetch('/subset_check', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify([cat_id]),
    })
    .then(async response => {
        // 409 if that subset gpkg path already exists
        if (response.status == 409) {
            const filename = await response.text();

            console.log('getting forcings');
            document.getElementById('forcings-button').disabled = true;
            document.getElementById('forcings-loading').style.visibility = "visible";

            const forcing_dir = filename;
            console.log('forcing_dir:', forcing_dir);
            const start_time = document.getElementById('start-time').value;
            const end_time = document.getElementById('end-time').value;
            if (forcing_dir === '' || start_time === '' || end_time === '') {
                alert('Please enter a valid output path, start time, and end time');
                document.getElementById('time-warning').style.color = 'red';
                return;
            }

            // get the position of the nwm aorc forcing toggle
            // false means nwm forcing, true means aorc forcing
            var nwm_aorc = document.getElementById('datasource-toggle').checked;
            var source = nwm_aorc ? 'aorc' : 'nwm';
            console.log('source:', source);

            document.getElementById('forcings-output-path').textContent = "Generating forcings...";
            fetch('/forcings', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ 'forcing_dir': forcing_dir, 'start_time': start_time, 'end_time': end_time , 'source': source}),
            }).then(response => response.text())
                .then(response_code => {
                    document.getElementById('forcings-output-path').textContent = "Forcings generated successfully";
                })
                .catch(error => {
                    console.error('Error:', error);
                }).finally(() => {
                    document.getElementById('forcings-button').disabled = false;
                    document.getElementById('forcings-loading').style.visibility = "hidden";

                });
        } else {
            alert('No existing geopackage found. Please subset the data before getting forcings');
            return;
        }
    })
}

async function realization() {
    if (document.getElementById('output-path').textContent === '') {
        alert('Please subset the data before getting a realization');
        return;
    }
    console.log('getting realization');
    document.getElementById('realization-button').disabled = true;
    const forcing_dir = document.getElementById('output-path').textContent;
    const start_time = document.getElementById('start-time').value;
    const end_time = document.getElementById('end-time').value;
    if (forcing_dir === '' || start_time === '' || end_time === '') {
        alert('Please enter a valid output path, start time, and end time');
        document.getElementById('time-warning').style.color = 'red';
        return;
    }
    document.getElementById('realization-output-path').textContent = "Generating realization...";
    fetch('/realization', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 'forcing_dir': forcing_dir, 'start_time': start_time, 'end_time': end_time }),
    }).then(response => response.text())
        .then(response_code => {
            document.getElementById('realization-output-path').textContent = "Realization generated";
        })
        .catch(error => {
            console.error('Error:', error);
        }).finally(() => {
            document.getElementById('realization-button').disabled = false;
        });
}

// These functions are exported by data_processing.js
document.getElementById('subset-button').addEventListener('click', subset);
document.getElementById('forcings-button').addEventListener('click', forcings);
document.getElementById('realization-button').addEventListener('click', realization);