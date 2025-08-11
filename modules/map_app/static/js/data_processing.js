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
    .then((response) => {
    // 409 if that subset gpkg path already exists
        if (response.status == 409) {
            console.log("check response")
            if (!confirm('A geopackage already exists with that catchment name. Overwrite?')) {
                alert("Subset canceled.");
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

function updateProgressBar(percent) {
    var bar = document.getElementById("bar");
    bar.style.width = percent + "%";
    var barText = document.getElementById("bar-text");
    barText.textContent = percent + "%";
}

function pollForcingsProgress(progressFile) {
    const interval = setInterval(() => {
        fetch('/forcings_progress', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(progressFile),
        })
            .then(response => response.text())
            .then(data => {
                if (data == "NaN") {
                    document.getElementById('forcings-output-path').textContent = "Downloading data...";
                    document.getElementById('bar-text').textContent = "Downloading...";
                    document.getElementById('bar').style.animation = "indeterminateAnimation 1s infinite linear";
                } else {
                    const percent = parseInt(data, 10);
                    updateProgressBar(percent);
                    if (percent > 0 && percent < 100) {
                        document.getElementById('bar').style.animation = "none"; // stop the indeterminate animation
                        document.getElementById('forcings-output-path').textContent = "Calculating zonal statistics. See progress below.";
                    } else if (percent >= 100) {
                        updateProgressBar(100); // Ensure the progress bar is full
                        clearInterval(interval);
                        document.getElementById('forcings-output-path').textContent = "Forcings generated successfully";
                    }
                }
            })
            .catch(error => {
                console.error('Progress polling error:', error);
                clearInterval(interval);
            });
    }, 1000); // Poll every second
}

async function forcings() {
    if (document.getElementById('output-path').textContent === '') {
        alert('Please subset the data before getting forcings');
        return;
    }
    console.log('getting forcings');
    document.getElementById('forcings-button').disabled = true;

    const forcing_dir = document.getElementById('output-path').textContent;
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

    fetch('/make_forcings_progress_file', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(forcing_dir),
    })
    .then(async (response) => response.text())
    .then(progressFile => { 
        pollForcingsProgress(progressFile); // Start polling for progress
    })
    
    fetch('/forcings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 'forcing_dir': forcing_dir, 'start_time': start_time, 'end_time': end_time , 'source': source}),
    })
    .then(response => response.text())
    .catch(error => {
        console.error('Error:', error);
    }).finally(() => {
        document.getElementById('forcings-button').disabled = false;

    });
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