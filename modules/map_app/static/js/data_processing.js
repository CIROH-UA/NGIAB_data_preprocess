// Use the shared workflow identifier field for manual basin-based steps.
function getSelectedIdentifier() {
    return document.getElementById("workflow-input").value.trim();
}

async function subset() {
    var cat_id = getSelectedIdentifier();

    if (!cat_id || !cat_id.startsWith("cat-")) {
        alert("Please select or enter a basin ID, like cat-2739307, before subsetting.");
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
            if (!confirm('A geopackage already exists with that catchment name. Overwrite?')) {
                alert("Subset canceled.");
                document.getElementById('output-path').innerHTML =
                    "Subset canceled. Geopackage located at " + filename;
                return;
            }
        }

        // check what kind of subset
        var subset_type = document.getElementById('radio-nexus').checked
            ? 'nexus'
            : 'catchment';

        const startTime = performance.now(); //Start the timer

        fetch('/subset', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            // body: JSON.stringify([cat_id])
            body: JSON.stringify({ 'cat_id': [cat_id], 'subset_type': subset_type }),
        })
        .then(response => response.text())
        .then(filename => {
            const endTime = performance.now();
            const duration = endTime - startTime;
            document.getElementById('output-path').innerHTML = `
                Done in ${(duration / 1000).toFixed(2)} s<br><br>
                <code>${filename}</code>
                <span
                    title="Copy path"
                    style="cursor:pointer; margin-left:8px;"
                    onclick="
                        navigator.clipboard.writeText('${filename}');
                        this.textContent='✔';
                        setTimeout(() => this.textContent='📋', 1200);
                    ">
                    📋
                </span>
            `;
        })
        .catch(error => {
            console.error('Error:', error);
        })
        .finally(() => {
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

// Update the forcings UI from one progress message ("NaN" while downloading,
// otherwise a percentage). Calls done() when the run completes.
function handleForcingsProgress(data, done) {
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
            done();
            document.getElementById('forcings-output-path').textContent = "Forcings generated successfully";
        }
    }
}

// Progress is pushed over a websocket; fall back to polling if that fails.
function pollForcingsProgress(progressFile) {
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const ws = new WebSocket(`${protocol}//${location.host}/ws/forcings_progress`);
    let receivedAnything = false;

    ws.onopen = () => ws.send(JSON.stringify(progressFile));
    ws.onmessage = (event) => {
        receivedAnything = true;
        handleForcingsProgress(event.data, () => ws.close());
    };
    ws.onerror = () => {
        if (!receivedAnything) pollForcingsProgressHttp(progressFile);
    };
}

function pollForcingsProgressHttp(progressFile) {
    const interval = setInterval(() => {
        fetch('/forcings_progress', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(progressFile),
        })
            .then(response => response.text())
            .then(data => handleForcingsProgress(data, () => clearInterval(interval)))
            .catch(error => {
                console.error('Progress polling error:', error);
                clearInterval(interval);
            });
    }, 1000); // Poll every second
}

async function forcings() {
    var cat_id = getSelectedIdentifier();

    if (!cat_id || !cat_id.startsWith("cat-")) {
        alert("Please select or enter a basin ID, like cat-2739307, before generating forcings.");
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
            console.log('getting forcings');
            document.getElementById('forcings-button').disabled = true;
            document.getElementById('forcings-loading').style.visibility = "visible";

            const forcing_dir = filename;
            console.log('forcing_dir:', forcing_dir);
            const start_time = document.getElementById('start-time').value;
            const end_time = document.getElementById('end-time').value;
            if (forcing_dir === '' || start_time === '' || end_time === '') {
                alert('Please enter a valid output path, start time, and end time');
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

// Collect workflow options from the UI and run the complete CLI workflow.
async function runWorkflow() {
    const inputType = document.getElementById("gage-checkbox").checked ? "gage" : "basin";
    const inputFeature = document.getElementById("workflow-input").value.trim();

    const startTime = document.getElementById("start-time").value;
    const endTime = document.getElementById("end-time").value;

    const runButton = document.getElementById("run-cli-button");
    const outputBox = document.getElementById("run-cli-output");

    if (!inputFeature) {
        alert(`Please enter a ${inputType} ID.`);
        return;
    }

    if (!startTime || !endTime) {
        alert("Please select both start and end times.");
        return;
    }

    if (new Date(startTime) >= new Date(endTime)) {
        alert("Start time must be before end time.");
        return;
    }

    runButton.disabled = true;
    runButton.textContent = "Running...";

    const workflowStartTime = Date.now();

    // Show a running state while the backend processes the workflow.
    outputBox.innerHTML = `
        <div><strong>⏳ Workflow running...</strong></div>

        <div style="margin-top:8px;">
            This may take several minutes, especially when generating forcings or running NextGen.
        </div>

        <div style="margin-top:10px; color: var(--secondary-text);">
            <strong>Workflow steps:</strong><br>
            Subset hydrofabric → Generate forcings → Create realization → Run NextGen simulation
        </div>

        <div style="margin-top:12px;">
            Elapsed time: <span id="workflow-elapsed">0s</span>
        </div>
    `;

    // Keep an elapsed timer visible until the workflow request completes.
    const elapsedTimer = setInterval(() => {
        const elapsedSeconds = Math.floor((Date.now() - workflowStartTime) / 1000);
        const minutes = Math.floor(elapsedSeconds / 60);
        const seconds = elapsedSeconds % 60;

        const elapsedElement = document.getElementById("workflow-elapsed");
        if (elapsedElement) {
            elapsedElement.textContent =
                minutes > 0 ? `${minutes}m ${seconds}s` : `${seconds}s`;
        }
    }, 1000);

    // Send the selected input, forcing source, and model to Flask.
    fetch("/run_cli", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            input_feature: inputFeature,
            input_type: inputType,
            start_time: startTime,
            end_time: endTime,
            source: document.getElementById("datasource-toggle").checked ? "aorc" : "nwm",
            model: document.getElementById("model-select").value
        })
    })
    .then(async response => {
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || "Failed to start workflow");
        }

        return data;
    })

    // Display the output folder and generated command after the workflow completes.
    .then(data => {
        const outputPath = data.output_dir;

        outputBox.innerHTML = `
            <div><strong>✅ Preprocessing and NextGen simulation completed successfully.</strong></div>

            <div style="margin-top:12px;">
                <strong>Output</strong><br>
                <code id="workflow-output-path">${outputPath}</code>
                <span
                    id="copy-output-path"
                    title="Copy output path"
                    style="cursor:pointer; margin-left:8px; user-select:none;"
                    onclick="
                        navigator.clipboard.writeText('${outputPath}');
                        this.textContent='✔';
                        this.title='Copied!';
                        setTimeout(() => {
                            this.textContent='📋';
                            this.title='Copy output path';
                        }, 1200);
                    ">
                    📋
                </span>
            </div>

            <details style="margin-top:12px;">
                <summary>Show Command</summary>
                <pre><code>${data.command}</code></pre>
            </details>
        `;

        // Point the results viewer at this run's output so its t-route
        // results can be loaded onto the map with one click.
        document.getElementById("results-dir").value = outputPath;
        setResultsStatus("", "Run complete — load its t-route output");
    })
    .catch(error => {
        outputBox.innerHTML =
            "❌ Workflow failed.<br>" + error.message;
    })
    .finally(() => {
        clearInterval(elapsedTimer);
        runButton.disabled = false;
        runButton.textContent = "Run Workflow";
    });
}

// These functions are exported by data_processing.js
document.getElementById('subset-button').addEventListener('click', subset);
document.getElementById('forcings-button').addEventListener('click', forcings);
document.getElementById('realization-button').addEventListener('click', realization);
document.getElementById('run-cli-button').addEventListener('click', runWorkflow);