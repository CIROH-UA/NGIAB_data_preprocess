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
        if (response.status == 409) {
            const filename = await response.text();
            if (!confirm('A geopackage already exists with that catchment name. Overwrite?')) {
                alert("Subset canceled.");
                document.getElementById('output-path').innerHTML =
                    "Subset canceled. Geopackage located at " + filename;
                return;
            }
        }

        var subset_type = document.getElementById('radio-nexus').checked
            ? 'nexus'
            : 'catchment';

        const startTime = performance.now();

        fetch('/subset', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
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

async function runWorkflow() {
    const inputType = document.querySelector('input[name="input-type"]:checked').value;
    const inputFeature = document.getElementById("workflow-input").value.trim();

    const startTime = document.getElementById("start-time").value;
    const endTime = document.getElementById("end-time").value;
    const runNgiab = document.getElementById("run-ngiab").checked;
    const outputRoot = document.getElementById("output-root").value.trim();

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

    outputBox.innerHTML = `
        <div><strong>⏳ Workflow running...</strong></div>

        <div style="margin-top:8px;">
            This may take several minutes, especially when generating forcings or running NextGen.
        </div>

        <div style="margin-top:10px; color: var(--secondary-text);">
            <strong>Workflow steps:</strong><br>
            Subset hydrofabric → Generate forcings → Create realization
            ${runNgiab ? " → Run NextGen simulation" : ""}
        </div>

        <div style="margin-top:12px;">
            Elapsed time: <span id="workflow-elapsed">0s</span>
        </div>
    `;

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
            output_root: outputRoot,
            source: document.getElementById("datasource-toggle").checked ? "aorc" : "nwm",
            model: document.getElementById("model-select").value,
            run_ngiab: runNgiab
        })
    })
    .then(async response => {
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || "Failed to start workflow");
        }

        return data;
    })
    .then(data => {
        const outputPath =
            data.output_dir ||
            (outputRoot
                ? `${outputRoot}/${inputType}-${inputFeature}`
                : "~/.ngiab/...");

        const successMessage = runNgiab
            ? "✅ Preprocessing and NextGen simulation completed successfully."
            : "✅ Preprocessing completed successfully.";

        outputBox.innerHTML = `
            <div><strong>${successMessage}</strong></div>

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