// Use the shared workflow identifier field for manual basin-based steps.
function getSelectedIdentifier() {
    return document.getElementById("workflow-input").value.trim();
}

// Validation shared by the step buttons; each returns null after alerting.
function requireBasinId(action) {
    const catId = getSelectedIdentifier();
    if (!catId || !catId.startsWith("cat-")) {
        alert(`Please select or enter a basin ID, like cat-2739307, before ${action}.`);
        return null;
    }
    return catId;
}

function requireTimes() {
    const startTime = document.getElementById("start-time").value;
    const endTime = document.getElementById("end-time").value;
    if (!startTime || !endTime) {
        alert("Please select both start and end times.");
        return null;
    }
    if (new Date(startTime) >= new Date(endTime)) {
        alert("Start time must be before end time.");
        return null;
    }
    return { startTime, endTime };
}

// Return the existing subset geopackage path for a basin, or null.
async function existingSubsetPath(catId) {
    const response = await fetch("/subset_check", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify([catId]),
    });
    return response.status == 409 ? await response.text() : null;
}

function setStepOutput(html) {
    document.getElementById("step-output").innerHTML = html;
}

function setStepButtonsDisabled(disabled) {
    for (const id of ["subset-button", "forcings-button", "configure-button", "run-button"]) {
        document.getElementById(id).disabled = disabled;
    }
}

async function subset() {
    const catId = requireBasinId("subsetting");
    if (!catId) return;

    const existing = await existingSubsetPath(catId);
    if (existing && !confirm("A geopackage already exists for that catchment. Overwrite?")) {
        setStepOutput(`Subset canceled. Geopackage located at <code>${existing}</code>`);
        return;
    }

    const subsetType = document.getElementById("radio-nexus").checked ? "nexus" : "catchment";
    const started = performance.now();

    setStepButtonsDisabled(true);
    setStepOutput("Creating subset...");
    try {
        const response = await fetch("/subset", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ cat_id: [catId], subset_type: subsetType }),
        });
        const filename = await response.text();
        const seconds = ((performance.now() - started) / 1000).toFixed(2);
        setStepOutput(`Done in ${seconds} s<br><code>${filename}</code>`);
    } catch (error) {
        setStepOutput(`Subset failed: ${error.message}`);
    } finally {
        setStepButtonsDisabled(false);
    }
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
        setStepOutput("Downloading data...");
        document.getElementById('bar-text').textContent = "Downloading...";
        document.getElementById('bar').style.animation = "indeterminateAnimation 1s infinite linear";
    } else {
        const percent = parseInt(data, 10);
        updateProgressBar(percent);
        if (percent > 0 && percent < 100) {
            document.getElementById('bar').style.animation = "none"; // stop the indeterminate animation
            setStepOutput("Calculating zonal statistics. See progress below.");
        } else if (percent >= 100) {
            updateProgressBar(100); // Ensure the progress bar is full
            done();
            setStepOutput("Forcings generated successfully");
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
    const catId = requireBasinId("generating forcings");
    if (!catId) return;
    const times = requireTimes();
    if (!times) return;

    const gpkg = await existingSubsetPath(catId);
    if (!gpkg) {
        alert("No existing geopackage found. Create a subset first.");
        return;
    }

    const source = document.getElementById("datasource-toggle").checked ? "aorc" : "nwm";

    setStepButtonsDisabled(true);
    setStepOutput("Generating forcings...");
    try {
        const progressResponse = await fetch("/make_forcings_progress_file", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(gpkg),
        });
        pollForcingsProgress(await progressResponse.text());

        await fetch("/forcings", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                forcing_dir: gpkg,
                start_time: times.startTime,
                end_time: times.endTime,
                source: source,
            }),
        });
    } catch (error) {
        setStepOutput(`Forcings failed: ${error.message}`);
    } finally {
        setStepButtonsDisabled(false);
    }
}

// Create the realization/model configuration for an existing subset.
async function configure() {
    const catId = requireBasinId("creating a realization");
    if (!catId) return;
    const times = requireTimes();
    if (!times) return;

    const gpkg = await existingSubsetPath(catId);
    if (!gpkg) {
        alert("No existing geopackage found. Create a subset first.");
        return;
    }

    setStepButtonsDisabled(true);
    setStepOutput("Creating realization...");
    try {
        await fetch("/realization", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                forcing_dir: gpkg,
                start_time: times.startTime,
                end_time: times.endTime,
            }),
        });
        setStepOutput("Realization created");
    } catch (error) {
        setStepOutput(`Configure failed: ${error.message}`);
    } finally {
        setStepButtonsDisabled(false);
    }
}

// Run NGIAB against the already-preprocessed output folder. The CLI's --run
// flag validates the folder and runs any missing steps first.
async function runNgiab() {
    const inputFeature = getSelectedIdentifier();
    if (!inputFeature) {
        alert("Please select or enter an ID first.");
        return;
    }
    const times = requireTimes();
    if (!times) return;

    setStepButtonsDisabled(true);
    setStepOutput("Running NGIAB... this can take a while.");
    try {
        const response = await fetch("/run_cli", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                input_feature: inputFeature,
                input_type: document.getElementById("gage-checkbox").checked ? "gage" : "basin",
                start_time: times.startTime,
                end_time: times.endTime,
                steps: ["run"],
            }),
        });
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Run failed");
        setStepOutput(`NGIAB run complete<br><code>${data.output_dir}</code>`);
    } catch (error) {
        setStepOutput(`Run failed: ${error.message}`);
    } finally {
        setStepButtonsDisabled(false);
    }
}

// Collect workflow options from the UI and run the complete CLI workflow.
async function runWorkflow() {
    const inputType = document.getElementById("gage-checkbox").checked ? "gage" : "basin";
    const inputFeature = getSelectedIdentifier();

    const runButton = document.getElementById("run-cli-button");
    const outputBox = document.getElementById("run-cli-output");

    if (!inputFeature) {
        alert(`Please enter a ${inputType} ID.`);
        return;
    }

    const times = requireTimes();
    if (!times) return;

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
            start_time: times.startTime,
            end_time: times.endTime,
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

document.getElementById('subset-button').addEventListener('click', subset);
document.getElementById('forcings-button').addEventListener('click', forcings);
document.getElementById('configure-button').addEventListener('click', configure);
document.getElementById('run-button').addEventListener('click', runNgiab);
document.getElementById('run-cli-button').addEventListener('click', runWorkflow);