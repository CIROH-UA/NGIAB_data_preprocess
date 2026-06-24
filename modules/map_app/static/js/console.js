document.getElementById('toggleConsole').addEventListener('click', function () {
    const consoleElement = document.getElementById('console');
    const bottomElement = document.getElementById('manual-workflow');

    consoleElement.classList.toggle('minimized');

    if (consoleElement.classList.contains('minimized')) {
        this.textContent = 'Show Console';

        if (bottomElement) {
            bottomElement.style.transition = 'padding-bottom 0.5s ease';
            bottomElement.style.paddingBottom = '40px';
        }
    } else {
        this.textContent = 'Hide Console';

        if (bottomElement) {
            bottomElement.style.transition = 'padding-bottom 0.5s ease';
            bottomElement.style.paddingBottom = '20vh';
        }
    }
});

function fetchLogs() {
    fetch('/logs')
        .then(response => response.json())
        .then(data => {
            const consoleElement = document.getElementById('logOutput');
            if (!consoleElement || !data.logs) return;

            consoleElement.innerHTML = data.logs.join('<br>');
            consoleElement.scrollTop = consoleElement.scrollHeight;
        });
}

setInterval(fetchLogs, 1000);