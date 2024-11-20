document.getElementById('toggleConsole').addEventListener('click', function () {
    var consoleElement = document.getElementById('console');
    // this is the element at the bottom of the page we need to pad so it's not hidden by the console
    var realizationElement = document.getElementById('realization');
    consoleElement.classList.toggle('minimized');
    if (consoleElement.classList.contains('minimized')) {
        this.textContent = 'Show Console';
        realizationElement.style.transition = 'padding-bottom 0.5s ease';
        realizationElement.style.paddingBottom = '40px';
        if ((window.innerHeight + window.scrollY) >= document.body.offsetHeight) {
            window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });
        }
        }
        else {
        this.textContent = 'Hide Console';
        realizationElement.style.transition = 'padding-bottom 0.5s ease';
        realizationElement.style.paddingBottom = '20vh';
    }
});

function fetchLogs() {
    fetch('/logs')
        .then(response => response.json())
        .then(data => {
            var consoleElement = document.getElementById('logOutput');
            consoleElement.innerHTML = data.logs.join('<br>');
            consoleElement.scrollTop = consoleElement.scrollHeight;
        }
        );

}

setInterval(fetchLogs, 1000); // Fetch logs every second
