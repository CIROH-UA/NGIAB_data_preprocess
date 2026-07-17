// Toggle the log console and adjust the bottom padding so the sidebar content
// remains visible when the console is expanded.
document.getElementById('toggleConsole').addEventListener('click', function () {
    const consoleElement = document.getElementById('console');
    const bottomElement = document.getElementById('sidebar');

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

// Application logs stream in over a websocket: recent history on connect,
// then each new line as the server writes it.
const MAX_CONSOLE_LINES = 500;

function appendLogLines(lines) {
    const output = document.getElementById('logOutput');

    // Stay pinned to the newest line, but don't yank the view back down
    // if the user has scrolled up to read something.
    const pinned =
        output.scrollHeight - output.scrollTop - output.clientHeight < 40;

    for (const line of lines) {
        const element = document.createElement('div');
        element.textContent = line;
        output.appendChild(element);
    }

    while (output.children.length > MAX_CONSOLE_LINES) {
        output.removeChild(output.firstChild);
    }

    if (pinned) {
        output.scrollTop = output.scrollHeight;
    }
}

function connectLogStream() {
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const ws = new WebSocket(`${protocol}//${location.host}/ws/logs`);

    // The server resends recent history on each connection.
    ws.onopen = () => {
        document.getElementById('logOutput').textContent = '';
    };

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        if (data.lines?.length) appendLogLines(data.lines);
    };

    ws.onclose = () => setTimeout(connectLogStream, 2000);
}

connectLogStream();