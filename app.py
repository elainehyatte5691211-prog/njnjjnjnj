import os
import atexit
import pty
import fcntl
import termios
import struct
import signal
import threading
from pyngrok import ngrok
from pyngrok.exception import PyngrokNgrokHTTPError
import queue
import uuid
import time
from collections import deque
from typing import Dict, Optional

from flask import (
    Flask,
    render_template,
    request,
    session,
    Response,
    stream_with_context,
    jsonify,
    abort,
)

# -----------------------------------------------------------------------------
# Flask app
# -----------------------------------------------------------------------------
app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-change-me')


def ensure_sid() -> str:
    sid = session.get('sid')
    if not sid:
        sid = str(uuid.uuid4())
        session['sid'] = sid
    return sid


@app.route('/')
def index():
    ensure_sid()
    return render_template('index.html')


# -----------------------------------------------------------------------------
# PTY-backed terminal
# -----------------------------------------------------------------------------
ALLOWED_SHELLS = ['/bin/bash', '/bin/sh', '/usr/bin/zsh']


def sanitize_shell(shell: Optional[str]) -> str:
    candidate = shell or '/bin/bash'
    if candidate in ALLOWED_SHELLS and os.path.exists(candidate):
        return candidate
    for fallback in ALLOWED_SHELLS:
        if os.path.exists(fallback):
            return fallback
    return '/bin/sh'


class TerminalSession:
    """PTY-backed terminal with scrollback retention."""

    HISTORY_LIMIT = 20 * 1024 * 1024  # 20 MiB
    PREFETCH_LIMIT = 512 * 1024

    def __init__(self, term_id: str, shell: str, rows: int, cols: int, title: Optional[str] = None):
        self.id = term_id
        self.title = title or f"Terminal {term_id[:4]}"
        self.shell = sanitize_shell(shell)
        self.rows = rows
        self.cols = cols

        self.created_at = time.time()
        self.last_active = self.created_at

        self.pid = None
        self.fd = None
        self.reader_thread = None
        self.alive = False

        self.listeners = set()
        self.lock = threading.Lock()
        self.prebuffer = deque()
        self.prebuffer_size = 0
        self.history = deque()
        self.history_size = 0

        self._spawn()

    # Metadata ------------------------------------------------------------
    def to_dict(self) -> Dict[str, object]:
        return {
            'id': self.id,
            'title': self.title,
            'shell': self.shell,
            'rows': self.rows,
            'cols': self.cols,
            'created_at': self.created_at,
            'last_active': self.last_active,
            'alive': self.alive,
            'history_bytes': self.history_size,
        }

    # Internal helpers ----------------------------------------------------
    def _append_history(self, data: bytes):
        if not data:
            return
        self.history.append(data)
        self.history_size += len(data)
        while self.history_size > self.HISTORY_LIMIT and self.history:
            removed = self.history.popleft()
            self.history_size -= len(removed)

    def _append_prebuffer(self, data: bytes):
        if not data:
            return
        self.prebuffer.append(data)
        self.prebuffer_size += len(data)
        while self.prebuffer_size > self.PREFETCH_LIMIT and self.prebuffer:
            removed = self.prebuffer.popleft()
            self.prebuffer_size -= len(removed)

    def _set_winsize(self, rows: int, cols: int):
        if self.fd is None:
            return
        winsize = struct.pack('HHHH', rows, cols, 0, 0)
        fcntl.ioctl(self.fd, termios.TIOCSWINSZ, winsize)

    def _spawn(self):
        pid, fd = pty.fork()
        if pid == 0:
            os.environ['TERM'] = os.environ.get('TERM', 'xterm-256color')
            os.environ['HISTFILE'] = '/dev/null'
            os.environ['PS1'] = '\\u@\\h:\\w$ '
            try:
                os.execv(self.shell, [self.shell, '-i'])
            except Exception:
                os._exit(1)
        else:
            self.pid = pid
            self.fd = fd
            self._set_winsize(self.rows, self.cols)
            self.alive = True
            self.last_active = time.time()
            self.reader_thread = threading.Thread(target=self._read_loop, name=f"pty-reader-{self.id}", daemon=True)
            self.reader_thread.start()
            try:
                os.write(self.fd, b'\n')
            except OSError:
                pass

    # Data plane ---------------------------------------------------------
    def _broadcast(self, data: bytes):
        if not data:
            return
        with self.lock:
            self._append_history(data)
            self.last_active = time.time()
            if self.listeners:
                listeners = list(self.listeners)
            else:
                self._append_prebuffer(data)
                return

        dead = []
        for q in listeners:
            try:
                q.put_nowait(data)
            except Exception:
                dead.append(q)
        if dead:
            with self.lock:
                for q in dead:
                    self.listeners.discard(q)

    def _read_loop(self):
        try:
            while self.alive and self.fd is not None:
                try:
                    data = os.read(self.fd, 4096)
                except OSError:
                    break
                if not data:
                    break
                self._broadcast(data)
        finally:
            self.alive = False
            with self.lock:
                listeners = list(self.listeners)
            for q in listeners:
                try:
                    q.put_nowait(b'')
                except Exception:
                    pass

    # Public API ---------------------------------------------------------
    def write(self, data: str):
        if not self.alive or self.fd is None:
            return
        try:
            os.write(self.fd, data.encode('utf-8'))
            self.last_active = time.time()
        except OSError:
            self.alive = False

    def resize(self, rows: int, cols: int):
        self.rows = rows
        self.cols = cols
        try:
            self._set_winsize(rows, cols)
            self.last_active = time.time()
        except Exception:
            pass

    def subscribe(self):
        q = queue.Queue()
        with self.lock:
            history_snapshot = list(self.history)
            prebuffer_snapshot = list(self.prebuffer)
            self.prebuffer.clear()
            self.prebuffer_size = 0
            self.listeners.add(q)
        for chunk in history_snapshot + prebuffer_snapshot:
            if chunk:
                try:
                    q.put_nowait(chunk)
                except Exception:
                    break
        return q

    def unsubscribe(self, q: queue.Queue):
        with self.lock:
            self.listeners.discard(q)

    def restart(self, shell: Optional[str] = None, rows: Optional[int] = None, cols: Optional[int] = None, reset_history: bool = True):
        self.close()
        self.alive = False
        if shell:
            self.shell = sanitize_shell(shell)
        if rows:
            self.rows = rows
        if cols:
            self.cols = cols
        if reset_history:
            self.history.clear()
            self.history_size = 0
        self.prebuffer.clear()
        self.prebuffer_size = 0
        self._spawn()

    def close(self):
        self.alive = False
        try:
            if self.fd is not None:
                try:
                    os.close(self.fd)
                except OSError:
                    pass
                self.fd = None
        finally:
            if self.pid:
                try:
                    pgid = os.getpgid(self.pid)
                    os.killpg(pgid, signal.SIGHUP)
                except Exception:
                    try:
                        os.kill(self.pid, signal.SIGHUP)
                    except Exception:
                        pass


class TerminalManager:
    def __init__(self):
        self._store: Dict[str, Dict[str, TerminalSession]] = {}
        self._lock = threading.Lock()

    def list(self, sid: str):
        with self._lock:
            return list(self._store.get(sid, {}).values())

    def get(self, sid: str, term_id: str) -> Optional[TerminalSession]:
        with self._lock:
            return self._store.get(sid, {}).get(term_id)

    def create(self, sid: str, shell: str, rows: int, cols: int, title: Optional[str] = None) -> TerminalSession:
        term_id = str(uuid.uuid4())
        term = TerminalSession(term_id=term_id, shell=sanitize_shell(shell), rows=rows, cols=cols, title=title)
        with self._lock:
            self._store.setdefault(sid, {})[term_id] = term
        return term

    def remove(self, sid: str, term_id: str) -> bool:
        with self._lock:
            user_terms = self._store.get(sid)
            term = user_terms.pop(term_id, None) if user_terms else None
        if term:
            term.close()
            return True
        return False

    def restart(self, sid: str, term_id: str, shell: Optional[str], rows: Optional[int], cols: Optional[int]):
        term = self.get(sid, term_id)
        if not term:
            return None
        term.restart(shell=shell, rows=rows, cols=cols)
        return term


manager = TerminalManager()


def find_terminal_or_404(sid: str, term_id: str) -> TerminalSession:
    term = manager.get(sid, term_id)
    if not term:
        abort(404, description='terminal not found')
    return term


# -----------------------------------------------------------------------------
# HTTP API
# -----------------------------------------------------------------------------
@app.get('/terminals')
def http_terminals_list():
    sid = ensure_sid()
    items = [term.to_dict() for term in manager.list(sid)]
    return jsonify({'items': items})


@app.post('/terminals')
def http_terminals_create():
    sid = ensure_sid()
    payload = request.get_json(silent=True) or {}
    shell = payload.get('shell')
    rows = int(payload.get('rows', 24))
    cols = int(payload.get('cols', 80))
    title = payload.get('title')
    term = manager.create(sid, shell or '/bin/bash', rows, cols, title=title)
    return jsonify(term.to_dict()), 201


@app.get('/terminals/<term_id>')
def http_terminals_detail(term_id: str):
    sid = ensure_sid()
    term = find_terminal_or_404(sid, term_id)
    return jsonify(term.to_dict())


@app.delete('/terminals/<term_id>')
def http_terminals_delete(term_id: str):
    sid = ensure_sid()
    removed = manager.remove(sid, term_id)
    return ('', 204) if removed else ('', 404)


@app.post('/terminals/<term_id>/restart')
def http_terminals_restart(term_id: str):
    sid = ensure_sid()
    payload = request.get_json(silent=True) or {}
    shell = payload.get('shell')
    rows = payload.get('rows')
    cols = payload.get('cols')
    term = manager.restart(sid, term_id, shell=shell, rows=int(rows) if rows else None, cols=int(cols) if cols else None)
    if not term:
        return ('', 404)
    return jsonify(term.to_dict())


@app.post('/terminals/<term_id>/resize')
def http_terminals_resize(term_id: str):
    sid = ensure_sid()
    term = find_terminal_or_404(sid, term_id)
    payload = request.get_json(silent=True) or {}
    rows = int(payload.get('rows', term.rows))
    cols = int(payload.get('cols', term.cols))
    term.resize(rows, cols)
    return ('', 204)


@app.post('/terminals/<term_id>/input')
def http_terminals_input(term_id: str):
    sid = ensure_sid()
    term = find_terminal_or_404(sid, term_id)
    data = request.get_data(as_text=True)
    if data:
        term.write(data)
    return ('', 204)


@app.get('/terminals/<term_id>/stream')
def http_terminals_stream(term_id: str):
    sid = ensure_sid()
    term = find_terminal_or_404(sid, term_id)
    q = term.subscribe()

    def gen():
        try:
            while True:
                try:
                    data = q.get(timeout=15)
                except queue.Empty:
                    yield 'event: ping\ndata: 1\n\n'
                    continue
                if data == b'':
                    break
                text = data.decode('utf-8', errors='ignore')
                yield 'data: ' + text.replace('\n', '\ndata: ') + '\n\n'
        finally:
            term.unsubscribe(q)

    headers = {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'X-Accel-Buffering': 'no',
        'Connection': 'keep-alive',
    }
    return Response(stream_with_context(gen()), headers=headers)


# -----------------------------------------------------------------------------
# Utility endpoints (optional)
# -----------------------------------------------------------------------------
@app.get("/_health")
def _health():
    return jsonify(ok=True), 200


@app.get("/_url")
def _url():
    # Вернёт текущий https public_url, если туннель уже поднят
    try:
        tunnels = ngrok.get_tunnels()
        pub = next((t.public_url for t in tunnels if getattr(t, "proto", "") == "https"), None)
    except Exception:
        pub = None
    return jsonify(public_url=pub), 200


# -----------------------------------------------------------------------------
# ngrok bootstrap
# -----------------------------------------------------------------------------
def start_ngrok(port: int):
    """
    Стартует новый HTTPS-туннель к localhost:port.
    На всякий случай убивает старый локальный агент pyngrok, чтобы избежать дублей.
    """
    try:
        ngrok.kill()
    except Exception:
        pass
    time.sleep(0.2)
    try:
        return ngrok.connect(port, bind_tls=True)
    except PyngrokNgrokHTTPError as e:
        # Если домен уже занят другой сессией (ERR_NGROK_334),
        # это означает, что где-то ещё поднят туннель с тем же зарезервированным доменом.
        # В таком случае нужно отключить ту сессию в Dashboard или использовать другой домен.
        raise e


# -----------------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    port = int(os.getenv('PORT', 6246))

    # Указываем токен через переменную окружения (не хардкодим в коде!)
    token = '34HUsxGI3fpuZseIoS8WzG1THa2_6N56nHJtSjf4wLVsr9DQq'
    if token:
        ngrok.set_auth_token(token)

    # Чистим агент при завершении процесса
    atexit.register(lambda: ngrok.kill())

    # Поднимаем публичный URL
    tunnel = start_ngrok(port)
    print("PUBLIC URL:", tunnel.public_url, flush=True)

    # ВАЖНО: без авторелоада, чтобы не плодить второй процесс и второй туннель
    app.run(host='127.0.0.1', port=port, debug=True, use_reloader=False, threaded=True)
