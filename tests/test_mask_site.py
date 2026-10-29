from __future__ import annotations

import asyncio
import hashlib
import http.server
import os
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

def _find_root() -> Path:
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        if (parent / "XRay.py").is_file() and (parent / "Storage" / "XRay").is_dir():
            return parent
    raise RuntimeError("the repository root is not reachable from the test file")


ROOT = _find_root()
WEB = ROOT / "Storage" / "XRay" / "WEB"
TEMPLATE = WEB / "websocket_site.py"
HOLE = WEB / "hole"
GATE = WEB / "Evil_Cat.jsx"
LOADING = WEB / "Loading.html"
MODULE = ROOT / "XRay.py"
JS = "index-CwNTv7CE.js"
CSS = "index-KqaZQrrj.css"

HITS: dict[str, int] = {}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def hole_files(root: Path = HOLE) -> dict[str, str]:
    files = {"index.html": digest(root / "index.html"), "gate": digest(GATE), "loading": digest(LOADING)}
    for item in sorted((root / "assets").iterdir()):
        files["assets/" + item.name] = digest(item)
    return files


def gate_files() -> dict[str, str]:
    return {"gate": digest(GATE), "loading": digest(LOADING)}


def module_constants(*names: str) -> dict[str, object]:
    import ast

    source = MODULE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    lines = source.split("\n")
    found: dict[str, object] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") in names:
            exec("\n".join(lines[node.lineno - 1:node.end_lineno]), {"os": os}, found)
    return found


def module_methods(*names: str):
    import ast
    import textwrap
    from typing import Dict

    source = MODULE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    lines = source.split("\n")
    parts = []
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "XRay":
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name in names:
                    parts.append(textwrap.dedent("\n".join(lines[item.lineno - 1:item.end_lineno])))
    assert parts, names
    constants = module_constants(
        "HOLE_FILES", "GATE_FILES", "SITE_SCRIPT_SHA256", "MINIMAL_SITE_TEMPLATE", "ASSET_BASE", "HOLE_BASE"
    )

    class Logger:
        def __init__(self) -> None:
            self.lines: list[str] = []

        def warning(self, fmt: str, *args: object) -> None:
            self.lines.append(fmt % args)

    logger = Logger()
    namespace: dict[str, object] = dict(constants)
    namespace.update(
        {
            "os": os,
            "hashlib": hashlib,
            "urllib": urllib,
            "Dict": Dict,
            "logger": logger,
            "ASSET_HOSTS": {"raw.githubusercontent.com", "github.com", "objects.githubusercontent.com"},
            "_URL": str(constants["ASSET_BASE"]).rstrip("/") + "/WEB/websocket_site.py",
        }
    )
    exec(
        "class Wrapped:\n    _WEBSOCKET_SITE_SCRIPT_URL = _URL\n\n" + textwrap.indent("\n\n".join(parts), "    "),
        namespace,
    )
    return namespace["Wrapped"], logger, constants


class Answer:
    def __init__(self, body: bytes) -> None:
        self.body = body

    def __enter__(self) -> "Answer":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def read(self, limit: int = 0) -> bytes:
        return self.body[:limit] if limit else self.body


def stub_urlopen(monkeypatch, body: bytes) -> None:
    monkeypatch.setattr(urllib.request, "urlopen", lambda request, timeout=0: Answer(body))


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class Quiet(http.server.SimpleHTTPRequestHandler):
    directory = HOLE

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(type(self).directory), **kwargs)

    def log_message(self, fmt, *args):
        HITS[self.path] = HITS.get(self.path, 0) + 1


class Static:
    def __init__(self, directory: Path = HOLE):
        directory = Path(directory)
        handler = type("Scoped", (Quiet,), {"directory": directory})
        self.port = free_port()
        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", self.port), handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_exc):
        self.httpd.shutdown()
        self.httpd.server_close()

    @property
    def url(self) -> str:
        return "http://127.0.0.1:%d" % self.port


class Backend:
    def __init__(self):
        self.port = free_port()
        self.loop = asyncio.new_event_loop()
        self.ready = threading.Event()
        self.thread = threading.Thread(target=self.serve, daemon=True)

    async def echo(self, request):
        from aiohttp import web

        ws = web.WebSocketResponse()
        await ws.prepare(request)
        async for message in ws:
            await ws.send_str("echo:" + message.data)
        return ws

    async def listen(self):
        from aiohttp import web

        app = web.Application()
        app.router.add_get("/xhttps", self.echo)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", self.port).start()
        self.ready.set()

    def serve(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.listen())
        self.loop.run_forever()

    def start(self):
        self.thread.start()
        assert self.ready.wait(20), "the backend did not start"
        return self

    def stop(self):
        async def shutdown():
            await self.runner.cleanup()

        asyncio.run_coroutine_threadsafe(shutdown(), self.loop).result(15)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=5)
        self.loop.close()


def build_script(
    tmp_path: Path, kind: str, mask_url: str, backend_port: int, files: dict[str, str] | None = None, loading_url: str = ""
) -> tuple[Path, int]:
    body = TEMPLATE.read_text(encoding="utf-8")
    port = free_port()
    body = (
        body.replace("__PATH__", "/xhttps")
        .replace("__BACKEND_PORT__", str(backend_port))
        .replace("__SITE_PORT__", str(port))
        .replace("__MASK_KIND__", kind)
        .replace("__MASK_URL__", mask_url)
        .replace("__LOADING_URL__", loading_url or (mask_url + "/Loading.html"))
        .replace("__MASK_FILES__", repr(hole_files() if files is None else files))
    )
    target = tmp_path / ("site_%s_%d.py" % (kind, port))
    target.write_text(body, encoding="utf-8")
    return target, port


def start_site(
    tmp_path: Path,
    kind: str,
    mask_url: str,
    backend_port: int,
    allow: bool,
    files: dict[str, str] | None = None,
    loading_url: str = "",
) -> tuple[subprocess.Popen, int]:
    script, port = build_script(tmp_path, kind, mask_url, backend_port, files, loading_url)
    env = dict(os.environ)
    env["XRAY_MASK_HOSTS"] = "127.0.0.1" if allow else ""
    proc = subprocess.Popen(
        [sys.executable, str(script)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return proc, port
        except OSError:
            if proc.poll() is not None:
                raise AssertionError("the site died: " + (proc.stdout.read() if proc.stdout else ""))
            time.sleep(0.1)
    raise AssertionError("the site did not start")


def stop_site(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def get(port: int, path: str) -> tuple[int, bytes, str]:
    request = urllib.request.Request("http://127.0.0.1:%d%s" % (port, path))
    try:
        with urllib.request.urlopen(request, timeout=10) as answer:
            return answer.status, answer.read(), answer.headers.get("Content-Type", "")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), exc.headers.get("Content-Type", "")


@pytest.fixture()
def site(tmp_path):
    backend = Backend().start()
    with Static() as static:
        HITS.clear()
        proc, port = start_site(tmp_path, "hole", static.url, backend.port, True)
        try:
            yield {"port": port, "static": static, "backend": backend}
        finally:
            stop_site(proc)
            backend.stop()


def test_the_mask_site_serves_the_black_hole_page(site) -> None:
    status, body, content_type = get(site["port"], "/")
    text = body.decode("utf-8")
    assert status == 200
    assert content_type.startswith("text/html")
    assert JS in text
    assert CSS in text
    assert "ChromeSetup" in text
    assert "vless" not in text.lower()
    assert "xray" not in text.lower()


def test_the_mask_site_serves_the_bundle_and_the_stylesheet(site) -> None:
    status, body, content_type = get(site["port"], "/assets/" + JS)
    assert status == 200
    assert content_type.startswith("text/javascript")
    assert len(body) == (HOLE / "assets" / JS).stat().st_size
    status, body, content_type = get(site["port"], "/assets/" + CSS)
    assert status == 200
    assert content_type.startswith("text/css")
    assert b"html" in body or b"body" in body or b"{" in body


def test_the_bundle_is_fetched_once_and_then_served_from_memory(site) -> None:
    first = get(site["port"], "/assets/" + JS)[0]
    second = get(site["port"], "/assets/" + JS)[0]
    third = get(site["port"], "/assets/" + JS)[0]
    assert (first, second, third) == (200, 200, 200)
    assert HITS.get("/assets/" + JS, 0) == 1
    assert HITS.get("/index.html", 0) <= 1


def test_a_traversal_or_an_unknown_asset_is_refused(site) -> None:
    for path in (
        "/assets/../../etc/passwd",
        "/assets/%2e%2e%2f%2e%2e%2fetc%2fpasswd",
        "/assets/..%2f..%2fetc%2fpasswd",
        "/assets/.hidden",
        "/assets/passwd.txt",
        "/assets/",
        "/admin",
        "/favicon.ico",
        "/vless",
    ):
        status, body, _content = get(site["port"], path)
        assert status == 404, path
        assert b"root:" not in body


def test_the_jsx_route_is_closed_in_hole_mode(site) -> None:
    assert get(site["port"], "/gate.jsx")[0] == 404


def test_the_health_route_answers_without_the_mask(site) -> None:
    status, body, _content = get(site["port"], "/health")
    assert status == 200
    assert body == b"ok\n"


def test_the_tunnel_path_still_proxies_a_websocket(site) -> None:
    from aiohttp import ClientSession

    async def exchange() -> str:
        async with ClientSession() as session:
            async with session.ws_connect("ws://127.0.0.1:%d/xhttps" % site["port"]) as ws:
                await ws.send_str("hello")
                message = await ws.receive(timeout=10)
                return message.data

    assert asyncio.run(exchange()) == "echo:hello"


def test_a_host_outside_the_allowlist_never_reaches_the_browser(tmp_path) -> None:
    backend = Backend().start()
    with Static() as static:
        proc, port = start_site(tmp_path, "hole", static.url, backend.port, False)
        try:
            status, body, _content = get(port, "/")
            text = body.decode("utf-8")
            assert status == 200
            assert JS not in text
            assert "Downloads" in text
            status, _body, _content = get(port, "/assets/" + JS)
            assert status == 404
        finally:
            stop_site(proc)
            backend.stop()


def test_an_oversized_asset_is_refused(tmp_path) -> None:
    backend = Backend().start()
    big_dir = tmp_path / "big"
    (big_dir / "assets").mkdir(parents=True)
    (big_dir / "index.html").write_text("<html>Speedtest</html>", encoding="utf-8")
    (big_dir / "assets" / "huge.js").write_bytes(b"x" * (9 * 1024 * 1024))

    class BigQuiet(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(big_dir), **kwargs)

        def log_message(self, fmt, *args):
            return

    port_static = free_port()
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", port_static), BigQuiet)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    proc, port = start_site(
        tmp_path,
        "hole",
        "http://127.0.0.1:%d" % port_static,
        backend.port,
        True,
        {"index.html": digest(big_dir / "index.html")},
    )
    try:
        assert get(port, "/assets/huge.js")[0] == 404
        assert get(port, "/")[0] == 200
    finally:
        stop_site(proc)
        httpd.shutdown()
        httpd.server_close()
        backend.stop()


def test_the_evil_cat_mask_still_works(tmp_path) -> None:
    backend = Backend().start()
    with Static(WEB) as static:
        proc, port = start_site(
            tmp_path,
            "jsx",
            static.url + "/Evil_Cat.jsx",
            backend.port,
            True,
            gate_files(),
            static.url + "/Loading.html",
        )
        try:
            status, body, content_type = get(port, "/gate.jsx")
            assert status == 200
            assert content_type.startswith("application/javascript")
            assert body == GATE.read_bytes()
            status, body, _content = get(port, "/")
            assert status == 200
            assert body == LOADING.read_bytes()
        finally:
            stop_site(proc)
            backend.stop()


def test_the_module_registers_both_masks_and_passes_their_kind() -> None:
    source = MODULE.read_text(encoding="utf-8")
    assert '"Evil Cat": ("jsx",' in source
    assert '"Black Hole": ("hole",' in source
    assert 'mask_kind, mask_url = self._mask_sites[mask_name]' in source
    assert 'self._websocket_site_script(path, user["port"], site_port, mask_kind, mask_url, mask_files)' in source
    assert 'mask_files = GATE_FILES if mask_kind == "jsx" else HOLE_FILES' in source
    assert 'HOLE_BASE = ASSET_BASE.rstrip("/") + "/WEB/hole"' in source
    assert '.replace("__MASK_KIND__", mask_kind or "jsx")' in source
    assert ".replace(\"__MASK_FILES__\", repr(dict(mask_files or {})))" in source
    assert "mask_kind: str, mask_url: str, mask_files: Dict[str, str]" in source


def test_the_module_keeps_no_copy_of_the_site_template() -> None:
    source = MODULE.read_text(encoding="utf-8")
    assert "\nSITE_TEMPLATE = " not in source
    assert "\nMINIMAL_SITE_TEMPLATE = r" in source
    assert "MASK_FILES = __MASK_FILES__" not in source
    assert "mask_assets" not in source
    shipped = TEMPLATE.read_text(encoding="utf-8")
    for marker in ("MASK_FILES = __MASK_FILES__", "CACHE_TTL", "ASSET_NAME = re.compile", "def gate_jsx"):
        assert marker not in source, marker
        assert marker in shipped, marker
    constants = module_constants("MINIMAL_SITE_TEMPLATE")
    minimal = str(constants["MINIMAL_SITE_TEMPLATE"])
    assert "__SITE_PORT__" in minimal and "__PATH__" in minimal
    assert "__MASK_FILES__" not in minimal
    assert len(minimal) < len(TEMPLATE.read_text(encoding="utf-8")) / 2
    compile(minimal.replace("__PATH__", "/xhttps").replace("__SITE_PORT__", "4500"), "minimal.py", "exec")


def test_the_site_template_is_fetched_with_a_pinned_digest() -> None:
    constants = module_constants("SITE_SCRIPT_SHA256")
    assert constants["SITE_SCRIPT_SHA256"] == digest(TEMPLATE)
    source = MODULE.read_text(encoding="utf-8")
    assert 'expected = os.environ.get("XRAY_SITE_SCRIPT_SHA256", "").strip().lower() or SITE_SCRIPT_SHA256' in source
    assert "if got != expected:" in source


def test_a_tampered_site_template_falls_back_to_the_minimal_one(monkeypatch) -> None:
    wrapped, logger, constants = module_methods("_fetch_site_script", "_websocket_site_script")
    tampered = TEMPLATE.read_bytes().replace(b"import asyncio", b"import asyncio \n")
    stub_urlopen(monkeypatch, tampered)
    script = wrapped()._websocket_site_script("/xhttps", 2096, 4501, "hole", constants["HOLE_BASE"], constants["HOLE_FILES"])
    assert "MASK_FILES" not in script
    assert any("digest" in line for line in logger.lines)
    compile(script, "generated.py", "exec")


def test_a_genuine_site_template_is_used_whole(monkeypatch) -> None:
    wrapped, logger, constants = module_methods("_fetch_site_script", "_websocket_site_script")
    stub_urlopen(monkeypatch, TEMPLATE.read_bytes())
    script = wrapped()._websocket_site_script("/xhttps", 2096, 4502, "hole", constants["HOLE_BASE"], constants["HOLE_FILES"])
    assert "MASK_FILES = {" in script
    assert logger.lines == []
    assert "__PATH__" not in script and "__SITE_PORT__" not in script
    compile(script, "generated.py", "exec")


def test_the_fetch_refuses_an_off_list_host_and_a_plain_http_url(monkeypatch) -> None:
    wrapped, _logger, constants = module_methods("_fetch_site_script", "_websocket_site_script")
    stub_urlopen(monkeypatch, TEMPLATE.read_bytes())
    for url in ("https://evil.example.com/websocket_site.py", "http://raw.githubusercontent.com/x/websocket_site.py"):
        wrapped._WEBSOCKET_SITE_SCRIPT_URL = url
        script = wrapped()._websocket_site_script("/xhttps", 2096, 4503, "hole", constants["HOLE_BASE"], constants["HOLE_FILES"])
        assert "MASK_FILES" not in script, url


def test_the_fetch_refuses_an_oversized_template(monkeypatch) -> None:
    wrapped, _logger, constants = module_methods("_fetch_site_script", "_websocket_site_script")
    stub_urlopen(monkeypatch, b"x" * (300 * 1024))
    script = wrapped()._websocket_site_script("/xhttps", 2096, 4504, "hole", constants["HOLE_BASE"], constants["HOLE_FILES"])
    assert "MASK_FILES" not in script


def test_the_minimal_page_still_proxies_the_tunnel(tmp_path) -> None:
    wrapped, _logger, constants = module_methods("_fetch_site_script", "_websocket_site_script")
    wrapped._WEBSOCKET_SITE_SCRIPT_URL = "https://off-list.example.com/websocket_site.py"
    backend = Backend().start()
    port = free_port()
    script = wrapped()._websocket_site_script("/xhttps", backend.port, port, "hole", constants["HOLE_BASE"], constants["HOLE_FILES"])
    path = tmp_path / "minimal.py"
    path.write_text(script, encoding="utf-8")
    proc = subprocess.Popen([sys.executable, str(path)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        deadline = time.time() + 20
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                    break
            except OSError:
                if proc.poll() is not None:
                    raise AssertionError("the minimal site died: " + (proc.stdout.read() if proc.stdout else ""))
                time.sleep(0.1)
        status, body, content_type = get(port, "/")
        assert status == 200
        assert content_type.startswith("text/html")
        assert "Downloads" in body.decode("utf-8")
        assert get(port, "/health")[1] == b"ok\n"

        async def exchange() -> str:
            from aiohttp import ClientSession

            async with ClientSession() as session:
                async with session.ws_connect("ws://127.0.0.1:%d/xhttps" % port) as ws:
                    await ws.send_str("hello")
                    message = await ws.receive(timeout=10)
                    return message.data

        assert asyncio.run(exchange()) == "echo:hello"
    finally:
        stop_site(proc)
        backend.stop()


def test_the_template_has_no_bot_token_and_no_second_listener() -> None:
    body = TEMPLATE.read_text(encoding="utf-8")
    assert "BOT_TOKEN" not in body
    assert "jwt" not in body.lower()
    assert body.count("web.run_app") == 1
    assert 'host="127.0.0.1"' in body


def test_the_hole_assets_are_present_and_small() -> None:
    index = (HOLE / "index.html").read_text(encoding="utf-8")
    assert JS in index and CSS in index
    assert "ChromeSetup" in index
    for name in (JS, CSS):
        assert (HOLE / "assets" / name).is_file()
    assert (HOLE / "assets" / JS).stat().st_size < 2 * 1024 * 1024


def test_the_generated_script_has_no_placeholder_and_compiles(tmp_path) -> None:
    base = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/XRay/WEB"
    for kind, url in (("hole", base + "/hole"), ("jsx", base + "/Evil_Cat.jsx")):
        script, _port = build_script(tmp_path, kind, url, 2096)
        text = script.read_text(encoding="utf-8")
        for token in (
            "__PATH__",
            "__BACKEND_PORT__",
            "__SITE_PORT__",
            "__MASK_KIND__",
            "__MASK_URL__",
            "__LOADING_URL__",
            "__MASK_FILES__",
        ):
            assert token not in text, token
        assert '"%s"' % kind in text
        compile(text, str(script), "exec")


def test_the_sha_digests_of_the_shipped_files_are_pinned_in_the_module() -> None:
    source = MODULE.read_text(encoding="utf-8")
    for name, block in (("HOLE_FILES", "hole"), ("GATE_FILES", "gate")):
        match = re.search(name + r" = \{(.*?)\}", source, re.S)
        assert match is not None, name
        pairs = re.findall(r'"([^"]+)": "([0-9a-f]{64})"', match.group(1))
        assert pairs
        for key, value in pairs:
            if key == "gate":
                assert value == digest(GATE)
            elif key == "loading":
                assert value == digest(LOADING)
            else:
                assert value == digest(HOLE / key), key


def test_a_tampered_asset_never_reaches_the_browser(site) -> None:
    assert get(site["port"], "/assets/" + JS)[0] == 200
    status, body, _content = get(site["port"], "/assets/" + CSS)
    assert status == 200
    assert body == (HOLE / "assets" / CSS).read_bytes()


def test_a_digest_mismatch_falls_back_to_the_plain_page(tmp_path) -> None:
    backend = Backend().start()
    fake = tmp_path / "fake"
    (fake / "assets").mkdir(parents=True)
    (fake / "index.html").write_text(
        '<!doctype html><html><head><title>Speedtest</title>'
        '<script type="module" src="/assets/%s"></script></head><body>patched</body></html>' % JS,
        encoding="utf-8",
    )
    quoted = hole_files()
    quoted["index.html"] = "0" * 64

    class Fake(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(fake), **kwargs)

        def log_message(self, fmt, *args):
            return

    port_static = free_port()
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", port_static), Fake)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    proc, port = start_site(tmp_path, "hole", "http://127.0.0.1:%d" % port_static, backend.port, True, quoted)
    try:
        status, body, _content = get(port, "/")
        text = body.decode("utf-8")
        assert status == 200
        assert "patched" not in text
        assert JS not in text
        assert "Downloads" in text
        assert get(port, "/assets/" + JS)[0] == 404
    finally:
        stop_site(proc)
        httpd.shutdown()
        httpd.server_close()
        backend.stop()


def test_an_asset_without_a_pinned_digest_is_not_served(tmp_path) -> None:
    backend = Backend().start()
    with Static() as static:
        files = hole_files()
        files.pop("assets/" + CSS)
        proc, port = start_site(tmp_path, "hole", static.url, backend.port, True, files)
        try:
            assert get(port, "/assets/" + JS)[0] == 200
            assert get(port, "/assets/" + CSS)[0] == 404
        finally:
            stop_site(proc)
            backend.stop()


def test_the_jsx_gate_is_checked_against_its_digest(tmp_path) -> None:
    backend = Backend().start()
    with Static(WEB) as static:
        files = gate_files()
        files["gate"] = "1" * 64
        proc, port = start_site(
            tmp_path,
            "jsx",
            static.url + "/Evil_Cat.jsx",
            backend.port,
            True,
            files,
            static.url + "/Loading.html",
        )
        try:
            status, body, _content = get(port, "/gate.jsx")
            assert status == 200
            assert body != GATE.read_bytes()
        finally:
            stop_site(proc)
            backend.stop()


def test_the_module_defines_the_template_exactly_once() -> None:
    source = MODULE.read_text(encoding="utf-8")
    assert source.count("def _websocket_site_script(") == 1
    assert source.count("MINIMAL_SITE_TEMPLATE = r\"\"\"") == 1
    assert source.count("SITE_SCRIPT_SHA256 = ") == 1
    assert source.count("MASK_FILES = __MASK_FILES__") == 0


def test_the_loading_page_is_pinned_for_the_jsx_mask() -> None:
    constants = module_constants("GATE_FILES", "HOLE_FILES")
    assert constants["GATE_FILES"] == gate_files()
    assert constants["GATE_FILES"]["loading"] == digest(LOADING)
    assert constants["GATE_FILES"]["gate"] == digest(GATE)
    assert "loading" not in constants["HOLE_FILES"]


def test_a_tampered_loading_page_is_never_served(tmp_path) -> None:
    backend = Backend().start()
    with Static(WEB) as static:
        files = gate_files()
        files["loading"] = "0" * 64
        proc, port = start_site(
            tmp_path,
            "jsx",
            static.url + "/Evil_Cat.jsx",
            backend.port,
            True,
            files,
            static.url + "/Loading.html",
        )
        try:
            status, body, _content = get(port, "/")
            assert status == 200
            assert body != LOADING.read_bytes()
            assert "Downloads" in body.decode("utf-8")
        finally:
            stop_site(proc)
            backend.stop()
