from __future__ import annotations

import ast
import asyncio
import os
import re
import stat
import textwrap
from pathlib import Path

import pytest


def _find_root() -> Path:
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        if (parent / "XRay.py").is_file():
            return parent
    raise RuntimeError("XRay.py is not reachable from the test file")


ROOT = _find_root()
VPN = ROOT / "XRay.py"
SOURCE = VPN.read_text(encoding="utf-8")
LINES = SOURCE.split("\n")


class _Logger:
    def __init__(self) -> None:
        self.records: list[str] = []

    def _put(self, text: str) -> None:
        self.records.append(text)

    def info(self, text: str, *args: object) -> None:
        self._put(text % args if args else text)

    warning = info
    error = info
    exception = info
    debug = info


def _grab(*names: str) -> dict[str, str]:
    found: dict[str, str] = {}
    for node in ast.walk(ast.parse(SOURCE)):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name in names:
            found[node.name] = textwrap.dedent("\n".join(LINES[node.lineno - 1:node.end_lineno]))
    assert set(found) == set(names), (sorted(found), sorted(names))
    return found


def _compile(*names: str, extra: dict | None = None) -> dict:
    namespace: dict = {
        "os": os,
        "sys": __import__("sys"),
        "re": re,
        "asyncio": asyncio,
        "time": __import__("time"),
        "socket": __import__("socket"),
        "stat": stat,
        "logger": _Logger(),
        "GATE_FILES": ("index.jsx",),
        "HOLE_FILES": ("index.html",),
        "UNLOAD_WAIT": 5.0,
        "Tuple": tuple,
        "Dict": dict,
        "List": list,
        "_guard_reason": lambda: "",
        "_escape": lambda value: value,
    }
    for name, body in _grab(*names).items():
        exec(body, namespace)
    namespace.update(extra or {})
    return namespace


def _bind(instance, namespace: dict, *names: str) -> None:
    for name in names:
        setattr(instance, name, namespace[name].__get__(instance, type(instance)))


class _Site:
    def __init__(self, root: Path) -> None:
        self._root = str(root)
        self._users = {"tesws": {"transport": "websocket", "port": 4444, "mask_site": "Evil Cat"}}
        self._mask_sites = {"Evil Cat": ("jsx", "https://example.org")}
        self._site_processes: dict = {}
        self._closing = False
        self.written: dict = {}
        self.units: list = []
        self.started: list = []
        self.saved = 0
        self.sequence = [(False, "site_start_failed")]

    def _unit_name(self, name: str, kind: str = "xray") -> str:
        return f"{name}-{kind}.service"

    async def _unit_active(self, unit: str) -> bool:
        return False

    def _find_free_loopback_port(self) -> int:
        return 4545

    def _websocket_site_script(self, path, port, site_port, kind, url, files) -> str:
        return "print('mask')\n"

    def _write_unit(self, unit, description, command, log_path) -> None:
        self.written[unit] = (description, command, log_path)

    async def _start_unit(self, unit: str):
        self.started.append(unit)
        return self.sequence.pop(0)

    async def _stop_unit(self, unit: str) -> None:
        self.started.append("stop:" + unit)

    def _save_users(self) -> None:
        self.saved += 1


@pytest.fixture()
def quiet_subprocess(monkeypatch):
    class Stub:
        returncode = 0

        async def communicate(self):
            return (b"", b"")

    async def fake(*args, **kwargs):
        return Stub()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake)
    return fake


def test_the_mask_site_is_written_into_a_fresh_user_directory(tmp_path, quiet_subprocess) -> None:
    site = _Site(tmp_path)
    user_dir = tmp_path / "users" / "tesws"
    assert not user_dir.exists()

    namespace = _compile("_write_private")
    _bind(site, namespace, "_write_private")
    _bind(site, _compile("_start_websocket_site", extra=namespace), "_start_websocket_site")

    ok, error = asyncio.run(site._start_websocket_site("tesws", str(user_dir)))

    assert ok is False
    assert error == "site_start_failed"
    script = user_dir / "websocket_site.py"
    assert script.is_file()
    assert script.read_text(encoding="utf-8") == "print('mask')\n"
    assert stat.S_IMODE(os.stat(script).st_mode) == 0o600
    assert stat.S_IMODE(os.stat(user_dir).st_mode) == 0o700
    assert site.units == []


def test_a_failed_directory_is_reported_and_not_raised(tmp_path, quiet_subprocess, monkeypatch) -> None:
    site = _Site(tmp_path)
    broken = tmp_path / "users" / "tesws"

    def denied(path, mode=0o700, exist_ok=False):
        raise PermissionError(13, "Permission denied", str(path))

    monkeypatch.setattr(os, "makedirs", denied)

    namespace = _compile("_write_private")
    _bind(site, namespace, "_write_private")
    _bind(site, _compile("_start_websocket_site", extra=namespace), "_start_websocket_site")

    ok, error = asyncio.run(site._start_websocket_site("tesws", str(broken)))
    assert ok is False
    assert error.startswith("user_dir_unavailable:")
    assert site.started == []


def test_a_write_failure_is_reported_and_not_raised(tmp_path, quiet_subprocess, monkeypatch) -> None:
    site = _Site(tmp_path)
    user_dir = tmp_path / "users" / "tesws"

    def denied(path, data):
        raise OSError(28, "No space left on device", str(path))

    namespace = _compile("_write_private")
    namespace["_write_private"] = denied
    _bind(site, namespace, "_write_private")
    _bind(site, _compile("_start_websocket_site", extra=namespace), "_start_websocket_site")

    ok, error = asyncio.run(site._start_websocket_site("tesws", str(user_dir)))
    assert ok is False
    assert error.startswith("site_script_write_failed:")
    assert site.started == []


class _Unload:
    def __init__(self, task) -> None:
        self._monitor_task = task
        self._closing = False
        self._sub = 0
        self._processes = {"a": "unit"}
        self._users = {"a": {"autostart": True}}
        self._tunnels = {}
        self._site_processes = {}
        self.saved = 0

    async def _sub_stop_task(self) -> None:
        self._sub += 1

    def _sub_stop_server(self) -> None:
        self._sub += 1

    async def _stop_user(self, name, reason="") -> None:
        self._processes.pop(name, None)

    async def _stop_websocket_tunnel(self, name) -> None:
        self._tunnels.pop(name, None)

    async def _stop_websocket_site(self, name) -> None:
        self._site_processes.pop(name, None)

    def _save_users(self) -> None:
        self.saved += 1


def test_a_crashed_monitor_does_not_break_the_unload() -> None:
    async def scenario():
        async def monitor():
            raise FileNotFoundError(2, "No such file or directory", "websocket_site.py")

        task = asyncio.create_task(monitor())
        await asyncio.sleep(0)
        instance = _Unload(task)
        _bind(
            instance,
            _compile("on_unload", "_stop_monitor", "_stop_all_units"),
            "on_unload",
            "_stop_monitor",
            "_stop_all_units",
        )
        await instance.on_unload()
        return instance, task

    instance, task = asyncio.run(scenario())
    assert instance._closing is True
    assert instance._monitor_task is None
    assert task.done()
    assert instance._sub == 2
    assert instance._users["a"]["resume_on_module_load"] is True
    assert instance.saved == 1


def test_a_hanging_monitor_does_not_block_the_unload(monkeypatch) -> None:
    async def scenario():
        async def monitor():
            while True:
                await asyncio.sleep(3600)

        task = asyncio.create_task(monitor())
        await asyncio.sleep(0)
        instance = _Unload(task)
        namespace = _compile(
            "on_unload", "_stop_monitor", "_stop_all_units", extra={"UNLOAD_WAIT": 0.05}
        )
        _bind(instance, namespace, "on_unload", "_stop_monitor", "_stop_all_units")
        await instance.on_unload()
        return instance

    instance = asyncio.run(scenario())
    assert instance._closing is True
    assert instance._monitor_task is None
    assert instance.saved == 1


def test_the_monitor_cycle_survives_an_error_and_stops_on_unload() -> None:
    monitor_source = _grab("_start_monitor", "_monitor_cycle")["_start_monitor"]
    assert "while not self._closing:" in monitor_source
    assert "await self._monitor_cycle()" in monitor_source
    assert "except Exception" in monitor_source

    cycle = _grab("_monitor_cycle")["_monitor_cycle"]
    assert "if self._closing or not self._xray_installed():" in cycle
    tail = cycle.split("if self._closing or not self._xray_installed():")[1]
    assert tail.lstrip().startswith("return"), tail[:80]
    assert cycle.count("if self._closing:") >= 2


def test_the_heal_path_does_nothing_while_the_module_is_closing() -> None:
    body = _grab("_heal_websocket_stack")["_heal_websocket_stack"]
    head = body.split("\n")[:4]
    assert any("self._closing" in line for line in head)

    class Idle:
        _closing = True
        _users = {"bob": {"transport": "websocket", "websocket_mode": "tls-fallback"}}
        calls: list = []

        def _unit_name(self, name, kind="xray"):
            return name

        async def _unit_active(self, unit):
            self.calls.append("active")
            return False

    instance = Idle()
    _bind(instance, _compile("_heal_websocket_stack"), "_heal_websocket_stack")
    asyncio.run(instance._heal_websocket_stack("bob"))
    assert instance.calls == []


def test_a_failing_cleanup_step_does_not_break_the_unload() -> None:
    async def scenario():
        async def monitor():
            return None

        task = asyncio.create_task(monitor())
        await asyncio.sleep(0)

        class Broken(_Unload):
            async def _stop_user(self, name, reason=""):
                raise FileNotFoundError(2, "No such file or directory", "websocket_site.py")

            async def _stop_websocket_tunnel(self, name):
                raise PermissionError(13, "Permission denied", "daemon.log")

            def _save_users(self):
                self.saved += 1

        instance = Broken(task)
        namespace = _compile("on_unload", "_stop_monitor", "_stop_all_units")
        _bind(instance, namespace, "on_unload", "_stop_monitor", "_stop_all_units")
        await instance.on_unload()
        return instance

    instance = asyncio.run(scenario())
    assert instance._closing is True
    assert instance.saved == 1


def test_the_mask_template_is_pulled_every_time_and_checked_locally() -> None:
    source = VPN.read_text(encoding="utf-8")
    fetch = _grab("_fetch_site_script")["_fetch_site_script"]
    assert "urlopen" in fetch
    assert "sha256" in fetch
    assert "cache" not in fetch.lower()
    assert "_WEBSOCKET_SITE_SCRIPT_URL" in fetch

    script = _grab("_websocket_site_script")["_websocket_site_script"]
    assert "for _ in range(2):" in script
    assert "self._fetch_site_script()" in script
    assert "MINIMAL_SITE_TEMPLATE" in script
    assert "self._site_template" not in source
