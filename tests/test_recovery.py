from __future__ import annotations

import ast
import asyncio
import os
import re
import stat
import textwrap
from pathlib import Path



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

    async def _stop_sub_tunnel(self, name) -> None:
        self._tunnels.pop(name, None)

    def _save_users(self) -> None:
        self.saved += 1


def test_a_crashed_monitor_does_not_break_the_unload() -> None:
    async def scenario():
        async def monitor():
            raise FileNotFoundError(2, "No such file or directory", "xray")

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


def test_a_failing_cleanup_step_does_not_break_the_unload() -> None:
    async def scenario():
        async def monitor():
            return None

        task = asyncio.create_task(monitor())
        await asyncio.sleep(0)

        class Broken(_Unload):
            async def _stop_user(self, name, reason=""):
                raise FileNotFoundError(2, "No such file or directory", "xray")

            async def _stop_sub_tunnel(self, name):
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

