from __future__ import annotations

import ast
import asyncio
import json
import os
import re
import textwrap
from pathlib import Path
from typing import Dict, List, Tuple

import pytest


def _find_root() -> Path:
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        if (parent / "XRay.py").is_file():
            return parent
    raise RuntimeError("XRay.py is not reachable from the test file")


ROOT = _find_root()
VPN = ROOT / "XRay.py"
WEB = ROOT / "WebDeployer.py"


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"))


def _source(path: Path, *names: str) -> list[str]:
    text = path.read_text(encoding="utf-8")
    lines = text.split("\n")
    found: list[str] = []
    for node in _tree(path).body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name in names:
            found.append(textwrap.dedent("\n".join(lines[node.lineno - 1:node.end_lineno])))
        if isinstance(node, ast.ClassDef):
            for item in node.body:
                if isinstance(item, ast.FunctionDef | ast.AsyncFunctionDef) and item.name in names:
                    found.append(textwrap.dedent("\n".join(lines[item.lineno - 1:item.end_lineno])))
    assert len(found) == len(names), (path.name, len(found), sorted(names))
    return found


def _namespace(path: Path, *names: str, **extra: object) -> dict[str, object]:
    namespace: dict[str, object] = {
        "os": os,
        "re": re,
        "json": json,
        "asyncio": asyncio,
        "time": __import__("time"),
        "logger": _Logger(),
        "HEAL_COOLDOWN": 300,
        "SITE_HEAL_COOLDOWN": 600,
        "_escape": lambda value: value,
        "Tuple": Tuple,
        "Dict": Dict,
        "List": List,
    }
    namespace.update(extra)
    exec("\n\n".join(_source(path, *names)), namespace)
    return namespace


class _Logger:
    def __init__(self) -> None:
        self.lines: list[str] = []

    def info(self, fmt: str, *args: object) -> None:
        self.lines.append(fmt % args)

    def warning(self, fmt: str, *args: object) -> None:
        self.lines.append(fmt % args)


class _Call:
    def __init__(self) -> None:
        self.edits: list[str] = []
        self.answers: list[str] = []

    async def edit(self, text: str, **kwargs: object) -> None:
        self.edits.append(text)

    async def answer(self, text: str, **kwargs: object) -> None:
        self.answers.append(text)


def test_the_command_decorator_stays_on_the_command() -> None:
    for path, expected in ((VPN, {"xr"}), (WEB, {"wd"})):
        decorated = set()
        for node in _tree(path).body:
            if not isinstance(node, ast.ClassDef):
                continue
            for item in node.body:
                if not isinstance(item, ast.FunctionDef | ast.AsyncFunctionDef):
                    continue
                for decorator in item.decorator_list:
                    target = getattr(decorator, "func", decorator)
                    name = getattr(target, "attr", getattr(target, "id", ""))
                    if name == "command":
                        decorated.add(item.name)
        assert decorated == expected, (path.name, decorated)
        assert "_guard_message" not in decorated, path.name
        assert "_guard_deny" not in decorated, path.name


def test_the_guard_helper_never_sits_between_a_decorator_and_a_function() -> None:
    for path in (VPN, WEB):
        text = path.read_text(encoding="utf-8")
        lines = text.split("\n")
        for index, line in enumerate(lines):
            if not line.strip().startswith("@loader.command"):
                continue
            probe = index
            while probe < len(lines) and (lines[probe].strip() == "" or lines[probe].startswith(" ")):
                if lines[probe].strip().startswith("async def ") or lines[probe].strip().startswith("def "):
                    break
                probe += 1
            assert lines[probe].strip().startswith(("async def ", "def ")), (path.name, lines[probe])
            assert "_guard" not in lines[probe], (path.name, lines[probe])


def test_the_tunnel_reader_takes_the_newest_host(tmp_path) -> None:
    log = tmp_path / "daemon.log"
    log.write_text(
        "INF Requesting new quick Tunnel on trycloudflare.com...\n"
        "INF +--------------------------------------------+\n"
        "INF |  https://old-name-abc.trycloudflare.com    |\n"
        "INF +--------------------------------------------+\n"
        "INF Registered tunnel connection connIndex=0\n"
        "INF |  https://new-name-xyz.trycloudflare.com    |\n",
        encoding="utf-8",
    )
    owner = type("Owner", (), {})()
    reader = _namespace(VPN, "_read_tunnel_host")["_read_tunnel_host"].__get__(owner, type(owner))
    assert reader(str(log)) == "new-name-xyz.trycloudflare.com"
    assert reader(str(log), os.path.getsize(log)) == ""
    assert reader(str(tmp_path / "missing.log")) == ""


def test_the_site_reader_returns_a_full_https_url(tmp_path) -> None:
    log = tmp_path / "cf.log"
    log.write_text(
        "2026-10-29 INF Thank you for trying Cloudflare Tunnel\n"
        "2026-10-29 INF |  https://first-one.trycloudflare.com  |\n"
        "2026-10-29 INF |  https://second-one.trycloudflare.com |\n",
        encoding="utf-8",
    )

    def tail(path: str, limit: int = 600) -> str:
        return Path(path).read_text(encoding="utf-8")[-limit:]

    owner = type("Owner", (), {})()
    owner._tail_log = tail
    reader = _namespace(WEB, "_read_site_host")["_read_site_host"].__get__(owner, type(owner))
    assert reader(str(log)) == "https://second-one.trycloudflare.com"
    assert reader("") == ""


def test_a_site_whose_link_moved_is_updated_and_announced() -> None:
    owner = type("Owner", (), {})()
    owner._sites = {"abc123": {"name": "index.html", "url": "https://old.trycloudflare.com", "cf_log": "/x"}}
    owner.strings = {"url_moved": "moved {name} {previous} {url}"}
    owner._notes = []

    def get_sites() -> dict:
        return owner._sites

    def set_sites(sites: dict) -> None:
        owner._sites = sites

    async def notify(text: str) -> None:
        owner._notes.append(text)

    namespace = _namespace(
        WEB,
        "_read_site_host",
        "_refresh_site_url",
        _escape=lambda value: value,
    )
    owner._tail_log = lambda path, limit=600: "  https://new.trycloudflare.com  "
    for name in ("_read_site_host", "_refresh_site_url"):
        setattr(owner, name, namespace[name].__get__(owner, type(owner)))
    owner._get_sites = get_sites
    owner._set_sites = set_sites
    owner._notify = notify

    assert asyncio.run(owner._refresh_site_url("abc123")) == "https://new.trycloudflare.com"
    assert owner._sites["abc123"]["url"] == "https://new.trycloudflare.com"
    assert owner._notes and "new.trycloudflare.com" in owner._notes[0]
    assert asyncio.run(owner._refresh_site_url("missing")) == ""


def test_a_dead_site_is_restarted_before_it_is_deleted() -> None:
    text = WEB.read_text(encoding="utf-8")
    body = text[text.index("async def _reattach_sites"):]
    body = body[:body.index("def _cf_installed")]
    assert body.index("_restart_site(site_id)") < body.index("_stop_unit(cf_unit)")
    assert "_refresh_site_url(site_id)" in body


def test_the_site_menu_offers_a_manual_refresh() -> None:
    text = WEB.read_text(encoding="utf-8")
    assert "async def _cb_refresh_site" in text
    assert '"callback": self._cb_refresh_site' in text
    assert 'self.strings["btn_refresh"]' in text
    detail = text[text.index("async def _cb_site_detail"):]
    detail = detail[:detail.index("async def _cb_refresh_site")]
    assert detail.index("_refresh_site_url(site_id)") < detail.index("call.edit(")


def test_a_moved_link_never_leaves_a_stale_button_behind() -> None:
    text = WEB.read_text(encoding="utf-8")
    for menu in ("async def _cb_sites_menu", "async def _cb_site_detail"):
        body = text[text.index(menu):]
        body = body[:body.index("async def ", 10)]
        assert "_refresh_site_url" in body or "site.get(\"url\"" in body, menu


def test_a_permanently_dead_site_is_announced_once_per_cooldown() -> None:
    owner = type("Owner", (), {})()
    owner._sites = {"abc123": {"name": "index.html", "http_unit": "a.service", "cf_unit": "b.service"}}
    owner._site_heal_attempts = {}
    owner._site_down_notified = set()
    owner.strings = {"site_down": "down {name}"}
    owner.notes = []
    owner.restarts = 0

    async def inactive(unit: str) -> bool:
        return False

    async def failing_restart(site_id: str) -> bool:
        owner.restarts += 1
        return False

    async def notify(text: str) -> None:
        owner.notes.append(text)

    namespace = _namespace(WEB, "_heal_sites")
    owner._heal_sites = namespace["_heal_sites"].__get__(owner, type(owner))
    owner._get_sites = lambda: owner._sites
    owner._unit_active = inactive
    owner._restart_site = failing_restart
    owner._notify = notify

    asyncio.run(owner._heal_sites())
    assert owner.restarts == 1 and owner.notes == ["down index.html"]
    for _ in range(4):
        asyncio.run(owner._heal_sites())
    assert owner.restarts == 1, owner.restarts
    assert owner.notes == ["down index.html"], owner.notes

    owner._site_heal_attempts = {"abc123": 0.0}
    asyncio.run(owner._heal_sites())
    assert owner.restarts == 2
    assert owner.notes == ["down index.html"], "a second window must not repeat the notice"

