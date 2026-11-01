from __future__ import annotations

import ast
import asyncio
import os
import re
import textwrap
import urllib.parse
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

    def info(self, text: str, *args: object) -> None:
        self.records.append(text % args if args else text)

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


def _compile(*names: str, **extra) -> dict:
    namespace: dict = {
        "os": os,
        "re": re,
        "asyncio": asyncio,
        "time": __import__("time"),
        "secrets": __import__("secrets"),
        "urllib": __import__("urllib"),
        "base64": __import__("base64"),
        "json": __import__("json"),
        "logger": _Logger(),
        "Tuple": tuple,
        "Dict": dict,
        "List": list,
        "HEAL_COOLDOWN": 300,
        "TUNNEL_PROBE_FAILS": 2,
        "TUNNEL_PROBE_INTERVAL": 120.0,
        "TUNNEL_PROBE_RETRY": 10.0,
        "TUNNEL_PROBE_TIMEOUT": 6.0,
        "_escape": lambda value: value,
        "_fmt_bytes": lambda value: str(value),
        "_guard_reason": lambda: "",
    }
    for name, body in _grab(*names).items():
        exec(body, namespace)
    namespace.update(extra)
    return namespace


class _Owner:
    def __init__(self) -> None:
        self._users = {}
        self._external_ip = "203.0.113.10"
        self._sub = {"port": 12345, "token": "tok", "prefix": "pre", "tunnel_host": "", "traffic": {}}
        self._tunnel_fails = {}
        self._tunnel_probe = {}
        self._heal_attempts = {}
        self._closing = False
        self.strings = {"tunnel_restarted": "tunnel {name} -> {host}"}
        self.saved = 0
        self.sent: list = []
        self.started: list = []
        self.stopped: list = []

    def _sub_save(self) -> None:
        self.saved += 1

    def _save_users(self) -> None:
        self.saved += 1

    def _sub_b64(self, raw: str) -> str:
        import base64

        return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")

    def _sub_primary(self) -> str:
        return "main"

    def _build_vless_link(self, user: dict) -> str:
        return f"vless://{user['uuid']}@host.example:443?type=ws#{user['name']}"

    def _unit_name(self, name: str, kind: str = "xray") -> str:
        suffix = {"cf": "-cf-tunnel", "site": "-site"}.get(kind, "")
        return f"{name}{suffix}.service"

    async def _unit_active(self, unit: str) -> bool:
        return True

    async def _stop_unit(self, unit: str) -> None:
        self.stopped.append(unit)

    async def _start_unit(self, unit: str):
        self.started.append(unit)
        return True, ""

    def _read_tunnel_host(self, path: str, start: int = 0) -> str:
        return ""

    async def _send_log(self, text: str, name: str = "") -> None:
        self.sent.append(text)

    async def _send_ws_link_to_log_topic(self, user: dict) -> None:
        self.sent.append("link")

    def _write_unit(self, unit, description, command, log_path) -> None:
        self.started.append(("write", unit))

    def _cloudflared_installed(self) -> bool:
        return True

    _cloudflared_path = "/usr/bin/cloudflared"
    _root = "/home/u/.xray_on_userbot/1"


def _bind(instance, namespace: dict, *names: str) -> None:
    for name in names:
        setattr(instance, name, namespace[name].__get__(instance, type(instance)))


def test_the_shared_subscription_lists_every_user():
    owner = _Owner()
    owner._users = {
        "main": {"uuid": "u1", "name": "main", "transport": "websocket", "port": 1},
        "second": {"uuid": "u2", "name": "second", "transport": "tcp", "port": 2},
        "proxy": {"uuid": "u3", "name": "proxy", "transport": "socks5", "port": 3},
        "third": {"uuid": "u4", "name": "third", "transport": "xhttp", "port": 4},
    }
    namespace = _compile("_sub_targets", "_sub_body", "_sub_link_host")
    _bind(owner, namespace, "_sub_targets", "_sub_body", "_sub_link_host")

    everything = owner._sub_body("")
    lines = everything.split("\n")
    assert len(lines) == 3, lines
    assert lines[0].endswith("#main")
    assert {line.rsplit("#", 1)[-1] for line in lines} == {"main", "second", "third"}

    assert owner._sub_body("second").endswith("#second")
    assert len(owner._sub_body("second").split("\n")) == 1


def test_an_https_tunnel_turns_the_subscription_link_secure():
    owner = _Owner()
    namespace = _compile("_sub_base_url", "_sub_url", "_sub_route", "_sub_prefix")
    _bind(owner, namespace, "_sub_base_url", "_sub_url", "_sub_route", "_sub_prefix")

    plain = owner._sub_url()
    assert plain.startswith("http://203.0.113.10:12345/")

    owner._sub["tunnel_host"] = "quiet-forest-123.trycloudflare.com"
    secure = owner._sub_url()
    assert secure.startswith("https://quiet-forest-123.trycloudflare.com/")
    assert ":12345" not in secure


def test_the_subscription_tunnel_is_started_and_stopped_with_the_server():
    source = _grab("_sub_enable", "_sub_disable", "_sub_autostart", "_stop_all_units")
    assert "_sub_tunnel_start" in source["_sub_enable"]
    assert "_sub_tunnel_stop" in source["_sub_disable"]
    assert "_sub_tunnel_start" in source["_sub_autostart"]
    assert "_sub_tunnel_stop" in source["_stop_all_units"]


def test_a_dead_tunnel_is_detected_and_rebuilt():
    owner = _Owner()
    owner._users = {
        "bob": {
            "transport": "websocket",
            "websocket_mode": "tls-fallback",
            "tunnel_host": "dead-host.trycloudflare.com",
            "path": "/xhttps",
        }
    }
    namespace = _compile(
        "_check_tunnel_health",
        "_host_alive",
        "_restart_tunnel",
    )
    _bind(owner, namespace, "_check_tunnel_health", "_host_alive", "_restart_tunnel")

    async def dead(host, path="/"):
        return False

    owner._host_alive = dead
    owner._stop_websocket_tunnel = lambda name: _noop()
    started = []

    async def start_tunnel(name, user_dir):
        started.append(name)
        owner._users[name]["tunnel_host"] = "fresh-host.trycloudflare.com"
        return True, ""

    owner._start_websocket_tunnel = start_tunnel

    assert asyncio.run(owner._check_tunnel_health("bob")) is True
    assert owner._tunnel_fails["bob"] == 1
    assert asyncio.run(owner._check_tunnel_health("bob")) is False

    assert asyncio.run(owner._restart_tunnel("bob")) is True
    assert started == ["bob"]
    assert owner._users["bob"]["tunnel_host"] == "fresh-host.trycloudflare.com"
    assert any("tunnel" in line.lower() for line in owner.sent)


async def _noop():
    return None


def test_a_healthy_tunnel_is_not_touched():
    owner = _Owner()
    owner._users = {"bob": {"transport": "websocket", "tunnel_host": "ok.trycloudflare.com"}}
    namespace = _compile("_check_tunnel_health", "_host_alive")
    _bind(owner, namespace, "_check_tunnel_health", "_host_alive")

    async def alive(host, path="/"):
        return True

    owner._host_alive = alive
    assert asyncio.run(owner._check_tunnel_health("bob")) is True
    assert owner._tunnel_fails == {}


def test_a_stale_unit_no_longer_blocks_a_new_user(tmp_path, monkeypatch):
    owner = _Owner()
    units = tmp_path / "systemd"
    units.mkdir()
    for suffix in ("", "-site", "-cf-tunnel"):
        (units / f"tesws{suffix}.service").write_text("[Unit]\n", encoding="utf-8")

    namespace = _compile("_cleanup_stale_units")
    owner._systemd_user_dir = str(units)
    owner._stop_unit = lambda unit: _stopped(owner, unit)
    owner._systemctl = lambda *args: _ok()
    _bind(owner, namespace, "_cleanup_stale_units")

    asyncio.run(owner._cleanup_stale_units("tesws"))
    left = sorted(item.name for item in units.iterdir())
    assert left == [], left
    assert owner.stopped == ["tesws.service", "tesws-site.service", "tesws-cf-tunnel.service"]


async def _stopped(owner, unit):
    owner.stopped.append(unit)


async def _ok():
    return True, ""


def test_a_used_name_is_still_reported():
    owner = _Owner()
    owner._users = {"tesws": {"port": 1}}
    namespace = _compile("_cleanup_stale_units")
    owner._systemd_user_dir = "/nonexistent"
    _bind(owner, namespace, "_cleanup_stale_units")
    asyncio.run(owner._cleanup_stale_units("tesws"))
    assert owner.stopped == []


def test_the_websocket_link_carries_the_cdn_friendly_parameters():
    class User:
        name = "testwshole"
        uuid = "71a4751e-141c-4009-a530-ed5003bf5602"
        port = 8443
        transport = "websocket"
        public_key = "pk"
        short_id = "sid"
        tunnel_host = "retailers-customize-thehun-reading.trycloudflare.com"
        path = "/xhttps"
        websocket_mode = "tls-fallback"

        def get(self, key, default=None):
            return getattr(self, key, default)

        def __getitem__(self, key):
            return getattr(self, key)

    owner = _Owner()
    owner._external_ip = "203.0.113.10"
    link = _compile("_build_vless_link")["_build_vless_link"].__get__(owner, type(owner))(User())

    parts = urllib.parse.urlsplit(link)
    assert parts.scheme == "vless"
    assert parts.hostname == "retailers-customize-thehun-reading.trycloudflare.com"
    assert parts.port == 443
    query = urllib.parse.parse_qs(parts.query)
    assert query["type"] == ["ws"]
    assert query["security"] == ["tls"]
    assert query["path"] == ["/xhttps"]
    assert query["host"] == ["retailers-customize-thehun-reading.trycloudflare.com"]
    assert query["sni"] == ["retailers-customize-thehun-reading.trycloudflare.com"]
    assert "alpn" not in query
    assert query["encryption"] == ["none"]
    assert link.endswith("#testwshole")


def test_the_card_shows_the_stack_state_and_a_refresh_button():
    source = _grab("_cb_user_menu")
    body = source["_cb_user_menu"]
    assert "ws_status" in body
    assert "btn_ws_refresh" in body
    assert "_cb_refresh_link" in body

    handler = _grab("_cb_refresh_link")["_cb_refresh_link"]
    assert "_host_alive" in handler
    assert "_restart_tunnel" in handler
    assert "_send_ws_link_to_log_topic" in handler


def test_the_subscription_menu_offers_the_https_tunnel():
    body = _grab("_cb_sub_menu")["_cb_sub_menu"]
    assert "btn_sub_tls" in body
    assert "sub_tls_on" in body and "sub_tls_off" in body

    handler = _grab("_cb_sub_tls")["_cb_sub_tls"]
    assert "_sub_tunnel_start" in handler

def test_the_link_file_is_rebuilt_for_every_retry():
    body = _grab("_send_ws_link_to_log_topic")["_send_ws_link_to_log_topic"]
    assert "payload = " in body
    assert "def link_file()" in body
    assert "link_file()" in body
    assert body.count("io.BytesIO(payload)") == 1


def test_the_websocket_inbound_stays_on_loopback():
    body = _grab("_build_config")["_build_config"]
    ws_branch = body.split("if transport == \"websocket\":", 1)[1].split("elif transport", 1)[0]
    assert '"listen"] = "127.0.0.1"' in ws_branch
    assert "fallbacks" not in ws_branch


def test_the_traffic_poll_is_quick_and_runs_on_demand():
    source = _grab("_sub_collect_traffic", "_sub_traffic_loop", "_refresh_traffic_now", "_cb_user_menu", "_cb_sub_menu")
    assert "SUB_POLL_INTERVAL = 10" in SOURCE
    assert "_unit_active" in source["_sub_collect_traffic"]
    assert "_sub_collect_traffic" in source["_refresh_traffic_now"]
    assert "_refresh_traffic_now" in source["_cb_user_menu"]
    assert "_refresh_traffic_now" in source["_cb_sub_menu"]


def test_the_ws_probe_reports_a_failure_instead_of_raising():
    owner = _Owner()
    namespace = _compile("_ws_probe")
    _bind(owner, namespace, "_ws_probe")
    ok, detail = asyncio.run(owner._ws_probe("127.0.0.1", 1, "/xhttps", tls=False, timeout=2.0))
    assert ok is False
    assert isinstance(detail, str) and detail


def test_the_ws_check_button_is_wired():
    body = _grab("_cb_user_menu")["_cb_user_menu"]
    assert "btn_ws_check" in body
    assert "_cb_ws_check" in body
    handler = _grab("_cb_ws_check", "_diagnose_websocket")
    assert "_diagnose_websocket" in handler["_cb_ws_check"]
    assert '"_unit_active"' in handler["_diagnose_websocket"] or "_unit_active" in handler["_diagnose_websocket"]
    assert "_ws_probe" in handler["_diagnose_websocket"]
