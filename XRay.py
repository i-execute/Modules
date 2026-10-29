# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (5, 0, 0)
# meta developer: Execute_forge.t.me 
# meta banner: https://github.com/i-execute/Modules/raw/main/Storage/XRay/MetaBanner.jpeg

import os
import hashlib
import asyncio
import base64
import io
import logging
import signal
import socket
import threading
import time
import platform
import json
import subprocess
import shutil
import uuid
import sys
import ipaddress
import tempfile
import re
import random
import secrets
import string
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional, Dict, List, Tuple

from telethon.tl.functions.messages import EditMessageRequest, EditForumTopicRequest
from telethon.tl.types import InputMediaWebPage

from .. import loader, utils
from ..inline.types import InlineCall

ASSET_HOSTS = {"raw.githubusercontent.com", "github.com", "objects.githubusercontent.com"}
ASSET_BASE = os.environ.get("XRAY_ASSET_BASE", "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/XRay")
RELOADING_MEDIA_URL = ASSET_BASE.rstrip("/") + "/Reloading.jpeg"
GREETING_MEDIA_URL = ASSET_BASE.rstrip("/") + "/Greetings.jpeg"
HOLE_BASE = ASSET_BASE.rstrip("/") + "/WEB/hole"
HOLE_FILES = {
    "index.html": "895134c4fe5b25cf73ce6c12b778f9c27d08340dbee9d3cb0ada9e4c07926203",
    "assets/index-CwNTv7CE.js": "d855e2e471488439237763d5198644cbbf9c4a5c4b7a65675758e47fb80c42c3",
    "assets/index-KqaZQrrj.css": "b84a4219c8354281b395a9b9231c083b47644d04e2178326684f1bf35cb54043",
}
GATE_FILES = {
    "gate": "0a6da4aca63c23df57c159e3a46199e6ce2438a5fc226cbb7dfa9ded8d582162",
    "loading": "73b06164fd50869859495155f017a325cd35e111e0e0e05cd0fa045fc12fb85f",
}
SITE_SCRIPT_SHA256 = "9b973766aa8a3706e74ed635a43c2ab4b3b26f8fa26249b61d947d82ff6d0cb9"

MINIMAL_SITE_TEMPLATE = r"""import asyncio
from aiohttp import web, ClientSession, WSMsgType

PATH = "__PATH__"
BACKEND = "ws://127.0.0.1:__BACKEND_PORT__" + PATH
PAGE = (
    "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
    "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
    "<title>Downloads</title><style>html,body{margin:0;height:100%;background:#0b0d10;"
    "color:#e7e7e8;font:16px system-ui,sans-serif}main{height:100%;display:grid;place-items:center}"
    "b{font-weight:600;font-size:22px;letter-spacing:.4px}</style></head>"
    "<body><main><b>Downloads</b></main></body></html>"
)


async def index(request):
    return web.Response(text=PAGE, content_type="text/html", headers={"Cache-Control": "no-store"})


async def health(request):
    return web.Response(text="ok\n")


async def proxy(request):
    client_ws = web.WebSocketResponse(autoping=False, heartbeat=30)
    await client_ws.prepare(request)
    try:
        async with ClientSession() as session:
            async with session.ws_connect(BACKEND, autoping=False, heartbeat=30) as backend_ws:
                async def forward(source, target):
                    async for message in source:
                        if message.type == WSMsgType.BINARY:
                            await target.send_bytes(message.data)
                        elif message.type == WSMsgType.TEXT:
                            await target.send_str(message.data)
                        elif message.type == WSMsgType.PING:
                            await target.ping()
                        elif message.type == WSMsgType.PONG:
                            await target.pong()
                        elif message.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR):
                            break
                await asyncio.gather(forward(client_ws, backend_ws), forward(backend_ws, client_ws), return_exceptions=True)
    except Exception:
        if not client_ws.closed:
            await client_ws.close(code=1011, message=b"backend unavailable")
    return client_ws


app = web.Application()
app.router.add_get(PATH, proxy)
app.router.add_get("/", index)
app.router.add_get("/health", health)
web.run_app(app, host="127.0.0.1", port=__SITE_PORT__)
"""


logger = logging.getLogger(__name__)

async def _retry_topic_send(send, refresh):
    try:
        return await send()
    except Exception:
        await refresh()
        return await send()


class _SubServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 64
    allow_reuse_address = True

    def get_request(self):
        sock, address = super().get_request()
        sock.settimeout(SUB_SOCKET_TIMEOUT)
        return sock, address

    def process_request(self, request, client_address):
        if not SUB_SLOTS.acquire(blocking=False):
            _sub_security(client_address[0], "sub_busy", "deny", "too many connections")
            try:
                request.sendall(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            except OSError:
                pass
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except OSError:
            SUB_SLOTS.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            SUB_SLOTS.release()


LOG_TRIM_SIZE = 10 * 1024 * 1024
BASE_PORT = 8443
AUTOSTART_INTERVAL = 10
SUB_POLL_INTERVAL = 30
SUB_TOKEN_LEN = 43
SUB_RATE_LIMIT = 10
SUB_RATE_WINDOW = 60.0
SUB_BLOCK_STEP = 60.0
SUB_BLOCK_MAX = 3600.0
SUB_NOT_FOUND_STEP = 10
SUB_MAX_CLIENTS = 32
SUB_MAX_LINE = 2048
SUB_SOCKET_TIMEOUT = 10.0
SUB_BUCKET_LIMIT = 4096
GUARD_ROOT = "root"
GUARD_DOCKER = "docker"
XRAY_CORE_VERSION = "v26.10.21"
XRAY_CORE_SHA256 = {
    "64": "0eec3ed4163c273d8c8bf821f7d219e257921cf584aa3fb970715e458e631978",
    "arm64-v8a": "d1304946c486d98e6b886e9b931eaaaebb23835b30e217638037b7f2721d4e9c",
}
CLOUDFLARED_VERSION = "2026.10.5"
CLOUDFLARED_SHA256 = {
    "cloudflared-linux-amd64": "e1bc0cf084fd9e590554fe769a86b1db46701c3f847a43d902f7b974a4e16298",
    "cloudflared-linux-arm64": "72e0d05a67e09da87cf495e409a28c3cc5994dd23fc66a982eebea6a55923b7d",
}
RELEASE_HOSTS = ("github.com", "objects.githubusercontent.com", "release-assets.githubusercontent.com", "api.github.com")
SUB_BIND = os.environ.get("XRAY_SUB_BIND", "0.0.0.0")
SUB_SECLOG = os.environ.get("OBHOD_SECURITY_LOG", "/var/log/obhod/security.log")
SUB_SECLOG_ON = os.environ.get("OBHOD_SECLOG", "1").strip().lower() not in {"0", "no", "off", "false", "disabled"}
SUB_LOCK = threading.Lock()
SUB_BUCKETS = {}
SUB_LOG_SEEN = {}
SUB_LOG_GAP = 2.0
SUB_SLOTS = threading.BoundedSemaphore(SUB_MAX_CLIENTS)
SUB_OFF_TEXT = {"0", "no", "off", "false", "disabled", "none", ""}
SUB_XRAY_CALL_TIMEOUT = 8

def _escape(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def _strip_md(text: str) -> str:
    import re
    return re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'', text).strip()

def _gen_secret(length: int) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def _sub_flag(value, default: bool) -> bool:
    if value is None:
        return bool(default)
    if value is True:
        return True
    if value is False:
        return False
    return str(value).strip().lower() not in SUB_OFF_TEXT


def _sub_clean(value, limit: int) -> str:
    text = re.sub(r"[^ -~]", "", str(value or ""))
    return re.sub(r"\s+", "_", text.strip())[:limit]


def _sub_security(ip: str, event: str, outcome: str, detail: str = "") -> bool:
    if not SUB_SECLOG_ON:
        return False
    line = "security ip=%s event=%s outcome=%s actor=0 detail=%s\n" % (
        _sub_clean(ip, 64) or "-",
        _sub_clean(event, 32) or "-",
        _sub_clean(outcome, 16) or "-",
        _sub_clean(detail, 120) or "-",
    )
    now = time.monotonic()
    key = (ip, event, detail)
    with SUB_LOCK:
        last = SUB_LOG_SEEN.get(key)
        if last is not None and now - last < SUB_LOG_GAP:
            return False
        SUB_LOG_SEEN[key] = now
        if len(SUB_LOG_SEEN) > SUB_BUCKET_LIMIT:
            for stale in sorted(SUB_LOG_SEEN, key=SUB_LOG_SEEN.get)[: len(SUB_LOG_SEEN) - SUB_BUCKET_LIMIT]:
                SUB_LOG_SEEN.pop(stale, None)
    try:
        folder = os.path.dirname(SUB_SECLOG)
        if folder and not os.path.isdir(folder):
            os.makedirs(folder, mode=0o700, exist_ok=True)
        with SUB_LOCK:
            if os.path.islink(SUB_SECLOG):
                return False
            handle = os.open(SUB_SECLOG, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            try:
                os.write(handle, line.encode("utf-8", "replace"))
            finally:
                os.close(handle)
        return True
    except OSError:
        return False


class _SubBucket:
    __slots__ = ("window", "count", "seen", "blocked", "misses", "offences")

    def __init__(self, now: float):
        self.window = now
        self.count = 0
        self.seen = now
        self.blocked = 0.0
        self.misses = 0
        self.offences = 0


def _sub_entry(ip: str, now: float) -> "_SubBucket":
    entry = SUB_BUCKETS.get(ip)
    if entry is None:
        entry = _SubBucket(now)
        SUB_BUCKETS[ip] = entry
    entry.seen = now
    if now - entry.window >= SUB_RATE_WINDOW:
        entry.window = now
        entry.count = 0
    entry.count += 1
    return entry


def _sub_sweep(now: float) -> None:
    stale = [key for key, value in SUB_BUCKETS.items() if now - value.seen > SUB_RATE_WINDOW * 2]
    for key in stale:
        SUB_BUCKETS.pop(key, None)
    if len(SUB_BUCKETS) > SUB_BUCKET_LIMIT:
        ordered = sorted(SUB_BUCKETS.items(), key=lambda pair: pair[1].seen)
        for key, _value in ordered[: len(SUB_BUCKETS) - SUB_BUCKET_LIMIT]:
            SUB_BUCKETS.pop(key, None)


def _sub_allowed(ip: str) -> tuple:
    now = time.monotonic()
    reason = ""
    with SUB_LOCK:
        entry = _sub_entry(ip, now)
        if now < entry.blocked:
            _sub_sweep(now)
            return False, ""
        if entry.count > SUB_RATE_LIMIT:
            reason = "requests over the limit"
            entry.offences = min(8, entry.offences + 1)
            entry.blocked = now + min(SUB_BLOCK_MAX, SUB_BLOCK_STEP * (2 ** (entry.offences - 1)))
        elif entry.misses >= SUB_NOT_FOUND_STEP:
            reason = "repeated misses"
            entry.offences = min(8, entry.offences + 1)
            entry.blocked = now + min(SUB_BLOCK_MAX, SUB_BLOCK_STEP * (2 ** (entry.offences - 1)))
            entry.misses = 0
        _sub_sweep(now)
    return (not reason), reason


def _sub_record(ip: str, missing: bool) -> None:
    now = time.monotonic()
    with SUB_LOCK:
        entry = SUB_BUCKETS.get(ip)
        if entry is None:
            entry = _SubBucket(now)
            SUB_BUCKETS[ip] = entry
        entry.seen = now
        if missing:
            entry.misses += 1
        else:
            entry.misses = 0
            entry.offences = 0


def _fmt_bytes(value: int) -> str:
    try:
        size = float(value or 0)
    except (TypeError, ValueError):
        return "0 B"
    units = ("B", "KB", "MB", "GB", "TB", "PB")
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024
        idx += 1
    if idx == 0:
        return f"{int(size)} {units[idx]}"
    return f"{size:.2f} {units[idx]}"

def _guard_reason() -> str:
    if os.name == "posix" and os.geteuid() == 0:
        return GUARD_ROOT
    if _in_docker():
        return GUARD_DOCKER
    return ""


def _release_allowed(url: str) -> bool:
    try:
        parts = urllib.parse.urlsplit(str(url or ""))
    except ValueError:
        return False
    if parts.scheme != "https":
        return False
    return (parts.hostname or "").lower() in RELEASE_HOSTS


def _sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _curl_header_config(token: str) -> Tuple[List[str], str]:
    clean = str(token or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_.+=/-]{8,2048}", clean):
        return [], ""
    fd, path = tempfile.mkstemp(prefix="xr-curl-", suffix=".cfg")
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(f'header = "Authorization: Bearer {clean}"\n')
    return ["-K", path], path


def _drop_curl_config(path: str) -> None:
    if not path:
        return
    try:
        os.unlink(path)
    except OSError:
        pass


def _write_private(path: str, data: str) -> None:
    handle = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.fchmod(handle, 0o600)
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            stream.write(data)
    except Exception:
        try:
            os.close(handle)
        except OSError:
            pass
        raise


def _in_docker():
    if os.path.isfile("/.dockerenv"):
        return True
    try:
        with open("/proc/1/cgroup", "rt") as f:
            data = f.read()
        docker_markers = ("docker", "kubepods", "containerd", "podman", "lxc")
        if any(marker in data for marker in docker_markers):
            return True
    except Exception:
        pass
    try:
        with open("/proc/1/environ", "rb") as f:
            env = f.read()
        if b"container=" in env:
            return True
    except Exception:
        pass
    return False

@loader.tds
class XRay(loader.Module):
    """Multi-user VPN with VLESS+Reality (XHTTP/TCP(RAW)+Vision/WebSocket and post-quantum encryption)"""

    strings = {
        "name": "XRay",
        "tunnel_moved": "<b>The tunnel link changed</b>\n<blockquote>User: {name}\nBefore: <code>{previous}</code>\nNow: <code>{host}</code>\nThe subscription and the config were rebuilt with the new host.</blockquote>",
        "guard_title": "<b>XRay is disabled on this server</b>",
        "guard_root": "The userbot is running as root (uid 0). XRay refuses to start in this mode.",
        "guard_docker": "The userbot is running inside a container (Docker, Podman, LXC or Kubernetes). XRay refuses to start in this mode.",
        "guard_why": "Why: this module starts long living daemons and holds VPN keys. Under root a single bug is a full server takeover, and inside a container the listeners escape the isolation the container promises. A userbot is not a system service.",
        "guard_how": "What to do: run the userbot as a regular user with a working <code>systemctl --user</code> session, the module creates only user units. Keep root off limits and do not run it in Docker.",
        "reloaded": "<blockquote><b>XRay reloaded</b></blockquote>\n<blockquote>Version: <b>{version}</b>\nUsers: <b>{users}</b>\nSubscriptions: <b>{subscriptions}</b>\nLog file limit: <b>{log_size} MB</b></blockquote>",
        "greeting": "<blockquote><b>XRay is ready</b></blockquote>\n<blockquote>Version: <b>{version}</b>\nConfigured users: <b>{users}</b>\nSubscriptions: <b>{subscriptions}</b>\nLog file limit: <b>{log_size} MB</b>\nUse <code>{prefix}xray</code> to manage the service.</blockquote>",
        
        "main_menu": (
            "<b>XRay Multi-User VPN</b>\n"
            "<blockquote>Total users: {total}\n"
            "Active: {active}\n"
            "XRay version: {version}\n"
            "Cloudflared version: {cloudflared_version}</blockquote>"
        ),
        
        "setup_menu": (
            "<b>Setup & Installation</b>\n"
            "<blockquote>"
            "XRay Core: {xray_status}\n"
            "Cloudflared: {cloudflared_status}\n"
            "GitHub API: {gh_status}"
            "</blockquote>"
        ),
        
        "xray_install_menu": (
            "<b>XRay Core Installation</b>\n"
            "<blockquote>"
            "Current: {current}\n"
            "Select version to install:"
            "</blockquote>"
        ),
        
        "xray_installing": (
            "<b>Installing XRay</b>\n"
            "<blockquote>Version: {version}\nPlease wait...</blockquote>"
        ),
        
        "loading": "<b>Loading...</b>",
        
        "collecting_versions": "<b>Collecting versions...</b>",
        
        "user_item": "{status} {name} ({port})",
        
        "users_menu": (
            "<b>Users Management</b>\n"
            "<blockquote>Total: {total}\nActive: {active}</blockquote>"
        ),
        
        "user_menu": (
            "<b>User: {name}</b>\n"
            "<blockquote>"
            "Status: {status}\n"
            "Transport: {transport}\n"
            "TLS: {tls}\n"
            "Port: {port}\n"
            "Autostart: {autostart}\n"
            "Device limit: {limit}\n"
            "Active devices: {active}\n"
            "Uptime: {uptime}"
            "</blockquote>"
        ),
        "btn_mask_site": "Mask Site",
        "mask_site_menu": "<b>WebSocket Mask Site</b>\n<blockquote>Current: <code>{current}</code></blockquote>",
        
        "user_settings": (
            "<b>Settings: {name}</b>\n"
            "<blockquote>"
            "Transport: {transport}\n"
            "SNI: {sni}\n"
            "Dest: {dest}\n"
            "Path: {path}\n"
            "Padding: {padding}\n"
            "Fingerprint: {fp}\n"
            "Encryption: {encryption}\n"
            "Restart: {restart}\n"
            "Device limit: {limit}"
            "</blockquote>"
        ),
        
        "add_user_name": (
            "<b>Add New User</b>\n"
            "<blockquote>Enter username (alphanumeric, no spaces):</blockquote>"
        ),
        
        "add_user_transport": (
            "<b>Add User: {name}</b>\n"
            "<blockquote>Select transport type:</blockquote>"
        ),
        
        "add_user_limit": (
            "<b>Add User: {name}</b>\n"
            "<blockquote>Enter device limit (0 = unlimited):</blockquote>"
        ),
        
        "user_created": (
            "<b>User Created</b>\n"
            "<blockquote>"
            "Name: {name}\n"
            "Port: {port}\n"
            "Transport: {transport}\n"
            "Limit: {limit}"
            "</blockquote>"
        ),
        
        "user_deleted": (
            "<b>User Deleted</b>\n"
            "<blockquote>{name} removed</blockquote>"
        ),
        
        "user_started": "<b>Started</b>",
        "user_stopped": "<b>Stoped</b>",
        
        "link_message": (
            "<b>VLESS Link: {name}</b>\n"
            "<blockquote>"
            "Clients:\n"
            "iOS/Android: Happ, v2RayTun"
            "</blockquote>"
        ),
        "link_sent": (
            "<b>VLESS Link: {name}</b>\n"
            "<blockquote>"
            "Clients:\n"
            "iOS/Android: Happ, v2RayTun"
            "</blockquote>"
        ),
        "socks5_sent": (
            "<b>SOCKS5: {name}</b>\n"
            "<blockquote>"
            "Host: {ip}\n"
            "Port: {port}\n"
            "Login: {user}\n"
            "Password: {pass}\n\n"
            "URL: <code>socks5://{user}:{pass}@{ip}:{port}</code>"
            "</blockquote>"
        ),
        
        "padding_menu": (
            "<b>Padding Bytes: {name}</b>\n"
            "<blockquote>"
            "Current: {min}-{max}\n"
            "Set minimum and maximum values"
            "</blockquote>"
        ),
        
        "padding_set": (
            "<b>Padding Updated</b>\n"
            "<blockquote>{min}-{max} bytes</blockquote>"
        ),
        
        "sni_set": (
            "<b>SNI Updated</b>\n"
            "<blockquote>{sni}</blockquote>"
        ),
        
        "dest_set": (
            "<b>Dest Updated</b>\n"
            "<blockquote>{dest}</blockquote>"
        ),
        
        "path_set": (
            "<b>Path Updated</b>\n"
            "<blockquote>{path}</blockquote>"
        ),
        
        "fp_set": (
            "<b>Fingerprint Updated</b>\n"
            "<blockquote>{fp}</blockquote>"
        ),
        
        "limit_set": (
            "<b>Device Limit Updated</b>\n"
            "<blockquote>{limit}</blockquote>"
        ),
        
        "btn_setup": "Setup",
        "btn_users": "Users",
        "btn_add_user": "Add User",
        "btn_back": "Back",
        "btn_close": "Close",
        "btn_start": "Start",
        "btn_stop": "Stop",
        "btn_restart": "Restart",
        "btn_get_link": "Get Link",
        "btn_get_logs": "Get Logs",
        "btn_settings": "Settings",
        "btn_delete": "Delete User",
        "btn_xhttp": "XHTTP",
        "btn_raw": "RAW",
        "btn_websocket": "WebSocket",
        "btn_set_sni": "Set SNI",
        "btn_set_dest": "Set Dest",
        "btn_set_path": "Set Path",
        "btn_set_padding": "Padding",
        "btn_set_fp": "Fingerprint",
        "btn_set_limit": "Device Limit",
        "btn_encryption": "Encryption",
        "btn_toggle_transport": "Switch Transport",
        "btn_socks5": "SOCKS5",
        "btn_transport": "Transport",
        "btn_autostart_on": "Autostart: On",
        "btn_autostart_off": "Autostart: Off",
        "btn_chrome": "Chrome",
        "btn_firefox": "Firefox",
        "btn_safari": "Safari",
        "btn_install_xray": "Install XRay Core",
        "btn_reinstall_xray": "Reinstall XRay Core",
        "btn_install_cloudflared": "Install Cloudflared",
        "btn_reinstall_cloudflared": "Reinstall Cloudflared",
        
        "input_name": "Enter username:",
        "input_limit": "Enter device limit:",
        "input_sni": "Enter SNI (e.g. www.microsoft.com):",
        "input_dest": "Enter dest (e.g. www.microsoft.com:443):",
        "input_path": "Enter path (e.g. /xhttps):",
        "input_padding_min": "Enter minimum padding bytes:",
        "input_padding_max": "Enter maximum padding bytes:",
        
        "err_docker": (
            "<b>Docker Detected</b>\n"
            "<blockquote>Module cannot work in containers</blockquote>"
        ),
        
        "err_name_exists": (
            "<b>Error</b>\n"
            "<blockquote>Username already exists</blockquote>"
        ),
        
        "err_invalid_name": (
            "<b>Error</b>\n"
            "<blockquote>Invalid username format</blockquote>"
        ),
        
        "err_invalid_limit": (
            "<b>Error</b>\n"
            "<blockquote>Limit must be a number</blockquote>"
        ),
        
        "err_invalid_padding": (
            "<b>Error</b>\n"
            "<blockquote>Max must be greater than min</blockquote>"
        ),
        
        "err_port_busy": (
            "<b>Error</b>\n"
            "<blockquote>Port {port} is busy</blockquote>"
        ),
        
        "err_not_running": (
            "<b>Error</b>\n"
            "<blockquote>User is not running</blockquote>"
        ),
        
        "err_already_running": (
            "<b>Error</b>\n"
            "<blockquote>User is already running</blockquote>"
        ),
        
        "setup_done": (
            "<b>Installation Complete</b>\n"
            "<blockquote>XRay {version} installed successfully</blockquote>"
        ),
        
        "setup_fail": (
            "<b>Installation Failed</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        
        "device_limit_exceeded": (
            "<b>Device Limit Exceeded</b>\n"
            "<blockquote>"
            "User: {name}\n"
            "Limit: {limit}\n"
            "Active: {active}\n"
            "Process stopped\n"
            "Autostart disabled"
            "</blockquote>"
        ),

        "log_user_started": (
            "<pre><code class=\"language-Started\">"
            "----------------\n"
            "User:      {name}\n"
            "Port:      {port}\n"
            "Transport: {transport}\n"
            "Autostart: {autostart}"
            "</code></pre>"
        ),
        "log_user_stopped": (
            "<pre><code class=\"language-Stopped\">"
            "----------------\n"
            "User:      {name}\n"
            "Port:      {port}\n"
            "Transport: {transport}\n"
            "Reason:    {reason}"
            "</code></pre>"
        ),
        "log_device_limit": (
            "<pre><code class=\"language-device limit exceeded\">"
            "---------------------\n"
            "User:      {name}\n"
            "Port:      {port}\n"
            "Transport: {transport}\n"
            "Limit:     {limit}\n"
            "Active:    {active}\n"
            "Autostart: disabled"
            "</code></pre>"
        ),
        "log_user_deleted": (
            "<pre><code class=\"language-user deleted\">"
            "-----------------\n"
            "User:      {name}\n"
            "Port:      {port}\n"
            "Transport: {transport}"
            "</code></pre>"
        ),
        "log_reason_manual": "manual",
        "log_reason_restart": "restart",
        "log_reason_limit": "device limit",

        "status_online": "Online",
        "status_offline": "Offline",

        "gh_auth_pending": (
            "<b>GitHub Authorization</b>\n"
            "<blockquote>"
            "Open: {url}\n"
            "Enter code: <code>{code}</code>\n\n"
            "Waiting for confirmation..."
            "</blockquote>"
        ),
        "gh_auth_done": (
            "<b>GitHub Authorized</b>\n"
            "<blockquote>Token saved. Rate limit is now higher.</blockquote>"
        ),
        "gh_auth_fail": (
            "<b>GitHub Auth Failed</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "gh_auth_already": (
            "<b>GitHub Already Authorized</b>\n"
            "<blockquote>Token is active. To re-authorize, revoke it first.</blockquote>"
        ),
        "btn_gh_auth": "GitHub Auth",
        "btn_gh_revoke": "Revoke Token",

        "transport_menu": (
            "<b>Transport: {name}</b>\n"
            "<blockquote>Current: {current}\nSelect transport:</blockquote>"
        ),

        "transport_set": (
            "<b>Transport Updated</b>\n"
            "<blockquote>{transport}</blockquote>"
        ),

        "socks5_info": (
            "<b>SOCKS5: {name}</b>\n"
            "<blockquote>"
            "Host: {ip}\n"
            "Port: {port}\n"
            "Login: {user}\n"
            "Password: {pass}\n\n"
            "URL: <code>socks5://{user}:{pass}@{ip}:{port}</code>"
            "</blockquote>"
        ),

        "autostart_on": (
            "<b>Autostart Enabled</b>\n"
            "<blockquote>{name} will restart automatically if the process crashes</blockquote>"
        ),

        "autostart_off": (
            "<b>Autostart Disabled</b>\n"
            "<blockquote>{name} will need to be started manually</blockquote>"
        ),

        "sub_on": "enabled",
        "sub_off": "disabled",

        "btn_sub": "Subscriptions",
        "btn_sub_toggle": "Enable / Disable",
        "btn_sub_port": "Change port",
        "btn_sub_path": "Rotate path",
        "btn_sub_url": "Link",
        "btn_sub_users": "Tokens",
        "btn_sub_stats": "Stats API",
        "btn_sub_apply": "Rebuild configs",
        "btn_sub_reset": "Reset counters",
        "btn_sub_interval": "Interval",
        "btn_sub_title": "Title",
        "btn_sub_regen": "New token",

        "sub_menu": (
            "<b>Subscriptions</b>\n"
            "<blockquote>"
            "Status: {status}\n"
            "Port: {port}\n"
            "Path: <code>{path}</code>\n"
            "Connections: {links}\n"
            "Traffic: {traffic}\n"
            "Stats API: {stats}\n"
            "Interval: {interval} h\n"
            "Title: {title}"
            "</blockquote>"
        ),

        "sub_url_msg": (
            "<b>Subscription link</b>\n"
            "<blockquote>"
            "Shared, all connections:\n"
            "<code>{url}</code>\n\n"
            "JSON: <code>{url_json}</code>\n\n"
            "The link always reports unlimited traffic, "
            "traffic is only counted."
            "</blockquote>"
        ),

        "sub_users_menu": (
            "<b>Subscription tokens</b>\n"
            "<blockquote>"
            "A separate link per connection. "
            "SOCKS5 is not included."
            "</blockquote>"
        ),

        "sub_user_msg": (
            "<b>Subscription: {name}</b>\n"
            "<blockquote>"
            "Transport: {transport}\n"
            "Port: {port}\n"
            "Traffic: {traffic}\n\n"
            "<code>{url}</code>"
            "</blockquote>"
        ),

        "sub_empty": (
            "<b>Subscriptions</b>\n"
            "<blockquote>"
            "No connections to serve. "
            "SOCKS5 is not included."
            "</blockquote>"
        ),

        "sub_started": (
            "<b>Subscriptions enabled</b>\n"
            "<blockquote>"
            "Port: {port}\n"
            "Path: <code>{path}</code>\n\n"
            "{hint}"
            "</blockquote>"
        ),

        "sub_stopped": (
            "<b>Subscriptions disabled</b>\n"
            "<blockquote>Server stopped, port released.</blockquote>"
        ),

        "sub_reset_done": (
            "<b>Counters reset</b>\n"
            "<blockquote>Accumulated traffic cleared.</blockquote>"
        ),

        "sub_apply_done": (
            "<b>Configs rebuilt</b>\n"
            "<blockquote>"
            "Restarted: {restarted}\n"
            "Waiting: {pending}"
            "</blockquote>"
        ),

        "sub_apply_hint": (
            "The stats API was added to the configs. "
            "Press rebuild to apply it to running connections."
        ),

        "sub_docker_warn": (
            "\n\nDocker detected. The port is only reachable "
            "when it is published by the container."
        ),

        "sub_no_ip": (
            "<b>Error</b>\n"
            "<blockquote>Could not detect the external IP.</blockquote>"
        ),

        "sub_err_start": (
            "<b>Start error</b>\n"
            "<blockquote>{error}</blockquote>"
        ),

        "sub_input_interval": "Enter update interval in hours (1-168):",
        "sub_input_title": "Enter profile title:",

        "sub_interval_set": (
            "<b>Interval updated</b>\n"
            "<blockquote>{interval} h</blockquote>"
        ),

        "sub_title_set": (
            "<b>Title updated</b>\n"
            "<blockquote>{title}</blockquote>"
        ),

        "sub_regen_done": (
            "<b>Token rotated</b>\n"
            "<blockquote>The old link no longer works.</blockquote>"
        ),

        "sub_port_done": (
            "<b>Port updated</b>\n"
            "<blockquote>New port: {port}</blockquote>"
        ),

        "sub_path_done": (
            "<b>Path rotated</b>\n"
            "<blockquote><code>{path}</code></blockquote>"
        ),
    }

    strings_ru = {
        "name": "XRay",
        "tunnel_moved": "<b>Ссылка туннеля изменилась</b>\n<blockquote>Юзер: {name}\nБыло: <code>{previous}</code>\nСтало: <code>{host}</code>\nПодписка и конфиг пересобраны с новым хостом.</blockquote>",
        "guard_title": "<b>XRay отключён на этом сервере</b>",
        "guard_root": "Юзербот запущен от root (uid 0). XRay в таком режиме работать отказывается.",
        "guard_docker": "Юзербот запущен внутри контейнера (Docker, Podman, LXC или Kubernetes). XRay в таком режиме работать отказывается.",
        "guard_why": "Почему: модуль поднимает долгоживущие демоны и держит ключи VPN. Под root одна ошибка это полный захват сервера, а внутри контейнера слушающие порты выходят за границы обещанной изоляции. Юзербот не является системной службой.",
        "guard_how": "Что делать: запустите юзербот обычным пользователем с рабочей сессией <code>systemctl --user</code>, модуль создаёт только пользовательские юниты. Root не использовать, в Docker не запускать.",
        "reloaded": "<blockquote><b>XRay перезагружен</b></blockquote>\n<blockquote>Версия: <b>{version}</b>\nПользователей: <b>{users}</b>\nПодписок: <b>{subscriptions}</b>\nЛимит файла лога: <b>{log_size} MB</b></blockquote>",
        "greeting": "<blockquote><b>XRay готов к работе</b></blockquote>\n<blockquote>Версия: <b>{version}</b>\nНастроено пользователей: <b>{users}</b>\nПодписок: <b>{subscriptions}</b>\nЛимит файла лога: <b>{log_size} MB</b>\nДля управления используйте <code>{prefix}xray</code>.</blockquote>",
        "main_menu": (
            "<b>XRay Мультиюзерный VPN</b>\n"
            "<blockquote>Всего юзеров: {total}\n"
            "Активных: {active}\n"
            "Версия XRay: {version}\n"
            "Версия Cloudflared: {cloudflared_version}</blockquote>"
        ),
        
        "setup_menu": (
            "<b>Установка и настройка</b>\n"
            "<blockquote>"
            "XRay Core: {xray_status}\n"
            "Cloudflared: {cloudflared_status}\n"
            "GitHub API: {gh_status}"
            "</blockquote>"
        ),
        
        "xray_install_menu": (
            "<b>Установка XRay Core</b>\n"
            "<blockquote>"
            "Текущая: {current}\n"
            "Выберите версию для установки:"
            "</blockquote>"
        ),
        
        "xray_installing": (
            "<b>Установка XRay</b>\n"
            "<blockquote>Версия: {version}\nПодождите...</blockquote>"
        ),
        
        "loading": "<b>Загрузка...</b>",
        
        "collecting_versions": "<b>Сбор версий...</b>",
        
        "user_item": "{status} {name} ({port})",
        
        "users_menu": (
            "<b>Управление юзерами</b>\n"
            "<blockquote>Всего: {total}\nАктивных: {active}</blockquote>"
        ),
        
        "user_menu": (
            "<b>Юзер: {name}</b>\n"
            "<blockquote>"
            "Статус: {status}\n"
            "Транспорт: {transport}\n"
            "TLS: {tls}\n"
            "Порт: {port}\n"
            "Автозапуск: {autostart}\n"
            "Лимит устройств: {limit}\n"
            "Активных устройств: {active}\n"
            "Аптайм: {uptime}"
            "</blockquote>"
        ),
        "btn_mask_site": "Сайт-маска",
        "mask_site_menu": "<b>Сайт-маска WebSocket</b>\n<blockquote>Текущий: <code>{current}</code></blockquote>",
        
        "user_settings": (
            "<b>Настройки: {name}</b>\n"
            "<blockquote>"
            "Транспорт: {transport}\n"
            "SNI: {sni}\n"
            "Dest: {dest}\n"
            "Путь: {path}\n"
            "Padding: {padding}\n"
            "Fingerprint: {fp}\n"
            "Encryption: {encryption}\n"
            "Restart: {restart}\n"
            "Лимит: {limit}"
            "</blockquote>"
        ),
        
        "add_user_name": (
            "<b>Добавить юзера</b>\n"
            "<blockquote>Введите имя (латиница, без пробелов):</blockquote>"
        ),
        
        "add_user_transport": (
            "<b>Добавить: {name}</b>\n"
            "<blockquote>Выберите транспорт:</blockquote>"
        ),
        
        "add_user_limit": (
            "<b>Добавить: {name}</b>\n"
            "<blockquote>Лимит устройств (0 = безлимит):</blockquote>"
        ),
        
        "user_created": (
            "<b>Юзер создан</b>\n"
            "<blockquote>"
            "Имя: {name}\n"
            "Порт: {port}\n"
            "Транспорт: {transport}\n"
            "Лимит: {limit}"
            "</blockquote>"
        ),
        
        "user_deleted": (
            "<b>Юзер удалён</b>\n"
            "<blockquote>{name} удалён</blockquote>"
        ),
        
        "user_started": "<b>Started</b>",
        "user_stopped": "<b>Stoped</b>",
        
        "link_message": (
            "<b>VLESS ссылка: {name}</b>\n"
            "<blockquote>"
            "Клиенты:\n"
            "iOS/Android: Happ, v2RayTun"
            "</blockquote>"
        ),
        "link_sent": (
            "<b>VLESS ссылка: {name}</b>\n"
            "<blockquote>"
            "Клиенты:\n"
            "iOS/Android: Happ, v2RayTun"
            "</blockquote>"
        ),
        
        "padding_menu": (
            "<b>Padding Bytes: {name}</b>\n"
            "<blockquote>"
            "Текущий: {min}-{max}\n"
            "Установите мин. и макс. значения"
            "</blockquote>"
        ),
        
        "padding_set": (
            "<b>Padding обновлён</b>\n"
            "<blockquote>{min}-{max} байт</blockquote>"
        ),
        
        "sni_set": (
            "<b>SNI обновлён</b>\n"
            "<blockquote>{sni}</blockquote>"
        ),
        
        "dest_set": (
            "<b>Dest обновлён</b>\n"
            "<blockquote>{dest}</blockquote>"
        ),
        
        "path_set": (
            "<b>Путь обновлён</b>\n"
            "<blockquote>{path}</blockquote>"
        ),
        
        "fp_set": (
            "<b>Fingerprint обновлён</b>\n"
            "<blockquote>{fp}</blockquote>"
        ),
        
        "limit_set": (
            "<b>Лимит обновлён</b>\n"
            "<blockquote>{limit}</blockquote>"
        ),
        
        "btn_setup": "Настройка",
        "btn_users": "Юзеры",
        "btn_add_user": "Добавить юзера",
        "btn_back": "Назад",
        "btn_close": "Закрыть",
        "btn_start": "Запустить",
        "btn_stop": "Остановить",
        "btn_restart": "Перезапустить",
        "btn_get_link": "Получить ссылку",
        "btn_get_logs": "Получить логи",
        "btn_settings": "Настройки",
        "btn_delete": "Удалить",
        "btn_xhttp": "XHTTP",
        "btn_raw": "RAW",
        "btn_websocket": "WebSocket",
        "btn_set_sni": "SNI",
        "btn_set_dest": "Dest",
        "btn_set_path": "Путь",
        "btn_set_padding": "Padding",
        "btn_set_fp": "Fingerprint",
        "btn_set_limit": "Лимит",
        "btn_encryption": "Шифрование",
        "btn_toggle_transport": "Сменить транспорт",
        "btn_chrome": "Chrome",
        "btn_firefox": "Firefox",
        "btn_safari": "Safari",
        "btn_install_xray": "Установить XRay Core",
        "btn_reinstall_xray": "Переустановить XRay Core",
        "btn_install_cloudflared": "Установить Cloudflared",
        "btn_reinstall_cloudflared": "Переустановить Cloudflared",
        
        "input_name": "Введите имя:",
        "input_limit": "Введите лимит:",
        "input_sni": "Введите SNI (напр. www.microsoft.com):",
        "input_dest": "Введите dest (напр. www.microsoft.com:443):",
        "input_path": "Введите путь (напр. /xhttps):",
        "input_padding_min": "Минимальный padding (байты):",
        "input_padding_max": "Максимальный padding (байты):",
        
        "err_docker": (
            "<b>Обнаружен Docker</b>\n"
            "<blockquote>Модуль не работает в контейнерах</blockquote>"
        ),
        
        "err_name_exists": (
            "<b>Ошибка</b>\n"
            "<blockquote>Имя уже занято</blockquote>"
        ),
        
        "err_invalid_name": (
            "<b>Ошибка</b>\n"
            "<blockquote>Неверный формат имени</blockquote>"
        ),
        
        "err_invalid_limit": (
            "<b>Ошибка</b>\n"
            "<blockquote>Лимит должен быть числом</blockquote>"
        ),
        
        "err_invalid_padding": (
            "<b>Ошибка</b>\n"
            "<blockquote>Макс должен быть больше мин</blockquote>"
        ),
        
        "err_port_busy": (
            "<b>Ошибка</b>\n"
            "<blockquote>Порт {port} занят</blockquote>"
        ),
        
        "err_not_running": (
            "<b>Ошибка</b>\n"
            "<blockquote>Юзер не запущен</blockquote>"
        ),
        
        "err_already_running": (
            "<b>Ошибка</b>\n"
            "<blockquote>Юзер уже запущен</blockquote>"
        ),
        
        "setup_done": (
            "<b>Установка завершена</b>\n"
            "<blockquote>XRay {version} успешно установлен</blockquote>"
        ),
        
        "setup_fail": (
            "<b>Ошибка установки</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        
        "device_limit_exceeded": (
            "<b>Превышен лимит устройств</b>\n"
            "<blockquote>"
            "Юзер: {name}\n"
            "Лимит: {limit}\n"
            "Активных: {active}\n"
            "Процесс остановлен\n"
            "Автозапуск отключён"
            "</blockquote>"
        ),

        "log_user_started": (
            "<pre><code class=\"language-Started\">"
            "----------------\n"
            "User:      {name}\n"
            "Port:      {port}\n"
            "Transport: {transport}\n"
            "Autostart: {autostart}"
            "</code></pre>"
        ),
        "log_user_stopped": (
            "<pre><code class=\"language-Stopped\">"
            "----------------\n"
            "User:      {name}\n"
            "Port:      {port}\n"
            "Transport: {transport}\n"
            "Reason:    {reason}"
            "</code></pre>"
        ),
        "log_device_limit": (
            "<pre><code class=\"language-device limit exceeded\">"
            "---------------------\n"
            "User:      {name}\n"
            "Port:      {port}\n"
            "Transport: {transport}\n"
            "Limit:     {limit}\n"
            "Active:    {active}\n"
            "Autostart: disabled"
            "</code></pre>"
        ),
        "log_user_deleted": (
            "<pre><code class=\"language-user deleted\">"
            "-----------------\n"
            "User:      {name}\n"
            "Port:      {port}\n"
            "Transport: {transport}"
            "</code></pre>"
        ),
        "log_reason_manual": "manual",
        "log_reason_restart": "restart",
        "log_reason_limit": "device limit",

        "status_online": "Онлайн",
        "status_offline": "Офлайн",

        "gh_auth_pending": (
            "<b>Авторизация GitHub</b>\n"
            "<blockquote>"
            "Откройте: {url}\n"
            "Введите код: <code>{code}</code>\n\n"
            "Ожидание подтверждения..."
            "</blockquote>"
        ),
        "gh_auth_done": (
            "<b>GitHub авторизован</b>\n"
            "<blockquote>Токен сохранён. Лимит запросов повышен.</blockquote>"
        ),
        "gh_auth_fail": (
            "<b>Ошибка авторизации GitHub</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "gh_auth_already": (
            "<b>GitHub уже авторизован</b>\n"
            "<blockquote>Токен активен. Для повторной авторизации сначала отзовите его.</blockquote>"
        ),
        "btn_gh_auth": "Авторизация GitHub",
        "btn_gh_revoke": "Отозвать токен",
        
        "loading": "<b>Загрузка...</b>",
        "collecting_versions": "<b>Сбор версий...</b>",

        "btn_socks5": "SOCKS5",
        "btn_transport": "Транспорт",
        "btn_autostart_on": "Автозапуск: Вкл",
        "btn_autostart_off": "Автозапуск: Выкл",

        "transport_menu": (
            "<b>Транспорт: {name}</b>\n"
            "<blockquote>Текущий: {current}\nВыберите транспорт:</blockquote>"
        ),

        "transport_set": (
            "<b>Транспорт обновлён</b>\n"
            "<blockquote>{transport}</blockquote>"
        ),

        "socks5_info": (
            "<b>SOCKS5: {name}</b>\n"
            "<blockquote>"
            "Host: {ip}\n"
            "Port: {port}\n"
            "Логин: {user}\n"
            "Пароль: {pass}\n\n"
            "URL: <code>socks5://{user}:{pass}@{ip}:{port}</code>"
            "</blockquote>"
        ),

        "socks5_sent": (
            "<b>SOCKS5: {name}</b>\n"
            "<blockquote>"
            "Host: {ip}\n"
            "Port: {port}\n"
            "Логин: {user}\n"
            "Пароль: {pass}\n\n"
            "URL: <code>socks5://{user}:{pass}@{ip}:{port}</code>\n\n"
            "Файл proxies.txt отправлен в чат"
            "</blockquote>"
        ),

        "autostart_on": (
            "<b>Автозапуск включён</b>\n"
            "<blockquote>{name} будет подниматься автоматически, если процесс упадёт</blockquote>"
        ),

        "autostart_off": (
            "<b>Автозапуск выключен</b>\n"
            "<blockquote>{name} нужно будет запускать вручную</blockquote>"
        ),

        "sub_on": "включено",
        "sub_off": "выключено",

        "btn_sub": "Подписки",
        "btn_sub_toggle": "Включить / Выключить",
        "btn_sub_port": "Сменить порт",
        "btn_sub_path": "Обновить путь",
        "btn_sub_url": "Ссылка",
        "btn_sub_users": "Токены",
        "btn_sub_stats": "API статистики",
        "btn_sub_apply": "Пересобрать конфиги",
        "btn_sub_reset": "Сбросить счётчики",
        "btn_sub_interval": "Интервал",
        "btn_sub_title": "Название",
        "btn_sub_regen": "Новый токен",

        "sub_menu": (
            "<b>Подписки</b>\n"
            "<blockquote>"
            "Статус: {status}\n"
            "Порт: {port}\n"
            "Путь: <code>{path}</code>\n"
            "Подключений: {links}\n"
            "Трафик: {traffic}\n"
            "API статистики: {stats}\n"
            "Интервал: {interval} ч\n"
            "Название: {title}"
            "</blockquote>"
        ),

        "sub_url_msg": (
            "<b>Ссылка подписки</b>\n"
            "<blockquote>"
            "Общая, все подключения:\n"
            "<code>{url}</code>\n\n"
            "JSON: <code>{url_json}</code>\n\n"
            "Лимит в ссылке всегда безлимитный, "
            "трафик только учитывается."
            "</blockquote>"
        ),

        "sub_users_menu": (
            "<b>Токены подписки</b>\n"
            "<blockquote>"
            "Отдельная ссылка на каждое подключение. "
            "SOCKS5 в подписку не входит."
            "</blockquote>"
        ),

        "sub_user_msg": (
            "<b>Подписка: {name}</b>\n"
            "<blockquote>"
            "Транспорт: {transport}\n"
            "Порт: {port}\n"
            "Трафик: {traffic}\n\n"
            "<code>{url}</code>"
            "</blockquote>"
        ),

        "sub_empty": (
            "<b>Подписки</b>\n"
            "<blockquote>"
            "Нет подключений для выдачи. "
            "SOCKS5 в подписку не входит."
            "</blockquote>"
        ),

        "sub_started": (
            "<b>Подписки включены</b>\n"
            "<blockquote>"
            "Порт: {port}\n"
            "Путь: <code>{path}</code>\n\n"
            "{hint}"
            "</blockquote>"
        ),

        "sub_stopped": (
            "<b>Подписки выключены</b>\n"
            "<blockquote>Сервер остановлен, порт освобождён.</blockquote>"
        ),

        "sub_reset_done": (
            "<b>Счётчики сброшены</b>\n"
            "<blockquote>Накопленный трафик обнулён.</blockquote>"
        ),

        "sub_apply_done": (
            "<b>Конфиги пересобраны</b>\n"
            "<blockquote>"
            "Перезапущено: {restarted}\n"
            "Ожидают запуска: {pending}"
            "</blockquote>"
        ),

        "sub_apply_hint": (
            "API статистики добавлен в конфиги. "
            "Нажмите пересборку, чтобы применить к работающим подключениям."
        ),

        "sub_docker_warn": (
            "\n\nОбнаружен Docker. Порт доступен только "
            "если он проброшен контейнером."
        ),

        "sub_no_ip": (
            "<b>Ошибка</b>\n"
            "<blockquote>Не удалось определить внешний IP.</blockquote>"
        ),

        "sub_err_start": (
            "<b>Ошибка запуска</b>\n"
            "<blockquote>{error}</blockquote>"
        ),

        "sub_input_interval": "Введите интервал обновления в часах (1-168):",
        "sub_input_title": "Введите название профиля:",

        "sub_interval_set": (
            "<b>Интервал обновлён</b>\n"
            "<blockquote>{interval} ч</blockquote>"
        ),

        "sub_title_set": (
            "<b>Название обновлено</b>\n"
            "<blockquote>{title}</blockquote>"
        ),

        "sub_regen_done": (
            "<b>Токен обновлён</b>\n"
            "<blockquote>Старая ссылка больше не работает.</blockquote>"
        ),

        "sub_port_done": (
            "<b>Порт обновлён</b>\n"
            "<blockquote>Новый порт: {port}</blockquote>"
        ),

        "sub_path_done": (
            "<b>Путь обновлён</b>\n"
            "<blockquote><code>{path}</code></blockquote>"
        ),
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "MAX_LOG_FILE_SIZE", 50,
                "Maximum size of each Xray/daemon log file in megabytes",
                validator=loader.validators.Integer(minimum=11, maximum=2048),
            ),
        )
        self._root = None
        self._xray_path = None
        self._users: Dict[str, Dict] = {}
        self._processes: Dict[str, str] = {}
        self._monitor_task = None
        self._external_ip = ""
        self._link_cache: Dict[str, str] = {}
        self._tunnels: Dict[str, str] = {}
        self._site_processes: Dict[str, str] = {}
        self._mask_sites = {
            "Evil Cat": ("jsx", ASSET_BASE.rstrip("/") + "/WEB/Evil_Cat.jsx"),
            "Black Hole": ("hole", HOLE_BASE),
        }
        self._logger_topic = None
        self._asset_channel = None
        self._sub: Dict = {}
        self._sub_server = None
        self._sub_thread = None
        self._sub_task = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._me = await client.get_me()

        tg_user_id = self._me.id
        self._root = os.path.join(os.path.expanduser("~"), ".xray_on_userbot", str(tg_user_id))
        self._xray_path = os.path.join(self._root, "xray")
        self._cloudflared_path = os.path.join(self._root, "cloudflared")
        
        os.makedirs(self._root, mode=0o700, exist_ok=True)
        os.chmod(self._root, 0o700)
        os.makedirs(os.path.join(self._root, "users"), mode=0o700, exist_ok=True)
        os.chmod(os.path.join(self._root, "users"), 0o700)
        reason = _guard_reason()
        if reason:
            logger.warning("[XR] the module is disabled on this host, reason: %s", reason)

        self._users = self._db.get("XR", "users", {})
        self._external_ip = await self._detect_external_ip()
        self._asset_channel = self._db.get("heroku.forums", "channel_id", None)
        self._sub = self._db.get("XR", "sub", {}) or {}
        self._sub_ensure()

        if self._asset_channel:
            await self._ensure_logger_topic()

        if self._logger_topic and self._asset_channel:
            chat_id = int(f"-100{self._asset_channel}")
            greeting_key = f"xray_greeted_{self._asset_channel}_{self._logger_topic.id}"
            already_greeted = self.get(greeting_key, False)
            values = {
                "version": ".".join(map(str, __version__)),
                "users": len(self._users),
                "subscriptions": len(self._sub),
                "log_size": self.config["MAX_LOG_FILE_SIZE"],
                "prefix": self.get_prefix(),
            }
            if already_greeted:
                await self._send_with_preview(chat_id, self.strings["reloaded"].format(**values))
            else:
                self.set(greeting_key, True)
                await self._send_with_preview(
                    chat_id,
                    self.strings["greeting"].format(**values),
                    is_greeting=True,
                )
        
        if not self._xray_installed():
            logger.warning("[XR] XRay not installed")
        
        await self._reattach_processes()
        for name, user in self._users.items():
            if user.pop("resume_on_module_load", False) and not user.get("restart_required"):
                await self._start_user(name)
        for name, user in self._users.items():
            if (user.get("transport") != "websocket" or user.get("websocket_mode") != "tls-fallback"
                    or name not in self._processes):
                continue
            user_dir = os.path.join(self._root, "users", name)
            await self._stop_websocket_site(name)
            await self._stop_websocket_tunnel(name)
            ok, error = await self._start_websocket_site(name, user_dir)
            if ok:
                await self._stop_user(name, reason="restart")
                ok, error = await self._start_user(name)
            if not ok:
                logger.error(f"[XR] WebSocket recovery failed for {name}: {error}")
        self._start_monitor()
        await self._sub_autostart()
        self._sub_start_task()

    async def _send_with_preview(self, chat_id, text, is_greeting=False):
        try:
            msg_text, entities = await self.inline.bot._parse_message_text(text, "html")
            msg = await _retry_topic_send(
                lambda: self.inline.bot.send_message(
                    chat_id,
                    msg_text,
                    parse_mode=None,
                    entities=entities,
                    message_thread_id=self._logger_topic.id,
                ),
                self._ensure_logger_topic,
            )
            if msg:
                try:
                    peer = await self.inline.bot.get_input_entity(chat_id)
                    current_msg = await self.inline.bot.get_messages(chat_id, ids=msg.id)
                    reply_markup = current_msg.reply_markup if current_msg else None
                    await self.inline.bot(EditMessageRequest(
                        peer=peer,
                        id=msg.id,
                        message=msg_text,
                        media=InputMediaWebPage(
                            url=GREETING_MEDIA_URL if is_greeting else RELOADING_MEDIA_URL,
                            optional=True,
                            force_large_media=True,
                        ),
                        invert_media=True,
                        reply_markup=reply_markup,
                        entities=entities,
                        no_webpage=False,
                    ))
                except Exception as e:
                    logger.error(f"[XR] Failed to add preview: {e}")
        except Exception as e:
            logger.error(f"[XR] Failed to send message with preview: {e}")

    async def _ensure_logger_topic(self):
        if not self._asset_channel:
            return None
        try:
            topic = await utils.asset_forum_topic(
                self._client,
                self._db,
                self._asset_channel,
                "XRay",
                description="XRay users and device limit logs",
                icon_emoji_id=5386654279341747455,
            )
            if getattr(topic, "closed", False):
                try:
                    peer = await self._client.get_input_entity(self._asset_channel)
                    await self._client(EditForumTopicRequest(
                        peer=peer,
                        topic_id=topic.id,
                        closed=False,
                    ))
                except Exception as e:
                    logger.warning(f"[XR] Failed to reopen log topic: {e}")
            if getattr(topic, "icon_emoji_id", None) != 5386654279341747455:
                try:
                    peer = await self._client.get_input_entity(self._asset_channel)
                    await self._client(EditForumTopicRequest(
                        peer=peer,
                        topic_id=topic.id,
                        icon_emoji_id=5386654279341747455,
                    ))
                except Exception as e:
                    logger.warning(f"[XR] Failed to update log topic icon: {e}")
            self._logger_topic = topic
            return topic
        except Exception as e:
            logger.error(f"[XR] Failed to create/open forum topic: {e}")
            return None

    async def _send_file_to_call_chat(self, call: InlineCall, file, **kwargs):
        chat = call.form.get("chat")
        if chat is None:
            raise RuntimeError("inline form chat is unavailable")
        peer = await self._client.get_input_entity(chat)
        return await self._client.send_file(peer, file, **kwargs)

    async def on_unload(self):
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

        await self._sub_stop_task()
        self._sub_stop_server()
        
        for name in list(self._processes.keys()):
            if name in self._users:
                self._users[name]["resume_on_module_load"] = True
            await self._stop_user(name)
        for name in list(self._tunnels.keys()):
            await self._stop_websocket_tunnel(name)
        for name in list(self._site_processes.keys()):
            await self._stop_websocket_site(name)
        self._save_users()

    def _guard_message(self) -> str:
        reason = _guard_reason()
        if not reason:
            return ""
        head = self.strings["guard_title"] + "\n"
        body = self.strings["guard_root"] if reason == GUARD_ROOT else self.strings["guard_docker"]
        return head + body + "\n\n" + self.strings["guard_why"] + "\n\n" + self.strings["guard_how"]

    async def _guard_deny(self, message) -> bool:
        text = self._guard_message()
        if not text:
            return False
        await utils.answer(message, text)
        return True

    def _transport_label(self, transport: str) -> str:
        labels = {
            "tcp": "RAW",
            "xhttp": "XHTTP",
            "socks5": "SOCKS5",
            "websocket": "WebSocket",
        }
        return labels.get(transport, str(transport).upper())

    def _find_free_loopback_port(self) -> int:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]
        finally:
            sock.close()

    def _unit_name(self, name: str, kind: str = "xray") -> str:
        """Return a stable user-systemd unit name for a module user."""
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", name)
        return f"{safe}{'-cf-tunnel' if kind == 'cf' else '-site' if kind == 'site' else ''}.service"

    @property
    def _systemd_user_dir(self) -> str:
        return os.path.join(os.path.expanduser("~"), ".config", "systemd", "user")

    async def _systemctl(self, *args: str) -> Tuple[bool, str]:
        try:
            cmd = ["systemctl", "--user"]
            env = dict(os.environ)
            env.setdefault("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
            proc = await asyncio.create_subprocess_exec(
                *cmd, *args, env=env,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            out, err = await proc.communicate()
            return proc.returncode == 0, (out + err).decode(errors="replace").strip()
        except Exception as e:
            return False, str(e)

    def _log_limit_bytes(self) -> int:
        return int(self.config["MAX_LOG_FILE_SIZE"]) * 1024 * 1024

    def _trim_log(self, path: str):
        try:
            if os.path.getsize(path) < self._log_limit_bytes():
                return
            with open(path, "rb") as source:
                source.seek(LOG_TRIM_SIZE)
                remaining = source.read()
            with open(path, "wb") as target:
                target.write(remaining)
        except OSError:
            pass

    def _trim_user_logs(self, name: str):
        base = os.path.join(self._root, "users", name)
        for filename in ("start.log", "error.log", "access.log", "daemon.log"):
            self._trim_log(os.path.join(base, filename))

    def _write_unit(self, unit: str, description: str, command: List[str], log_path: str):
        os.makedirs(self._systemd_user_dir, mode=0o700, exist_ok=True)
        self._trim_log(log_path)
        quoted = " ".join(subprocess.list2cmdline([part]) for part in command)
        content = (
            "[Unit]\n"
            f"Description={description}\n"
            "After=network-online.target\nWants=network-online.target\n\n"
            "[Service]\nType=simple\n"
            f"WorkingDirectory={os.path.dirname(log_path)}\n"
            f"ExecStart={quoted}\n"
            "Restart=on-failure\nRestartSec=3\n"
            f"StandardOutput=append:{log_path}\nStandardError=append:{log_path}\n\n"
            "[Install]\nWantedBy=default.target\n"
        )
        _write_private(os.path.join(self._systemd_user_dir, unit), content)

    async def _unit_active(self, unit: str) -> bool:
        ok, out = await self._systemctl("is-active", "--quiet", unit)
        return ok and out == ""

    async def _start_unit(self, unit: str) -> Tuple[bool, str]:
        ok, output = await self._systemctl("daemon-reload")
        if not ok:
            return False, output
        return await self._systemctl("start", unit)

    async def _stop_unit(self, unit: str, disable: bool = False):
        await self._systemctl("stop", unit)
        if disable:
            await self._systemctl("disable", unit)

    async def _mark_restart_required(self, name: str):
        user = self._users.get(name)
        if not user:
            return
        if name in self._processes or await self._unit_active(self._unit_name(name)):
            await self._stop_user(name, reason="configuration changed")
        user["restart_required"] = True
        self._save_users()

    _WEBSOCKET_SITE_SCRIPT_URL = ASSET_BASE.rstrip("/") + "/WEB/websocket_site.py"

    def _fetch_site_script(self) -> str:
        parsed = urllib.parse.urlsplit(self._WEBSOCKET_SITE_SCRIPT_URL)
        if parsed.scheme != "https" or not parsed.hostname:
            return ""
        if parsed.hostname not in ASSET_HOSTS:
            return ""
        request = urllib.request.Request(self._WEBSOCKET_SITE_SCRIPT_URL, headers={"User-Agent": "curl/8.5.0"})
        with urllib.request.urlopen(request, timeout=15) as response:
            body = response.read(256 * 1024 + 1)
        if len(body) > 256 * 1024:
            return ""
        expected = os.environ.get("XRAY_SITE_SCRIPT_SHA256", "").strip().lower() or SITE_SCRIPT_SHA256
        got = hashlib.sha256(body).hexdigest()
        if got != expected:
            logger.warning("[XR] the site template digest is %s instead of %s, the minimal one is used", got, expected)
            return ""
        text = body.decode("utf-8")
        for token in ("__PATH__", "__SITE_PORT__", "__MASK_FILES__"):
            if token not in text:
                logger.warning("[XR] the site template has no %s placeholder, the minimal one is used", token)
                return ""
        return text

    def _websocket_site_script(
        self, path: str, backend_port: int, site_port: int, mask_kind: str, mask_url: str, mask_files: Dict[str, str]
    ) -> str:
        try:
            template = self._fetch_site_script()
            if not template:
                raise ValueError("no template")
        except Exception:
            template = MINIMAL_SITE_TEMPLATE
        return (
            template.replace("__PATH__", path)
            .replace("__BACKEND_PORT__", str(backend_port))
            .replace("__SITE_PORT__", str(site_port))
            .replace("__MASK_KIND__", mask_kind or "jsx")
            .replace("__MASK_URL__", mask_url)
            .replace("__MASK_FILES__", repr(dict(mask_files or {})))
            .replace("__LOADING_URL__", ASSET_BASE.rstrip("/") + "/WEB/Loading.html")
        )

    async def _start_websocket_site(self, name: str, user_dir: str) -> Tuple[bool, str]:
        reason = _guard_reason()
        if reason:
            return False, reason
        unit = self._unit_name(name, "site")
        if await self._unit_active(unit):
            return True, ""
        self._site_processes.pop(name, None)
        user = self._users[name]
        site_port = self._find_free_loopback_port()
        path = user.get("path") or "/xhttps"
        if not path.startswith("/"):
            path = "/" + path
        user["path"] = path
        user["site_port"] = site_port
        mask_name = user.get("mask_site", "Evil Cat")
        if mask_name not in self._mask_sites:
            mask_name = "Evil Cat"
        mask_kind, mask_url = self._mask_sites[mask_name]
        user["mask_site"] = mask_name
        mask_files = GATE_FILES if mask_kind == "jsx" else HOLE_FILES
        script_path = os.path.join(user_dir, "websocket_site.py")
        try:
            p = await asyncio.create_subprocess_exec(
                sys.executable, "-c", "import aiohttp",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await p.communicate()
            if p.returncode != 0:
                p2 = await asyncio.create_subprocess_exec(
                    sys.executable, "-m", "pip", "install", "--quiet", "aiohttp",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                await p2.communicate()
        except Exception:
            pass
        try:
            script_content = self._websocket_site_script(path, user["port"], site_port, mask_kind, mask_url, mask_files)
        except Exception as e:
            return False, f"site_script_fetch_failed: {e}"
        with open(script_path, "w", encoding="utf-8") as f:
            f.write(script_content)
        log_path = os.path.join(user_dir, "daemon.log")
        try:
            self._write_unit(unit, f"XRay WebSocket site for {name}", [sys.executable, script_path], log_path)
            ok, output = await self._start_unit(unit)
            if not ok:
                return False, output
            deadline = time.time() + 10
            while time.time() < deadline:
                await asyncio.sleep(0.25)
                if not await self._unit_active(unit):
                    tail = open(log_path, "r", errors="replace").read()[-500:] if os.path.exists(log_path) else ""
                    return False, tail or "site_start_failed"
                try:
                    with socket.create_connection(("127.0.0.1", site_port), timeout=0.5):
                        break
                except OSError:
                    continue
            else:
                await self._stop_unit(unit)
                return False, "site_healthcheck_timeout"
            self._site_processes[name] = unit
            self._save_users()
            return True, ""
        except Exception as e:
            return False, str(e)

    async def _stop_websocket_site(self, name: str):
        self._site_processes.pop(name, None)
        await self._stop_unit(self._unit_name(name, "site"))

    async def _start_websocket_tunnel(self, name: str, user_dir: str) -> Tuple[bool, str]:
        reason = _guard_reason()
        if reason:
            return False, reason
        unit = self._unit_name(name, "cf")
        if await self._unit_active(unit):
            return True, ""
        self._tunnels.pop(name, None)
        user = self._users[name]
        site_port = user.get("site_port")
        if not site_port:
            return False, "site_not_running"
        cloudflared = self._cloudflared_path
        if not self._cloudflared_installed():
            return False, "cloudflared_not_installed"
        log_path = os.path.join(user_dir, "daemon.log")
        start_size = os.path.getsize(log_path) if os.path.exists(log_path) else 0
        try:
            self._write_unit(unit, f"XRay CF Tunnel for {name}", [cloudflared, "tunnel", "--protocol", "http2", "--url", f"http://127.0.0.1:{site_port}", "--no-autoupdate"], log_path)
            ok, output = await self._start_unit(unit)
            if not ok:
                return False, output
            deadline = time.time() + 40
            hostname = ""
            while time.time() < deadline:
                await asyncio.sleep(1)
                try:
                    with open(log_path, "r", errors="replace") as f:
                        f.seek(start_size)
                        text = f.read()
                except OSError:
                    text = ""
                matches = re.findall(r"https://([a-z0-9-]+\.trycloudflare\.com)", text, re.I)
                if matches:
                    hostname = matches[-1]
                    break
                if not await self._unit_active(unit):
                    break
            if not hostname:
                await self._stop_unit(unit)
                return False, "cloudflared_tunnel_failed"
            self._tunnels[name] = unit
            user["tunnel_host"] = hostname
            self._save_users()
            await self._send_ws_link_to_log_topic(user)
            return True, ""
        except Exception as e:
            return False, str(e)

    def _read_tunnel_host(self, log_path: str, start_size: int = 0) -> str:
        try:
            with open(log_path, "r", errors="replace") as f:
                if start_size:
                    f.seek(start_size)
                text = f.read()
        except OSError:
            return ""
        matches = re.findall(r"https://([a-z0-9-]+\.trycloudflare\.com)", text, re.I)
        return matches[-1] if matches else ""

    async def _refresh_tunnel_host(self, name: str) -> str:
        user = self._users.get(name)
        if not user or user.get("transport") != "websocket":
            return ""
        user_dir = os.path.join(self._root, "users", name)
        host = self._read_tunnel_host(os.path.join(user_dir, "daemon.log"))
        if not host or host == user.get("tunnel_host"):
            return ""
        previous = user.get("tunnel_host") or ""
        user["tunnel_host"] = host
        self._save_users()
        logger.info("[XR] the tunnel of %s moved from %s to %s", name, previous or "none", host)
        await self._send_log(
            self.strings["tunnel_moved"].format(
                name=_escape(name),
                previous=_escape(previous or "none"),
                host=_escape(host),
            ),
            name=name,
        )
        await self._send_ws_link_to_log_topic(user)
        return host

    async def _heal_websocket_stack(self, name: str) -> None:
        user = self._users.get(name)
        if not user or user.get("transport") != "websocket":
            return
        if user.get("websocket_mode") != "tls-fallback":
            return
        user_dir = os.path.join(self._root, "users", name)
        site_unit = self._unit_name(name, "site")
        cf_unit = self._unit_name(name, "cf")
        if not await self._unit_active(site_unit):
            ok, error = await self._start_websocket_site(name, user_dir)
            if not ok:
                logger.warning("[XR] the mask site of %s did not come back: %s", name, error)
                return
        if not await self._unit_active(cf_unit):
            ok, error = await self._start_websocket_tunnel(name, user_dir)
            if not ok:
                logger.warning("[XR] the tunnel of %s did not come back: %s", name, error)
                return
        await self._refresh_tunnel_host(name)

    async def _stop_websocket_tunnel(self, name: str):
        self._tunnels.pop(name, None)
        await self._stop_unit(self._unit_name(name, "cf"))

    async def _send_ws_link_to_log_topic(self, user: Dict):
        if not self._logger_topic or not self._asset_channel:
            return
        link = self._build_vless_link(user)
        if not link:
            return
        name = str(user.get("name", "user"))
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", name)
        try:
            link_file = io.BytesIO((link + "\n").encode("utf-8"))
            link_file.name = f"link_for_{safe_name}.txt"
            await _retry_topic_send(
                lambda: self.inline.bot.send_file(
                    int(f"-100{self._asset_channel}"),
                    link_file,
                    caption=(
                        f"<b>WebSocket TLS link refreshed</b>\n"
                        f"<blockquote>User: <code>{_escape(name)}</code>\n"
                        f"TLS: <code>{_escape(user.get('tunnel_host', '?'))}</code></blockquote>"
                    ),
                    parse_mode="html",
                    message_thread_id=self._logger_topic.id,
                    force_document=True,
                ),
                self._ensure_logger_topic,
            )
        except Exception as e:
            logger.error(f"[XR] Failed to send WebSocket link file: {e}")

    @staticmethod
    def _html_to_plain(text: str) -> str:
        """Strip HTML tags and unescape entities for plain-text log files."""
        text = re.sub(r"<[^>]+>", "", text)
        text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", '"')
        return text.strip()

    async def _send_log(self, text: str, name: str = None):
        """Send log to Telegram forum topic and save to file."""
        if name:
            user_dir = os.path.join(self._root, "users", name)
            os.makedirs(user_dir, exist_ok=True)
            log_file = os.path.join(user_dir, "daemon.log")
            try:
                plain = self._html_to_plain(text)
                ts = datetime.utcfromtimestamp(int(time.time())).strftime("%Y-%m-%d %H:%M:%S")
                with open(log_file, "a") as f:
                    f.write(f"[{ts}] {plain}\n")
            except Exception as e:
                logger.error(f"[XR] Failed to write daemon log for {name}: {e}")
        
        if not self._logger_topic or not self._asset_channel:
            return
        chat_id = int(f"-100{self._asset_channel}")
        try:
            bot = getattr(self.inline, "bot", None)
            if bot is None:
                raise RuntimeError("inline bot not ready")
            await _retry_topic_send(
                lambda: bot.send_message(
                    chat_id,
                    text,
                    parse_mode="html",
                    message_thread_id=self._logger_topic.id,
                    link_preview=False,
                ),
                self._ensure_logger_topic,
            )
        except Exception as e:
            logger.error(f"[XR] Failed to send log to Telegram: {e}")

    def _log_user_data(self, user: Dict) -> Dict:
        return {
            "name": _escape(user.get("name", "unknown")),
            "port": user.get("port", "n/a"),
            "transport": self._transport_label(user.get("transport", "unknown")),
            "autostart": "on" if user.get("autostart") else "off",
        }

    async def _reattach_processes(self):
        for name, user in self._users.items():
            unit = self._unit_name(name)
            if await self._unit_active(unit):
                self._processes[name] = unit
            if user.get("transport") == "websocket":
                if await self._unit_active(self._unit_name(name, "site")):
                    self._site_processes[name] = self._unit_name(name, "site")
                if await self._unit_active(self._unit_name(name, "cf")):
                    self._tunnels[name] = self._unit_name(name, "cf")
                    await self._refresh_tunnel_host(name)

    async def _is_user_running(self, name: str) -> bool:
        if name in self._processes:
            return True
        unit = self._unit_name(name)
        return await self._unit_active(unit)

    async def _get_active_count(self) -> int:
        count = 0
        for name in self._users:
            if await self._is_user_running(name):
                count += 1
        return count

    async def _get_user_uptime_async(self, name: str) -> str:
        user = self._users.get(name)
        if not user:
            return "offline"
        if not await self._is_user_running(name):
            return "offline"
        start = user.get("start_time", 0)
        if start == 0:
            return "n/a"
        elapsed = int(time.time() - start)
        d, rem = divmod(elapsed, 86400)
        h, rem = divmod(rem, 3600)
        m, s = divmod(rem, 60)
        parts = []
        if d:
            parts.append(f"{d}d")
        if h:
            parts.append(f"{h}h")
        if m:
            parts.append(f"{m}m")
        parts.append(f"{s}s")
        return " ".join(parts)

    def _cloudflared_installed(self) -> bool:
        return (
            getattr(self, "_cloudflared_path", None)
            and os.path.isfile(self._cloudflared_path)
            and os.access(self._cloudflared_path, os.X_OK)
        )

    async def _install_cloudflared(self) -> Tuple[bool, str]:
        reason = _guard_reason()
        if reason:
            return False, reason
        arch = platform.machine().lower()
        asset = {
            "x86_64": "cloudflared-linux-amd64",
            "amd64": "cloudflared-linux-amd64",
            "aarch64": "cloudflared-linux-arm64",
            "arm64": "cloudflared-linux-arm64",
        }.get(arch)
        if not asset:
            return False, f"Unsupported arch: {arch}"
        expected = CLOUDFLARED_SHA256.get(asset, "")
        if not expected:
            return False, f"No pinned digest for {asset}"
        tmp_path = f"{self._cloudflared_path}.tmp"
        try:
            process = await asyncio.create_subprocess_exec(
                "curl", "-fL", "--max-time", "120", "-o", tmp_path,
                f"https://github.com/cloudflare/cloudflared/releases/download/{CLOUDFLARED_VERSION}/{asset}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, err = await process.communicate()
            if process.returncode != 0 or not os.path.isfile(tmp_path):
                return False, err.decode(errors="replace")[:200] or "Download failed"
            got = _sha256_file(tmp_path)
            if got != expected:
                os.unlink(tmp_path)
                return False, f"Digest mismatch for {asset}: {got}"
            os.chmod(tmp_path, 0o755)
            check = await asyncio.create_subprocess_exec(
                tmp_path, "--version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await check.communicate()
            if check.returncode != 0:
                return False, "Cloudflared validation failed"
            os.replace(tmp_path, self._cloudflared_path)
            return True, await self._get_version("cloudflared")
        except Exception as e:
            return False, str(e)
        finally:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

    def _xray_installed(self) -> bool:
        return (
            self._xray_path
            and os.path.isfile(self._xray_path)
            and os.access(self._xray_path, os.X_OK)
        )

    async def _detect_external_ip(self) -> str:
        for svc in [
            "https://api.ipify.org",
            "https://ifconfig.me/ip",
            "https://icanhazip.com",
        ]:
            for tool in [
                ["curl", "-4", "-s", "--max-time", "5", svc],
                ["wget", "-qO-", "--timeout=5", svc],
            ]:
                try:
                    p = await asyncio.create_subprocess_exec(
                        *tool,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    out, _ = await p.communicate()
                    if p.returncode == 0:
                        ip = out.decode().strip()
                        try:
                            ipaddress.IPv4Address(ip)
                            return ip
                        except:
                            continue
                except FileNotFoundError:
                    continue
                except Exception:
                    continue
        return ""

    def _gh_token(self) -> str:
        return self._db.get("XR", "gh_token", "")

    async def _gh_get_releases(self) -> List[Dict]:
        gh_token = self._gh_token()
        curl_cmd = ["curl", "-sL", "--max-time", "15"]
        extra, cfg = _curl_header_config(gh_token)
        curl_cmd += extra
        curl_cmd.append("https://api.github.com/repos/XTLS/Xray-core/releases")
        
        try:
            p = await asyncio.create_subprocess_exec(
                *curl_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await p.communicate()
        finally:
            _drop_curl_config(cfg)
        
        if p.returncode != 0:
            return []
        
        try:
            data = json.loads(out.decode())
            if isinstance(data, list):
                return data[:5]
        except:
            pass
        
        return []

    async def _gh_device_flow(self, call: InlineCall):
        token = self._gh_token()
        if token:
            await call.edit(
                self.strings["gh_auth_already"],
                reply_markup=[[
                    {"text": self.strings["btn_gh_revoke"], "callback": self._cb_gh_revoke, "style": "danger"},
                    {"text": self.strings["btn_back"], "callback": self._cb_setup_menu, "style": "primary"},
                ]]
            )
            return

        client_id = "178c6fc778ccc68e1d6a"

        p = await asyncio.create_subprocess_exec(
            "curl", "-sX", "POST",
            "https://github.com/login/device/code",
            "-H", "Accept: application/json",
            "-H", "Content-Type: application/x-www-form-urlencoded",
            "-d", f"client_id={client_id}&scope=repo",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await p.communicate()

        if p.returncode != 0:
            await call.edit(
                self.strings["gh_auth_fail"].format(error=f"curl error: {err.decode()[:200]}"),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_setup_menu, "style": "primary"}]]
            )
            return

        try:
            data = json.loads(out.decode())
        except Exception as e:
            await call.edit(
                self.strings["gh_auth_fail"].format(error=f"JSON parse error: {str(e)[:200]}"),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_setup_menu, "style": "primary"}]]
            )
            return

        if "error" in data:
            await call.edit(
                self.strings["gh_auth_fail"].format(error=str(data)[:200]),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_setup_menu, "style": "primary"}]]
            )
            return

        device_code = data.get("device_code")
        user_code = data.get("user_code")
        verification_uri = data.get("verification_uri", "https://github.com/login/device")
        interval = int(data.get("interval", 5))
        expires_in = int(data.get("expires_in", 900))

        if not device_code or not user_code:
            await call.edit(
                self.strings["gh_auth_fail"].format(error="Missing device_code or user_code"),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_setup_menu, "style": "primary"}]]
            )
            return

        await call.edit(
            self.strings["gh_auth_pending"].format(url=verification_uri, code=user_code),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_setup_menu, "style": "primary"}]]
        )

        deadline = time.time() + expires_in
        while time.time() < deadline:
            await asyncio.sleep(interval)

            p = await asyncio.create_subprocess_exec(
                "curl", "-sX", "POST",
                "https://github.com/login/oauth/access_token",
                "-H", "Accept: application/json",
                "-H", "Content-Type: application/x-www-form-urlencoded",
                "-d", f"client_id={client_id}&device_code={device_code}&grant_type=urn:ietf:params:oauth:grant-type:device_code",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await p.communicate()

            try:
                resp = json.loads(out.decode())
            except Exception:
                continue

            err = resp.get("error")
            if err == "authorization_pending":
                continue
            if err == "slow_down":
                interval += 5
                continue
            if err in ("expired_token", "access_denied"):
                break

            access_token = resp.get("access_token")
            if access_token:
                self._db.set("XR", "gh_token", access_token)
                await call.edit(
                    self.strings["gh_auth_done"],
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_setup_menu, "style": "primary"}]]
                )
                return

        await call.edit(
            self.strings["gh_auth_fail"].format(error="Expired or denied"),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_setup_menu, "style": "primary"}]]
        )

    async def _cb_gh_revoke(self, call: InlineCall):
        self._db.set("XR", "gh_token", "")
        await self._cb_setup_menu(call)

    async def _install_xray(self, tag: str) -> Tuple[bool, str]:
        reason = _guard_reason()
        if reason:
            return False, reason
        if tag != XRAY_CORE_VERSION:
            return False, f"Refused: only {XRAY_CORE_VERSION} is allowed, {tag} is not pinned"

        for name in list(self._processes.keys()):
            await self._stop_user(name)

        arch = platform.machine().lower()
        arch_map = {
            "x86_64": "64",
            "amd64": "64",
            "aarch64": "arm64-v8a",
            "arm64": "arm64-v8a",
        }
        go_arch = arch_map.get(arch)
        if not go_arch:
            return False, f"Unsupported arch: {arch}"
        
        try:
            tmp = None
            gh_token = self._gh_token()
            curl_cmd = ["curl", "-sL", "--max-time", "15"]
            extra, cfg = _curl_header_config(gh_token)
            curl_cmd += extra
            curl_cmd.append(f"https://api.github.com/repos/XTLS/Xray-core/releases/tags/{tag}")
            
            try:
                p = await asyncio.create_subprocess_exec(
                    *curl_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                out, _ = await p.communicate()
            finally:
                _drop_curl_config(cfg)
            if p.returncode != 0:
                return False, "GitHub API failed"
            
            raw = out.decode()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                return False, f"GitHub API bad response: {raw[:200]}"

            if "message" in data:
                return False, f"GitHub API: {data['message']}"

            assets = data.get("assets", [])
            download_url = None

            for asset in assets:
                name = asset.get("name", "").lower()
                if "linux" in name and go_arch.lower() in name and name.endswith(".zip"):
                    download_url = asset["browser_download_url"]
                    break

            if not download_url:
                available = [a.get("name", "") for a in assets]
                return False, f"No download URL for arch={go_arch} in {available}"
            if not _release_allowed(download_url):
                return False, "Refused: the asset URL is outside the GitHub release hosts"
            expected = XRAY_CORE_SHA256.get(go_arch, "")
            if not expected:
                return False, f"No pinned digest for arch={go_arch}"
            
            tmp = os.path.join(self._root, "tmp_install")
            os.makedirs(tmp, exist_ok=True)
            
            dl = os.path.join(tmp, "xray.zip")
            p = await asyncio.create_subprocess_exec(
                "curl", "-sL", "--max-time", "120", "-o", dl, download_url,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await p.communicate()
            
            if not os.path.exists(dl):
                return False, "Download failed"

            got = _sha256_file(dl)
            if got != expected:
                return False, f"Digest mismatch for arch={go_arch}: {got}"
            
            p = await asyncio.create_subprocess_exec(
                "python3", "-m", "zipfile", "-e", dl, tmp,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await p.communicate()
            
            binary = None
            for root_dir, _, files in os.walk(tmp):
                if "xray" in files:
                    binary = os.path.join(root_dir, "xray")
                    break
            
            if not binary:
                return False, "Binary not found in archive"

            try:
                os.remove(self._xray_path)
            except OSError:
                pass
            shutil.copy2(binary, self._xray_path)
            os.chmod(self._xray_path, 0o755)
            
            version = await self._get_version("xray")
            return True, version
            
        except Exception as e:
            return False, str(e)
        finally:
            if tmp and os.path.exists(tmp):
                shutil.rmtree(tmp, ignore_errors=True)

    async def _get_version(self, binary: str = "xray") -> str:
        specs = {
            "xray": (getattr(self, "_xray_path", None), self._xray_installed, ("version",)),
            "cloudflared": (getattr(self, "_cloudflared_path", None), self._cloudflared_installed, ("--version",)),
        }
        path, installed, args = specs[binary]
        if not installed():
            return "not installed"
        try:
            p = await asyncio.create_subprocess_exec(
                path, *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, err = await p.communicate()
            text = out.decode(errors="replace").strip() or err.decode(errors="replace").strip()
            return text.split("\n")[0][:80] if text else "unknown"
        except Exception:
            return "unknown"

    async def _generate_vless_encryption(self) -> Tuple[Optional[str], Optional[str]]:
        if not self._xray_installed():
            return None, None
        try:
            p = await asyncio.create_subprocess_exec(
                self._xray_path, "vlessenc",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await p.communicate()
            if p.returncode != 0:
                return None, None
            txt = out.decode(errors="replace")
            decs = re.findall(r'"decryption"\s*:\s*"([^"]+)"', txt)
            encs = re.findall(r'"encryption"\s*:\s*"([^"]+)"', txt)
            if decs and encs and len(decs) == len(encs):
                return decs[-1], encs[-1]
            if decs and encs:
                return decs[0], encs[0]
            pairs = re.findall(r'"decryption":\s*"([^"]+)"\s*\n"encryption":\s*"([^"]+)"', txt)
            if len(pairs) >= 2:
                return pairs[-1]
            return pairs[0] if pairs else (None, None)
        except Exception:
            return None, None

    async def _generate_x25519(self) -> Tuple[Optional[str], Optional[str]]:
        if not self._xray_installed():
            return None, None
        try:
            p = await asyncio.create_subprocess_exec(
                self._xray_path, "x25519",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await p.communicate()
            text = out.decode().strip()
            
            private_key = None
            public_key = None
            
            for line in text.split("\n"):
                stripped = line.strip()
                lower = stripped.lower()
                if "private" in lower and ":" in lower:
                    private_key = stripped.split(":", 1)[1].strip()
                elif "public" in lower and ":" in lower:
                    public_key = stripped.split(":", 1)[1].strip()
            
            return private_key, public_key
        except:
            return None, None

    def _generate_short_id(self) -> str:
        return os.urandom(8).hex()[:16]

    async def _get_next_port(self) -> int:
        used_ports = {u["port"] for u in self._users.values()}
        if self._sub.get("port"):
            used_ports.add(int(self._sub["port"]))
        port = BASE_PORT
        
        if await self._check_port_available(port) and port not in used_ports:
            return port
        
        for _ in range(100):
            port = random.randint(BASE_PORT, 50000)
            if port not in used_ports and await self._check_port_available(port):
                return port
        
        return port

    async def _check_port_available(self, port: int) -> bool:
        try:
            p = await asyncio.create_subprocess_exec(
                "ss", "-tuln",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await p.communicate()
            return f":{port}" not in out.decode()
        except:
            return True

    def _build_config(self, user: Dict) -> Dict:
        transport = user["transport"]

        if transport == "socks5":
            socks_config = {
                "log": {
                    "loglevel": "warning",
                    "access": os.path.join(self._root, "users", user["name"], "access.log"),
                    "error": os.path.join(self._root, "users", user["name"], "error.log"),
                },
                "inbounds": [{
                    "listen": "0.0.0.0",
                    "port": user["port"],
                    "protocol": "socks",
                    "settings": {
                        "auth": "password",
                        "accounts": [{
                            "user": user.get("socks_user", ""),
                            "pass": user.get("socks_pass", ""),
                        }],
                        "udp": True,
                    },
                    "sniffing": {
                        "enabled": False,
                        "destOverride": ["http", "tls", "quic", "fakedns"],
                        "metadataOnly": False,
                        "routeOnly": False,
                    },
                }],
                "outbounds": [
                    {"protocol": "freedom", "tag": "direct"},
                ],
            }
            self._inject_stats_api(user, socks_config)
            return socks_config

        if transport == "tcp":
            sni = user.get("sni", "www.cloudflare.com")
            dest = user.get("dest", "www.cloudflare.com:443")
        else:
            sni = user.get("sni", "www.cloudflare.com")
            dest = user.get("dest", "www.cloudflare.com:443")
        short_id = user["short_id"]

        config = {
            "log": {
                "loglevel": "warning",
                "access": os.path.join(self._root, "users", user["name"], "access.log"),
                "error": os.path.join(self._root, "users", user["name"], "error.log"),
            },
            "inbounds": [{
                "listen": "0.0.0.0",
                "port": user["port"],
                "protocol": "vless",
                "settings": {
                    "clients": [{
                        "id": user["uuid"],
                    }],
                    "decryption": user.get("vless_decryption", "none"),
                },
                "sniffing": {
                    "enabled": False,
                    "destOverride": ["http", "tls", "quic", "fakedns"],
                    "metadataOnly": False,
                    "routeOnly": False,
                },
            }],
            "outbounds": [
                {"protocol": "freedom", "tag": "direct"},
            ],
        }

        if transport == "websocket":
            ws_path = user.get("path") or "/xhttps"
            if not ws_path.startswith("/"):
                ws_path = "/" + ws_path
            config["inbounds"][0]["settings"]["decryption"] = user.get("vless_decryption", "none")
            config["inbounds"][0]["streamSettings"] = {
                "network": "ws",
                "security": "none",
                "wsSettings": {
                    "path": ws_path,
                },
            }
            if user.get("websocket_mode") == "tls-fallback":
                config["inbounds"][0]["settings"]["decryption"] = "none"
                config["inbounds"][0]["settings"]["fallbacks"] = [{
                    "dest": user.get("site_port", 0),
                    "xver": 0,
                }]
        elif transport == "xhttp":
            config["inbounds"][0]["streamSettings"] = {
                "network": "xhttp",
                "security": "reality",
                "xhttpSettings": {
                    "path": user.get("path", "/xhttps"),
                },
                "realitySettings": {
                    "show": False,
                    "target": dest,
                    "xver": 0,
                    "serverNames": [sni],
                    "privateKey": user["private_key"],
                    "shortIds": [short_id],
                },
            }
        else:
            if user.get("vless_decryption", "none") == "none":
                config["inbounds"][0]["settings"]["clients"][0]["flow"] = "xtls-rprx-vision"
            config["inbounds"][0]["streamSettings"] = {
                "network": "tcp",
                "security": "reality",
                "tcpSettings": {
                    "acceptProxyProtocol": False,
                    "header": {"type": "none"},
                },
                "realitySettings": {
                    "show": False,
                    "target": dest,
                    "xver": 0,
                    "serverNames": [sni],
                    "privateKey": user["private_key"],
                    "shortIds": [short_id],
                },
            }

        self._inject_stats_api(user, config)
        return config

    def _build_vless_link(self, user: Dict) -> str:
        import urllib.parse
        
        name = user["name"]
        uuid_str = user["uuid"]
        ip = self._external_ip
        port = user["port"]
        transport = user["transport"]
        public_key = user["public_key"]
        short_id = user["short_id"]
        fp = user.get("fingerprint", "firefox")

        import json as _json

        if transport == "websocket":
            path = user.get("path") or "/xhttps"
            if not path.startswith("/"):
                path = "/" + path
            fallback = user.get("websocket_mode") == "tls-fallback"
            params = urllib.parse.urlencode({
                "type": "ws",
                "encryption": "none" if fallback else user.get("vless_encryption", "none"),
                "path": path,
                "host": user.get("tunnel_host", "") if fallback else "",
                "security": "tls" if fallback else "none",
                **({"sni": user.get("tunnel_host", "")} if fallback else {}),
            })
            host = user.get("tunnel_host") if fallback else ip
            return f"vless://{uuid_str}@{host}:{443 if fallback else port}?{params}#{urllib.parse.quote(name, safe='')}"

        if transport == "xhttp":
            sni = user.get("sni", "www.cloudflare.com")
            path = user.get("path", "/xhttps")

            params = urllib.parse.urlencode({
                "type": "xhttp",
                "encryption": user.get("vless_encryption", "none"),
                "security": "reality",
                "path": path,
                "pbk": public_key,
                "fp": fp,
                "sni": sni,
                "sid": short_id,
                "spx": "/",
            })
            return f"vless://{uuid_str}@{ip}:{port}?{params}#{urllib.parse.quote(name, safe='')}"
        else:
            sni = user.get("sni", "www.cloudflare.com")
            params = {
                "type": "tcp",
                "encryption": user.get("vless_encryption", "none"),
                "security": "reality",
                "pbk": public_key,
                "fp": fp,
                "sni": sni,
                "sid": short_id,
                "spx": "/",
            }
            if user.get("vless_decryption", "none") == "none":
                params["flow"] = "xtls-rprx-vision"
            return f"vless://{uuid_str}@{ip}:{port}?{urllib.parse.urlencode(params)}#{urllib.parse.quote(name, safe='')}"

    async def _start_user(self, name: str) -> Tuple[bool, str]:
        if _guard_reason():
            return False, "refused: root or container detected"
        return await self._start_user_impl(name)

    async def _start_user_impl(self, name: str) -> Tuple[bool, str]:
        unit = self._unit_name(name)
        if name in self._processes or await self._unit_active(unit):
            return False, "already_running"
        
        user = self._users.get(name)
        if not user:
            return False, "user_not_found"

        if not self._xray_installed():
            return False, "xray_not_installed"
        
        port_ok = await self._check_port_available(user["port"])
        if not port_ok:
            await self._reattach_processes()
            if name in self._processes:
                return True, ""
            return False, f"port_busy_{user['port']}"
        
        user_dir = os.path.join(self._root, "users", name)
        os.makedirs(user_dir, exist_ok=True)

        if user["transport"] == "websocket" and user.get("websocket_mode") == "tls-fallback":
            ok, error = await self._start_websocket_site(name, user_dir)
            if not ok:
                return False, error
        
        config = self._build_config(user)
        config_path = os.path.join(user_dir, "config.json")
        _write_private(config_path, json.dumps(config, indent=2))
        
        try:
            start_log = os.path.join(user_dir, "start.log")
            self._write_unit(unit, f"XRay VPN user {name}", [self._xray_path, "run", "-config", config_path], start_log)
            ok, output = await self._start_unit(unit)
            if not ok:
                return False, output
            await asyncio.sleep(1)
            if not await self._unit_active(unit):
                tail = ""
                for path in (os.path.join(user_dir, "error.log"), start_log):
                    if os.path.exists(path):
                        with open(path, "rb") as f:
                            f.seek(max(0, os.path.getsize(path) - 2048))
                            tail = f.read().decode(errors="replace").strip()
                        if tail:
                            break
                return False, tail or "startup_failed"

            self._processes[name] = unit
            user["start_time"] = time.time()
            user["restart_required"] = False

            if user["transport"] == "websocket" and user.get("websocket_mode") == "tls-fallback":
                ok, error = await self._start_websocket_tunnel(name, user_dir)
                if not ok:
                    await self._stop_unit(unit)
                    self._processes.pop(name, None)
                    await self._stop_websocket_site(name)
                    return False, error

            self._save_users()
            await self._send_log(
                self.strings["log_user_started"].format(**self._log_user_data(user)),
                name=name
            )

            return True, ""

        except Exception as e:
            return False, str(e)

    async def _stop_user(self, name: str, reason: str = "manual") -> bool:
        unit = self._unit_name(name)
        if name not in self._processes and not await self._unit_active(unit):
            return False
        await self._stop_unit(unit)
        self._processes.pop(name, None)

        if self._users.get(name, {}).get("transport") == "websocket":
            await self._stop_websocket_tunnel(name)
            await self._stop_websocket_site(name)
        
        if name in self._users:
            self._users[name]["start_time"] = 0
            self._save_users()
            user = self._users[name]
            await self._send_log(
                self.strings["log_user_stopped"].format(
                    **self._log_user_data(user),
                    reason=_escape(reason),
                ),
                name=name
            )
        
        return True

    def _get_active_connections(
        self, port: int, transport: str = "", site_port: Optional[int] = None
    ) -> int:
        try:
            watch_port = site_port if transport == "websocket" and site_port else port
            proc = subprocess.run(
                ["ss", "-Htn", "state", "established", f"sport = :{watch_port}"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if proc.returncode != 0:
                return 0

            unique_ips = set()
            for line in proc.stdout.strip().splitlines():
                parts = line.split()
                if len(parts) < 2:
                    continue
                peer = parts[-1]
                if peer.startswith("["):
                    ip = peer[1:].split("]", 1)[0]
                else:
                    ip = peer.rsplit(":", 1)[0]
                try:
                    addr = ipaddress.ip_address(ip)
                    if not addr.is_loopback and not addr.is_link_local:
                        unique_ips.add(addr.compressed)
                except ValueError:
                    continue
            
            return len(unique_ips)
        except:
            return 0

    def _start_monitor(self):
        async def monitor_loop():
            while True:
                await asyncio.sleep(AUTOSTART_INTERVAL)
                for name in self._users:
                    self._trim_user_logs(name)
                
                for name, proc in list(self._processes.items()):
                    user = self._users.get(name)
                    if not user:
                        continue
                    
                    limit = user.get("device_limit", 0)
                    if limit == 0:
                        continue
                    
                    active = self._get_active_connections(
                        user["port"], user.get("transport", ""), user.get("site_port")
                    )
                    
                    if active > limit:
                        user["autostart"] = False
                        self._save_users()
                        await self._stop_user(name, self.strings["log_reason_limit"])
                        await self._send_log(
                            self.strings["log_device_limit"].format(
                                **self._log_user_data(user),
                                limit=limit,
                                active=active,
                            ),
                            name=name
                        )
                        
                if not self._xray_installed():
                    continue

                for name, user in list(self._users.items()):
                    if user.get("transport") == "websocket":
                        await self._heal_websocket_stack(name)

                for name, user in list(self._users.items()):
                    if not user.get("autostart"):
                        continue
                    if name in self._processes:
                        continue
                    
                    await self._start_user(name)
        
        if self._monitor_task:
            self._monitor_task.cancel()
        
        self._monitor_task = asyncio.create_task(monitor_loop())

    def _save_users(self):
        self._db.set("XR", "users", self._users)

    def _get_user_uptime(self, name: str) -> str:
        user = self._users.get(name)
        if not user or name not in self._processes:
            return "offline"
        
        start = user.get("start_time", 0)
        if start == 0:
            return "n/a"
        
        elapsed = int(time.time() - start)
        d, rem = divmod(elapsed, 86400)
        h, rem = divmod(rem, 3600)
        m, s = divmod(rem, 60)
        
        parts = []
        if d:
            parts.append(f"{d}d")
        if h:
            parts.append(f"{h}h")
        if m:
            parts.append(f"{m}m")
        parts.append(f"{s}s")
        
        return " ".join(parts)

    def _sub_save(self):
        self._db.set("XR", "sub", self._sub)

    def _sub_default(self) -> Dict:
        return {
            "enabled": False,
            "port": 0,
            "prefix": "",
            "token": "",
            "stats": True,
            "interval": 12,
            "title": "",
            "primary": "",
            "traffic": {},
            "baseline": {},
        }

    def _sub_ensure(self) -> Dict:
        changed = False
        for key, value in self._sub_default().items():
            if key not in self._sub:
                self._sub[key] = value
                changed = True
        if not self._sub.get("port"):
            self._sub["port"] = self._sub_free_port(self._sub_taken_ports())
            changed = True
        if not self._sub.get("prefix"):
            self._sub["prefix"] = _gen_secret(SUB_TOKEN_LEN)
            changed = True
        if not self._sub.get("token"):
            self._sub["token"] = _gen_secret(SUB_TOKEN_LEN)
            changed = True
        if changed:
            self._sub_save()
        return self._sub

    def _sub_b64(self, raw: str) -> str:
        return base64.urlsafe_b64encode(str(raw).encode("utf-8")).decode("ascii").rstrip("=")

    def _port_in_use(self, port: int) -> bool:
        try:
            probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except OSError:
            return True
        try:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(("0.0.0.0", int(port)))
        except OSError:
            return True
        finally:
            probe.close()
        return False

    def _sub_taken_ports(self) -> set:
        taken = set()
        for user in self._users.values():
            for key in ("port", "site_port", "api_port"):
                value = user.get(key)
                if value:
                    try:
                        taken.add(int(value))
                    except (TypeError, ValueError):
                        continue
        if self._sub.get("port"):
            try:
                taken.add(int(self._sub["port"]))
            except (TypeError, ValueError):
                pass
        return taken

    def _sub_free_port(self, taken: set) -> int:
        for _ in range(64):
            port = random.randint(20000, 60000)
            if port not in taken and not self._port_in_use(port):
                return port
        for _ in range(64):
            port = self._find_free_loopback_port()
            if port not in taken and not self._port_in_use(port):
                return port
        return random.randint(20000, 60000)

    def _sub_forget_user(self, name: str):
        if not self._sub:
            return
        self._sub.setdefault("traffic", {}).pop(name, None)
        self._sub.setdefault("baseline", {}).pop(name, None)
        self._sub_save()

    def _sub_user_token(self, name: str) -> str:
        user = self._users.get(name)
        if not user:
            return ""
        token = user.get("sub_token")
        if not token:
            token = _gen_secret(SUB_TOKEN_LEN)
            user["sub_token"] = token
            self._save_users()
        return token

    def _sub_prefix(self) -> str:
        return self._sub_b64(self._sub.get("prefix", ""))

    def _sub_route(self, token: str) -> str:
        return f"/{self._sub_prefix()}/{self._sub_b64(token)}"

    def _sub_masked_path(self) -> str:
        return f"/{self._sub_prefix()}/*"

    def _sub_base_url(self) -> str:
        if not self._external_ip:
            return ""
        return f"http://{self._external_ip}:{self._sub.get('port', 0)}"

    def _sub_url(self, token: str = "") -> str:
        base = self._sub_base_url()
        if not base:
            return ""
        if not token:
            token = self._sub.get("token", "")
        return base + self._sub_route(token)

    def _sub_link_host(self, link: str) -> str:
        parts = link.split("@", 1)
        if len(parts) < 2:
            return ""
        tail = parts[1].split("?", 1)[0]
        if tail.startswith("["):
            end = tail.find("]")
            if end < 0:
                return ""
            return tail[1:end]
        if ":" in tail:
            return tail.rsplit(":", 1)[0]
        return tail

    def _sub_targets(self, only: str = "") -> List[Tuple]:
        out = []
        if not self._external_ip:
            return out
        primary = self._sub_primary()
        for name, user in list(self._users.items()):
            if only and name != only:
                continue
            if not only and name != primary:
                continue
            if user.get("transport") == "socks5":
                continue
            try:
                link = self._build_vless_link(user)
            except Exception:
                continue
            if not link or "@" not in link:
                continue
            host = self._sub_link_host(link)
            if not host or host.lower() in ("none", "null", "0.0.0.0"):
                continue
            out.append((name, user, link))
        return out

    def _sub_body(self, only: str = "") -> str:
        return "\n".join(link for _, _, link in self._sub_targets(only))

    def _sub_traffic_for(self, only: str = "") -> Tuple[int, int]:
        up = 0
        down = 0
        rows = self._sub.get("traffic") or {}
        for name, row in rows.items():
            if only and name != only:
                continue
            if not isinstance(row, dict):
                continue
            up += int(row.get("up") or 0)
            down += int(row.get("down") or 0)
        return up, down

    def _sub_userinfo(self, only: str = "") -> str:
        up, down = self._sub_traffic_for(only)
        return f"upload={up}; download={down}; total=0; expire=0"

    def _sub_resolve(self, presented: str) -> Tuple[bool, str]:
        given = presented.encode("utf-8", "ignore")
        for name, user in self._users.items():
            token = user.get("sub_token")
            if not token:
                continue
            if secrets.compare_digest(given, self._sub_b64(token).encode("ascii")):
                return True, name
        if secrets.compare_digest(given, self._sub_b64(self._sub.get("token", "")).encode("ascii")):
            return True, ""
        return False, ""

    def _sub_outbound(self, name: str, user: Dict) -> Dict:
        transport = user.get("transport")
        uuid_str = user.get("uuid") or ""
        if not uuid_str:
            return {}
        address = self._external_ip
        port = int(user.get("port") or 0)
        vnext_user = {
            "id": uuid_str,
            "encryption": user.get("vless_encryption") or "none",
            "email": name,
        }
        settings = {"vnext": [{"address": address, "port": port, "users": [vnext_user]}]}

        if transport == "websocket":
            path = user.get("path") or "/xhttps"
            if not path.startswith("/"):
                path = "/" + path
            fallback = user.get("websocket_mode") == "tls-fallback"
            host = (user.get("tunnel_host") or address) if fallback else address
            target_port = 443 if fallback else port
            settings["vnext"][0]["address"] = host
            settings["vnext"][0]["port"] = target_port
            stream = {"network": "ws", "wsSettings": {"path": path}}
            if fallback:
                stream["security"] = "tls"
                stream["tlsSettings"] = {"serverName": host}
            else:
                stream["security"] = "none"
            return {
                "tag": "proxy",
                "protocol": "vless",
                "settings": settings,
                "streamSettings": stream,
            }

        if transport == "xhttp":
            path = user.get("path") or "/xhttps"
            if not path.startswith("/"):
                path = "/" + path
            stream = {
                "network": "xhttp",
                "security": "reality",
                "xhttpSettings": {"path": path},
                "realitySettings": {
                    "serverName": user.get("sni") or "www.cloudflare.com",
                    "fingerprint": user.get("fingerprint") or "firefox",
                    "publicKey": user.get("public_key") or "",
                    "shortId": user.get("short_id") or "",
                    "spiderX": "/",
                },
            }
            return {
                "tag": "proxy",
                "protocol": "vless",
                "settings": settings,
                "streamSettings": stream,
            }

        stream = {
            "network": "tcp",
            "security": "reality",
            "tcpSettings": {"header": {"type": "none"}},
            "realitySettings": {
                "serverName": user.get("sni") or "www.cloudflare.com",
                "fingerprint": user.get("fingerprint") or "firefox",
                "publicKey": user.get("public_key") or "",
                "shortId": user.get("short_id") or "",
                "spiderX": "/",
            },
        }
        if (user.get("vless_decryption") or "none") == "none":
            vnext_user["flow"] = "xtls-rprx-vision"
        return {
            "tag": "proxy",
            "protocol": "vless",
            "settings": settings,
            "streamSettings": stream,
        }

    def _sub_client_config(self, name: str, user: Dict) -> Dict:
        outbound = self._sub_outbound(name, user)
        if not outbound:
            return {}
        return {
            "log": {"loglevel": "warning"},
            "inbounds": [
                {
                    "tag": "socks-in",
                    "listen": "127.0.0.1",
                    "port": 1080,
                    "protocol": "socks",
                    "settings": {"auth": "noauth", "udp": True},
                },
                {
                    "tag": "http-in",
                    "listen": "127.0.0.1",
                    "port": 1081,
                    "protocol": "http",
                    "settings": {},
                },
            ],
            "outbounds": [
                outbound,
                {"protocol": "freedom", "tag": "direct"},
                {"protocol": "blackhole", "tag": "block"},
            ],
            "routing": {
                "domainStrategy": "AsIs",
                "rules": [
                    {
                        "type": "field",
                        "ip": [
                            "10.0.0.0/8",
                            "172.16.0.0/12",
                            "192.168.0.0/16",
                            "127.0.0.0/8",
                            "::1/128",
                            "fc00::/7",
                        ],
                        "outboundTag": "direct",
                    }
                ],
            },
        }

    def _sub_json_payload(self, only: str = "") -> bytes:
        configs = []
        for name, user, _ in self._sub_targets(only):
            config = self._sub_client_config(name, user)
            if config:
                configs.append(config)
        payload = configs[0] if len(configs) == 1 else configs
        return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

    def _sub_info_payload(self, only: str = "", up: int = 0, down: int = 0) -> Dict:
        targets = self._sub_targets(only)
        return {
            "profiles": [name for name, _, _ in targets],
            "links": len(targets),
            "upload": up,
            "download": down,
            "total": 0,
            "expire": 0,
            "unlimited": True,
            "interval": self._sub.get("interval", 12),
        }

    def _sub_make_handler(self):
        outer = self

        class SubHandler(BaseHTTPRequestHandler):
            server_version = "nginx"
            sys_version = ""
            protocol_version = "HTTP/1.1"
            timeout = SUB_SOCKET_TIMEOUT

            def log_message(self, fmt, *args):
                return

            def handle_one_request(self):
                try:
                    self.raw_requestline = self.rfile.readline(SUB_MAX_LINE + 1)
                    if len(self.raw_requestline) > SUB_MAX_LINE:
                        self.raw_requestline = self.raw_requestline[:SUB_MAX_LINE]
                        self.requestline = ""
                        self.request_version = ""
                        self.command = ""
                        _sub_record(self.client_address[0], True)
                        _sub_security(self.client_address[0], "sub_request", "deny", "request line too long")
                        self.send_error(414, "uri too long")
                        self.close_connection = True
                        return
                    if not self.raw_requestline:
                        self.close_connection = True
                        return
                    if not self.parse_request():
                        return
                    method = getattr(self, "do_" + self.command, None)
                    if method is None:
                        self.send_error(501, "unsupported method")
                        return
                    method()
                    self.wfile.flush()
                except (TimeoutError, OSError):
                    self.close_connection = True

            def _abort(self):
                _sub_record(self.client_address[0], True)
                _sub_security(self.client_address[0], "sub_request", "deny", "unknown subscription")
                try:
                    self.send_response(404)
                    self.send_header("Content-Length", "0")
                    self.send_header("Connection", "close")
                    self.end_headers()
                except Exception:
                    pass

            def _reply(self, code, payload, ctype, userinfo):
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(payload)))
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.send_header("Subscription-Userinfo", userinfo)
                self.send_header("Profile-Update-Interval", str(outer._sub.get("interval", 12)))
                title = outer._sub.get("title") or ""
                if title:
                    self.send_header("Profile-Title", "base64:" + outer._sub_b64(title))
                self.end_headers()
                if self.command != "HEAD" and payload:
                    self.wfile.write(payload)
                if code == 200:
                    _sub_record(self.client_address[0], False)

            def do_HEAD(self):
                self.do_GET()

            def do_GET(self):
                try:
                    self._dispatch()
                except Exception:
                    self._abort()

            def _dispatch(self):
                allowed, reason = _sub_allowed(self.client_address[0])
                if not allowed:
                    if reason:
                        _sub_security(self.client_address[0], "sub_rate", "block", reason)
                    self._reply(429, b"slow down\n", "text/plain; charset=utf-8", "upload=0; download=0; total=0; expire=0")
                    return
                raw = self.path or "/"
                query = ""
                if "?" in raw:
                    raw, query = raw.split("?", 1)
                if "#" in raw:
                    raw = raw.split("#", 1)[0]
                segments = [part for part in raw.split("/") if part]
                fmt = ""
                if len(segments) == 3:
                    fmt = segments[2].lower()
                    segments = segments[:2]
                for chunk in query.split("&"):
                    if not chunk:
                        continue
                    key, _, value = chunk.partition("=")
                    if key.lower() == "format":
                        fmt = value.lower()
                if len(segments) != 2:
                    self._abort()
                    return
                if not secrets.compare_digest(
                    segments[0].encode("utf-8", "ignore"),
                    outer._sub_prefix().encode("ascii"),
                ):
                    self._abort()
                    return
                resolved, only = outer._sub_resolve(segments[1])
                if not resolved:
                    self._abort()
                    return
                userinfo = outer._sub_userinfo(only)
                if fmt != "info" and not outer._sub_targets(only):
                    self._abort()
                    return
                if fmt == "info":
                    up, down = outer._sub_traffic_for(only)
                    body = json.dumps(outer._sub_info_payload(only, up, down), ensure_ascii=False).encode("utf-8")
                    self._reply(200, body, "application/json; charset=utf-8", userinfo)
                    return
                if fmt == "json":
                    self._reply(200, outer._sub_json_payload(only), "application/json; charset=utf-8", userinfo)
                    return
                if fmt == "raw":
                    self._reply(200, outer._sub_body(only).encode("utf-8"), "text/plain; charset=utf-8", userinfo)
                    return
                encoded = base64.b64encode(outer._sub_body(only).encode("utf-8"))
                self._reply(200, encoded, "text/plain; charset=utf-8", userinfo)

        return SubHandler

    def _sub_primary(self) -> str:
        name = str(self._sub.get("primary") or "")
        if name and name in self._users:
            return name
        for key, user in self._users.items():
            if user.get("transport") != "socks5":
                return key
        return ""

    def _sub_start_server(self) -> Tuple[bool, str]:
        reason = _guard_reason()
        if reason:
            return False, reason
        if self._sub_server:
            return True, ""
        self._sub_ensure()
        port = int(self._sub["port"])
        if self._port_in_use(port):
            port = self._sub_free_port(self._sub_taken_ports())
            self._sub["port"] = port
            self._sub_save()
        try:
            httpd = _SubServer((SUB_BIND, port), self._sub_make_handler())
        except OSError as exc:
            return False, str(exc)
        httpd.daemon_threads = True
        self._sub_server = httpd
        self._sub_thread = threading.Thread(target=httpd.serve_forever, name="xray-sub", daemon=True)
        self._sub_thread.start()
        logger.info(f"[XR] subscription server listening on {port}")
        return True, ""

    def _sub_stop_server(self):
        httpd = self._sub_server
        self._sub_server = None
        self._sub_thread = None
        if not httpd:
            return
        try:
            httpd.shutdown()
        except Exception:
            pass
        try:
            httpd.server_close()
        except Exception:
            pass

    async def _sub_autostart(self):
        if not self._sub.get("enabled"):
            return
        ok, err = self._sub_start_server()
        if not ok:
            logger.error(f"[XR] subscription server failed to start: {err}")

    async def _sub_enable(self) -> Tuple[bool, str]:
        self._sub_ensure()
        self._sub["enabled"] = True
        self._sub_save()
        ok, err = self._sub_start_server()
        if not ok:
            self._sub["enabled"] = False
            self._sub_save()
            return False, err
        self._sub_start_task()
        return True, ""

    async def _sub_disable(self):
        self._sub["enabled"] = False
        self._sub_save()
        await self._sub_stop_task()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._sub_stop_server)

    def _sub_start_task(self):
        if self._sub_task and not self._sub_task.done():
            return
        self._sub_task = asyncio.create_task(self._sub_traffic_loop())

    async def _sub_stop_task(self):
        task = self._sub_task
        self._sub_task = None
        if not task:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    async def _sub_traffic_loop(self):
        while True:
            await asyncio.sleep(SUB_POLL_INTERVAL)
            try:
                if self._sub.get("enabled") and self._sub.get("stats"):
                    await self._sub_collect_traffic()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(f"[XR] traffic poll failed: {exc}")

    async def _sub_collect_traffic(self):
        if not self._xray_installed():
            return
        changed = False
        for name, user in list(self._users.items()):
            port = user.get("api_port")
            if not port or name not in self._processes:
                continue
            raw = await self._sub_query_stats(port)
            if raw is None:
                continue
            totals = dict((self._sub["traffic"].get(name)) or {"up": 0, "down": 0})
            baseline = self._sub["baseline"].get(name)
            if not isinstance(baseline, dict):
                self._sub["baseline"][name] = {"up": raw[0], "down": raw[1]}
                self._sub["traffic"][name] = totals
                changed = True
                continue
            for key, value in (("up", raw[0]), ("down", raw[1])):
                previous = int(baseline.get(key) or 0)
                current = int(totals.get(key) or 0)
                if value < previous:
                    totals[key] = current + value
                else:
                    totals[key] = current + (value - previous)
                baseline[key] = value
            self._sub["baseline"][name] = baseline
            self._sub["traffic"][name] = totals
            changed = True
        if changed:
            self._sub_save()

    async def _sub_query_stats(self, port: int):
        try:
            proc = await asyncio.create_subprocess_exec(
                self._xray_path,
                "api",
                "statsquery",
                f"--server=127.0.0.1:{int(port)}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except Exception:
            return None
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=SUB_XRAY_CALL_TIMEOUT)
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:
                pass
            return None
        except Exception:
            return None
        if proc.returncode != 0:
            return None
        return self._sub_parse_stats(out.decode("utf-8", "replace"))

    def _sub_parse_stats(self, text: str):
        text = (text or "").strip()
        if not text:
            return None
        start = text.find("{")
        if start < 0:
            return None
        try:
            data = json.loads(text[start:])
        except Exception:
            return None
        user_up = 0
        user_down = 0
        seen_user = False
        inbound_up = 0
        inbound_down = 0
        for item in data.get("stat") or []:
            if not isinstance(item, dict):
                continue
            parts = str(item.get("name") or "").split(">>>")
            if len(parts) != 4 or parts[2] != "traffic":
                continue
            try:
                value = int(item.get("value") or 0)
            except (TypeError, ValueError):
                continue
            kind, who, _, direction = parts
            if kind == "user":
                seen_user = True
                if direction == "uplink":
                    user_up += value
                elif direction == "downlink":
                    user_down += value
            elif kind == "inbound" and who != "api":
                if direction == "uplink":
                    inbound_up += value
                elif direction == "downlink":
                    inbound_down += value
        if seen_user:
            return user_up, user_down
        return inbound_up, inbound_down

    def _inject_stats_api(self, user: Dict, config: Dict):
        if not self._sub.get("enabled") or not self._sub.get("stats"):
            return
        if not user.get("api_port"):
            user["api_port"] = self._sub_free_port(self._sub_taken_ports())
        tag = f"in-{user.get('name', 'user')}"
        for inbound in config.get("inbounds") or []:
            if not isinstance(inbound, dict):
                continue
            if not inbound.get("tag"):
                inbound["tag"] = tag
            clients = (inbound.get("settings") or {}).get("clients")
            if isinstance(clients, list):
                for client in clients:
                    if isinstance(client, dict) and not client.get("email"):
                        client["email"] = user.get("name", "")
        config["stats"] = {}
        config["api"] = {"tag": "api", "services": ["StatsService"]}
        config["policy"] = {
            "levels": {"0": {"statsUserUplink": True, "statsUserDownlink": True}},
            "system": {
                "statsInboundUplink": True,
                "statsInboundDownlink": True,
                "statsOutboundUplink": True,
                "statsOutboundDownlink": True,
            },
        }
        config.setdefault("inbounds", []).append(
            {
                "tag": "api",
                "listen": "127.0.0.1",
                "port": int(user["api_port"]),
                "protocol": "dokodemo-door",
                "settings": {"address": "127.0.0.1"},
            }
        )
        routing = config.setdefault("routing", {})
        routing.setdefault("domainStrategy", "AsIs")
        rules = routing.setdefault("rules", [])
        if not any(rule.get("outboundTag") == "api" for rule in rules if isinstance(rule, dict)):
            rules.append({"type": "field", "inboundTag": ["api"], "outboundTag": "api"})
        outbounds = config.setdefault("outbounds", [])
        if not any(outbound.get("tag") == "api" for outbound in outbounds if isinstance(outbound, dict)):
            outbounds.append({"protocol": "blackhole", "tag": "api"})

    async def _cb_setup_menu(self, call: InlineCall):
        xray_status = await self._get_version("xray") if self._xray_installed() else "Not installed"
        cloudflared_status = await self._get_version("cloudflared") if self._cloudflared_installed() else "Not installed"
        
        gh_token = self._gh_token()
        gh_status = "Authorized" if gh_token else "Not authorized"
        
        text = self.strings["setup_menu"].format(
            xray_status=xray_status,
            cloudflared_status=cloudflared_status,
            gh_status=gh_status,
        )
        
        markup = []
        
        if self._xray_installed():
            markup.append([{
                "text": self.strings["btn_reinstall_xray"],
                "callback": self._cb_xray_install_menu,
                "style": "primary",
            }])
        else:
            markup.append([{
                "text": self.strings["btn_install_xray"],
                "callback": self._cb_xray_install_menu,
                "style": "primary",
            }])
        
        if self._cloudflared_installed():
            markup.append([{
                "text": self.strings["btn_reinstall_cloudflared"],
                "callback": self._cb_install_cloudflared,
                "style": "primary",
            }])
        else:
            markup.append([{
                "text": self.strings["btn_install_cloudflared"],
                "callback": self._cb_install_cloudflared,
                "style": "primary",
            }])

        markup.append([{
            "text": self.strings["btn_gh_auth"],
            "callback": self._gh_device_flow,
            "style": "primary",
        }])
        
        markup.append([{
            "text": self.strings["btn_back"],
            "callback": self._cb_main_menu,
            "style": "primary",
        }])
        
        await call.edit(text, reply_markup=markup)

    async def _cb_install_cloudflared(self, call: InlineCall):
        await call.edit(self.strings["loading"], reply_markup=[])
        ok, result = await self._install_cloudflared()
        if ok:
            text = "<b>Cloudflared Installed</b>\n<blockquote>{}</blockquote>".format(_escape(result))
        else:
            text = self.strings["setup_fail"].format(error=_escape(result[:200]))
        await call.edit(
            text,
            reply_markup=[[{
                "text": self.strings["btn_back"],
                "callback": self._cb_setup_menu,
                "style": "primary",
            }]],
        )

    async def _cb_xray_install_menu(self, call: InlineCall):
        await call.edit(self.strings["collecting_versions"])
        
        current_version = await self._get_version("xray")
        
        text = self.strings["xray_install_menu"].format(current=current_version)
        
        releases = await self._gh_get_releases()
        
        markup = []
        
        for release in releases:
            tag = release.get("tag_name", "")
            if not tag:
                continue
            
            markup.append([{
                "text": f"{tag}",
                "callback": self._cb_install_xray_version,
                "args": (tag,),
                "style": "primary",
            }])
        
        if not markup:
            markup.append([{
                "text": XRAY_CORE_VERSION,
                "callback": self._cb_install_xray_version,
                "args": (XRAY_CORE_VERSION,),
                "style": "primary",
            }])
        
        markup.append([{
            "text": self.strings["btn_back"],
            "callback": self._cb_setup_menu,
            "style": "primary",
        }])
        
        await call.edit(text, reply_markup=markup)

    async def _cb_install_xray_version(self, call: InlineCall, tag: str):
        await call.edit(
            self.strings["xray_installing"].format(version=tag),
            reply_markup=[]
        )
        
        ok, result = await self._install_xray(tag)
        
        if ok:
            text = self.strings["setup_done"].format(version=result)
        else:
            text = self.strings["setup_fail"].format(error=_escape(result[:200]))
        
        await call.edit(
            text,
            reply_markup=[[{
                "text": self.strings["btn_back"],
                "callback": self._cb_setup_menu,
                "style": "primary",
            }]]
        )

    async def _cb_users_menu(self, call: InlineCall):
        await self._reattach_processes()
        total = len(self._users)
        active = await self._get_active_count()
        
        text = self.strings["users_menu"].format(
            total=total,
            active=active,
        )
        
        markup = []
        
        for name, user in self._users.items():
            is_run = await self._is_user_running(name)
            status = self.strings["status_online"] if is_run else self.strings["status_offline"]
            markup.append([{
                "text": self.strings["user_item"].format(
                    status=status,
                    name=name,
                    port=user["port"],
                ),
                "callback": self._cb_user_menu,
                "args": (name,),
                "style": "primary",
            }])
        
        markup.append([{
            "text": self.strings["btn_add_user"],
            "callback": self._cb_add_user_name,
            "style": "primary",
        }])
        
        markup.append([{
            "text": self.strings["btn_back"],
            "callback": self._cb_main_menu,
            "style": "primary",
        }])
        
        await call.edit(text, reply_markup=markup)

    async def _cb_main_menu(self, call: InlineCall):
        await self._reattach_processes()
        total = len(self._users)
        active = await self._get_active_count()
        version = await self._get_version("xray")
        cf_version = await self._get_version("cloudflared")
        
        text = self.strings["main_menu"].format(
            total=total,
            active=active,
            version=version,
            cloudflared_version=cf_version,
        )
        
        markup = [
            [{
                "text": self.strings["btn_users"],
                "callback": self._cb_users_menu,
                "style": "primary",
            }],
            [{
                "text": self.strings["btn_sub"],
                "callback": self._cb_sub_menu,
                "style": "primary",
            }],
            [{
                "text": self.strings["btn_setup"],
                "callback": self._cb_setup_menu,
                "style": "primary",
            }],
            [{
                "text": self.strings["btn_close"],
                "callback": self._cb_close,
                "style": "danger",
            }],
        ]
        
        await call.edit(text, reply_markup=markup)

    async def _cb_user_menu(self, call: InlineCall, name: str):
        await call.edit(self.strings["loading"])
        
        user = self._users.get(name)
        if not user:
            await call.answer("User not found", show_alert=True)
            return
        
        await self._reattach_processes()
        is_running = await self._is_user_running(name)
        status = "STOPPED AND WAITING FOR RESTART" if user.get("restart_required") else (self.strings["status_online"] if is_running else self.strings["status_offline"])
        transport = user["transport"].upper()
        limit = user.get("device_limit", 0)
        limit_text = "Unlimited" if limit == 0 else str(limit)
        
        active = 0
        if is_running:
            active = self._get_active_connections(
                user["port"], user.get("transport", ""), user.get("site_port")
            )
        tls_host = user.get("tunnel_host", "n/a") if user.get("transport") == "websocket" else "n/a"
        
        uptime = await self._get_user_uptime_async(name)
        autostart = user.get("autostart", False)
        autostart_text = self.strings["btn_autostart_on"].split(":")[1].strip() if autostart else self.strings["btn_autostart_off"].split(":")[1].strip()
        
        text = self.strings["user_menu"].format(
            name=_escape(name),
            status=status,
            transport=transport,
            tls=_escape(tls_host),
            port=user["port"],
            autostart=autostart_text,
            limit=limit_text,
            active=active if is_running else "n/a",
            uptime=uptime,
        )
        
        markup = []
        
        if is_running:
            markup.append([
                {"text": self.strings["btn_stop"], "callback": self._cb_stop_user, "args": (name,), "style": "danger"},
                {"text": self.strings["btn_restart"], "callback": self._cb_restart_user, "args": (name,), "style": "primary"},
            ])
        else:
            markup.append([
                {"text": self.strings["btn_start"], "callback": self._cb_start_user, "args": (name,), "style": "primary"},
            ])
        
        markup.append([
            {
                "text": self.strings["btn_autostart_on"] if autostart else self.strings["btn_autostart_off"],
                "callback": self._cb_toggle_autostart,
                "args": (name,),
                "style": "success" if autostart else "danger",
            },
        ])
        
        markup.append([
            {"text": self.strings["btn_get_link"], "callback": self._cb_get_user_link, "args": (name,), "style": "primary"},
        ])

        if user.get("transport") != "socks5":
            markup.append([
                {"text": self.strings["btn_sub"], "callback": self._cb_sub_user_link, "args": (name,), "style": "primary"},
            ])
        
        markup.append([
            {"text": "Xray-core logs", "callback": self._cb_logs_menu, "args": (name, "xray"), "style": "primary"},
            {"text": "Daemon logs", "callback": self._cb_logs_menu, "args": (name, "daemon"), "style": "primary"},
        ])
        
        markup.append([
            {"text": self.strings["btn_settings"], "callback": self._cb_user_settings, "args": (name,), "style": "primary"},
        ])
        
        if user.get("transport") == "websocket" and user.get("websocket_mode") == "tls-fallback":
            markup.append([
                {"text": self.strings["btn_mask_site"], "callback": self._cb_mask_site_menu, "args": (name,), "style": "primary"},
            ])

        markup.append([
            {"text": self.strings["btn_delete"], "callback": self._cb_delete_user, "args": (name,), "style": "danger"},
        ])
        
        markup.append([
            {"text": self.strings["btn_back"], "callback": self._cb_users_menu, "style": "primary"},
        ])
        
        await call.edit(text, reply_markup=markup)

    async def _cb_toggle_autostart(self, call: InlineCall, name: str):
        user = self._users.get(name)
        if not user:
            await call.answer("User not found", show_alert=True)
            return
        
        user["autostart"] = not user.get("autostart", False)
        self._save_users()
        
        await self._cb_user_menu(call, name)

    async def _cb_start_user(self, call: InlineCall, name: str):
        await call.edit(self.strings["loading"])
        
        ok, err = await self._start_user(name)
        
        if ok:
            text = self.strings["user_started"].format(name=_escape(name))
        elif "already_running" in err:
            text = self.strings["err_already_running"]
        elif "port_busy" in err:
            port = err.split("_")[-1]
            text = self.strings["err_port_busy"].format(port=port)
        else:
            text = self.strings["setup_fail"].format(error=_escape(err[:200]))
        
        await call.edit(
            text,
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_user_menu, "args": (name,), "style": "primary"}]]
        )

    async def _cb_stop_user(self, call: InlineCall, name: str):
        await call.edit(self.strings["loading"])
        
        ok = await self._stop_user(name)
        
        if ok:
            text = self.strings["user_stopped"].format(name=_escape(name))
        else:
            text = self.strings["err_not_running"]
        
        await call.edit(
            text,
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_user_menu, "args": (name,), "style": "primary"}]]
        )

    async def _cb_restart_user(self, call: InlineCall, name: str):
        await call.edit(self.strings["loading"])
        
        await self._stop_user(name, self.strings["log_reason_restart"])
        await asyncio.sleep(1)
        ok, err = await self._start_user(name)
        
        if ok:
            text = self.strings["user_started"].format(name=_escape(name))
        else:
            text = self.strings["setup_fail"].format(error=_escape(err[:200]))
        
        await call.edit(
            text,
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_user_menu, "args": (name,), "style": "primary"}]]
        )

    async def _cb_delete_user(self, call: InlineCall, name: str):
        await call.edit(self.strings["loading"])
        
        user = self._users.get(name)
        if not user:
            await call.answer("User not found", show_alert=True)
            return

        await self._stop_user(name, self.strings["log_reason_manual"])
        for kind in ("xray", "site", "cf"):
            unit = self._unit_name(name, kind)
            await self._stop_unit(unit, disable=True)
            try:
                os.unlink(os.path.join(self._systemd_user_dir, unit))
            except OSError:
                pass
        await self._systemctl("daemon-reload")
        await self._send_log(
            self.strings["log_user_deleted"].format(**self._log_user_data(user)),
            name=name
        )
        
        user_dir = os.path.join(self._root, "users", name)
        if os.path.exists(user_dir):
            shutil.rmtree(user_dir, ignore_errors=True)
        
        del self._users[name]
        self._save_users()
        self._sub_forget_user(name)
        
        await call.edit(
            self.strings["user_deleted"].format(name=_escape(name)),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_users_menu, "style": "primary"}]]
        )

    async def _cb_get_user_link(self, call: InlineCall, name: str):
        await call.edit(self.strings["loading"])

        user = self._users.get(name)
        if not user:
            await call.answer("User not found", show_alert=True)
            return

        if not self._external_ip:
            await call.edit(
                self.strings["setup_fail"].format(error="Could not detect external IP"),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_user_menu, "args": (name,), "style": "primary"}]]
            )
            return

        if user["transport"] == "socks5":
            ip = self._external_ip
            port = user["port"]
            socks_user = user.get("socks_user", "")
            socks_pass = user.get("socks_pass", "")
            proxy_url = f"socks5://{socks_user}:{socks_pass}@{ip}:{port}"

            proxies_text = (
                "proxies = {\n"
                f'    "http": "{proxy_url}",\n'
                f'    "https": "{proxy_url}",\n'
                "}\n"
            )

            import tempfile, os
            tmp = tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".txt",
                prefix=f"proxies_{name}_",
                delete=False,
            )
            tmp.write(proxies_text)
            tmp.close()

            try:
                await self._send_file_to_call_chat(
                    call,
                    tmp.name,
                    force_document=True,
                )
            except Exception as e:
                logger.exception("[XR] send_file failed: %s", e)
                await call.edit(
                    self.strings["setup_fail"].format(error=f"Failed to send file: {e}"),
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_user_menu, "args": (name,), "style": "primary"}]]
                )
                return
            finally:
                os.unlink(tmp.name)

            await call.edit(
                self.strings["socks5_sent"].format(
                    name=_escape(name),
                    ip=ip,
                    port=port,
                    user=_escape(socks_user),
                    **{"pass": _escape(socks_pass)},
                ),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_user_menu, "args": (name,), "style": "primary"}]]
            )
            return

        link = self._build_vless_link(user)

        import tempfile, os
        tmp = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".txt",
            prefix=f"link_for_{name}_",
            delete=False,
        )
        tmp.write(link)
        tmp.close()

        try:
            await self._send_file_to_call_chat(
                call,
                tmp.name,
                force_document=True,
            )
        except Exception as e:
            logger.exception("[XR] send_file failed: %s", e)
            await call.edit(
                self.strings["setup_fail"].format(error=f"Failed to send file: {e}"),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_user_menu, "args": (name,), "style": "primary"}]]
            )
            return
        finally:
            os.unlink(tmp.name)

        await call.edit(
            self.strings["link_sent"].format(name=_escape(name)),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_user_menu, "args": (name,), "style": "primary"}]],
        )

    async def _cb_logs_menu(self, call: InlineCall, name: str, kind: str):
        title = "Xray-core logs" if kind == "xray" else "Daemon logs"
        await call.edit(
            f"<b>{title}: {_escape(name)}</b>\n<blockquote>Select an action.</blockquote>",
            reply_markup=[
                [{"text": "Send logs", "callback": self._cb_get_user_logs, "args": (name, kind), "style": "primary"}],
                [{"text": "Clear logs", "callback": self._cb_clear_user_logs, "args": (name, kind), "style": "danger"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_user_menu, "args": (name,), "style": "primary"}],
            ],
        )

    async def _cb_clear_user_logs(self, call: InlineCall, name: str, kind: str):
        user_dir = os.path.join(self._root, "users", name)
        paths = (["start.log", "error.log", "access.log"] if kind == "xray" else ["daemon.log"])
        for filename in paths:
            path = os.path.join(user_dir, filename)
            try:
                with open(path, "wb"):
                    pass
            except OSError:
                pass
        await self._cb_logs_menu(call, name, kind)

    def _make_upload_progress_cb(self, state: dict, label: str):
        def _cb(current: int, total: int):
            total_safe = total if total else current or 1
            state[label] = (current, total_safe)
        return _cb

    async def _upload_progress_render_loop(
        self,
        call: InlineCall,
        state: dict,
        labels: list,
        done_event: asyncio.Event,
        header: str,
    ):
        BAR_LEN = 12
        while not done_event.is_set():
            try:
                await asyncio.sleep(2)
                if done_event.is_set():
                    break
                parts = []
                for label in labels:
                    cur, tot = state.get(label, (0, 0))
                    cur_mb = cur / 1024 / 1024
                    tot_mb = tot / 1024 / 1024 if tot else cur_mb
                    pct = (cur / tot * 100) if tot > 0 else 0.0
                    filled = int(BAR_LEN * pct / 100)
                    bar = "\u2588" * filled + "\u2591" * (BAR_LEN - filled)
                    parts.append(
                        f"{bar}  {pct:.0f}%\n"
                        f"{cur_mb:.1f} / {tot_mb:.1f} MB"
                    )
                text = (
                    f"<b>{_escape(header)}</b>\n"
                    f"<blockquote>{''.join(parts)}</blockquote>"
                )
                try:
                    await call.edit(text)
                except Exception:
                    pass
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def _cb_get_user_logs(self, call: InlineCall, name: str, kind: str = "xray"):
        await call.edit(self.strings["loading"])

        user_dir = os.path.join(self._root, "users", name)
        names = ["start.log", "error.log", "access.log"] if kind == "xray" else ["daemon.log"]
        chosen = [(os.path.join(user_dir, fn), fn) for fn in names
                  if os.path.exists(os.path.join(user_dir, fn))
                  and os.path.getsize(os.path.join(user_dir, fn)) > 0]

        if not chosen:
            await call.edit(
                self.strings["setup_fail"].format(error="No logs found"),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_user_menu, "args": (name,), "style": "primary"}]]
            )
            return

        back_markup = [[{"text": self.strings["btn_back"], "callback": self._cb_user_menu, "args": (name,), "style": "primary"}]]

        labels = [label for _, label in chosen]
        paths = [p for p, _ in chosen]

        state = {}
        done_event = asyncio.Event()

        import zipfile
        if len(chosen) > 1:
            zip_path = os.path.join(user_dir, f"xray_logs_{name}.zip")
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for fpath, label in chosen:
                    zf.write(fpath, arcname=label)
            
            shared_label = "xray_logs.zip"
            state[shared_label] = (0, 0)
            header = f"Uploading Xray-core logs for {_escape(name)}"
            render_labels = [shared_label]
            cb_label = shared_label
            send_paths = zip_path
            caption = f"<b>Xray-core logs:</b> <code>{_escape(name)}</code>"
        else:
            label = labels[0]
            state[label] = (0, 0)
            header = f"Uploading {label} for {_escape(name)}"
            render_labels = [label]
            cb_label = label
            send_paths = paths[0]
            caption = f"<b>{kind.title()} {label}:</b> <code>{_escape(name)}</code>"

        render_task = asyncio.create_task(
            self._upload_progress_render_loop(call, state, render_labels, done_event, header)
        )

        try:
            cb = self._make_upload_progress_cb(state, cb_label)
            await self._send_file_to_call_chat(
                call,
                send_paths,
                force_document=True,
                caption=caption,
                parse_mode="html",
                progress_callback=cb,
            )
        except Exception as e:
            logger.exception(f"[XR] album send_file failed: {e}")
            try:
                for fpath, _ in chosen:
                    await self._send_file_to_call_chat(
                        call,
                        fpath,
                        force_document=True,
                    )
                    await asyncio.sleep(0.3)
            except Exception as e2:
                logger.exception(f"[XR] fallback single send failed: {e2}")
        finally:
            done_event.set()
            render_task.cancel()
            try:
                await render_task
            except asyncio.CancelledError:
                pass

        await call.edit(
            f"<b>Logs sent for {_escape(name)}</b>",
            reply_markup=back_markup,
        )

    async def _cb_mask_site_menu(self, call: InlineCall, name: str):
        user = self._users.get(name)
        if not user or user.get("transport") != "websocket":
            await call.answer("WebSocket user not found", show_alert=True)
            return
        current = user.get("mask_site", "Evil Cat")
        if current not in self._mask_sites:
            current = "Evil Cat"
        markup = []
        for mask in self._mask_sites:
            markup.append([{
                "text": f"{mask} (current)" if mask == current else mask,
                "callback": self._cb_set_mask_site,
                "args": (name, mask),
                "style": "success" if mask == current else "primary",
            }])
        markup.append([{"text": self.strings["btn_back"], "callback": self._cb_user_menu, "args": (name,), "style": "primary"}])
        await call.edit(
            self.strings["mask_site_menu"].format(current=_escape(current)),
            reply_markup=markup,
        )

    async def _cb_set_mask_site(self, call: InlineCall, name: str, mask: str):
        user = self._users.get(name)
        if not user or mask not in self._mask_sites:
            await call.answer("Mask site not found", show_alert=True)
            return
        user["mask_site"] = mask
        await self._mark_restart_required(name)
        await self._cb_user_menu(call, name)

    async def _cb_user_settings(self, call: InlineCall, name: str):
        user = self._users.get(name)
        if not user:
            await call.answer("User not found", show_alert=True)
            return
        
        is_socks5 = user["transport"] == "socks5"
        is_websocket = user["transport"] == "websocket"
        transport = self._transport_label(user["transport"])
        sni = user.get("sni", "www.cloudflare.com") if not is_socks5 and not is_websocket else "n/a"
        dest = user.get("dest", "www.cloudflare.com:443") if not is_socks5 and not is_websocket else "n/a"
        path = user.get("path", "/xhttps") if user["transport"] in {"xhttp", "websocket"} else "n/a"
        padding = user.get("padding", "100-1000") if user["transport"] == "xhttp" else "n/a"
        fp = user.get("fingerprint", "firefox") if not is_socks5 and not is_websocket else "n/a"
        encryption = user.get("encryption_mode", "none")
        restart = "Need restart for updating changes" if user.get("restart_required") else "Not required"
        limit = user.get("device_limit", 0)
        limit_text = "Unlimited" if limit == 0 else str(limit)
        
        text = self.strings["user_settings"].format(
            name=_escape(name),
            transport=transport,
            sni=_escape(sni),
            dest=_escape(dest),
            path=_escape(path),
            padding=padding,
            fp=fp,
            encryption=_escape(encryption),
            restart=restart,
            limit=limit_text,
        )
        
        markup = [
            [{"text": self.strings["btn_transport"], "callback": self._cb_transport_menu, "args": (name,), "style": "primary"}],
        ]
        
        if not is_socks5:
            markup.append([{"text": self.strings["btn_encryption"], "callback": self._cb_encryption_menu, "args": (name,), "style": "primary"}])
            markup.append([{"text": self.strings["btn_set_sni"], "input": self.strings["input_sni"], "handler": self._cb_set_sni, "args": (name,), "style": "primary"}])
            markup.append([{"text": self.strings["btn_set_dest"], "input": self.strings["input_dest"], "handler": self._cb_set_dest, "args": (name,), "style": "primary"}])
            
            if user["transport"] == "xhttp":
                markup.append([{"text": self.strings["btn_set_path"], "input": self.strings["input_path"], "handler": self._cb_set_path, "args": (name,), "style": "primary"}])
                markup.append([{"text": self.strings["btn_set_padding"], "callback": self._cb_padding_menu, "args": (name,), "style": "primary"}])
            
            markup.append([{"text": self.strings["btn_set_fp"], "callback": self._cb_fp_menu, "args": (name,), "style": "primary"}])
        
        markup.append([{"text": self.strings["btn_set_limit"], "input": self.strings["input_limit"], "handler": self._cb_set_limit, "args": (name,), "style": "primary"}])
        markup.append([{"text": self.strings["btn_back"], "callback": self._cb_user_menu, "args": (name,), "style": "primary"}])
        
        await call.edit(text, reply_markup=markup)

    async def _cb_encryption_menu(self, call: InlineCall, name: str):
        user = self._users.get(name)
        if not user:
            return
        await call.edit(
            f"<b>VLESS encryption: {_escape(user.get('encryption_mode', 'none'))}</b>\n"
            "<blockquote>ML-KEM-768 is generated by the installed Xray core. "
            "Fallback mode is available only for WebSocket.</blockquote>",
            reply_markup=[
                [{"text": "ML-KEM-768", "callback": self._cb_set_encryption, "args": (name, "ml-kem-768"), "style": "primary"}],
                [{"text": "None", "callback": self._cb_set_encryption, "args": (name, "none"), "style": "primary"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_user_settings, "args": (name,), "style": "primary"}],
            ],
        )

    async def _cb_set_encryption(self, call: InlineCall, name: str, mode: str):
        user = self._users.get(name)
        if not user:
            return
        if mode == "ml-kem-768":
            dec, enc = await self._generate_vless_encryption()
            if not dec or not enc:
                await call.answer("ML-KEM-768 is not supported by the installed Xray core", show_alert=True)
                return
            user["vless_decryption"], user["vless_encryption"] = dec, enc
        else:
            user["vless_decryption"], user["vless_encryption"] = "none", "none"
        user["encryption_mode"] = mode
        if user.get("transport") == "websocket":
            user["websocket_mode"] = "ml-kem-768" if mode == "ml-kem-768" else "tls-fallback"
        await self._mark_restart_required(name)
        await self._cb_user_settings(call, name)

    async def _cb_transport_menu(self, call: InlineCall, name: str):
        user = self._users.get(name)
        if not user:
            await call.answer("User not found", show_alert=True)
            return
        
        markup = [
            [{"text": self.strings["btn_raw"], "callback": self._cb_set_transport, "args": (name, "tcp"), "style": "primary"}],
            [{"text": self.strings["btn_xhttp"], "callback": self._cb_set_transport, "args": (name, "xhttp"), "style": "primary"}],
            [{"text": self.strings["btn_websocket"], "callback": self._cb_set_transport, "args": (name, "websocket"), "style": "primary"}],
            [{"text": self.strings["btn_socks5"], "callback": self._cb_set_transport, "args": (name, "socks5"), "style": "primary"}],
            [{"text": self.strings["btn_back"], "callback": self._cb_user_settings, "args": (name,), "style": "primary"}],
        ]
        
        await call.edit(
            self.strings["transport_menu"].format(name=_escape(name), current=user["transport"].upper()),
            reply_markup=markup,
        )

    async def _cb_set_transport(self, call: InlineCall, name: str, transport: str):
        user = self._users.get(name)
        if not user:
            await call.answer("User not found", show_alert=True)
            return
        
        if user["transport"] == transport:
            await self._cb_user_settings(call, name)
            return
        
        await call.edit(self.strings["loading"])
        if name in self._processes:
            await self._stop_user(name, reason="configuration changed")
        user["transport"] = transport
        
        if transport == "socks5" and not user.get("socks_user"):
            user["socks_user"] = _gen_secret(8)
            user["socks_pass"] = _gen_secret(14)
        
        if transport == "websocket":
            user.pop("tunnel_host", None)
            user.pop("site_port", None)
            if not user.get("path"):
                user["path"] = "/xhttps"

        user["restart_required"] = True
        self._save_users()
        
        await call.edit(
            self.strings["transport_set"].format(transport=transport.upper()),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_user_settings, "args": (name,), "style": "primary"}]]
        )

    async def _cb_set_sni(self, call: InlineCall, sni: str, name: str):
        user = self._users.get(name)
        if not user:
            return
        
        user["sni"] = _strip_md(sni).strip().lower()
        await self._mark_restart_required(name)
        
        await call.edit(
            self.strings["sni_set"].format(sni=_escape(sni)),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_user_settings, "args": (name,), "style": "primary"}]]
        )

    async def _cb_set_dest(self, call: InlineCall, dest: str, name: str):
        user = self._users.get(name)
        if not user:
            return
        
        dest = _strip_md(dest).strip()
        if ":" not in dest:
            dest += ":443"
        
        user["dest"] = dest
        await self._mark_restart_required(name)
        
        await call.edit(
            self.strings["dest_set"].format(dest=_escape(dest)),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_user_settings, "args": (name,), "style": "primary"}]]
        )

    async def _cb_set_path(self, call: InlineCall, path: str, name: str):
        user = self._users.get(name)
        if not user:
            return
        
        path = path.strip()
        if not path.startswith("/"):
            path = "/" + path
        
        user["path"] = path
        await self._mark_restart_required(name)
        
        await call.edit(
            self.strings["path_set"].format(path=_escape(path)),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_user_settings, "args": (name,), "style": "primary"}]]
        )

    async def _cb_padding_menu(self, call: InlineCall, name: str):
        user = self._users.get(name)
        if not user:
            return
        
        padding = user.get("padding", "100-1000")
        min_p, max_p = padding.split("-")
        
        text = self.strings["padding_menu"].format(
            name=_escape(name),
            min=min_p,
            max=max_p,
        )
        
        markup = [
            [{"text": self.strings["input_padding_min"], "input": self.strings["input_padding_min"], "handler": self._cb_set_padding_min, "args": (name,), "style": "primary"}],
            [{"text": self.strings["input_padding_max"], "input": self.strings["input_padding_max"], "handler": self._cb_set_padding_max, "args": (name,), "style": "primary"}],
            [{"text": self.strings["btn_back"], "callback": self._cb_user_settings, "args": (name,), "style": "primary"}],
        ]
        
        await call.edit(text, reply_markup=markup)

    async def _cb_set_padding_min(self, call: InlineCall, min_str: str, name: str):
        user = self._users.get(name)
        if not user:
            return
        
        try:
            min_val = int(min_str.strip())
        except:
            await call.answer(self.strings["err_invalid_limit"], show_alert=True)
            return
        
        padding = user.get("padding", "100-1000")
        _, max_val = padding.split("-")
        max_val = int(max_val)
        
        if min_val >= max_val:
            await call.answer(self.strings["err_invalid_padding"], show_alert=True)
            return
        
        user["padding"] = f"{min_val}-{max_val}"
        self._save_users()
        
        await self._cb_padding_menu(call, name)

    async def _cb_set_padding_max(self, call: InlineCall, max_str: str, name: str):
        user = self._users.get(name)
        if not user:
            return
        
        try:
            max_val = int(max_str.strip())
        except:
            await call.answer(self.strings["err_invalid_limit"], show_alert=True)
            return
        
        padding = user.get("padding", "100-1000")
        min_val, _ = padding.split("-")
        min_val = int(min_val)
        
        if max_val <= min_val:
            await call.answer(self.strings["err_invalid_padding"], show_alert=True)
            return
        
        user["padding"] = f"{min_val}-{max_val}"
        self._save_users()
        
        await self._cb_padding_menu(call, name)

    async def _cb_fp_menu(self, call: InlineCall, name: str):
        markup = [
            [{"text": self.strings["btn_chrome"], "callback": self._cb_set_fp, "args": (name, "chrome"), "style": "primary"}],
            [{"text": self.strings["btn_firefox"], "callback": self._cb_set_fp, "args": (name, "firefox"), "style": "primary"}],
            [{"text": self.strings["btn_safari"], "callback": self._cb_set_fp, "args": (name, "safari"), "style": "primary"}],
            [{"text": self.strings["btn_back"], "callback": self._cb_user_settings, "args": (name,), "style": "primary"}],
        ]
        
        await call.edit(
            f"<b>Select Fingerprint</b>\n<blockquote>Current: {self._users[name].get('fingerprint', 'firefox')}</blockquote>",
            reply_markup=markup
        )

    async def _cb_set_fp(self, call: InlineCall, name: str, fp: str):
        user = self._users.get(name)
        if not user:
            return
        
        user["fingerprint"] = fp
        await self._mark_restart_required(name)
        
        await call.edit(
            self.strings["fp_set"].format(fp=fp),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_user_settings, "args": (name,), "style": "primary"}]]
        )

    async def _cb_set_limit(self, call: InlineCall, limit_str: str, name: str):
        user = self._users.get(name)
        if not user:
            return
        
        try:
            limit = int(limit_str.strip())
            if limit < 0:
                raise ValueError
        except:
            await call.answer(self.strings["err_invalid_limit"], show_alert=True)
            return
        
        user["device_limit"] = limit
        self._save_users()
        
        limit_text = "Unlimited" if limit == 0 else str(limit)
        
        await call.edit(
            self.strings["limit_set"].format(limit=limit_text),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_user_settings, "args": (name,), "style": "primary"}]]
        )

    async def _cb_add_user_name(self, call: InlineCall):
        await call.edit(
            self.strings["add_user_name"],
            reply_markup=[[
                {
                    "text": self.strings["input_name"],
                    "input": self.strings["input_name"],
                    "handler": self._cb_add_user_transport_choice,
                    "style": "primary",
                }
            ]]
        )

    async def _cb_add_user_transport_choice(self, call: InlineCall, name: str):
        name = name.strip()
        
        if not re.match(r"^[a-zA-Z0-9_-]+$", name):
            await call.answer(self.strings["err_invalid_name"], show_alert=True)
            return
        
        if name in self._users:
            await call.answer(self.strings["err_name_exists"], show_alert=True)
            return
        if os.path.exists(os.path.join(self._systemd_user_dir, self._unit_name(name))):
            await call.answer("A systemd service with this username already exists", show_alert=True)
            return
        
        text = self.strings["add_user_transport"].format(name=_escape(name))
        
        markup = [
            [{"text": self.strings["btn_xhttp"], "callback": self._cb_add_user_limit_input, "args": (name, "xhttp"), "style": "primary"}],
            [{"text": self.strings["btn_raw"], "callback": self._cb_add_user_limit_input, "args": (name, "tcp"), "style": "primary"}],
            [{"text": self.strings["btn_websocket"], "callback": self._cb_add_user_limit_input, "args": (name, "websocket"), "style": "primary"}],
            [{"text": self.strings["btn_socks5"], "callback": self._cb_add_user_limit_input, "args": (name, "socks5"), "style": "primary"}],
            [{"text": self.strings["btn_back"], "callback": self._cb_users_menu, "style": "primary"}],
        ]
        
        await call.edit(text, reply_markup=markup)

    async def _cb_add_user_limit_input(self, call: InlineCall, name: str, transport: str):
        text = self.strings["add_user_limit"].format(name=_escape(name))
        
        await call.edit(
            text,
            reply_markup=[[
                {
                    "text": self.strings["input_limit"],
                    "input": self.strings["input_limit"],
                    "handler": self._cb_add_user_encryption_choice,
                    "args": (name, transport),
                    "style": "primary",
                }
            ]]
        )

    async def _cb_add_user_encryption_choice(self, call: InlineCall, limit_str: str, name: str, transport: str):
        try:
            limit = int(limit_str.strip())
            if limit < 0:
                raise ValueError
        except Exception:
            await call.answer(self.strings["err_invalid_limit"], show_alert=True)
            return
        if transport != "websocket":
            await self._cb_create_user_final(call, str(limit), name, transport, "ml-kem-768")
            return
        await call.edit(
            "<b>WebSocket security</b>\n<blockquote>Select an endpoint mode:</blockquote>",
            reply_markup=[
                [{"text": "Use TLS with fallback", "callback": self._cb_create_user_final, "args": (str(limit), name, transport, "tls-fallback"), "style": "primary"}],
                [{"text": "Use post-quantum encryption", "callback": self._cb_create_user_final, "args": (str(limit), name, transport, "ml-kem-768"), "style": "primary"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_add_user_transport_choice, "args": (name,), "style": "primary"}],
            ],
        )

    async def _cb_create_user_final(self, call: InlineCall, limit_str: str, name: str, transport: str, encryption_mode: str = "ml-kem-768"):
        try:
            limit = int(limit_str.strip())
            if limit < 0:
                raise ValueError
        except:
            await call.answer(self.strings["err_invalid_limit"], show_alert=True)
            return
        
        await call.edit(self.strings["loading"])
        
        if not self._xray_installed():
            await call.edit(
                self.strings["setup_fail"].format(error="XRay not installed. Use Setup menu to install."),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_users_menu, "style": "primary"}]]
            )
            return
        
        private_key, public_key = "", ""
        vless_decryption, vless_encryption = "none", "none"
        if transport != "socks5":
            private_key, public_key = await self._generate_x25519()
            if not private_key or not public_key:
                await call.edit(
                    self.strings["setup_fail"].format(error="Key generation failed"),
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_users_menu, "style": "primary"}]]
                )
                return
            if encryption_mode == "ml-kem-768":
                vless_decryption, vless_encryption = await self._generate_vless_encryption()
            if encryption_mode == "ml-kem-768" and (not vless_decryption or not vless_encryption):
                await call.edit(
                    self.strings["setup_fail"].format(error="ML-KEM-768 VLESS encryption generation failed"),
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_users_menu, "style": "primary"}]]
                )
                return
        
        port = await self._get_next_port()
        
        if transport == "tcp":
            default_sni = "www.cloudflare.com"
            default_dest = "www.cloudflare.com:443"
        else:
            default_sni = "www.cloudflare.com"
            default_dest = "www.cloudflare.com:443"

        user = {
            "name": name,
            "uuid": str(uuid.uuid4()),
            "port": port,
            "transport": transport,
            "device_limit": limit,
            "private_key": private_key,
            "public_key": public_key,
            "short_id": self._generate_short_id(),
            "vless_decryption": vless_decryption,
            "vless_encryption": vless_encryption,
            "encryption_mode": "none" if transport == "socks5" else encryption_mode,
            "websocket_mode": encryption_mode if transport == "websocket" else "",
            "mask_site": "Evil Cat",
            "sni": default_sni,
            "dest": default_dest,
            "path": "/xhttps",
            "padding": "100-1000",
            "fingerprint": "firefox",
            "socks_user": _gen_secret(8),
            "socks_pass": _gen_secret(14),
            "sub_token": _gen_secret(SUB_TOKEN_LEN),
            "autostart": False,
            "start_time": 0,
            "services": {
                "xray": self._unit_name(name),
                "site": self._unit_name(name, "site"),
                "cf_tunnel": self._unit_name(name, "cf"),
            },
        }
        
        self._users[name] = user
        self._save_users()
        
        limit_text = "Unlimited" if limit == 0 else str(limit)
        
        await call.edit(
            self.strings["user_created"].format(
                name=_escape(name),
                port=port,
                transport=transport.upper(),
                limit=limit_text,
            ),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_users_menu, "style": "primary"}]]
        )

    async def _cb_sub_menu(self, call: InlineCall):
        self._sub_ensure()
        targets = self._sub_targets()
        up, down = self._sub_traffic_for()
        running = bool(self._sub_server)

        markup = [
            [{"text": self.strings["btn_sub_toggle"], "callback": self._cb_sub_toggle, "style": "primary"}],
        ]

        if self._sub.get("enabled"):
            markup.append([
                {"text": self.strings["btn_sub_url"], "callback": self._cb_sub_url, "style": "primary"},
                {"text": self.strings["btn_sub_users"], "callback": self._cb_sub_users, "style": "primary"},
            ])
            markup.append([
                {"text": self.strings["btn_sub_port"], "callback": self._cb_sub_port, "style": "primary"},
                {"text": self.strings["btn_sub_path"], "callback": self._cb_sub_path, "style": "primary"},
            ])
            markup.append([
                {"text": self.strings["btn_sub_title"], "callback": self._cb_sub_title, "style": "primary"},
                {"text": self.strings["btn_sub_interval"], "callback": self._cb_sub_interval, "style": "primary"},
            ])
            markup.append([
                {"text": self.strings["btn_sub_stats"], "callback": self._cb_sub_stats, "style": "primary"},
                {"text": self.strings["btn_sub_reset"], "callback": self._cb_sub_reset, "style": "danger"},
            ])
            markup.append([
                {"text": self.strings["btn_sub_apply"], "callback": self._cb_sub_apply, "style": "primary"},
            ])

        markup.append([{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}])

        text = self.strings["sub_menu"].format(
                status=self.strings["sub_on"] if running else self.strings["sub_off"],
                port=self._sub.get("port", 0),
                path=_escape(self._sub_masked_path()),
                links=len(targets),
                traffic=f"{_fmt_bytes(up)} / {_fmt_bytes(down)}",
                stats=self.strings["sub_on"] if self._sub.get("stats") else self.strings["sub_off"],
                interval=self._sub.get("interval", 12),
                title=_escape(self._sub.get("title") or "-"),
        )

        if _in_docker():
            text = text.replace("</blockquote>", self.strings["sub_docker_warn"] + "</blockquote>")

        await call.edit(text, reply_markup=markup)

    async def _cb_sub_toggle(self, call: InlineCall):
        await call.edit(self.strings["loading"], reply_markup=[])

        if self._sub.get("enabled"):
            await self._sub_disable()
            await call.edit(
                self.strings["sub_stopped"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_sub_menu, "style": "primary"}]],
            )
            return

        if not self._external_ip:
            self._external_ip = await self._detect_external_ip()
        if not self._external_ip:
            await call.edit(
                self.strings["sub_no_ip"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_sub_menu, "style": "primary"}]],
            )
            return

        ok, err = await self._sub_enable()
        if not ok:
            await call.edit(
                self.strings["sub_err_start"].format(error=_escape(str(err)[:200])),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_sub_menu, "style": "primary"}]],
            )
            return

        await call.edit(
            self.strings["sub_started"].format(
                port=self._sub.get("port", 0),
                path=_escape(self._sub_masked_path()),
                hint=self.strings["sub_apply_hint"],
            ),
            reply_markup=[
                [{"text": self.strings["btn_sub_apply"], "callback": self._cb_sub_apply, "style": "primary"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_sub_menu, "style": "primary"}],
            ],
        )

    async def _cb_sub_url(self, call: InlineCall):
        if not self._sub_targets():
            await call.edit(
                self.strings["sub_empty"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_sub_menu, "style": "primary"}]],
            )
            return

        url = self._sub_url()
        if not url:
            await call.edit(
                self.strings["sub_no_ip"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_sub_menu, "style": "primary"}]],
            )
            return

        json_url = f"{url}?format=json"
        info_url = f"{url}?format=info"

        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", prefix="sub_", delete=False)
        try:
            tmp.write(url + "\n" + json_url + "\n" + info_url + "\n")
            tmp.close()
            try:
                await self._send_file_to_call_chat(call, tmp.name, force_document=True)
            except Exception as exc:
                logger.error(f"[XR] subscription url send failed: {exc}")
        finally:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass

        await call.edit(
            self.strings["sub_url_msg"].format(url=_escape(url), url_json=_escape(json_url)),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_sub_menu, "style": "primary"}]],
        )

    async def _cb_sub_users(self, call: InlineCall):
        markup = []
        for name, user, _ in self._sub_targets():
            label = f"{name} ({self._transport_label(user.get('transport', ''))})"
            markup.append([{
                "text": label,
                "callback": self._cb_sub_user,
                "args": (name,),
                "style": "primary",
            }])

        if not markup:
            await call.edit(
                self.strings["sub_empty"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_sub_menu, "style": "primary"}]],
            )
            return

        markup.append([{"text": self.strings["btn_back"], "callback": self._cb_sub_menu, "style": "primary"}])
        await call.edit(self.strings["sub_users_menu"], reply_markup=markup)

    async def _cb_sub_user(self, call: InlineCall, name: str):
        user = self._users.get(name)
        if not user:
            await call.answer("Not found", show_alert=True)
            return

        token = self._sub_user_token(name)
        up, down = self._sub_traffic_for(name)

        await call.edit(
            self.strings["sub_user_msg"].format(
                name=_escape(name),
                transport=_escape(self._transport_label(user.get("transport", ""))),
                port=user.get("port", 0),
                traffic=f"{_fmt_bytes(up)} / {_fmt_bytes(down)}",
                url=_escape(self._sub_url(token)),
            ),
            reply_markup=[
                [{"text": self.strings["btn_sub_regen"], "callback": self._cb_sub_regen, "args": (name,), "style": "danger"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_sub_users, "style": "primary"}],
            ],
        )

    async def _cb_sub_regen(self, call: InlineCall, name: str):
        user = self._users.get(name)
        if user:
            user["sub_token"] = _gen_secret(SUB_TOKEN_LEN)
            self._save_users()
        await self._cb_sub_user(call, name)

    async def _cb_sub_port(self, call: InlineCall):
        await call.edit(self.strings["loading"], reply_markup=[])

        port = self._sub_free_port(self._sub_taken_ports())
        self._sub["port"] = port
        self._sub_save()

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._sub_stop_server)
        ok, err = self._sub_start_server()
        if not ok:
            self._sub["enabled"] = False
            self._sub_save()
            await self._sub_stop_task()
            await call.edit(
                self.strings["sub_err_start"].format(error=_escape(str(err)[:200])),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_sub_menu, "style": "primary"}]],
            )
            return

        await call.edit(
            self.strings["sub_port_done"].format(port=port),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_sub_menu, "style": "primary"}]],
        )

    async def _cb_sub_path(self, call: InlineCall):
        self._sub["prefix"] = _gen_secret(SUB_TOKEN_LEN)
        self._sub_save()
        await call.edit(
            self.strings["sub_path_done"].format(path=_escape(self._sub_masked_path())),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_sub_menu, "style": "primary"}]],
        )

    async def _cb_sub_stats(self, call: InlineCall):
        self._sub["stats"] = not self._sub.get("stats")
        self._sub_save()
        await self._cb_sub_menu(call)

    async def _cb_sub_reset(self, call: InlineCall):
        self._sub["traffic"] = {}
        self._sub["baseline"] = {}
        self._sub_save()
        await call.edit(
            self.strings["sub_reset_done"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_sub_menu, "style": "primary"}]],
        )

    async def _cb_sub_apply(self, call: InlineCall):
        await call.edit(self.strings["loading"], reply_markup=[])

        restarted = 0
        failed = 0

        for name in list(self._processes.keys()):
            await self._stop_user(name, reason="rebuild")
            ok, _ = await self._start_user(name)
            if ok:
                restarted += 1
            else:
                failed += 1
                user = self._users.get(name)
                if user is not None:
                    user["restart_required"] = True

        self._save_users()

        await call.edit(
            self.strings["sub_apply_done"].format(restarted=restarted, pending=failed),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_sub_menu, "style": "primary"}]],
        )

    async def _cb_sub_interval(self, call: InlineCall):
        await call.edit(
            self.strings["sub_input_interval"],
            reply_markup=[[
                {
                    "text": self.strings["btn_sub_interval"],
                    "input": self.strings["sub_input_interval"],
                    "handler": self._cb_set_sub_interval,
                    "style": "primary",
                }
            ]],
        )

    async def _cb_set_sub_interval(self, call: InlineCall, raw: str):
        try:
            interval = int(str(raw).strip())
        except (TypeError, ValueError):
            await call.answer(self.strings["err_invalid_limit"], show_alert=True)
            return

        if interval < 1 or interval > 168:
            await call.answer(self.strings["err_invalid_limit"], show_alert=True)
            return

        self._sub["interval"] = interval
        self._sub_save()

        await call.edit(
            self.strings["sub_interval_set"].format(interval=interval),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_sub_menu, "style": "primary"}]],
        )

    async def _cb_sub_title(self, call: InlineCall):
        await call.edit(
            self.strings["sub_input_title"],
            reply_markup=[[
                {
                    "text": self.strings["btn_sub_title"],
                    "input": self.strings["sub_input_title"],
                    "handler": self._cb_set_sub_title,
                    "style": "primary",
                }
            ]],
        )

    async def _cb_set_sub_title(self, call: InlineCall, raw: str):
        self._sub["title"] = _strip_md(str(raw)).strip()[:64]
        self._sub_save()
        await call.edit(
            self.strings["sub_title_set"].format(title=_escape(self._sub["title"] or "-")),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_sub_menu, "style": "primary"}]],
        )

    async def _cb_sub_user_link(self, call: InlineCall, name: str):
        user = self._users.get(name)
        if not user:
            await call.answer("Not found", show_alert=True)
            return

        if not self._sub.get("enabled"):
            await call.edit(
                self.strings["sub_stopped"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_user_menu, "args": (name,), "style": "primary"}]],
            )
            return

        if user.get("transport") == "socks5":
            await call.edit(
                self.strings["sub_empty"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_user_menu, "args": (name,), "style": "primary"}]],
            )
            return

        await self._cb_sub_user(call, name)

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    @loader.command(
        ru_doc="- XRay многопользовательский VPN менеджер",
        en_doc="- XRay multi-user VPN manager",
    )
    async def xr(self, message):
        """- XRay multi-user VPN manager"""
        if await self._guard_deny(message):
            return
        await self._reattach_processes()
        active_cnt = await self._get_active_count()
        await self.inline.form(
            text=self.strings["main_menu"].format(
                total=len(self._users),
                active=active_cnt,
                version=await self._get_version("xray"),
                cloudflared_version=await self._get_version("cloudflared"),
            ),
            message=message,
            reply_markup=[
                [{
                    "text": self.strings["btn_users"],
                    "callback": self._cb_users_menu,
                    "style": "primary",
                }],
                [{
                    "text": self.strings["btn_sub"],
                    "callback": self._cb_sub_menu,
                    "style": "primary",
                }],
                [{
                    "text": self.strings["btn_setup"],
                    "callback": self._cb_setup_menu,
                    "style": "primary",
                }],
                [{
                    "text": self.strings["btn_close"],
                    "callback": self._cb_close,
                    "style": "danger",
                }],
            ],
            silent=True,
        )