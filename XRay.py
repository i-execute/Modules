# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (7, 0, 0)
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
import ssl
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
import urllib.error
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple

from telethon.tl.functions.messages import EditMessageRequest, EditForumTopicRequest
from telethon.tl.types import InputMediaWebPage

from .. import loader, utils
from ..inline.types import InlineCall

ASSET_BASE = os.environ.get("XRAY_ASSET_BASE", "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/XRay")
RELOADING_MEDIA_URL = ASSET_BASE.rstrip("/") + "/Reloading.jpeg"
GREETING_MEDIA_URL = ASSET_BASE.rstrip("/") + "/Greetings.jpeg"
logger = logging.getLogger(__name__)

async def _retry_topic_send(send, refresh):
    try:
        return await send()
    except Exception:
        await refresh()
        return await send()

LOG_TRIM_SIZE = 10 * 1024 * 1024
BASE_PORT = 8443
AUTOSTART_INTERVAL = 10
UNLOAD_WAIT = 5.0
SOCKS_EVENT_GAP = 300.0

GATE_TEMPLATE = r"""import asyncio
import ipaddress

ALLOWED = __ALLOWED__
BACKEND_PORT = __BACKEND_PORT__
LISTEN_PORT = __LISTEN_PORT__
BUFFER = 65536

nets = [ipaddress.ip_network(a, strict=False) for a in ALLOWED]

def allowed(peer):
    try:
        addr = ipaddress.ip_address(peer)
    except ValueError:
        return False
    return any(peer_net.supernet_of(ipaddress.ip_network(addr)) if False else addr in peer_net for peer_net in nets)

async def pump(reader, writer):
    try:
        while True:
            chunk = await reader.read(BUFFER)
            if not chunk:
                break
            writer.write(chunk)
            await writer.drain()
    except Exception:
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass

async def handle(reader, writer):
    peer = writer.get_extra_info("peername")
    ip = peer[0] if peer else ""
    if not ip or not allowed(ip):
        print("gate block " + (ip or "?"), flush=True)
        try:
            writer.close()
        except Exception:
            pass
        return
    print("gate allow " + ip, flush=True)
    try:
        back_reader, back_writer = await asyncio.open_connection("127.0.0.1", BACKEND_PORT)
    except Exception:
        writer.close()
        return
    await asyncio.gather(pump(reader, back_writer), pump(back_reader, writer))

async def main():
    server = await asyncio.start_server(handle, "0.0.0.0", LISTEN_PORT)
    async with server:
        await server.serve_forever()

asyncio.run(main())
"""
GUARD_ROOT = "root"
GUARD_DOCKER = "docker"
XRAY_CORE_VERSION = "v26.10.31"
XRAY_CORE_SHA256 = {
    "64": "e4bd9119f50ae7c41658b1abd0679ae06f98bd864a2907ff33f2f2cf7081b214",
    "arm64-v8a": "1811c8f1da0c3b6dc707ebaf1c8390e987ba39859a8f0a7aadd8526157f2ec8d",
}
XRAY_MIN_VERSION = (26, 10, 31)
XRAY_MIN_TAG = "v26.10.31"
XRAY_MIN_ADVISORY = "CVE-2026-98432"
RELEASE_HOSTS = ("github.com", "objects.githubusercontent.com", "release-assets.githubusercontent.com", "api.github.com")

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

def _version_tuple(text) -> tuple:
    for token in re.split(r"[^0-9A-Za-z.]+", str(text or "")):
        parts = token.lstrip("vV").split(".")
        if len(parts) < 2 or not parts[0].isdigit():
            continue
        try:
            numbers = [int(part) for part in parts[:3]]
        except ValueError:
            continue
        return tuple(numbers + [0] * (3 - len(numbers)))
    return ()

def _core_too_old(tag) -> bool:
    version = _version_tuple(tag)
    if not version:
        return False
    return version < XRAY_MIN_VERSION

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
    """Multi-user VPN: VLESS+Reality (XHTTP/TCP(RAW)+Vision, post-quantum encryption) and SOCKS5 with an IP-whitelist firewall"""

    strings = {
        "name": "XRay",
        "guard_title": "<b>XRay is disabled on this server</b>",
        "guard_root": "The userbot is running as root (uid 0). XRay refuses to start in this mode.",
        "guard_docker": "The userbot is running inside a container (Docker, Podman, LXC or Kubernetes). XRay refuses to start in this mode.",
        "guard_why": "Why: this module starts long living daemons and holds VPN keys. Under root a single bug is a full server takeover, and inside a container the listeners escape the isolation the container promises. A userbot is not a system service.",
        "guard_how": "What to do: run the userbot as a regular user with a working <code>systemctl --user</code> session, the module creates only user units. Keep root off limits and do not run it in Docker.",
        "reloaded": "<blockquote><b>XRay reloaded</b></blockquote>\n<blockquote>Version: <b>{version}</b>\nUsers: <b>{users}</b>\nLog file limit: <b>{log_size} MB</b></blockquote>",
        "greeting": "<blockquote><b>XRay is ready</b></blockquote>\n<blockquote>Version: <b>{version}</b>\nConfigured users: <b>{users}</b>\nLog file limit: <b>{log_size} MB</b>\nUse <code>{prefix}xray</code> to manage the service.</blockquote>",

        "main_menu": (
            "<b>XRay Multi-User VPN</b>\n"
            "<blockquote>Total users: {total}\n"
            "Active: {active}\n"
            "XRay version: {version}\n"
            "</blockquote>"
        ),

        "setup_menu": (
            "<b>Setup & Installation</b>\n"
            "<blockquote>"
            "XRay Core: {xray_status}\n"
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
            "Port: {port}\n"
            "Autostart: {autostart}\n"
            "Device limit: {limit}\n"
            "Active devices: {active}\n"
            "Uptime: {uptime}"
            "</blockquote>"
        ),

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
        "btn_firewall": "Firewall",
        "fw_menu": "<b>Firewall: {name}</b>\n<blockquote>Whitelist mode: <b>{state}</b>\nWhen it is on, the proxy works without a password and only the addresses below can connect. Everyone else is dropped.\n\n{ips}</blockquote>",
        "fw_state_on": "on",
        "fw_state_off": "off",
        "fw_empty_hint": "the list is empty, nobody passes",
        "btn_fw_toggle_on": "Whitelist: ON",
        "btn_fw_toggle_off": "Whitelist: OFF",
        "btn_fw_add": "Add addresses",
        "btn_fw_del": "Remove",
        "input_fw_add": "Send IPv4/IPv6 or CIDR, several separated by spaces",
        "fw_bad_ip": "Bad address: {value}",
        "log_socks_conn": "<b>SOCKS5 connection</b>\n<blockquote>User: <code>{name}</code>\nClient: <code>{ip}</code>\nTarget: <code>{target}</code></blockquote>",
        "log_socks_gate": "<b>SOCKS5 firewall</b>\n<blockquote>User: <code>{name}</code>\nClient: <code>{ip}</code>\nVerdict: {action}</blockquote>",
        "log_socks_allowed": "<b>allowed</b>",
        "log_socks_blocked": "<b>blocked</b>",
        "socks5_sent_fw": "<b>SOCKS5: {name}</b>\n<blockquote>Host: {ip}\nPort: {port}\nLogin: none needed\n\nURL: <code>socks5://{ip}:{port}</code>\nWhitelist: {ips}</blockquote>",
        "btn_settings": "Settings",
        "btn_delete": "Delete User",
        "btn_xhttp": "XHTTP",
        "btn_raw": "RAW",
        "btn_set_sni": "Set SNI",
        "btn_set_dest": "Set Dest",
        "btn_set_path": "Set Path",
        "btn_set_padding": "Padding",
        "btn_set_fp": "Fingerprint",
        "btn_set_limit": "Device Limit",
        "btn_encryption": "Encryption",
        "btn_socks5": "SOCKS5",
        "btn_transport": "Transport",
        "core_old_warn": (
            "<blockquote><b>Vulnerable core</b> | <code>{version}</code>\n"
            "Anything below <b>{min}</b> keeps MPTCP listeners open and gives a remote root "
            "through {advisory}, press install to move to the pinned release</blockquote>"
        ),
        "core_old_hidden": "<blockquote>{count} releases below <b>{min}</b> are hidden, they are vulnerable to {advisory}</blockquote>",
        "btn_autostart_on": "Autostart: On",
        "btn_autostart_off": "Autostart: Off",
        "btn_chrome": "Chrome",
        "btn_firefox": "Firefox",
        "btn_safari": "Safari",
        "btn_install_xray": "Install XRay Core",
        "btn_reinstall_xray": "Reinstall XRay Core",
        "cf_no_releases": "<blockquote>The release list is unreachable, only the pinned {version} is offered</blockquote>",
        "cf_installed_mark": " (installed)",
        "not_installed": "Not installed",

        "input_name": "Enter username:",
        "input_limit": "Enter device limit:",
        "input_sni": "Enter SNI (e.g. www.microsoft.com):",
        "input_dest": "Enter dest (e.g. www.microsoft.com:443):",
        "input_path": "Enter path (e.g. /xhttps):",
        "input_padding_min": "Enter minimum padding bytes:",
        "input_padding_max": "Enter maximum padding bytes:",

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




















    }

    strings_ru = {
        "name": "XRay",
        "guard_title": "<b>XRay отключён на этом сервере</b>",
        "guard_root": "Юзербот запущен от root (uid 0). XRay в таком режиме работать отказывается.",
        "guard_docker": "Юзербот запущен внутри контейнера (Docker, Podman, LXC или Kubernetes). XRay в таком режиме работать отказывается.",
        "guard_why": "Почему: модуль поднимает долгоживущие демоны и держит ключи VPN. Под root одна ошибка это полный захват сервера, а внутри контейнера слушающие порты выходят за границы обещанной изоляции. Юзербот не является системной службой.",
        "guard_how": "Что делать: запустите юзербот обычным пользователем с рабочей сессией <code>systemctl --user</code>, модуль создаёт только пользовательские юниты. Root не использовать, в Docker не запускать.",
        "reloaded": "<blockquote><b>XRay перезагружен</b></blockquote>\n<blockquote>Версия: <b>{version}</b>\nПользователей: <b>{users}</b>\nЛимит файла лога: <b>{log_size} MB</b></blockquote>",
        "greeting": "<blockquote><b>XRay готов к работе</b></blockquote>\n<blockquote>Версия: <b>{version}</b>\nНастроено пользователей: <b>{users}</b>\nЛимит файла лога: <b>{log_size} MB</b>\nДля управления используйте <code>{prefix}xray</code>.</blockquote>",
        "main_menu": (
            "<b>XRay Мультиюзерный VPN</b>\n"
            "<blockquote>Всего юзеров: {total}\n"
            "Активных: {active}\n"
            "Версия XRay: {version}\n"
            "</blockquote>"
        ),

        "setup_menu": (
            "<b>Установка и настройка</b>\n"
            "<blockquote>"
            "XRay Core: {xray_status}\n"
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
            "Порт: {port}\n"
            "Автозапуск: {autostart}\n"
            "Лимит устройств: {limit}\n"
            "Активных устройств: {active}\n"
            "Аптайм: {uptime}"
            "</blockquote>"
        ),

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
        "btn_firewall": "Файервол",
        "fw_menu": "<b>Файервол: {name}</b>\n<blockquote>Белый список: <b>{state}</b>\nКогда включён, прокси работает без пароля, и подключиться могут только адреса из списка. Остальные отбрасываются.\n\n{ips}</blockquote>",
        "fw_state_on": "включён",
        "fw_state_off": "выключен",
        "fw_empty_hint": "список пуст, никто не проходит",
        "btn_fw_toggle_on": "Белый список: ВКЛ",
        "btn_fw_toggle_off": "Белый список: ВЫКЛ",
        "btn_fw_add": "Добавить адреса",
        "btn_fw_del": "Убрать",
        "input_fw_add": "Пришлите IPv4/IPv6 или CIDR, несколько через пробел",
        "fw_bad_ip": "Неверный адрес: {value}",
        "log_socks_conn": "<b>Подключение SOCKS5</b>\n<blockquote>Юзер: <code>{name}</code>\nКлиент: <code>{ip}</code>\nЦель: <code>{target}</code></blockquote>",
        "log_socks_gate": "<b>Файервол SOCKS5</b>\n<blockquote>Юзер: <code>{name}</code>\nКлиент: <code>{ip}</code>\nРешение: {action}</blockquote>",
        "log_socks_allowed": "<b>пропущен</b>",
        "log_socks_blocked": "<b>отброшен</b>",
        "socks5_sent_fw": "<b>SOCKS5: {name}</b>\n<blockquote>Хост: {ip}\nПорт: {port}\nЛогин: не нужен\n\nURL: <code>socks5://{ip}:{port}</code>\nБелый список: {ips}</blockquote>",
        "btn_settings": "Настройки",
        "btn_delete": "Удалить",
        "btn_xhttp": "XHTTP",
        "btn_raw": "RAW",
        "btn_set_sni": "SNI",
        "btn_set_dest": "Dest",
        "btn_set_path": "Путь",
        "btn_set_padding": "Padding",
        "btn_set_fp": "Fingerprint",
        "btn_set_limit": "Лимит",
        "btn_encryption": "Шифрование",
        "btn_chrome": "Chrome",
        "btn_firefox": "Firefox",
        "btn_safari": "Safari",
        "btn_install_xray": "Установить XRay Core",
        "btn_reinstall_xray": "Переустановить XRay Core",
        "cf_no_releases": "<blockquote>Список релизов недоступен, предлагается только запиненный {version}</blockquote>",
        "cf_installed_mark": " (установлена)",
        "not_installed": "Не установлен",

        "input_name": "Введите имя:",
        "input_limit": "Введите лимит:",
        "input_sni": "Введите SNI (напр. www.microsoft.com):",
        "input_dest": "Введите dest (напр. www.microsoft.com:443):",
        "input_path": "Введите путь (напр. /xhttps):",
        "input_padding_min": "Минимальный padding (байты):",
        "input_padding_max": "Максимальный padding (байты):",

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

        "btn_socks5": "SOCKS5",
        "btn_transport": "Транспорт",
        "core_old_warn": (
            "<blockquote><b>Ядро уязвимо</b> | <code>{version}</code>\n"
            "Всё ниже <b>{min}</b> держит MPTCP-слушатели открытыми и даёт удалённый root "
            "через {advisory}, нажмите установку, чтобы перейти на пинованную версию</blockquote>"
        ),
        "core_old_hidden": "<blockquote>Релизы ниже <b>{min}</b> скрыты ({count}), они уязвимы к {advisory}</blockquote>",
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
        self._closing = False
        self._external_ip = ""
        self._link_cache: Dict[str, str] = {}
        self._logger_topic = None
        self._asset_channel = None
        self._event_offsets: Dict[str, int] = {}
        self._socks_seen: Dict[str, float] = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._db.set("XR", "load_ts", time.time())
        self._me = await client.get_me()

        tg_user_id = self._me.id
        self._root = os.path.join(os.path.expanduser("~"), ".xray_on_userbot", str(tg_user_id))
        self._xray_path = os.path.join(self._root, "xray")

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

        if self._asset_channel:
            await self._ensure_logger_topic()

        if self._logger_topic and self._asset_channel:
            chat_id = int(f"-100{self._asset_channel}")
            greeting_key = f"xray_greeted_{self._asset_channel}_{self._logger_topic.id}"
            already_greeted = self.get(greeting_key, False)
            values = {
                "version": ".".join(map(str, __version__)),
                "users": len(self._users),
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
        for name in [n for n, u in self._users.items() if u.get("transport") not in ("tcp", "xhttp", "socks5")]:
            for kind in ("xray", "site", "cf", "gate"):
                try:
                    await self._stop_unit(self._unit_name(name, kind))
                except Exception:
                    pass
            self._users.pop(name, None)
            shutil.rmtree(os.path.join(self._root, "users", name), ignore_errors=True)
            logger.info("[XR] dropped outdated user %s", name)
            await self._cleanup_stale_units(name)
        self._start_monitor()
        try:
            await self._stop_unit("xray-sub-tunnel.service")
            stale = os.path.join(self._systemd_user_dir, "xray-sub-tunnel.service")
            if os.path.exists(stale):
                os.remove(stale)
            os.remove(os.path.join(self._root, "sub-tunnel.log"))
        except OSError:
            pass
        try:
            await self._systemctl("daemon-reload")
        except Exception:
            pass

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
        self._closing = True
        try:
            await self._stop_monitor()
        except Exception as e:
            logger.warning("[XR] the monitor loop was not stopped cleanly: %s", e)
        for step in (
            self._stop_all_units,
        ):
            try:
                result = step()
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.warning("[XR] unload step %s failed: %s", getattr(step, "__name__", step), e)

    async def _stop_monitor(self):
        task = self._monitor_task
        self._monitor_task = None
        if not task:
            return
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=UNLOAD_WAIT)
        except asyncio.CancelledError:
            pass
        except asyncio.TimeoutError:
            logger.warning("[XR] the monitor loop did not stop in %.0fs", UNLOAD_WAIT)
        except Exception as e:
            logger.warning("[XR] the monitor loop ended with an error: %s", e)

    async def _stop_all_units(self):
        for name in list(self._processes.keys()):
            if name in self._users:
                self._users[name]["resume_on_module_load"] = True
            try:
                await self._stop_user(name)
            except Exception as e:
                logger.warning("[XR] the user %s was not stopped: %s", name, e)
        try:
            self._save_users()
        except Exception as e:
            logger.warning("[XR] the users were not saved: %s", e)

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
        suffix = {"xray": "", "site": "-site", "cf": "-cf-tunnel", "gate": "-gate"}.get(kind, "")
        return f"{safe}{suffix}.service"

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

    def _parse_ip_list(self, text: str):
        out = []
        bad = []
        for raw in re.split(r"[\s,;]+", str(text or "").strip()):
            if not raw:
                continue
            try:
                net = ipaddress.ip_network(raw, strict=False)
            except ValueError:
                bad.append(raw)
                continue
            out.append(str(net))
        return out, bad

    async def _start_gate(self, name: str, user_dir: str) -> Tuple[bool, str]:
        reason = _guard_reason()
        if reason:
            return False, reason
        unit = self._unit_name(name, "gate")
        if await self._unit_active(unit):
            return True, ""
        user = self._users[name]
        ips = user.get("fw_ips") or []
        script_path = os.path.join(user_dir, "gate.py")
        content = (
            GATE_TEMPLATE
            .replace("__ALLOWED__", repr(list(ips)))
            .replace("__BACKEND_PORT__", str(user["port"]))
            .replace("__LISTEN_PORT__", str(user["port"]))
        )
        try:
            _write_private(script_path, content)
        except OSError as e:
            return False, f"gate_write_failed: {e}"
        log_path = os.path.join(user_dir, "daemon.log")
        try:
            self._write_unit(unit, f"XRay firewall gate for {name}", [sys.executable, script_path], log_path)
            ok, output = await self._start_unit(unit)
            if not ok:
                return False, output
            deadline = time.time() + 10
            while time.time() < deadline:
                await asyncio.sleep(0.25)
                try:
                    with socket.create_connection(("127.0.0.1", user["port"]), timeout=0.5):
                        return True, ""
                except OSError:
                    if not await self._unit_active(unit):
                        tail = open(log_path, errors="replace").read()[-400:] if os.path.exists(log_path) else ""
                        return False, tail or "gate_start_failed"
            await self._stop_unit(unit)
            return False, "gate_healthcheck_timeout"
        except Exception as e:
            return False, str(e)

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
        if _core_too_old(tag):
            return False, (
                f"Refused: {tag} is below {XRAY_MIN_TAG}, it opens MPTCP listeners "
                f"and allows a remote root through {XRAY_MIN_ADVISORY}, install {XRAY_CORE_VERSION}"
            )
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
        path = self._xray_path
        if not self._xray_installed():
            return "not installed"
        try:
            p = await asyncio.create_subprocess_exec(
                path, "version",
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
            behind_gate = bool(user.get("fw_enabled"))
            socks_config = {
                "log": {
                    "loglevel": "warning",
                    "access": os.path.join(self._root, "users", user["name"], "access.log"),
                    "error": os.path.join(self._root, "users", user["name"], "error.log"),
                },
                "inbounds": [{
                    "listen": "127.0.0.1" if behind_gate else "0.0.0.0",
                    "port": user["port"],
                    "protocol": "socks",
                    "settings": (
                        {"auth": "noauth", "udp": True}
                        if behind_gate
                        else {
                            "auth": "password",
                            "accounts": [{
                                "user": user.get("socks_user", ""),
                                "pass": user.get("socks_pass", ""),
                            }],
                            "udp": True,
                        }
                    ),
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

        if transport == "xhttp":
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

            if user["transport"] == "socks5" and user.get("fw_enabled"):
                ok, error = await self._start_gate(name, user_dir)
                if not ok:
                    await self._stop_unit(unit)
                    self._processes.pop(name, None)
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
        await self._stop_unit(self._unit_name(name, "gate"))
        self._processes.pop(name, None)

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

    def _get_active_connections(self, port: int) -> int:
        try:
            proc = subprocess.run(
                ["ss", "-Htn", "state", "established", f"sport = :{port}"],
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

    async def _monitor_cycle(self):
        for name in self._users:
            self._trim_user_logs(name)

        for name, proc in list(self._processes.items()):
            user = self._users.get(name)
            if not user:
                continue

            limit = user.get("device_limit", 0)
            if limit == 0:
                continue

            active = self._get_active_connections(user["port"])

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

        for name, user in list(self._processes.items()):
            if self._closing:
                return
            u = self._users.get(name)
            if u and u.get("transport") == "socks5":
                await self._ship_socks_events(name, u)

        if self._closing or not self._xray_installed():
            return

        for name, user in list(self._users.items()):
            if self._closing:
                return
            if not user.get("autostart"):
                continue
            if name in self._processes:
                continue

            await self._start_user(name)

    async def _ship_socks_events(self, name: str, user: Dict) -> None:
        if self._closing or not self._logger_topic or not self._asset_channel:
            return
        base = os.path.join(self._root, "users", name)
        sources = [(os.path.join(base, "access.log"), False)]
        if user.get("fw_enabled"):
            sources.append((os.path.join(base, "daemon.log"), True))
        now = time.time()
        for path, gate in sources:
            if not os.path.exists(path):
                continue
            key = f"{name}:{os.path.basename(path)}"
            size = os.path.getsize(path)
            pos = self._event_offsets.get(key)
            if pos is None or pos > size:
                self._event_offsets[key] = size
                continue
            try:
                with open(path, "r", errors="replace") as f:
                    f.seek(pos)
                    chunk = f.read(256 * 1024)
                self._event_offsets[key] = size
            except OSError:
                continue
            for line in chunk.splitlines():
                if gate:
                    m = re.match(r"gate (allow|block) (\S+)", line.strip())
                    if not m:
                        continue
                    ip = m.group(2)
                    if now - self._socks_seen.get(f"{name}:{ip}", 0) < SOCKS_EVENT_GAP:
                        continue
                    self._socks_seen[f"{name}:{ip}"] = now
                    text = self.strings["log_socks_gate"].format(
                        name=_escape(name), ip=_escape(ip),
                        action=self.strings["log_socks_allowed"] if m.group(1) == "allow" else self.strings["log_socks_blocked"],
                    )
                else:
                    m = re.search(r"from ([0-9A-Fa-f:.]+):\d+ accepted (\S+)", line)
                    if not m:
                        continue
                    ip, target = m.group(1), m.group(2)
                    if ip.startswith("127.") or ip == "::1":
                        continue
                    dedup = f"{name}:{ip}:{target}"
                    if now - self._socks_seen.get(dedup, 0) < SOCKS_EVENT_GAP:
                        continue
                    self._socks_seen[dedup] = now
                    text = self.strings["log_socks_conn"].format(
                        name=_escape(name), ip=_escape(ip), target=_escape(target),
                    )
                try:
                    await self._send_log(text, name=name)
                except Exception as e:
                    logger.warning("[XR] socks event was not logged: %s", e)
        if len(self._socks_seen) > 8192:
            cutoff = now - 3600
            self._socks_seen = {k: v for k, v in self._socks_seen.items() if v > cutoff}

    def _start_monitor(self):
        async def monitor_loop():
            while not self._closing:
                await asyncio.sleep(AUTOSTART_INTERVAL)
                if self._closing:
                    return
                try:
                    await self._monitor_cycle()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning("[XR] monitor cycle failed: %s", e)

        if self._monitor_task:
            self._monitor_task.cancel()

        self._monitor_task = asyncio.create_task(monitor_loop())
        self._monitor_task.add_done_callback(self._monitor_done)

    @staticmethod
    def _monitor_done(task):
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.warning("[XR] the monitor loop stopped: %s", error)

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

    async def _cb_setup_menu(self, call: InlineCall):
        xray_status = await self._get_version("xray") if self._xray_installed() else "Not installed"

        gh_token = self._gh_token()
        gh_status = "Authorized" if gh_token else "Not authorized"

        text = self.strings["setup_menu"].format(
            xray_status=xray_status,
            gh_status=gh_status,
        )
        if self._xray_installed() and _core_too_old(xray_status):
            text += "\n" + self.strings["core_old_warn"].format(
                version=_escape(xray_status),
                min=XRAY_MIN_TAG,
                advisory=XRAY_MIN_ADVISORY,
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

    async def _cb_xray_install_menu(self, call: InlineCall):
        await call.edit(self.strings["collecting_versions"])

        current_version = await self._get_version("xray")

        text = self.strings["xray_install_menu"].format(current=current_version)
        if _core_too_old(current_version):
            text += "\n" + self.strings["core_old_warn"].format(
                version=_escape(current_version),
                min=XRAY_MIN_TAG,
                advisory=XRAY_MIN_ADVISORY,
            )

        releases = await self._gh_get_releases()

        markup = []

        hidden = 0
        for release in releases:
            tag = release.get("tag_name", "")
            if not tag:
                continue
            if _core_too_old(tag):
                hidden += 1
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

        if hidden:
            text += "\n" + self.strings["core_old_hidden"].format(
                count=hidden, min=XRAY_MIN_TAG, advisory=XRAY_MIN_ADVISORY
            )

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

        text = self.strings["main_menu"].format(
            total=total,
            active=active,
            version=version,
        )

        markup = [
            [{
                "text": self.strings["btn_users"],
                "callback": self._cb_users_menu,
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
            active = self._get_active_connections(user["port"])

        uptime = await self._get_user_uptime_async(name)
        autostart = user.get("autostart", False)
        autostart_text = self.strings["btn_autostart_on"].split(":")[1].strip() if autostart else self.strings["btn_autostart_off"].split(":")[1].strip()

        text = self.strings["user_menu"].format(
            name=_escape(name),
            status=status,
            transport=transport,
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

        markup.append([
            {"text": "Xray-core logs", "callback": self._cb_logs_menu, "args": (name, "xray"), "style": "primary"},
            {"text": "Daemon logs", "callback": self._cb_logs_menu, "args": (name, "daemon"), "style": "primary"},
        ])

        markup.append([
            {"text": self.strings["btn_settings"], "callback": self._cb_user_settings, "args": (name,), "style": "primary"},
        ])

        if user["transport"] == "socks5":
            markup.append([
                {"text": self.strings["btn_firewall"], "callback": self._cb_fw_menu, "args": (name,), "style": "primary"},
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
        for kind in ("xray", "site", "cf", "gate"):
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
            behind_gate = bool(user.get("fw_enabled"))
            proxy_url = f"socks5://{ip}:{port}" if behind_gate else f"socks5://{socks_user}:{socks_pass}@{ip}:{port}"

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

            if behind_gate:
                ips_text = ", ".join(user.get("fw_ips") or []) or self.strings["fw_empty_hint"]
                await call.edit(
                    self.strings["socks5_sent_fw"].format(
                        name=_escape(name),
                        ip=ip,
                        port=port,
                        ips=_escape(ips_text),
                    ),
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_user_menu, "args": (name,), "style": "primary"}]]
                )
                return
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

    async def _cb_user_settings(self, call: InlineCall, name: str):
        user = self._users.get(name)
        if not user:
            await call.answer("User not found", show_alert=True)
            return

        is_socks5 = user["transport"] == "socks5"
        transport = self._transport_label(user["transport"])
        sni = user.get("sni", "www.cloudflare.com") if not is_socks5 else "n/a"
        dest = user.get("dest", "www.cloudflare.com:443") if not is_socks5 else "n/a"
        path = user.get("path", "/xhttps") if user["transport"] == "xhttp" else "n/a"
        padding = user.get("padding", "100-1000") if user["transport"] == "xhttp" else "n/a"
        fp = user.get("fingerprint", "firefox") if not is_socks5 else "n/a"
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
            "<blockquote>ML-KEM-768 is generated by the installed Xray core.</blockquote>",
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

    async def _cb_fw_menu(self, call: InlineCall, name: str):
        user = self._users.get(name)
        if not user or user["transport"] != "socks5":
            await call.answer("SOCKS5 user not found", show_alert=True)
            return
        ips = user.get("fw_ips") or []
        enabled = bool(user.get("fw_enabled"))
        lines = "\n".join(f"{i + 1}. <code>{_escape(ip)}</code>" for i, ip in enumerate(ips)) or self.strings["fw_empty_hint"]
        text = self.strings["fw_menu"].format(
            name=_escape(name),
            state=self.strings["fw_state_on"] if enabled else self.strings["fw_state_off"],
            ips=lines,
        )
        markup = [[{
            "text": (self.strings["btn_fw_toggle_on"] if enabled else self.strings["btn_fw_toggle_off"]),
            "callback": self._cb_fw_toggle,
            "args": (name,),
            "style": "success" if enabled else "danger",
        }]]
        markup.append([{
            "text": self.strings["btn_fw_add"],
            "input": self.strings["input_fw_add"],
            "handler": self._cb_fw_add,
            "args": (name,),
            "style": "primary",
        }])
        for ip in ips:
            markup.append([{
                "text": f"{self.strings['btn_fw_del']}: {ip}",
                "callback": self._cb_fw_del,
                "args": (name, ip),
                "style": "danger",
            }])
        markup.append([{"text": self.strings["btn_back"], "callback": self._cb_user_menu, "args": (name,), "style": "primary"}])
        await call.edit(text, reply_markup=markup)

    async def _cb_fw_toggle(self, call: InlineCall, name: str):
        user = self._users.get(name)
        if not user:
            return
        user["fw_enabled"] = not bool(user.get("fw_enabled"))
        await self._mark_restart_required(name)
        await self._cb_fw_menu(call, name)

    async def _cb_fw_add(self, call: InlineCall, value: str, name: str):
        user = self._users.get(name)
        if not user:
            return
        good, bad = self._parse_ip_list(value)
        if bad:
            await call.answer(self.strings["fw_bad_ip"].format(value=_escape(", ".join(bad[:3]))), show_alert=True)
            return
        ips = user.setdefault("fw_ips", [])
        for ip in good:
            if ip not in ips:
                ips.append(ip)
        await self._mark_restart_required(name)
        await self._cb_fw_menu(call, name)

    async def _cb_fw_del(self, call: InlineCall, name: str, ip: str):
        user = self._users.get(name)
        if not user:
            return
        ips = user.get("fw_ips") or []
        if ip in ips:
            ips.remove(ip)
        await self._mark_restart_required(name)
        await self._cb_fw_menu(call, name)

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

    async def _cleanup_stale_units(self, name: str) -> None:
        if name in self._users:
            return
        for kind in ("xray", "site", "cf", "gate"):
            unit = self._unit_name(name, kind)
            path = os.path.join(self._systemd_user_dir, unit)
            if not os.path.exists(path):
                continue
            try:
                await self._stop_unit(unit)
            except Exception:
                pass
            try:
                os.remove(path)
                logger.info("[XR] removed the stale unit %s", unit)
            except OSError:
                pass
        try:
            await self._systemctl("daemon-reload")
        except Exception:
            pass

    async def _cb_add_user_transport_choice(self, call: InlineCall, name: str):
        name = name.strip()

        if not re.match(r"^[a-zA-Z0-9_-]+$", name):
            await call.answer(self.strings["err_invalid_name"], show_alert=True)
            return

        if name in self._users:
            await call.answer(self.strings["err_name_exists"], show_alert=True)
            return
        await self._cleanup_stale_units(name)

        text = self.strings["add_user_transport"].format(name=_escape(name))

        markup = [
            [{"text": self.strings["btn_xhttp"], "callback": self._cb_add_user_limit_input, "args": (name, "xhttp"), "style": "primary"}],
            [{"text": self.strings["btn_raw"], "callback": self._cb_add_user_limit_input, "args": (name, "tcp"), "style": "primary"}],
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
                    "handler": self._cb_create_user_final,
                    "args": (name, transport),
                    "style": "primary",
                }
            ]]
        )

    async def _cb_create_user_final(self, call: InlineCall, limit_str: str, name: str, transport: str):
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
            vless_decryption, vless_encryption = await self._generate_vless_encryption()
            if not vless_decryption or not vless_encryption:
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
            "encryption_mode": "none" if transport == "socks5" else "ml-kem-768",
            "sni": default_sni,
            "dest": default_dest,
            "path": "/xhttps",
            "padding": "100-1000",
            "fingerprint": "firefox",
            "socks_user": _gen_secret(8),
            "socks_pass": _gen_secret(14),
            "fw_enabled": False,
            "fw_ips": [],
            "autostart": False,
            "start_time": 0,
            "services": {
                "xray": self._unit_name(name),
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
            ),
            message=message,
            reply_markup=[
                [{
                    "text": self.strings["btn_users"],
                    "callback": self._cb_users_menu,
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
