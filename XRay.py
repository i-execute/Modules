__version__ = (4, 0, 4)
# meta developer: I_execute.t.me 
# meta banner: https://github.com/i-execute/Modules/raw/main/Storage/XRay/MetaBanner.jpeg

import os
import asyncio
import logging
import signal
import time
import platform
import json
import subprocess
import shutil
import uuid
import ipaddress
import tempfile
import re
import random
import secrets
import string
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

LOG_MAX_SIZE = 30 * 1024 * 1024
LOG_KEEP_SIZE = 10 * 1024 * 1024
BASE_PORT = 8443
AUTOSTART_INTERVAL = 10

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
    """Multi-user VPN with VLESS+Reality (XHTTP/TCP+Vision)"""

    strings = {
        "name": "XRay",
        
        "main_menu": (
            "<b>XRay Multi-User VPN</b>\n"
            "<blockquote>Total users: {total}\n"
            "Active: {active}\n"
            "XRay version: {version}</blockquote>"
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
        
        "user_started": (
            "<b>Started</b>\n"
            "<blockquote>{name} is now online</blockquote>"
        ),
        
        "user_stopped": (
            "<b>Stopped</b>\n"
            "<blockquote>{name} is now offline</blockquote>"
        ),
        
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
        "btn_tcp": "TCP+Vision",
        "btn_set_sni": "Set SNI",
        "btn_set_dest": "Set Dest",
        "btn_set_path": "Set Path",
        "btn_set_padding": "Padding",
        "btn_set_fp": "Fingerprint",
        "btn_set_limit": "Device Limit",
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
            "Process killed"
            "</blockquote>"
        ),
        
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
    }

    strings_ru = {
        "main_menu": (
            "<b>XRay Мультиюзерный VPN</b>\n"
            "<blockquote>Всего юзеров: {total}\n"
            "Активных: {active}\n"
            "Версия XRay: {version}</blockquote>"
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
        
        "user_started": (
            "<b>Запущен</b>\n"
            "<blockquote>{name} онлайн</blockquote>"
        ),
        
        "user_stopped": (
            "<b>Остановлен</b>\n"
            "<blockquote>{name} офлайн</blockquote>"
        ),
        
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
        "btn_tcp": "TCP+Vision",
        "btn_set_sni": "SNI",
        "btn_set_dest": "Dest",
        "btn_set_path": "Путь",
        "btn_set_padding": "Padding",
        "btn_set_fp": "Fingerprint",
        "btn_set_limit": "Лимит",
        "btn_toggle_transport": "Сменить транспорт",
        "btn_chrome": "Chrome",
        "btn_firefox": "Firefox",
        "btn_safari": "Safari",
        "btn_install_xray": "Установить XRay Core",
        "btn_reinstall_xray": "Переустановить XRay Core",
        
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
            "Процесс убит"
            "</blockquote>"
        ),
        
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
    }

    def __init__(self):
        self._root = None
        self._xray_path = None
        self._users: Dict[str, Dict] = {}
        self._processes: Dict[str, subprocess.Popen] = {}
        self._monitor_task = None
        self._external_ip = ""
        self._link_cache: Dict[str, str] = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._me = await client.get_me()

        tg_user_id = self._me.id
        self._root = os.path.join(tempfile.gettempdir(), f"XRay_{tg_user_id}")
        self._xray_path = os.path.join(self._root, "xray")
        
        os.makedirs(self._root, exist_ok=True)
        os.makedirs(os.path.join(self._root, "users"), exist_ok=True)

        self._users = self._db.get("XR", "users", {})
        self._external_ip = await self._detect_external_ip()
        
        if not self._xray_installed():
            logger.warning("[XR] XRay not installed")
        
        await self._reattach_processes()
        self._start_monitor()

    async def on_unload(self):
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        
        for name in list(self._processes.keys()):
            await self._stop_user(name)


    async def _reattach_processes(self):
        if not self._xray_installed():
            return
        import re as _re

        try:
            p = await asyncio.create_subprocess_exec(
                "ss", "-tlnp",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await p.communicate()
            ss_output = out.decode()
        except Exception:
            return

        for name, user in self._users.items():
            if name in self._processes:
                continue
            port = user.get("port")
            if not port:
                continue
            if f":{port}" not in ss_output:
                continue
            try:
                p2 = await asyncio.create_subprocess_exec(
                    "ss", "-tlnp", f"sport = :{port}",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                out2, _ = await p2.communicate()
                line = out2.decode()
                m = _re.search(r'pid=(\d+)', line)
                if not m:
                    continue
                pid = int(m.group(1))
                proc_path = f"/proc/{pid}/exe"
                if not os.path.exists(proc_path):
                    continue
                exe = os.readlink(proc_path)
                if "xray" not in exe.lower():
                    continue
                try:
                    with open(f"/proc/{pid}/environ", "rb") as f:
                        env = f.read()
                    if b"XRAY_MODULE_MANAGED" not in env:
                        continue
                except OSError:
                    continue
                try:
                    os.kill(pid, 0)
                except OSError:
                    continue
                fake = subprocess.Popen.__new__(subprocess.Popen)
                object.__setattr__(fake, 'pid', pid)
                fake._child_created = False
                self._processes[name] = fake
                logger.info(f"[XR] Reattached {name} pid={pid} port={port}")
            except Exception as e:
                logger.warning(f"[XR] Reattach failed for {name}: {e}")

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
        if gh_token:
            curl_cmd += ["-H", f"Authorization: Bearer {gh_token}"]
        curl_cmd.append("https://api.github.com/repos/XTLS/Xray-core/releases")
        
        p = await asyncio.create_subprocess_exec(
            *curl_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, _ = await p.communicate()
        
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
        if _in_docker():
            return False, "docker"

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
            if gh_token:
                curl_cmd += ["-H", f"Authorization: Bearer {gh_token}"]
            curl_cmd.append(f"https://api.github.com/repos/XTLS/Xray-core/releases/tags/{tag}")
            
            p = await asyncio.create_subprocess_exec(
                *curl_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await p.communicate()
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
            
            version = await self._get_xray_version()
            return True, version
            
        except Exception as e:
            return False, str(e)
        finally:
            if tmp and os.path.exists(tmp):
                shutil.rmtree(tmp, ignore_errors=True)

    async def _get_xray_version(self) -> str:
        if not self._xray_installed():
            return "not installed"
        try:
            p = await asyncio.create_subprocess_exec(
                self._xray_path, "version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await p.communicate()
            text = out.decode().strip()
            return text.split("\n")[0][:50] if text else "unknown"
        except:
            return "unknown"

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
            return {
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

        if transport == "tcp":
            sni = user.get("sni", "www.sony.com")
            dest = user.get("dest", "www.sony.com:443")
        else:
            sni = user.get("sni", "www.microsoft.com")
            dest = user.get("dest", "www.microsoft.com:443")
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
                    "decryption": "none",
                    "encryption": "none",
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
            padding = user.get("padding", "100-1000")
            config["inbounds"][0]["streamSettings"] = {
                "network": "xhttp",
                "security": "reality",
                "xhttpSettings": {
                    "path": user.get("path", "/xhttps"),
                    "host": sni,
                    "mode": "auto",
                    "noSSEHeader": False,
                    "xPaddingBytes": padding,
                    "scMaxBufferedPosts": 30,
                    "scMaxEachPostBytes": "1000000",
                    "scStreamUpServerSecs": "20-80",
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

        import json as _json

        if transport == "xhttp":
            sni = user.get("sni", "www.microsoft.com")
            path = user.get("path", "/xhttps")
            padding = user.get("padding", "100-1000")
            extra = _json.dumps({"xPaddingBytes": padding}, separators=(",", ":"))

            params = urllib.parse.urlencode({
                "type": "xhttp",
                "encryption": "none",
                "security": "reality",
                "path": path,
                "host": sni,
                "mode": "auto",
                "extra": extra,
                "pbk": public_key,
                "fp": fp,
                "sni": sni,
                "sid": short_id,
                "spx": "/",
            })
            return f"vless://{uuid_str}@{ip}:{port}?{params}#{urllib.parse.quote(name, safe='')}"
        else:
            sni = user.get("sni", "www.sony.com")
            params = urllib.parse.urlencode({
                "type": "tcp",
                "encryption": "none",
                "security": "reality",
                "flow": "xtls-rprx-vision",
                "pbk": public_key,
                "fp": fp,
                "sni": sni,
                "sid": short_id,
                "spx": "/",
            })
            return f"vless://{uuid_str}@{ip}:{port}?{params}#{urllib.parse.quote(name, safe='')}"

    async def _start_user(self, name: str) -> Tuple[bool, str]:
        if name in self._processes:
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
        
        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)
        
        run_log_path = os.path.join(user_dir, "run.log")
        error_log_path = os.path.join(user_dir, "error.log")

        try:
            run_log_fd = open(run_log_path, "ab")
        except Exception as e:
            return False, str(e)

        try:
            proc = subprocess.Popen(
                [self._xray_path, "run", "-config", config_path],
                stdout=run_log_fd,
                stderr=run_log_fd,
                preexec_fn=os.setsid if hasattr(os, "setsid") else None,
                env={**os.environ, "XRAY_MODULE_MANAGED": self._root},
            )

            await asyncio.sleep(2)

            if proc.poll() is not None:
                run_log_fd.flush()
                run_log_fd.close()
                tail = ""
                for path in (error_log_path, run_log_path):
                    if os.path.exists(path):
                        size = os.path.getsize(path)
                        if size > 0:
                            with open(path, "rb") as f:
                                f.seek(max(0, size - 2048))
                                tail = f.read().decode(errors="replace").strip()
                            if tail:
                                break
                return False, tail or "startup_failed (no output)"

            self._processes[name] = proc
            user["start_time"] = time.time()
            self._save_users()

            return True, ""

        except Exception as e:
            try:
                run_log_fd.close()
            except Exception:
                pass
            return False, str(e)

    async def _stop_user(self, name: str) -> bool:
        proc = self._processes.get(name)
        if not proc:
            return False
        
        try:
            if hasattr(os, "killpg"):
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            else:
                proc.terminate()
            
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                if hasattr(os, "killpg"):
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                else:
                    proc.kill()
                proc.wait(timeout=3)
        except:
            pass
        
        del self._processes[name]
        
        if name in self._users:
            self._users[name]["start_time"] = 0
            self._save_users()
        
        return True

    def _get_active_connections(self, port: int) -> int:
        try:
            p = subprocess.run(
                ["ss", "-tn", "state", "established", f"sport = :{port}"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if p.returncode != 0:
                return 0
            
            unique_ips = set()
            for line in p.stdout.strip().split("\n")[1:]:
                parts = line.split()
                if len(parts) >= 4:
                    peer = parts[3]
                    m = re.match(r"(\d+\.\d+\.\d+\.\d+):\d+", peer)
                    if m:
                        ip = m.group(1)
                        try:
                            addr = ipaddress.IPv4Address(ip)
                            if not addr.is_private and not addr.is_loopback:
                                unique_ips.add(ip)
                        except:
                            pass
            
            return len(unique_ips)
        except:
            return 0

    def _start_monitor(self):
        async def monitor_loop():
            while True:
                await asyncio.sleep(AUTOSTART_INTERVAL)
                
                for name, proc in list(self._processes.items()):
                    user = self._users.get(name)
                    if not user:
                        continue
                    
                    limit = user.get("device_limit", 0)
                    if limit == 0:
                        continue
                    
                    active = self._get_active_connections(user["port"])
                    
                    if active > limit:
                        await self._stop_user(name)
                        
                        try:
                            await self._client.send_message(
                                self._me.id,
                                self.strings["device_limit_exceeded"].format(
                                    name=_escape(name),
                                    limit=limit,
                                    active=active,
                                ),
                                parse_mode="html",
                            )
                        except:
                            pass
                
                if not self._xray_installed():
                    continue
                
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

    async def _cb_setup_menu(self, call: InlineCall):
        xray_version = await self._get_xray_version()
        xray_status = f"{xray_version}" if self._xray_installed() else "Not installed"
        
        gh_token = self._gh_token()
        gh_status = "Authorized" if gh_token else "Not authorized"
        
        text = self.strings["setup_menu"].format(
            xray_status=xray_status,
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
        
        current_version = await self._get_xray_version()
        
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
                "text": "Failed to load releases",
                "callback": self._cb_setup_menu,
                "style": "danger",
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
        total = len(self._users)
        active = len(self._processes)
        
        text = self.strings["users_menu"].format(
            total=total,
            active=active,
        )
        
        markup = []
        
        for name, user in self._users.items():
            status = self.strings["status_online"] if name in self._processes else self.strings["status_offline"]
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
        total = len(self._users)
        active = len(self._processes)
        version = await self._get_xray_version()
        
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
        
        is_running = name in self._processes
        status = self.strings["status_online"] if is_running else self.strings["status_offline"]
        transport = user["transport"].upper()
        limit = user.get("device_limit", 0)
        limit_text = "Unlimited" if limit == 0 else str(limit)
        
        active = 0
        if is_running:
            active = self._get_active_connections(user["port"])
        
        uptime = self._get_user_uptime(name)
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
            {"text": self.strings["btn_get_logs"], "callback": self._cb_get_user_logs, "args": (name,), "style": "primary"},
        ])
        
        markup.append([
            {"text": self.strings["btn_settings"], "callback": self._cb_user_settings, "args": (name,), "style": "primary"},
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
        
        await self._stop_user(name)
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
        
        await self._stop_user(name)
        
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
                await self._client.send_file(
                    call.form["chat"],
                    tmp.name,
                    attributes=[],
                    force_document=True,
                    file_name=f"proxies_{name}.txt",
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
            await self._client.send_file(
                call.form["chat"],
                tmp.name,
                attributes=[],
                force_document=True,
                file_name=f"link_for_{name}.txt",
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

        markup = [
            [{"text": self.strings["btn_back"], "callback": self._cb_user_menu, "args": (name,), "style": "primary"}],
        ]
        await call.edit(
            self.strings["link_sent"].format(name=_escape(name)),
            reply_markup=markup,
        )

    async def _cb_get_user_logs(self, call: InlineCall, name: str):
        await call.edit(self.strings["loading"])

        user_dir = os.path.join(self._root, "users", name)
        error_log = os.path.join(user_dir, "error.log")
        run_log = os.path.join(user_dir, "run.log")

        chosen = None
        label = None
        for path, lbl in ((error_log, "error.log"), (run_log, "run.log")):
            if os.path.exists(path) and os.path.getsize(path) > 0:
                chosen = path
                label = lbl
                break

        if not chosen:
            await call.edit(
                self.strings["setup_fail"].format(error="No logs found"),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_user_menu, "args": (name,), "style": "primary"}]]
            )
            return

        size = os.path.getsize(chosen)

        import tempfile as _tf
        with open(chosen, "rb") as f:
            f.seek(max(0, size - 50 * 1024))
            tail = f.read()

        tmp = _tf.NamedTemporaryFile(
            mode="wb",
            suffix=".txt",
            prefix=f"xray_{name}_",
            delete=False,
        )
        tmp.write(tail)
        tmp.close()

        try:
            await self._client.send_file(
                call.form["chat"],
                tmp.name,
                force_document=True,
                file_name=f"xray_{name}_{label}.txt",
                caption=f"<b>XRay {label}:</b> <code>{name}</code>",
            )
        except Exception as e:
            logger.exception("[XR] send_file failed: %s", e)
        finally:
            os.unlink(tmp.name)

        markup = [
            [{"text": self.strings["btn_back"], "callback": self._cb_user_menu, "args": (name,), "style": "primary"}],
        ]
        await call.edit(
            f"<b>Logs sent for {_escape(name)}</b>",
            reply_markup=markup,
        )

    async def _cb_user_settings(self, call: InlineCall, name: str):
        user = self._users.get(name)
        if not user:
            await call.answer("User not found", show_alert=True)
            return
        
        is_socks5 = user["transport"] == "socks5"
        transport = user["transport"].upper()
        sni = user.get("sni", "www.microsoft.com") if not is_socks5 else "n/a"
        dest = user.get("dest", "www.microsoft.com:443") if not is_socks5 else "n/a"
        path = user.get("path", "/xhttps") if user["transport"] == "xhttp" else "n/a"
        padding = user.get("padding", "100-1000") if user["transport"] == "xhttp" else "n/a"
        fp = user.get("fingerprint", "firefox") if not is_socks5 else "n/a"
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
            limit=limit_text,
        )
        
        markup = [
            [{"text": self.strings["btn_transport"], "callback": self._cb_transport_menu, "args": (name,), "style": "primary"}],
        ]
        
        if not is_socks5:
            markup.append([{"text": self.strings["btn_set_sni"], "input": self.strings["input_sni"], "handler": self._cb_set_sni, "args": (name,), "style": "primary"}])
            markup.append([{"text": self.strings["btn_set_dest"], "input": self.strings["input_dest"], "handler": self._cb_set_dest, "args": (name,), "style": "primary"}])
            
            if user["transport"] == "xhttp":
                markup.append([{"text": self.strings["btn_set_path"], "input": self.strings["input_path"], "handler": self._cb_set_path, "args": (name,), "style": "primary"}])
                markup.append([{"text": self.strings["btn_set_padding"], "callback": self._cb_padding_menu, "args": (name,), "style": "primary"}])
            
            markup.append([{"text": self.strings["btn_set_fp"], "callback": self._cb_fp_menu, "args": (name,), "style": "primary"}])
        
        markup.append([{"text": self.strings["btn_set_limit"], "input": self.strings["input_limit"], "handler": self._cb_set_limit, "args": (name,), "style": "primary"}])
        markup.append([{"text": self.strings["btn_back"], "callback": self._cb_user_menu, "args": (name,), "style": "primary"}])
        
        await call.edit(text, reply_markup=markup)

    async def _cb_transport_menu(self, call: InlineCall, name: str):
        user = self._users.get(name)
        if not user:
            await call.answer("User not found", show_alert=True)
            return
        
        markup = [
            [{"text": self.strings["btn_tcp"], "callback": self._cb_set_transport, "args": (name, "tcp"), "style": "primary"}],
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
        
        user["transport"] = transport
        
        if transport == "socks5" and not user.get("socks_user"):
            user["socks_user"] = _gen_secret(8)
            user["socks_pass"] = _gen_secret(14)
        
        self._save_users()
        
        if name in self._processes and user.get("autostart"):
            await self._stop_user(name)
            await self._start_user(name)
        
        await call.edit(
            self.strings["transport_set"].format(transport=transport.upper()),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_user_settings, "args": (name,), "style": "primary"}]]
        )

    async def _cb_set_sni(self, call: InlineCall, sni: str, name: str):
        user = self._users.get(name)
        if not user:
            return
        
        user["sni"] = _strip_md(sni).strip().lower()
        self._save_users()
        
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
        self._save_users()
        
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
        self._save_users()
        
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
        self._save_users()
        
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
        
        text = self.strings["add_user_transport"].format(name=_escape(name))
        
        markup = [
            [{"text": self.strings["btn_xhttp"], "callback": self._cb_add_user_limit_input, "args": (name, "xhttp"), "style": "primary"}],
            [{"text": self.strings["btn_tcp"], "callback": self._cb_add_user_limit_input, "args": (name, "tcp"), "style": "primary"}],
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
        if transport != "socks5":
            private_key, public_key = await self._generate_x25519()
            if not private_key or not public_key:
                await call.edit(
                    self.strings["setup_fail"].format(error="Key generation failed"),
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_users_menu, "style": "primary"}]]
                )
                return
        
        port = await self._get_next_port()
        
        if transport == "tcp":
            default_sni = "www.sony.com"
            default_dest = "www.sony.com:443"
        else:
            default_sni = "www.microsoft.com"
            default_dest = "www.microsoft.com:443"

        user = {
            "name": name,
            "uuid": str(uuid.uuid4()),
            "port": port,
            "transport": transport,
            "device_limit": limit,
            "private_key": private_key,
            "public_key": public_key,
            "short_id": self._generate_short_id(),
            "sni": default_sni,
            "dest": default_dest,
            "path": "/xhttps",
            "padding": "100-1000",
            "fingerprint": "firefox",
            "socks_user": _gen_secret(8),
            "socks_pass": _gen_secret(14),
            "autostart": False,
            "start_time": 0,
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

    @loader.command()
    async def xr(self, message):
        """XRay multi-user VPN manager"""
        await self.inline.form(
            text=self.strings["main_menu"].format(
                total=len(self._users),
                active=len(self._processes),
                version=await self._get_xray_version(),
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