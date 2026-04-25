__version__ = (3, 5, 6)
# meta developer: I_execute.t.me

import os
import asyncio
import logging
import signal
import time
import platform
import json
import subprocess
import shutil
import socket
import uuid
import ipaddress
import tempfile
import re
from datetime import datetime, timedelta

from aiogram.types import (
    Message as AiogramMessage,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CopyTextButton,
    LinkPreviewOptions,
)
from .. import loader, utils

logger = logging.getLogger(__name__)

LOG_MAX_SIZE = 30 * 1024 * 1024
LOG_KEEP_SIZE = 10 * 1024 * 1024
STATS_API_PORT = 10085
STATS_API_TAG = "api"


def _escape(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _in_docker():
    if os.path.exists("/.dockerenv"):
        return True
    try:
        with open("/proc/1/cgroup", "r") as f:
            content = f.read()
            if "docker" in content or "containerd" in content:
                return True
    except Exception:
        pass
    try:
        with open("/proc/self/mountinfo", "r") as f:
            content = f.read()
            if "docker" in content or "overlay" in content:
                return True
    except Exception:
        pass
    return False


@loader.tds
class XRay(loader.Module):
    """Run VPN on your VPS server (VLESS + reality)"""

    strings = {
        "name": "XRay",
        "help": (
            "<b>XRay VLESS+Reality</b>\n\n"
            "<b>Setup:</b>\n"
            "<blockquote>"
            "<code>{prefix}xr setup</code> install + generate keys + config\n"
            "<code>{prefix}xr start</code> / <code>{prefix}xr stop</code> / <code>{prefix}xr restart</code>\n"
            "<code>{prefix}xr status</code> status, traffic, connections"
            "</blockquote>\n\n"
            "<b>Settings:</b>\n"
            "<blockquote>"
            "<code>{prefix}xr port [port]</code> port (default 8443)\n"
            "<code>{prefix}xr dest [domain:port]</code> Reality dest\n"
            "<code>{prefix}xr sni [domain]</code> SNI\n"
            "<code>{prefix}xr ip [address]</code> external IP\n"
            "<code>{prefix}xr keys</code> show all keys (PM only)\n"
            "<code>{prefix}xr overwrite</code> new keys + uuid + restart"
            "</blockquote>\n\n"
            "<b>Access:</b>\n"
            "<blockquote>"
            "<code>{prefix}xr add</code> reply to add trusted user\n"
            "<code>{prefix}xr rm</code> reply to remove trusted user\n"
            "<code>{prefix}xr users</code> list trusted users"
            "</blockquote>\n\n"
            "<b>Debug:</b>\n"
            "<blockquote>"
            "<code>{prefix}xr log</code> / <code>{prefix}xr log full</code>\n"
            "<code>{prefix}xr debug</code> debug info\n"
            "<code>{prefix}xr diagnose</code> diagnostics\n"
            "<code>{prefix}xr checkout</code> IPs connected last 24h\n"
            "<code>{prefix}xr ping</code> host speed test"
            "</blockquote>\n\n"
            "<b>Bot:</b>\n"
            "<blockquote>"
            "/xray get vless link (trusted users only)"
            "</blockquote>"
        ),

        "not_installed": "<b>xray not installed</b>\n<code>{prefix}xr setup</code>",
        "setup_progress": "<b>Setting up XRay...</b>",
        "setup_installing": "Downloading XRay...",
        "setup_installed": "XRay installed: {version}",
        "setup_keys": "Keys generated",
        "setup_config": "Config written",
        "setup_done": "<b>Setup complete</b>\n\nNow: <code>{prefix}xr start</code>",
        "setup_fail": "<b>Setup failed</b>\n\n<code>{error}</code>",
        "setup_docker": (
            "<b>Docker detected</b>\n\n"
            "This module cannot work inside Docker container.\n"
            "Unload: <code>{prefix}ulm XRay</code>"
        ),

        "already_running": "<b>Proxy already running</b>",
        "not_running": "<b>Proxy not running</b>",
        "starting": "<b>Starting XRay...</b>",
        "started": (
            "<b>XRay started</b>\n\n"
            "<blockquote>"
            "Port: <code>{port}</code>\n"
            "IP: <code>{ip}</code>"
            "</blockquote>"
        ),
        "start_fail": "<b>Start failed</b>\n\n<code>{error}</code>",
        "stopped": "<b>Proxy stopped</b>",
        "restarting": "<b>Restarting...</b>",
        "port_busy": (
            "<b>Port {port} is busy</b>\n\n"
            "Change port: <code>{prefix}xr port [port]</code>\n"
            "Then: <code>{prefix}xr start</code>"
        ),

        "status_on": (
            "<b>XRay Status</b>\n\n"
            "<blockquote>"
            "<b>State:</b> running\n"
            "<b>PID:</b> <code>{pid}</code>\n"
            "<b>Uptime:</b> <code>{uptime}</code>"
            "</blockquote>\n\n"
            "<b>Traffic (since start):</b>\n"
            "<blockquote>"
            "RX: <code>{rx}</code>\n"
            "TX: <code>{tx}</code>\n"
            "Total: <code>{total}</code>"
            "</blockquote>\n\n"
            "<b>Connections:</b>\n"
            "<blockquote>"
            "Active clients: <code>{active}</code>\n"
            "Unique IPs (24h): <code>{unique_ips}</code>"
            "</blockquote>\n\n"
            "Trusted users: <code>{trusted_count}</code>\n"
            "Port: <code>{port}</code>"
        ),
        "status_off": "<b>XRay Status</b>\n\n<blockquote><b>State:</b> stopped</blockquote>",

        "keys_info": (
            "<b>Reality keys</b>\n\n"
            "<b>Private:</b>\n"
            "<blockquote><code>{private_key}</code></blockquote>\n\n"
            "<b>Public:</b>\n"
            "<blockquote><code>{public_key}</code></blockquote>\n\n"
            "<b>UUID:</b>\n"
            "<blockquote><code>{uid}</code></blockquote>\n\n"
            "Short ID: <code>{short_id}</code>\n"
            "SNI: <code>{sni}</code>\n"
            "Dest: <code>{dest}</code>\n"
            "Port: <code>{port}</code>"
        ),
        "keys_pm_only": "<b>Keys only in PM</b>",

        "port_current": "Port: <code>{port}</code>",
        "port_set": "Port: <code>{port}</code>\n<code>{prefix}xr restart</code>",
        "port_invalid": "<b>Port must be 1025-65535</b>",

        "dest_set": "Dest: <code>{dest}</code>\n<code>{prefix}xr restart</code>",
        "dest_current": "Dest: <code>{dest}</code>",
        "sni_set": "SNI: <code>{sni}</code>\n<code>{prefix}xr restart</code>",
        "sni_current": "SNI: <code>{sni}</code>",

        "ip_set": "IP: <code>{ip}</code>",
        "ip_detected": "IP: <code>{ip}</code>",
        "ip_fail": "<b>IP not detected</b>\n<code>{prefix}xr ip [address]</code>",
        "ip_invalid": "<b>Invalid IP address</b>",

        "user_added": "<b>Added:</b> <blockquote><code>{uid}</code></blockquote>",
        "user_removed": "<b>Removed:</b> <blockquote><code>{uid}</code></blockquote>",
        "user_not_found": "<b>Not found</b>",
        "user_need_reply": "<b>Reply to a message to add/remove user</b>",
        "users_list": "<b>Trusted users:</b>\n\n<blockquote>{users}</blockquote>",
        "users_empty": "<b>No trusted users</b>",

        "log_empty": "<b>Log empty</b>",
        "log_title": "<b>XRay log:</b>\n\n<blockquote>",
        "log_suffix": "</blockquote>",

        "need_setup": "<b>Setup first</b>\n<code>{prefix}xr setup</code>",
        "no_config": (
            "<b>Not configured</b>\n\n"
            "1. <code>{prefix}xr setup</code>\n"
            "2. <code>{prefix}xr start</code>"
        ),

        "overwrite_progress": "<b>Overwriting all credentials...</b>",
        "overwrite_done": (
            "<b>All credentials overwritten</b>\n\n"
            "Old links are now dead.\n"
            "Get new link via /xray in bot."
        ),
        "overwrite_fail": "<b>Overwrite failed</b>\n\n<code>{error}</code>",

        "debug_info": (
            "<b>XRay Debug</b>\n\n"
            "<b>System:</b>\n"
            "<blockquote>"
            "OS: <code>{os_name}</code>\n"
            "Arch: <code>{arch}</code>\n"
            "Python: <code>{python}</code>"
            "</blockquote>\n\n"
            "<b>XRay:</b>\n"
            "<blockquote>"
            "Installed: {installed}\n"
            "Path: <code>{xray_path}</code>\n"
            "Version: <code>{xray_version}</code>"
            "</blockquote>\n\n"
            "<b>Proxy:</b>\n"
            "<blockquote>"
            "Status: {status}\n"
            "PID: <code>{pid}</code>\n"
            "Port: <code>{port}</code>\n"
            "SNI: <code>{sni}</code>\n"
            "Dest: <code>{dest}</code>\n"
            "IP: <code>{ip}</code>"
            "</blockquote>\n\n"
            "<b>Checks:</b>\n"
            "<blockquote>"
            "Port listening: {port_listening}\n"
            "Config: {config_exists}\n"
            "Docker: {docker}\n"
            "Work dir: <code>{work_dir}</code>"
            "</blockquote>"
        ),

        "diagnose_title": "<b>Diagnostics</b>\n\n<blockquote>",
        "diagnose_suffix": "</blockquote>",

        "bot_link_response": (
            "<b>Your VLESS link</b>\n\n"
            "Download client:\n"
            '<a href="https://apps.apple.com/app/id6476628951">v2RayTun for iOS</a>\n'
            '<a href="https://play.google.com/store/apps/details?id=com.v2raytun.android">v2RayTun for Android</a>\n\n'
            "Press the button below to copy the link, then open v2RayTun, "
            "tap add server and paste."
        ),
        "bot_not_configured": "<b>Proxy not configured yet</b>",
        "bot_copy_button": "VPN LINK",

        "checkout_empty": "<b>No connections in last 24h</b>",

        "ping_progress": "<b>Running speed test...</b>",
        "ping_result": (
            "<b>Host Speed Test</b>\n\n"
            "<b>Download:</b>\n"
            "<blockquote>{download}</blockquote>\n\n"
            "<b>Upload:</b>\n"
            "<blockquote>{upload}</blockquote>\n\n"
            "<b>Latency:</b>\n"
            "<blockquote>{latency}</blockquote>"
        ),
        "ping_fail": "<b>Speed test failed</b>\n\n<code>{error}</code>",
    }

    strings_ru = {
        "help": (
            "<b>XRay VLESS+Reality</b>\n\n"
            "<b>Установка:</b>\n"
            "<blockquote>"
            "<code>{prefix}xr setup</code> установка + генерация ключей + конфиг\n"
            "<code>{prefix}xr start</code> / <code>{prefix}xr stop</code> / <code>{prefix}xr restart</code>\n"
            "<code>{prefix}xr status</code> статус, трафик, подключения"
            "</blockquote>\n\n"
            "<b>Настройки:</b>\n"
            "<blockquote>"
            "<code>{prefix}xr port [порт]</code> порт (по умолчанию 8443)\n"
            "<code>{prefix}xr dest [домен:порт]</code> Reality dest\n"
            "<code>{prefix}xr sni [домен]</code> SNI\n"
            "<code>{prefix}xr ip [адрес]</code> внешний IP\n"
            "<code>{prefix}xr keys</code> показать все ключи (только в ЛС)\n"
            "<code>{prefix}xr overwrite</code> новые ключи + uuid + перезапуск"
            "</blockquote>\n\n"
            "<b>Доступ:</b>\n"
            "<blockquote>"
            "<code>{prefix}xr add</code> ответ на сообщение для добавления пользователя\n"
            "<code>{prefix}xr rm</code> ответ на сообщение для удаления пользователя\n"
            "<code>{prefix}xr users</code> список доверенных пользователей"
            "</blockquote>\n\n"
            "<b>Отладка:</b>\n"
            "<blockquote>"
            "<code>{prefix}xr log</code> / <code>{prefix}xr log full</code>\n"
            "<code>{prefix}xr debug</code> отладочная информация\n"
            "<code>{prefix}xr diagnose</code> диагностика\n"
            "<code>{prefix}xr checkout</code> IP за последние 24ч\n"
            "<code>{prefix}xr ping</code> тест скорости хоста"
            "</blockquote>\n\n"
            "<b>Бот:</b>\n"
            "<blockquote>"
            "/xray получить vless ссылку (только доверенные пользователи)"
            "</blockquote>"
        ),

        "not_installed": "<b>xray не установлен</b>\n<code>{prefix}xr setup</code>",
        "setup_progress": "<b>Настройка XRay...</b>",
        "setup_installing": "Скачивание XRay...",
        "setup_installed": "XRay установлен: {version}",
        "setup_keys": "Ключи сгенерированы",
        "setup_config": "Конфиг записан",
        "setup_done": "<b>Настройка завершена</b>\n\nТеперь: <code>{prefix}xr start</code>",
        "setup_fail": "<b>Настройка не удалась</b>\n\n<code>{error}</code>",
        "setup_docker": (
            "<b>Обнаружен Docker</b>\n\n"
            "Этот модуль не может работать внутри Docker контейнера.\n"
            "Выгрузить: <code>{prefix}ulm XRay</code>"
        ),

        "already_running": "<b>Прокси уже запущен</b>",
        "not_running": "<b>Прокси не запущен</b>",
        "starting": "<b>Запуск XRay...</b>",
        "started": (
            "<b>XRay запущен</b>\n\n"
            "<blockquote>"
            "Порт: <code>{port}</code>\n"
            "IP: <code>{ip}</code>"
            "</blockquote>"
        ),
        "start_fail": "<b>Ошибка запуска</b>\n\n<code>{error}</code>",
        "stopped": "<b>Прокси остановлен</b>",
        "restarting": "<b>Перезапуск...</b>",
        "port_busy": (
            "<b>Порт {port} занят</b>\n\n"
            "Сменить порт: <code>{prefix}xr port [порт]</code>\n"
            "Затем: <code>{prefix}xr start</code>"
        ),

        "status_on": (
            "<b>Статус XRay</b>\n\n"
            "<blockquote>"
            "<b>Состояние:</b> работает\n"
            "<b>PID:</b> <code>{pid}</code>\n"
            "<b>Аптайм:</b> <code>{uptime}</code>"
            "</blockquote>\n\n"
            "<b>Трафик (с запуска):</b>\n"
            "<blockquote>"
            "RX: <code>{rx}</code>\n"
            "TX: <code>{tx}</code>\n"
            "Всего: <code>{total}</code>"
            "</blockquote>\n\n"
            "<b>Подключения:</b>\n"
            "<blockquote>"
            "Активных клиентов: <code>{active}</code>\n"
            "Уникальных IP (24ч): <code>{unique_ips}</code>"
            "</blockquote>\n\n"
            "Доверенных пользователей: <code>{trusted_count}</code>\n"
            "Порт: <code>{port}</code>"
        ),
        "status_off": "<b>Статус XRay</b>\n\n<blockquote><b>Состояние:</b> остановлен</blockquote>",

        "keys_info": (
            "<b>Ключи Reality</b>\n\n"
            "<b>Приватный:</b>\n"
            "<blockquote><code>{private_key}</code></blockquote>\n\n"
            "<b>Публичный:</b>\n"
            "<blockquote><code>{public_key}</code></blockquote>\n\n"
            "<b>UUID:</b>\n"
            "<blockquote><code>{uid}</code></blockquote>\n\n"
            "Short ID: <code>{short_id}</code>\n"
            "SNI: <code>{sni}</code>\n"
            "Dest: <code>{dest}</code>\n"
            "Порт: <code>{port}</code>"
        ),
        "keys_pm_only": "<b>Ключи только в ЛС</b>",

        "port_current": "Порт: <code>{port}</code>",
        "port_set": "Порт: <code>{port}</code>\n<code>{prefix}xr restart</code>",
        "port_invalid": "<b>Порт должен быть 1025-65535</b>",

        "dest_set": "Dest: <code>{dest}</code>\n<code>{prefix}xr restart</code>",
        "dest_current": "Dest: <code>{dest}</code>",
        "sni_set": "SNI: <code>{sni}</code>\n<code>{prefix}xr restart</code>",
        "sni_current": "SNI: <code>{sni}</code>",

        "ip_set": "IP: <code>{ip}</code>",
        "ip_detected": "IP: <code>{ip}</code>",
        "ip_fail": "<b>IP не определён</b>\n<code>{prefix}xr ip [адрес]</code>",
        "ip_invalid": "<b>Неверный IP адрес</b>",

        "user_added": "<b>Добавлен:</b> <blockquote><code>{uid}</code></blockquote>",
        "user_removed": "<b>Удалён:</b> <blockquote><code>{uid}</code></blockquote>",
        "user_not_found": "<b>Не найден</b>",
        "user_need_reply": "<b>Ответьте на сообщение для добавления/удаления пользователя</b>",
        "users_list": "<b>Доверенные пользователи:</b>\n\n<blockquote>{users}</blockquote>",
        "users_empty": "<b>Нет доверенных пользователей</b>",

        "log_empty": "<b>Лог пуст</b>",
        "log_title": "<b>Лог XRay:</b>\n\n<blockquote>",
        "log_suffix": "</blockquote>",

        "need_setup": "<b>Сначала настройте</b>\n<code>{prefix}xr setup</code>",
        "no_config": (
            "<b>Не настроено</b>\n\n"
            "1. <code>{prefix}xr setup</code>\n"
            "2. <code>{prefix}xr start</code>"
        ),

        "overwrite_progress": "<b>Перезапись всех учётных данных...</b>",
        "overwrite_done": (
            "<b>Все учётные данные перезаписаны</b>\n\n"
            "Старые ссылки больше не работают.\n"
            "Получите новую ссылку через /xray в боте."
        ),
        "overwrite_fail": "<b>Ошибка перезаписи</b>\n\n<code>{error}</code>",

        "debug_info": (
            "<b>Отладка XRay</b>\n\n"
            "<b>Система:</b>\n"
            "<blockquote>"
            "ОС: <code>{os_name}</code>\n"
            "Арх: <code>{arch}</code>\n"
            "Python: <code>{python}</code>"
            "</blockquote>\n\n"
            "<b>XRay:</b>\n"
            "<blockquote>"
            "Установлен: {installed}\n"
            "Путь: <code>{xray_path}</code>\n"
            "Версия: <code>{xray_version}</code>"
            "</blockquote>\n\n"
            "<b>Прокси:</b>\n"
            "<blockquote>"
            "Статус: {status}\n"
            "PID: <code>{pid}</code>\n"
            "Порт: <code>{port}</code>\n"
            "SNI: <code>{sni}</code>\n"
            "Dest: <code>{dest}</code>\n"
            "IP: <code>{ip}</code>"
            "</blockquote>\n\n"
            "<b>Проверки:</b>\n"
            "<blockquote>"
            "Порт слушает: {port_listening}\n"
            "Конфиг: {config_exists}\n"
            "Docker: {docker}\n"
            "Рабочая директория: <code>{work_dir}</code>"
            "</blockquote>"
        ),

        "diagnose_title": "<b>Диагностика</b>\n\n<blockquote>",
        "diagnose_suffix": "</blockquote>",

        "bot_link_response": (
            "<b>Ваша VLESS ссылка</b>\n\n"
            "Скачайте клиент:\n"
            '<a href="https://apps.apple.com/app/id6476628951">v2RayTun для iOS</a>\n'
            '<a href="https://play.google.com/store/apps/details?id=com.v2raytun.android">v2RayTun для Android</a>\n\n'
            "Нажмите кнопку ниже чтобы скопировать ссылку, затем откройте v2RayTun, "
            "нажмите добавить сервер и вставьте."
        ),
        "bot_not_configured": "<b>Прокси ещё не настроен</b>",
        "bot_copy_button": "VPN ССЫЛКА",

        "checkout_empty": "<b>Нет подключений за последние 24ч</b>",

        "ping_progress": "<b>Запуск теста скорости...</b>",
        "ping_result": (
            "<b>Тест скорости хоста</b>\n\n"
            "<b>Скачивание:</b>\n"
            "<blockquote>{download}</blockquote>\n\n"
            "<b>Загрузка:</b>\n"
            "<blockquote>{upload}</blockquote>\n\n"
            "<b>Задержка:</b>\n"
            "<blockquote>{latency}</blockquote>"
        ),
        "ping_fail": "<b>Тест скорости не удался</b>\n\n<code>{error}</code>",
    }

    def __init__(self):
        self._proc = None
        self._start_time = 0
        self._log_lines = []
        self._max_log_lines = 500
        self._root = None
        self._xray_path = None
        self._config_path = None
        self._log_reader_task = None
        self._log_fd = None
        self._proxy_lock = asyncio.Lock()
        self._traffic_rx = 0
        self._traffic_tx = 0
        self._traffic_task = None
        self._log_rotation_task = None

    def _s(self, key, **kwargs):
        prefix = self.get_prefix()
        text = self.strings.get(key, "")
        try:
            return text.format(prefix=prefix, **kwargs)
        except (KeyError, IndexError):
            return text

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._me = await client.get_me()

        tg_user_id = self._me.id
        self._root = os.path.join(
            tempfile.gettempdir(), f"XRay_{tg_user_id}"
        )
        self._xray_path = os.path.join(self._root, "xray")
        self._config_path = os.path.join(self._root, "config.json")

        os.makedirs(self._root, exist_ok=True)

        defaults = {
            "port": 8443,
            "sni": "www.google.com",
            "dest": "www.google.com:443",
            "trusted_users": [],
            "external_ip": "",
            "vless_uuid": "",
            "private_key": "",
            "public_key": "",
            "short_id": "",
        }
        for k, v in defaults.items():
            if self._db.get("XR", k) is None:
                self._db.set("XR", k, v)

        self._start_log_rotation_scheduler()

        if self._db.get("XR", "proxy_autostart", False):
            if self._xray_installed() and os.path.exists(self._config_path):
                try:
                    await self._do_start_proxy()
                except Exception as e:
                    logger.error("[XR] Proxy autostart error: %s", e)

    async def on_unload(self):
        await self._full_cleanup()

    async def _full_cleanup(self):
        if self._log_rotation_task:
            self._log_rotation_task.cancel()
            try:
                await self._log_rotation_task
            except (asyncio.CancelledError, Exception):
                pass
            self._log_rotation_task = None

        if self._traffic_task:
            self._traffic_task.cancel()
            try:
                await self._traffic_task
            except (asyncio.CancelledError, Exception):
                pass
            self._traffic_task = None

        if self._log_reader_task:
            self._log_reader_task.cancel()
            try:
                await self._log_reader_task
            except (asyncio.CancelledError, Exception):
                pass
            self._log_reader_task = None

        if self._proc:
            pid = self._proc.pid
            try:
                if hasattr(os, "killpg"):
                    os.killpg(os.getpgid(pid), signal.SIGTERM)
                else:
                    self._proc.terminate()
                try:
                    self._proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    if hasattr(os, "killpg"):
                        os.killpg(os.getpgid(pid), signal.SIGKILL)
                    else:
                        self._proc.kill()
                    self._proc.wait(timeout=3)
            except Exception:
                try:
                    self._proc.kill()
                except Exception:
                    pass
            self._proc = None
            self._start_time = 0

        if self._log_fd:
            try:
                self._log_fd.close()
            except Exception:
                pass
            self._log_fd = None

        try:
            p = await asyncio.create_subprocess_exec(
                "pkill", "-f", f"xray.*{self._root}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(p.communicate(), timeout=5)
        except Exception:
            pass

        self._db.set("XR", "proxy_autostart", False)

        if self._root and os.path.exists(self._root):
            try:
                shutil.rmtree(self._root, ignore_errors=True)
            except Exception:
                pass

    def _is_owner(self, uid):
        return uid == self._me.id

    def _is_trusted(self, uid):
        return self._is_owner(uid) or uid in self._db.get(
            "XR", "trusted_users", []
        )

    def _proxy_running(self):
        proc = self._proc
        return proc is not None and proc.poll() is None

    def _get_uptime(self):
        if not self._proxy_running() or self._start_time == 0:
            return "n/a"
        e = int(time.time() - self._start_time)
        d, e = divmod(e, 86400)
        h, r = divmod(e, 3600)
        m, s = divmod(r, 60)
        parts = []
        if d:
            parts.append(f"{d}d")
        if h:
            parts.append(f"{h}h")
        if m:
            parts.append(f"{m}m")
        parts.append(f"{s}s")
        return " ".join(parts)

    def _xray_installed(self):
        return (
            self._xray_path
            and os.path.isfile(self._xray_path)
            and os.access(self._xray_path, os.X_OK)
        )

    def _validate_ip(self, ip_str):
        try:
            addr = ipaddress.IPv4Address(ip_str)
            return not addr.is_private and not addr.is_loopback
        except ValueError:
            return False

    async def _check_port_listening(self, port):
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection("127.0.0.1", port), timeout=2
            )
            writer.close()
            await writer.wait_closed()
            return True
        except Exception:
            return False

    async def _get_external_ip(self):
        saved = self._db.get("XR", "external_ip", "")
        if saved:
            return saved
        for svc in [
            "https://api.ipify.org",
            "https://ifconfig.me/ip",
            "https://icanhazip.com",
            "https://ident.me",
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
                        if self._validate_ip(ip):
                            self._db.set("XR", "external_ip", ip)
                            return ip
                except FileNotFoundError:
                    continue
                except Exception:
                    continue
        return ""

    async def _get_xray_version(self):
        if not self._xray_installed():
            return "not installed"
        try:
            p = await asyncio.create_subprocess_exec(
                self._xray_path, "version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, err = await p.communicate()
            text = (out or err or b"").decode().strip()
            return text.split("\n")[0][:200] if text else "unknown"
        except Exception:
            return "unknown"

    async def _safe_edit(self, msg, text):
        try:
            if isinstance(msg, list):
                msg = msg[0]
            await msg.edit(text)
        except Exception:
            pass

    def _parse_xray_x25519_output(self, text):
        result = {}
        for line in text.split("\n"):
            line = line.strip()
            if not line or ":" not in line:
                continue
            key, _, val = line.partition(":")
            key = key.strip().lower()
            val = val.strip()
            if "private" in key:
                result["private"] = val
            elif "password" in key:
                result["public"] = val
            elif "public" in key:
                result["public"] = val
        return result

    async def _generate_x25519(self):
        if not self._xray_installed():
            return None, None
        try:
            p = await asyncio.create_subprocess_exec(
                self._xray_path, "x25519",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, err = await p.communicate()
            text = (out or b"").decode().strip()
            if not text:
                text = (err or b"").decode().strip()

            parsed = self._parse_xray_x25519_output(text)
            private_key = parsed.get("private", "")
            public_key = parsed.get("public", "")

            if private_key and public_key:
                return private_key, public_key

            return None, None

        except Exception as e:
            logger.error("[XR] x25519 error: %s", e)
            return None, None

    def _generate_short_id(self):
        return os.urandom(4).hex()

    def _generate_uuid(self):
        return str(uuid.uuid4())

    def _build_config(self):
        port = self._db.get("XR", "port", 8443)
        vless_uuid = self._db.get("XR", "vless_uuid", "")
        private_key = self._db.get("XR", "private_key", "")
        short_id = self._db.get("XR", "short_id", "")
        dest = self._db.get("XR", "dest", "www.google.com:443")
        sni = self._db.get("XR", "sni", "www.google.com")

        return {
            "stats": {},
            "api": {
                "tag": STATS_API_TAG,
                "services": [
                    "StatsService",
                ],
            },
            "policy": {
                "system": {
                    "statsInboundUplink": True,
                    "statsInboundDownlink": True,
                    "statsOutboundUplink": True,
                    "statsOutboundDownlink": True,
                },
            },
            "log": {
                "loglevel": "info",
                "access": os.path.join(self._root, "access.log"),
                "error": os.path.join(self._root, "error.log"),
            },
            "inbounds": [
                {
                    "tag": "vless-reality",
                    "listen": "0.0.0.0",
                    "port": port,
                    "protocol": "vless",
                    "settings": {
                        "clients": [
                            {
                                "id": vless_uuid,
                                "flow": "xtls-rprx-vision",
                            }
                        ],
                        "decryption": "none",
                    },
                    "streamSettings": {
                        "network": "tcp",
                        "security": "reality",
                        "realitySettings": {
                            "show": False,
                            "dest": dest,
                            "xver": 0,
                            "serverNames": [sni],
                            "privateKey": private_key,
                            "shortIds": [short_id, ""],
                        },
                    },
                    "sniffing": {
                        "enabled": True,
                        "destOverride": [
                            "http", "tls", "quic",
                        ],
                    },
                },
                {
                    "tag": STATS_API_TAG,
                    "listen": "127.0.0.1",
                    "port": STATS_API_PORT,
                    "protocol": "dokodemo-door",
                    "settings": {
                        "address": "127.0.0.1",
                    },
                },
            ],
            "outbounds": [
                {"tag": "direct", "protocol": "freedom"},
                {"tag": "block", "protocol": "blackhole"},
            ],
            "routing": {
                "rules": [
                    {
                        "inboundTag": [STATS_API_TAG],
                        "outboundTag": STATS_API_TAG,
                        "type": "field",
                    },
                ],
            },
        }

    def _write_config(self):
        config = self._build_config()
        tmp = self._config_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(config, f, indent=2)
        os.replace(tmp, self._config_path)

    def _build_vless_link(self, ip):
        vless_uuid = self._db.get("XR", "vless_uuid", "")
        port = self._db.get("XR", "port", 8443)
        sni = self._db.get("XR", "sni", "www.google.com")
        public_key = self._db.get("XR", "public_key", "")
        short_id = self._db.get("XR", "short_id", "")

        if not all([vless_uuid, ip, public_key]):
            return None

        return (
            f"vless://{vless_uuid}@{ip}:{port}"
            f"?encryption=none"
            f"&flow=xtls-rprx-vision"
            f"&security=reality"
            f"&sni={sni}"
            f"&fp=chrome"
            f"&pbk={public_key}"
            f"&sid={short_id}"
            f"&type=tcp"
            f"&headerType=none"
            f"#I-execute"
        )

    def _format_bytes(self, b):
        if b < 1024:
            return f"{b} B"
        if b < 1024 * 1024:
            return f"{b / 1024:.1f} KB"
        if b < 1024 * 1024 * 1024:
            return f"{b / (1024 * 1024):.1f} MB"
        return f"{b / (1024 * 1024 * 1024):.2f} GB"

    def _format_speed(self, bps):
        if bps < 1024:
            return f"{bps:.0f} B/s"
        if bps < 1024 * 1024:
            return f"{bps / 1024:.1f} KB/s"
        if bps < 1024 * 1024 * 1024:
            return f"{bps / (1024 * 1024):.1f} MB/s"
        return f"{bps / (1024 * 1024 * 1024):.2f} GB/s"

    async def _query_xray_stats(self):
        if not self._proxy_running():
            return 0, 0
        try:
            p = await asyncio.create_subprocess_exec(
                self._xray_path, "api", "statsquery",
                f"--server=127.0.0.1:{STATS_API_PORT}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await asyncio.wait_for(p.communicate(), timeout=5)
            if p.returncode != 0:
                return self._traffic_rx, self._traffic_tx

            text = out.decode()
            rx = 0
            tx = 0
            lines = text.strip().split("\n")
            i = 0
            while i < len(lines):
                line = lines[i].strip()
                if "inbound" in line and "downlink" in line.lower():
                    if i + 1 < len(lines):
                        val_line = lines[i + 1].strip()
                        m = re.search(r"(\d+)", val_line)
                        if m:
                            rx += int(m.group(1))
                elif "inbound" in line and "uplink" in line.lower():
                    if i + 1 < len(lines):
                        val_line = lines[i + 1].strip()
                        m = re.search(r"(\d+)", val_line)
                        if m:
                            tx += int(m.group(1))
                i += 1

            return rx, tx

        except (asyncio.TimeoutError, Exception):
            return self._traffic_rx, self._traffic_tx

    def _start_traffic_monitor(self):
        self._traffic_rx = 0
        self._traffic_tx = 0

        async def monitor():
            try:
                while self._proxy_running():
                    rx, tx = await self._query_xray_stats()
                    self._traffic_rx = rx
                    self._traffic_tx = tx
                    await asyncio.sleep(5)
            except asyncio.CancelledError:
                pass

        if self._traffic_task:
            self._traffic_task.cancel()
        self._traffic_task = asyncio.ensure_future(monitor())

    def _extract_ip_from_ss_peer(self, peer):
        m = re.match(r"\[::ffff:(\d+\.\d+\.\d+\.\d+)\]:\d+", peer)
        if m:
            return m.group(1)
        m = re.match(r"(\d+\.\d+\.\d+\.\d+):\d+", peer)
        if m:
            return m.group(1)
        m = re.match(r"\[([0-9a-fA-F:]+)\]:\d+", peer)
        if m:
            return None
        return None

    def _get_active_clients(self):
        port = self._db.get("XR", "port", 8443)
        unique_ips = set()
        try:
            p = subprocess.run(
                ["ss", "-tn", "state", "established", f"sport = :{port}"],
                capture_output=True, text=True, timeout=5
            )
            if p.returncode == 0:
                lines = p.stdout.strip().split("\n")
                for line in lines[1:]:
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split()
                    if len(parts) >= 4:
                        peer = parts[3]
                        ip_str = self._extract_ip_from_ss_peer(peer)
                        if ip_str:
                            try:
                                addr = ipaddress.IPv4Address(ip_str)
                                if not addr.is_loopback and not addr.is_private:
                                    unique_ips.add(ip_str)
                            except ValueError:
                                continue
        except Exception:
            pass
        return len(unique_ips)

    def _get_unique_ips_24h(self):
        acc_log = os.path.join(self._root, "access.log")
        ips = set()
        if not os.path.exists(acc_log):
            return ips

        cutoff = datetime.now() - timedelta(hours=24)

        try:
            with open(acc_log, "r") as f:
                for line in f:
                    ts_match = re.match(
                        r"(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})", line
                    )
                    if ts_match:
                        try:
                            ts = datetime.strptime(
                                ts_match.group(1), "%Y/%m/%d %H:%M:%S"
                            )
                            if ts < cutoff:
                                continue
                        except ValueError:
                            pass

                    ip_match = re.findall(
                        r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})", line
                    )
                    for ip in ip_match:
                        try:
                            addr = ipaddress.IPv4Address(ip)
                            if (
                                not addr.is_private
                                and not addr.is_loopback
                                and str(addr) != "0.0.0.0"
                            ):
                                ips.add(str(addr))
                        except ValueError:
                            pass
        except Exception:
            pass

        return ips

    async def _run_speed_test(self, progress_cb=None):
        download_results = []
        upload_results = []
        latency_results = []

        test_files = [
            ("Cloudflare", "https://speed.cloudflare.com/__down?bytes=10000000", 10_000_000),
            ("Hetzner", "http://speed.hetzner.de/10MB.bin", 10_000_000),
        ]

        if progress_cb:
            await progress_cb("Testing download speed...")

        for name, url, expected_size in test_files:
            try:
                tmp_file = os.path.join(self._root, "speedtest.tmp")
                p = await asyncio.create_subprocess_exec(
                    "curl", "-sL", "--max-time", "30",
                    "-o", tmp_file,
                    "-w", "%{speed_download} %{time_total} %{size_download}",
                    url,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                out, _ = await asyncio.wait_for(
                    p.communicate(), timeout=35
                )

                if p.returncode == 0 and out:
                    parts = out.decode().strip().split()
                    if len(parts) >= 3:
                        elapsed = float(parts[1])
                        size = int(float(parts[2]))

                        if size > 0 and elapsed > 0:
                            real_speed = size / elapsed
                            download_results.append(
                                (name, real_speed, elapsed, size)
                            )

                try:
                    os.remove(tmp_file)
                except Exception:
                    pass

                if download_results:
                    break

            except FileNotFoundError:
                break
            except (asyncio.TimeoutError, Exception):
                continue

        if not download_results:
            for name, url, expected_size in test_files:
                try:
                    tmp_file = os.path.join(self._root, "speedtest.tmp")
                    p = await asyncio.create_subprocess_exec(
                        "wget", "-q", "--timeout=30",
                        "-O", tmp_file, url,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    start = time.time()
                    await asyncio.wait_for(p.communicate(), timeout=35)
                    elapsed = time.time() - start

                    if p.returncode == 0 and os.path.exists(tmp_file):
                        size = os.path.getsize(tmp_file)
                        if size > 0 and elapsed > 0:
                            speed = size / elapsed
                            download_results.append(
                                (name, speed, elapsed, size)
                            )

                    try:
                        os.remove(tmp_file)
                    except Exception:
                        pass

                    if download_results:
                        break

                except FileNotFoundError:
                    break
                except (asyncio.TimeoutError, Exception):
                    continue

        if progress_cb:
            await progress_cb("Testing upload speed...")

        upload_size = 5 * 1024 * 1024
        upload_file = os.path.join(self._root, "upload.tmp")
        try:
            with open(upload_file, "wb") as f:
                f.write(os.urandom(upload_size))

            upload_targets = [
                ("Cloudflare", [
                    "curl", "-sL", "--max-time", "30",
                    "-X", "POST",
                    "-F", f"file=@{upload_file}",
                    "-w", "%{speed_upload} %{time_total} %{size_upload}",
                    "-o", "/dev/null",
                    "https://speed.cloudflare.com/__up",
                ]),
            ]

            for name, cmd in upload_targets:
                try:
                    p = await asyncio.create_subprocess_exec(
                        *cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    out, _ = await asyncio.wait_for(
                        p.communicate(), timeout=35
                    )

                    if p.returncode == 0 and out:
                        parts = out.decode().strip().split()
                        if len(parts) >= 3:
                            elapsed = float(parts[1])
                            size = int(float(parts[2]))

                            if size > 0 and elapsed > 0:
                                real_speed = size / elapsed
                                upload_results.append(
                                    (name, real_speed, elapsed, size)
                                )

                    if upload_results:
                        break

                except (asyncio.TimeoutError, FileNotFoundError, Exception):
                    continue

            if not upload_results:
                try:
                    start = time.time()
                    p = await asyncio.create_subprocess_exec(
                        "curl", "-sL", "--max-time", "30",
                        "-T", upload_file,
                        "-o", "/dev/null",
                        "https://speed.cloudflare.com/__up",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    await asyncio.wait_for(p.communicate(), timeout=35)
                    elapsed = time.time() - start

                    if elapsed > 0:
                        speed = upload_size / elapsed
                        upload_results.append(
                            ("Cloudflare", speed, elapsed, upload_size)
                        )
                except Exception:
                    pass

        except Exception:
            pass
        finally:
            try:
                os.remove(upload_file)
            except Exception:
                pass

        if progress_cb:
            await progress_cb("Testing latency...")

        ping_targets = [
            ("Cloudflare", "1.1.1.1"),
            ("Google", "8.8.8.8"),
            ("Telegram DC2", "149.154.167.51"),
            ("Telegram DC4", "149.154.167.91"),
        ]

        for name, host in ping_targets:
            try:
                p = await asyncio.create_subprocess_exec(
                    "ping", "-c", "3", "-W", "3", host,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                out, _ = await asyncio.wait_for(
                    p.communicate(), timeout=15
                )

                if p.returncode == 0 and out:
                    text = out.decode()
                    rtt_match = re.search(
                        r"min/avg/max.*?=\s*([\d.]+)/([\d.]+)/([\d.]+)",
                        text
                    )
                    if rtt_match:
                        rtt_min = float(rtt_match.group(1))
                        rtt_avg = float(rtt_match.group(2))
                        rtt_max = float(rtt_match.group(3))
                        latency_results.append(
                            (name, host, rtt_min, rtt_avg, rtt_max)
                        )
                    else:
                        loss_match = re.search(
                            r"(\d+)% packet loss", text
                        )
                        if loss_match:
                            loss = int(loss_match.group(1))
                            latency_results.append(
                                (name, host, -1, -1, -1, loss)
                            )
            except FileNotFoundError:
                break
            except (asyncio.TimeoutError, Exception):
                continue

        return download_results, upload_results, latency_results

    def _format_speed_results(self, download, upload, latency):
        dl_lines = []
        if download:
            for name, speed, elapsed, size in download:
                dl_lines.append(
                    f"{name}: <code>{self._format_speed(speed)}</code>"
                    f"\n{self._format_bytes(size)} in {elapsed:.1f}s"
                )
        else:
            dl_lines.append("n/a")

        ul_lines = []
        if upload:
            for name, speed, elapsed, size in upload:
                ul_lines.append(
                    f"{name}: <code>{self._format_speed(speed)}</code>"
                    f"\n{self._format_bytes(size)} in {elapsed:.1f}s"
                )
        else:
            ul_lines.append("n/a")

        lat_lines = []
        if latency:
            for item in latency:
                if len(item) == 5:
                    name, host, rtt_min, rtt_avg, rtt_max = item
                    lat_lines.append(
                        f"{name} ({host}): "
                        f"<code>{rtt_avg:.1f}ms</code>"
                        f" (min {rtt_min:.1f} / max {rtt_max:.1f})"
                    )
                elif len(item) == 6:
                    name, host, _, _, _, loss = item
                    lat_lines.append(
                        f"{name} ({host}): "
                        f"<code>{loss}% loss</code>"
                    )
        else:
            lat_lines.append("n/a")

        return "\n".join(dl_lines), "\n".join(ul_lines), "\n".join(lat_lines)

    async def _install_xray(self):
        arch = platform.machine().lower()
        arch_map = {
            "x86_64": "64", "amd64": "64",
            "aarch64": "arm64-v8a", "arm64": "arm64-v8a",
            "armv7l": "arm32-v7a", "armv6l": "arm32-v6",
        }
        go_arch = arch_map.get(arch)
        if not go_arch:
            return False, f"Unsupported arch: {arch}"
        if platform.system().lower() != "linux":
            return False, "Linux only"

        tag = None
        download_url = None

        try:
            p = await asyncio.create_subprocess_exec(
                "curl", "-sL", "--max-time", "15",
                "https://api.github.com/repos/XTLS/Xray-core/releases/latest",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await p.communicate()
            if p.returncode == 0:
                data = json.loads(out.decode())
                tag = data.get("tag_name", "")
                for asset in data.get("assets", []):
                    name = asset.get("name", "").lower()
                    if (
                        "linux" in name
                        and go_arch.lower() in name
                        and name.endswith(".zip")
                    ):
                        download_url = asset["browser_download_url"]
                        break
        except Exception as e:
            logger.warning("[XR] GitHub API: %s", e)

        if not tag or not download_url:
            return False, "Failed to get download URL from GitHub API"

        tmp = os.path.join(self._root, "tmp_install")
        os.makedirs(tmp, exist_ok=True)

        try:
            dl = os.path.join(tmp, "xray.zip")
            ok = False
            for tool in [
                ["curl", "-sL", "--max-time", "120", "-o", dl, download_url],
                ["wget", "-q", "--timeout=120", "-O", dl, download_url],
            ]:
                try:
                    p = await asyncio.create_subprocess_exec(
                        *tool,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    await p.communicate()
                    if (
                        p.returncode == 0
                        and os.path.exists(dl)
                        and os.path.getsize(dl) > 0
                    ):
                        ok = True
                        break
                except FileNotFoundError:
                    continue
            if not ok:
                return False, "Download failed"

            try:
                p = await asyncio.create_subprocess_exec(
                    "unzip", "-o", dl, "-d", tmp,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                await p.communicate()
                if p.returncode != 0:
                    raise RuntimeError("unzip failed")
            except (FileNotFoundError, RuntimeError):
                import zipfile
                with zipfile.ZipFile(dl, "r") as zf:
                    zf.extractall(tmp)

            binary = None
            for root_dir, _, files in os.walk(tmp):
                for f in files:
                    if f == "xray":
                        binary = os.path.join(root_dir, f)
                        break
                if binary:
                    break

            if not binary:
                return False, "xray binary not found in archive"

            shutil.copy2(binary, self._xray_path)
            os.chmod(self._xray_path, 0o755)

            for geo in ["geoip.dat", "geosite.dat"]:
                src = os.path.join(tmp, geo)
                if os.path.exists(src):
                    shutil.copy2(src, os.path.join(self._root, geo))

            return True, await self._get_xray_version()

        except Exception as e:
            return False, str(e)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    async def _do_setup(self, progress_cb=None):
        if _in_docker():
            return False, "docker"

        if not self._xray_installed():
            if progress_cb:
                await progress_cb(self.strings["setup_installing"])
            ok, res = await self._install_xray()
            if not ok:
                return False, f"Install failed: {res}"
            if progress_cb:
                await progress_cb(
                    self.strings["setup_installed"].format(
                        version=_escape(res)
                    )
                )

        private_key, public_key = await self._generate_x25519()
        if not private_key or not public_key:
            return False, "Failed to generate x25519 keys"

        vless_uuid = self._generate_uuid()
        short_id = self._generate_short_id()

        self._db.set("XR", "private_key", private_key)
        self._db.set("XR", "public_key", public_key)
        self._db.set("XR", "vless_uuid", vless_uuid)
        self._db.set("XR", "short_id", short_id)

        if progress_cb:
            await progress_cb(self.strings["setup_keys"])

        try:
            self._write_config()
        except Exception as e:
            return False, f"Config write error: {e}"

        if progress_cb:
            await progress_cb(self.strings["setup_config"])

        return True, None

    async def _do_start_proxy(self):
        async with self._proxy_lock:
            if self._proxy_running():
                return False, "already_running"
            if not self._xray_installed():
                return False, "not_installed"

            vless_uuid = self._db.get("XR", "vless_uuid", "")
            private_key = self._db.get("XR", "private_key", "")
            if not vless_uuid or not private_key:
                return False, "need_setup"

            port = self._db.get("XR", "port", 8443)
            listening = await self._check_port_listening(port)
            if listening:
                return False, "port_busy"

            try:
                self._write_config()
            except Exception as e:
                return False, f"Config error: {e}"

            self._check_and_rotate_logs()

            env = os.environ.copy()
            env["XRAY_LOCATION_ASSET"] = self._root

            log_path = os.path.join(self._root, "xray_run.log")
            try:
                with open(log_path, "w") as f:
                    f.write(
                        f"--- START {time.strftime('%Y-%m-%d %H:%M:%S')} ---\n"
                    )
            except Exception:
                pass

            cmd = [self._xray_path, "run", "-config", self._config_path]

            try:
                self._log_fd = open(log_path, "a")
                self._proc = subprocess.Popen(
                    cmd,
                    stdout=self._log_fd,
                    stderr=subprocess.STDOUT,
                    env=env,
                    preexec_fn=os.setsid if hasattr(os, "setsid") else None,
                )
                self._start_time = time.time()

                await asyncio.sleep(3)

                if self._proc.poll() is not None:
                    rc = self._proc.returncode
                    self._proc = None
                    self._start_time = 0
                    self._log_fd.close()
                    self._log_fd = None
                    try:
                        with open(log_path, "r") as f:
                            err_text = "".join(f.readlines()[-30:])
                    except Exception:
                        err_text = f"Exit code {rc}"
                    return False, err_text[:600]

                self._db.set("XR", "proxy_autostart", True)
                self._start_log_reader(log_path)
                self._start_traffic_monitor()

                await asyncio.sleep(1)
                port_ok = await self._check_port_listening(port)
                if not port_ok:
                    logger.warning("[XR] Port %d not listening yet", port)

                return True, None

            except Exception as e:
                if self._log_fd:
                    self._log_fd.close()
                    self._log_fd = None
                self._proc = None
                self._start_time = 0
                return False, str(e)

    async def _do_stop_proxy(self):
        async with self._proxy_lock:
            if self._traffic_task:
                self._traffic_task.cancel()
                try:
                    await self._traffic_task
                except (asyncio.CancelledError, Exception):
                    pass
                self._traffic_task = None

            if self._log_reader_task:
                self._log_reader_task.cancel()
                try:
                    await self._log_reader_task
                except (asyncio.CancelledError, Exception):
                    pass
                self._log_reader_task = None

            if self._proc:
                pid = self._proc.pid
                try:
                    if hasattr(os, "killpg"):
                        os.killpg(os.getpgid(pid), signal.SIGTERM)
                    else:
                        self._proc.terminate()
                    try:
                        self._proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        if hasattr(os, "killpg"):
                            os.killpg(os.getpgid(pid), signal.SIGKILL)
                        else:
                            self._proc.kill()
                        self._proc.wait(timeout=3)
                except Exception:
                    try:
                        self._proc.kill()
                    except Exception:
                        pass
                self._proc = None
                self._start_time = 0

            if self._log_fd:
                try:
                    self._log_fd.close()
                except Exception:
                    pass
                self._log_fd = None

            self._db.set("XR", "proxy_autostart", False)

    def _check_and_rotate_logs(self):
        for name in ["xray_run.log", "error.log", "access.log"]:
            lp = os.path.join(self._root, name)
            if os.path.exists(lp):
                try:
                    if os.path.getsize(lp) >= LOG_MAX_SIZE:
                        self._trim_log_file(lp, LOG_KEEP_SIZE)
                except Exception:
                    pass

    def _trim_log_file(self, path, keep_bytes):
        try:
            size = os.path.getsize(path)
            if size <= keep_bytes:
                return

            tmp_path = path + ".trim_tmp"
            with open(path, "rb") as f:
                f.seek(size - keep_bytes)
                f.readline()
                tail = f.read()

            with open(tmp_path, "wb") as f:
                f.write(tail)

            os.replace(tmp_path, path)
            logger.info(
                "[XR] Trimmed %s: %s -> %s",
                os.path.basename(path),
                self._format_bytes(size),
                self._format_bytes(len(tail)),
            )
        except Exception as e:
            logger.error("[XR] Failed to trim %s: %s", path, e)
            try:
                os.remove(path + ".trim_tmp")
            except Exception:
                pass

    def _start_log_rotation_scheduler(self):
        async def rotation_loop():
            try:
                while True:
                    now = datetime.now()
                    next_midnight = (now + timedelta(days=1)).replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
                    wait_seconds = (next_midnight - now).total_seconds()
                    await asyncio.sleep(wait_seconds)
                    self._check_and_rotate_logs()
            except asyncio.CancelledError:
                pass

        if self._log_rotation_task:
            self._log_rotation_task.cancel()
        self._log_rotation_task = asyncio.ensure_future(rotation_loop())

    def _read_log_file_sync(self):
        lines = []
        for name in ["xray_run.log", "error.log"]:
            lp = os.path.join(self._root, name)
            if os.path.exists(lp):
                try:
                    with open(lp, "r") as f:
                        lines.extend(f.readlines()[-200:])
                except Exception:
                    pass
        return lines[-self._max_log_lines:]

    async def _read_log_file(self):
        loop = asyncio.get_running_loop()
        self._log_lines = await loop.run_in_executor(
            None, self._read_log_file_sync
        )

    def _start_log_reader(self, log_path):
        async def reader():
            try:
                while self._proxy_running():
                    self._check_and_rotate_logs()
                    await self._read_log_file()
                    await asyncio.sleep(5)
            except asyncio.CancelledError:
                pass

        if self._log_reader_task:
            self._log_reader_task.cancel()
        self._log_reader_task = asyncio.ensure_future(reader())

    async def _send_log_files(self, chat_id):
        files_sent = False
        for name in ["xray_run.log", "error.log", "access.log"]:
            lp = os.path.join(self._root, name)
            if not os.path.exists(lp) or os.path.getsize(lp) == 0:
                continue
            txt_path = lp + ".txt"
            shutil.copy2(lp, txt_path)
            try:
                await self._client.send_file(chat_id, txt_path)
                files_sent = True
            except Exception:
                pass
            finally:
                try:
                    os.remove(txt_path)
                except Exception:
                    pass
        return files_sent

    async def _run_diagnose(self):
        results = []
        prefix = self.get_prefix()

        if _in_docker():
            results.append("FAIL Running inside Docker")
        else:
            results.append("OK Not in Docker")

        if self._xray_installed():
            ver = await self._get_xray_version()
            results.append(f"OK XRay: <code>{_escape(ver[:100])}</code>")
        else:
            results.append(
                f"FAIL XRay NOT installed, use <code>{prefix}xr setup</code>"
            )
            return results

        private_key = self._db.get("XR", "private_key", "")
        public_key = self._db.get("XR", "public_key", "")
        vless_uuid = self._db.get("XR", "vless_uuid", "")
        short_id = self._db.get("XR", "short_id", "")

        if private_key and public_key:
            results.append("OK Reality keys")
        else:
            results.append("FAIL Keys not generated")

        if vless_uuid:
            results.append("OK UUID set")
        else:
            results.append("FAIL UUID not set")

        if short_id:
            results.append(f"OK Short ID: <code>{_escape(short_id)}</code>")
        else:
            results.append("FAIL Short ID not set")

        if os.path.exists(self._config_path):
            results.append("OK Config exists")
            try:
                with open(self._config_path, "r") as f:
                    cfg = json.load(f)
                inbounds = cfg.get("inbounds", [])
                if inbounds:
                    ib = inbounds[0]
                    results.append(
                        f"   Protocol: <code>{ib.get('protocol', '?')}</code>"
                    )
                    results.append(
                        f"   Port: <code>{ib.get('port', '?')}</code>"
                    )
                    rs = (
                        ib.get("streamSettings", {})
                        .get("realitySettings", {})
                    )
                    if rs:
                        results.append(
                            f"   SNI: <code>"
                            f"{rs.get('serverNames', ['?'])[0]}</code>"
                        )
                        results.append(
                            f"   Dest: <code>{rs.get('dest', '?')}</code>"
                        )

                has_stats = "stats" in cfg
                has_api = "api" in cfg
                results.append(
                    f"   Stats API: {'OK' if has_stats and has_api else 'FAIL'}"
                )
            except Exception as e:
                results.append(f"WARN Config broken: <code>{e}</code>")
        else:
            results.append("FAIL Config missing")

        sni = self._db.get("XR", "sni", "www.google.com")
        try:
            loop = asyncio.get_running_loop()
            info = await loop.getaddrinfo(sni, 443)
            if info:
                ips = list(set(i[4][0] for i in info[:5]))
                results.append(
                    f"OK DNS {sni}: <code>{', '.join(ips[:3])}</code>"
                )
        except Exception as e:
            results.append(f"FAIL DNS {sni}: <code>{e}</code>")

        try:
            p = await asyncio.create_subprocess_exec(
                "curl", "-s", "--max-time", "5", "-o", "/dev/null",
                "-w", "%{http_code}", f"https://{sni}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await p.communicate()
            code = out.decode().strip()
            results.append(f"OK TLS {sni}: HTTP {code}")
        except FileNotFoundError:
            results.append("WARN curl not available")
        except Exception as e:
            results.append(f"WARN TLS {sni}: <code>{e}</code>")

        port = self._db.get("XR", "port", 8443)
        listening = await self._check_port_listening(port)
        results.append(
            f"{'OK' if listening else 'FAIL'} Port {port}: "
            f"{'listening' if listening else 'NOT listening'}"
        )

        stats_listening = await self._check_port_listening(STATS_API_PORT)
        results.append(
            f"{'OK' if stats_listening else 'FAIL'} Stats API port {STATS_API_PORT}: "
            f"{'listening' if stats_listening else 'NOT listening'}"
        )

        ip = await self._get_external_ip()
        results.append(
            f"{'OK' if ip else 'FAIL'} External IP: <code>{ip or '?'}</code>"
        )

        tg_dcs = {
            1: "149.154.175.53", 2: "149.154.167.51",
            3: "149.154.175.100", 4: "149.154.167.91",
            5: "91.108.56.130",
        }
        dc_res = []
        for dc_num, dc_ip in tg_dcs.items():
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(3)
                r = s.connect_ex((dc_ip, 443))
                s.close()
                dc_res.append(
                    f"DC{dc_num}:{'OK' if r == 0 else 'FAIL'}"
                )
            except Exception:
                dc_res.append(f"DC{dc_num}:FAIL")
        results.append(f"TG DCs: {' '.join(dc_res)}")

        if self._proxy_running():
            results.append(f"OK Process: PID {self._proc.pid}")

            err_log = os.path.join(self._root, "error.log")
            if os.path.exists(err_log):
                try:
                    with open(err_log, "r") as f:
                        err_lines = f.readlines()[-50:]
                    errors = [
                        l for l in err_lines
                        if "error" in l.lower() or "fatal" in l.lower()
                    ]
                    if errors:
                        results.append(f"FAIL Errors: {len(errors)}")
                        last = errors[-1].strip()[:150]
                        results.append(
                            f"   Last: <code>{_escape(last)}</code>"
                        )
                    else:
                        results.append("OK Errors: 0")
                except Exception:
                    pass
        else:
            results.append("FAIL Process not running")

        results.append(f"Work dir: <code>{_escape(self._root)}</code>")

        return results

    async def _txt_status(self):
        if self._proxy_running():
            port = self._db.get("XR", "port", 8443)
            active = self._get_active_clients()
            unique_ips = self._get_unique_ips_24h()
            trusted = self._db.get("XR", "trusted_users", [])

            rx = max(0, self._traffic_rx)
            tx = max(0, self._traffic_tx)

            return self._s(
                "status_on",
                pid=self._proc.pid,
                uptime=self._get_uptime(),
                rx=self._format_bytes(rx),
                tx=self._format_bytes(tx),
                total=self._format_bytes(rx + tx),
                active=active,
                unique_ips=len(unique_ips),
                trusted_count=len(trusted),
                port=port,
            )
        return self._s("status_off")

    def _txt_log(self):
        self._log_lines = self._read_log_file_sync()
        if not self._log_lines:
            return self._s("log_empty")
        last = self._log_lines[-50:]
        c = "".join(last)
        if len(c) > 3800:
            c = c[-3800:]
        return (
            self._s("log_title")
            + "<code>" + _escape(c) + "</code>"
            + self.strings["log_suffix"]
        )

    async def _txt_debug(self):
        ip = await self._get_external_ip()
        ver = await self._get_xray_version()
        port = self._db.get("XR", "port", 8443)
        sni = self._db.get("XR", "sni", "www.google.com")
        dest = self._db.get("XR", "dest", "www.google.com:443")
        listening = await self._check_port_listening(port)

        return self.strings["debug_info"].format(
            os_name=platform.system(),
            arch=platform.machine(),
            python=platform.python_version(),
            installed="yes" if self._xray_installed() else "no",
            xray_path=_escape(self._xray_path or "n/a"),
            xray_version=_escape(ver),
            status="running" if self._proxy_running() else "stopped",
            pid=self._proc.pid if self._proxy_running() else "n/a",
            port=port, sni=_escape(sni), dest=_escape(dest),
            ip=ip or "?",
            port_listening="yes" if listening else "no",
            config_exists=(
                "yes" if os.path.exists(self._config_path) else "no"
            ),
            docker="yes" if _in_docker() else "no",
            work_dir=_escape(self._root),
        )

    async def aiogram_watcher(self, message: AiogramMessage):
        if not message.text:
            return

        text = message.text.strip()
        if not text.startswith("/xray"):
            return

        uid = message.from_user.id

        if not self._is_trusted(uid):
            return

        try:
            ip = await self._get_external_ip()
            if not ip:
                await message.answer(
                    self._s("bot_not_configured"),
                    parse_mode="HTML",
                    link_preview_options=LinkPreviewOptions(is_disabled=True),
                )
                return

            link = self._build_vless_link(ip)
            if not link:
                await message.answer(
                    self._s("bot_not_configured"),
                    parse_mode="HTML",
                    link_preview_options=LinkPreviewOptions(is_disabled=True),
                )
                return

            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text=self.strings["bot_copy_button"],
                            copy_text=CopyTextButton(text=link),
                        )
                    ]
                ]
            )

            await message.answer(
                self._s("bot_link_response"),
                parse_mode="HTML",
                reply_markup=keyboard,
                link_preview_options=LinkPreviewOptions(is_disabled=True),
            )
        except Exception as e:
            logger.error("[XR] aiogram_watcher error: %s", e)

    @loader.command(
        ru_doc="Управление XRay VLESS+Reality VPN",
        en_doc="XRay VLESS+Reality VPN management",
    )
    async def xr(self, message):
        """XRay VLESS+Reality VPN management"""
        args = utils.get_args_raw(message).strip()
        prefix = self.get_prefix()
        if not args:
            await utils.answer(
                message,
                self.strings["help"].format(prefix=prefix),
            )
            return

        parts = args.split(maxsplit=1)
        cmd = parts[0].lower()

        h = {
            "setup": self._u_setup,
            "start": self._u_start, "stop": self._u_stop,
            "restart": self._u_restart,
            "status": self._u_status,
            "port": self._u_port, "dest": self._u_dest,
            "sni": self._u_sni, "ip": self._u_ip,
            "keys": self._u_keys, "overwrite": self._u_overwrite,
            "add": self._u_add, "rm": self._u_rm,
            "users": self._u_users,
            "log": self._u_log, "debug": self._u_debug,
            "diagnose": self._u_diagnose,
            "checkout": self._u_checkout,
            "ping": self._u_ping,
        }.get(cmd)

        if h:
            try:
                await h(message, parts)
            except Exception as e:
                logger.error("[XR] Command %s error: %s", cmd, e)
                await utils.answer(
                    message,
                    f"<b>Error:</b> <code>{_escape(str(e)[:300])}</code>"
                )
        else:
            await utils.answer(
                message,
                self.strings["help"].format(prefix=prefix),
            )

    async def _u_setup(self, msg, parts):
        if _in_docker():
            await utils.answer(msg, self._s("setup_docker"))
            return

        m = await utils.answer(msg, self._s("setup_progress"))
        log_lines = []

        async def progress_cb(text):
            log_lines.append(text)
            display = self._s("setup_progress") + "\n\n"
            display += "\n".join(
                f"<code>{_escape(l)}</code>" for l in log_lines
            )
            await self._safe_edit(m, display)

        ok, err = await self._do_setup(progress_cb=progress_cb)
        if ok:
            t = self._s("setup_done")
        elif err == "docker":
            t = self._s("setup_docker")
        else:
            t = self._s("setup_fail", error=_escape(str(err)))
        await self._safe_edit(m, t)

    async def _u_start(self, msg, parts):
        m = await utils.answer(msg, self._s("starting"))
        ok, err = await self._do_start_proxy()
        if ok:
            port = self._db.get("XR", "port", 8443)
            ip = await self._get_external_ip()
            t = self._s("started", port=port, ip=ip or "?")
        elif err == "already_running":
            t = self._s("already_running")
        elif err == "not_installed":
            t = self._s("not_installed")
        elif err == "need_setup":
            t = self._s("need_setup")
        elif err == "port_busy":
            port = self._db.get("XR", "port", 8443)
            t = self._s("port_busy", port=port)
        else:
            t = self._s("start_fail", error=_escape(str(err)))
        await self._safe_edit(m, t)

    async def _u_stop(self, msg, parts):
        if not self._proxy_running():
            await utils.answer(msg, self._s("not_running"))
            return
        await self._do_stop_proxy()
        await utils.answer(msg, self._s("stopped"))

    async def _u_restart(self, msg, parts):
        m = await utils.answer(msg, self._s("restarting"))
        await self._do_stop_proxy()
        await asyncio.sleep(1)
        ok, err = await self._do_start_proxy()
        if ok:
            port = self._db.get("XR", "port", 8443)
            ip = await self._get_external_ip()
            t = self._s("started", port=port, ip=ip or "?")
        elif err == "port_busy":
            port = self._db.get("XR", "port", 8443)
            t = self._s("port_busy", port=port)
        else:
            t = self._s("start_fail", error=_escape(str(err)))
        await self._safe_edit(m, t)

    async def _u_status(self, msg, parts):
        await utils.answer(msg, await self._txt_status())

    async def _u_port(self, msg, parts):
        if len(parts) < 2:
            await utils.answer(
                msg,
                self._s("port_current", port=self._db.get("XR", "port", 8443)),
            )
            return
        try:
            p = int(parts[1])
            if not (1025 <= p <= 65535):
                raise ValueError
        except ValueError:
            await utils.answer(msg, self._s("port_invalid"))
            return
        self._db.set("XR", "port", p)
        await utils.answer(msg, self._s("port_set", port=p))

    async def _u_dest(self, msg, parts):
        if len(parts) < 2:
            await utils.answer(
                msg,
                self._s(
                    "dest_current",
                    dest=_escape(self._db.get("XR", "dest", "www.google.com:443")),
                ),
            )
            return
        dest = parts[1].strip()
        if ":" not in dest:
            dest += ":443"
        self._db.set("XR", "dest", dest)
        await utils.answer(msg, self._s("dest_set", dest=_escape(dest)))

    async def _u_sni(self, msg, parts):
        if len(parts) < 2:
            await utils.answer(
                msg,
                self._s(
                    "sni_current",
                    sni=_escape(self._db.get("XR", "sni", "www.google.com")),
                ),
            )
            return
        sni = parts[1].strip().lower()
        self._db.set("XR", "sni", sni)
        await utils.answer(msg, self._s("sni_set", sni=_escape(sni)))

    async def _u_ip(self, msg, parts):
        if len(parts) >= 2:
            ip_str = parts[1].strip()
            if not self._validate_ip(ip_str):
                await utils.answer(msg, self._s("ip_invalid"))
                return
            self._db.set("XR", "external_ip", ip_str)
            await utils.answer(msg, self._s("ip_set", ip=_escape(ip_str)))
            return
        ip = await self._get_external_ip()
        if ip:
            await utils.answer(msg, self._s("ip_detected", ip=ip))
        else:
            await utils.answer(msg, self._s("ip_fail"))

    async def _u_keys(self, msg, parts):
        if not msg.is_private:
            await utils.answer(msg, self._s("keys_pm_only"))
            return
        private_key = self._db.get("XR", "private_key", "")
        public_key = self._db.get("XR", "public_key", "")
        short_id = self._db.get("XR", "short_id", "")
        vless_uuid = self._db.get("XR", "vless_uuid", "")
        sni = self._db.get("XR", "sni", "www.google.com")
        dest = self._db.get("XR", "dest", "www.google.com:443")
        port = self._db.get("XR", "port", 8443)
        if not private_key:
            await utils.answer(msg, self._s("need_setup"))
            return
        await utils.answer(
            msg,
            self._s(
                "keys_info",
                private_key=private_key,
                public_key=public_key,
                short_id=short_id,
                uid=vless_uuid,
                sni=_escape(sni),
                dest=_escape(dest),
                port=port,
            ),
        )

    async def _u_overwrite(self, msg, parts):
        if not self._xray_installed():
            await utils.answer(msg, self._s("not_installed"))
            return

        m = await utils.answer(msg, self._s("overwrite_progress"))

        private_key, public_key = await self._generate_x25519()
        if not private_key:
            await self._safe_edit(
                m, self._s("overwrite_fail", error="Key generation failed")
            )
            return

        new_uuid = self._generate_uuid()
        new_short_id = self._generate_short_id()

        self._db.set("XR", "private_key", private_key)
        self._db.set("XR", "public_key", public_key)
        self._db.set("XR", "vless_uuid", new_uuid)
        self._db.set("XR", "short_id", new_short_id)

        try:
            self._write_config()
        except Exception as e:
            await self._safe_edit(
                m, self._s("overwrite_fail", error=f"Config write error: {e}")
            )
            return

        was_running = self._proxy_running()

        if was_running:
            await self._do_stop_proxy()
            await asyncio.sleep(1)
            ok, err = await self._do_start_proxy()
            if not ok:
                await self._safe_edit(
                    m, self._s("overwrite_fail", error=f"Restart failed: {err}")
                )
                return

        await self._safe_edit(m, self._s("overwrite_done"))

    async def _u_add(self, msg, parts):
        reply = await msg.get_reply_message()
        if not reply or not reply.sender_id:
            await utils.answer(msg, self._s("user_need_reply"))
            return
        uid = reply.sender_id
        t = self._db.get("XR", "trusted_users", [])
        if uid not in t:
            t.append(uid)
            self._db.set("XR", "trusted_users", t)
        await utils.answer(msg, self._s("user_added", uid=uid))

    async def _u_rm(self, msg, parts):
        reply = await msg.get_reply_message()
        if not reply or not reply.sender_id:
            await utils.answer(msg, self._s("user_need_reply"))
            return
        uid = reply.sender_id
        t = self._db.get("XR", "trusted_users", [])
        if uid in t:
            t.remove(uid)
            self._db.set("XR", "trusted_users", t)
            await utils.answer(msg, self._s("user_removed", uid=uid))
        else:
            await utils.answer(msg, self._s("user_not_found"))

    async def _u_users(self, msg, parts):
        t = self._db.get("XR", "trusted_users", [])
        if not t:
            await utils.answer(msg, self._s("users_empty"))
            return
        await utils.answer(
            msg,
            self._s(
                "users_list",
                users="\n".join(f"<code>{u}</code>" for u in t),
            ),
        )

    async def _u_log(self, msg, parts):
        if len(parts) >= 2 and parts[1].lower() == "full":
            chat_id = msg.chat_id
            try:
                await msg.delete()
            except Exception:
                pass
            files_sent = await self._send_log_files(chat_id)
            if not files_sent:
                await self._client.send_message(
                    chat_id, self._s("log_empty"), parse_mode="html"
                )
            return
        await utils.answer(msg, self._txt_log())

    async def _u_debug(self, msg, parts):
        await utils.answer(msg, await self._txt_debug())

    async def _u_diagnose(self, msg, parts):
        m = await utils.answer(msg, "<b>Diagnosing...</b>")
        r = await self._run_diagnose()
        await self._safe_edit(
            m,
            self.strings["diagnose_title"]
            + "\n".join(r)
            + self.strings["diagnose_suffix"]
        )

    async def _u_checkout(self, msg, parts):
        ips = self._get_unique_ips_24h()
        chat_id = msg.chat_id
        try:
            await msg.delete()
        except Exception:
            pass

        if not ips:
            await self._client.send_message(
                chat_id, self._s("checkout_empty"), parse_mode="html"
            )
            return

        content = f"XRay connected IPs (last 24h)\n"
        content += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        content += f"Total unique IPs: {len(ips)}\n"
        content += f"{'=' * 40}\n\n"

        for ip in sorted(ips):
            content += f"{ip}\n"

        file_path = os.path.join(self._root, "checkout_24h.txt")
        try:
            with open(file_path, "w") as f:
                f.write(content)
            await self._client.send_file(chat_id, file_path)
        except Exception as e:
            await self._client.send_message(
                chat_id,
                f"<b>Error:</b> <code>{_escape(str(e)[:200])}</code>",
                parse_mode="html",
            )
        finally:
            try:
                os.remove(file_path)
            except Exception:
                pass

    async def _u_ping(self, msg, parts):
        m = await utils.answer(msg, self._s("ping_progress"))
        step_lines = []

        async def progress_cb(text):
            step_lines.append(text)
            display = self._s("ping_progress") + "\n\n"
            display += "\n".join(
                f"<code>{_escape(l)}</code>" for l in step_lines
            )
            await self._safe_edit(m, display)

        try:
            dl, ul, lat = await self._run_speed_test(
                progress_cb=progress_cb
            )
            dl_text, ul_text, lat_text = self._format_speed_results(
                dl, ul, lat
            )
            await self._safe_edit(
                m,
                self._s(
                    "ping_result",
                    download=dl_text,
                    upload=ul_text,
                    latency=lat_text,
                ),
            )
        except Exception as e:
            await self._safe_edit(
                m,
                self._s("ping_fail", error=_escape(str(e)[:300])),
            )