__version__ = (1, 0, 1)
# meta developer: FireJester.t.me

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
import secrets
import re
import tempfile
from datetime import datetime, timedelta

from aiogram.types import (
    Message as AiogramMessage,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    LinkPreviewOptions,
)
from .. import loader, utils

logger = logging.getLogger(__name__)

LOG_MAX_SIZE = 30 * 1024 * 1024
LOG_KEEP_SIZE = 10 * 1024 * 1024


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


def _extract_bot_secret(full_secret):
    if not full_secret:
        return ""
    s = full_secret.lower()
    if s.startswith("ee") and len(s) >= 34:
        return s[2:34]
    if len(s) == 32:
        return s
    return s[:32] if len(s) > 32 else s


@loader.tds
class MTProxy(loader.Module):
    """MTProxy server manager"""

    strings = {
        "name": "MTProxy",
        "help": (
            "<b>MTProxy</b>\n\n"
            "<b>Setup:</b>\n"
            "<blockquote>"
            "<code>{prefix}mtp setup</code> install mtg + generate secret + config\n"
            "<code>{prefix}mtp start</code> / <code>{prefix}mtp stop</code> / <code>{prefix}mtp restart</code>\n"
            "<code>{prefix}mtp status</code> status and connections"
            "</blockquote>\n\n"
            "<b>Settings:</b>\n"
            "<blockquote>"
            "<code>{prefix}mtp port [port]</code> port (default 443)\n"
            "<code>{prefix}mtp domain [domain]</code> fake TLS domain\n"
            "<code>{prefix}mtp ip [address]</code> external IP\n"
            "<code>{prefix}mtp doh [ip]</code> DNS-over-HTTPS server\n"
            "<code>{prefix}mtp timeout [sec]</code> timeout (default 30)\n"
            "<code>{prefix}mtp secret</code> show current secret (PM only)\n"
            "<code>{prefix}mtp botinfo</code> info for @MTProxybot (PM only)\n"
            "<code>{prefix}mtp overwrite</code> new secret + restart"
            "</blockquote>\n\n"
            "<b>Access:</b>\n"
            "<blockquote>"
            "<code>{prefix}mtp add</code> reply to add trusted user\n"
            "<code>{prefix}mtp rm</code> reply to remove trusted user\n"
            "<code>{prefix}mtp users</code> list trusted users"
            "</blockquote>\n\n"
            "<b>Debug:</b>\n"
            "<blockquote>"
            "<code>{prefix}mtp log</code> / <code>{prefix}mtp log full</code>\n"
            "<code>{prefix}mtp debug</code> debug info\n"
            "<code>{prefix}mtp diagnose</code> diagnostics\n"
            "<code>{prefix}mtp ping</code> host speed test"
            "</blockquote>\n\n"
            "<b>Bot:</b>\n"
            "<blockquote>"
            "/mtp get proxy link (trusted users only)"
            "</blockquote>"
        ),

        "not_installed": "<b>mtg not installed</b>\n<code>{prefix}mtp setup</code>",
        "setup_progress": "<b>Setting up MTProxy...</b>",
        "setup_installing": "Downloading mtg...",
        "setup_installed": "mtg installed: {version}",
        "setup_secret": "Secret generated",
        "setup_done": "<b>Setup complete</b>\n\nNow: <code>{prefix}mtp start</code>",
        "setup_fail": "<b>Setup failed</b>\n\n<code>{error}</code>",
        "setup_docker": (
            "<b>Docker detected</b>\n\n"
            "This module cannot work inside Docker container.\n"
            "Unload: <code>{prefix}ulm MTProxy</code>"
        ),

        "already_running": "<b>Proxy already running</b>",
        "not_running": "<b>Proxy not running</b>",
        "starting": "<b>Starting MTProxy...</b>",
        "started": (
            "<b>MTProxy started</b>\n\n"
            "<blockquote>"
            "Port: <code>{port}</code>\n"
            "Domain: <code>{domain}</code>\n"
            "IP: <code>{ip}</code>"
            "</blockquote>"
        ),
        "start_fail": "<b>Start failed</b>\n\n<code>{error}</code>",
        "stopped": "<b>Proxy stopped</b>",
        "restarting": "<b>Restarting...</b>",
        "port_busy": (
            "<b>Port {port} is busy</b>\n\n"
            "Change port: <code>{prefix}mtp port [port]</code>\n"
            "Then: <code>{prefix}mtp start</code>"
        ),

        "status_on": (
            "<b>MTProxy Status</b>\n\n"
            "<blockquote>"
            "<b>State:</b> running\n"
            "<b>PID:</b> <code>{pid}</code>\n"
            "<b>Uptime:</b> <code>{uptime}</code>"
            "</blockquote>\n\n"
            "<b>Config:</b>\n"
            "<blockquote>"
            "Port: <code>{port}</code>\n"
            "Domain: <code>{domain}</code>\n"
            "IP: <code>{ip}</code>\n"
            "DoH: <code>{doh}</code>"
            "</blockquote>\n\n"
            "Trusted users: <code>{trusted_count}</code>"
        ),
        "status_off": "<b>MTProxy Status</b>\n\n<blockquote><b>State:</b> stopped</blockquote>",

        "secret_info": (
            "<b>MTProxy Secret</b>\n\n"
            "<b>Full secret (for clients):</b>\n"
            "<blockquote><code>{secret}</code></blockquote>\n\n"
            "<b>Base secret (for @MTProxybot):</b>\n"
            "<blockquote><code>{bot_secret}</code></blockquote>\n\n"
            "Domain: <code>{domain}</code>\n"
            "Port: <code>{port}</code>\n"
            "IP: <code>{ip}</code>"
        ),
        "secret_pm_only": "<b>Secret only in PM</b>",

        "botinfo_title": (
            "<b>Info for @MTProxybot</b>\n\n"
            "<b>Step 1. Send host:port to bot:</b>\n"
            "<blockquote><code>{host_port}</code></blockquote>\n\n"
            "<b>Step 2. Send secret to bot:</b>\n"
            "<blockquote><code>{bot_secret}</code></blockquote>\n\n"
            "Send these values to @MTProxybot one by one."
        ),
        "botinfo_pm_only": "<b>Bot info only in PM</b>",

        "port_current": "Port: <code>{port}</code>",
        "port_set": "Port: <code>{port}</code>\n<code>{prefix}mtp restart</code>",
        "port_invalid": "<b>Port must be 1-65535</b>",

        "domain_set": "Domain: <code>{domain}</code>\n<code>{prefix}mtp restart</code>",
        "domain_current": "Domain: <code>{domain}</code>",

        "ip_set": "IP: <code>{ip}</code>",
        "ip_detected": "IP: <code>{ip}</code>",
        "ip_fail": "<b>IP not detected</b>\n<code>{prefix}mtp ip [address]</code>",
        "ip_invalid": "<b>Invalid IP address</b>",

        "doh_set": "DoH: <code>{doh}</code>\n<code>{prefix}mtp restart</code>",
        "doh_current": "DoH: <code>{doh}</code>",

        "timeout_set": "Timeout: <code>{timeout}s</code>\n<code>{prefix}mtp restart</code>",
        "timeout_current": "Timeout: <code>{timeout}s</code>",

        "user_added": "<b>Added:</b> <blockquote><code>{uid}</code></blockquote>",
        "user_removed": "<b>Removed:</b> <blockquote><code>{uid}</code></blockquote>",
        "user_not_found": "<b>Not found</b>",
        "user_need_reply": "<b>Reply to a message to add/remove user</b>",
        "users_list": "<b>Trusted users:</b>\n\n<blockquote>{users}</blockquote>",
        "users_empty": "<b>No trusted users</b>",

        "log_empty": "<b>Log empty</b>",
        "log_title": "<b>MTProxy log:</b>\n\n<blockquote>",
        "log_suffix": "</blockquote>",

        "need_setup": "<b>Setup first</b>\n<code>{prefix}mtp setup</code>",
        "no_config": (
            "<b>Not configured</b>\n\n"
            "1. <code>{prefix}mtp setup</code>\n"
            "2. <code>{prefix}mtp start</code>"
        ),

        "overwrite_progress": "<b>Overwriting secret...</b>",
        "overwrite_done": (
            "<b>Secret overwritten</b>\n\n"
            "Old links are now dead.\n"
            "Get new link via /mtp in bot."
        ),
        "overwrite_fail": "<b>Overwrite failed</b>\n\n<code>{error}</code>",

        "debug_info": (
            "<b>MTProxy Debug</b>\n\n"
            "<b>System:</b>\n"
            "<blockquote>"
            "OS: <code>{os_name}</code>\n"
            "Arch: <code>{arch}</code>\n"
            "Python: <code>{python}</code>"
            "</blockquote>\n\n"
            "<b>mtg:</b>\n"
            "<blockquote>"
            "Installed: {installed}\n"
            "Path: <code>{mtg_path}</code>\n"
            "Version: <code>{mtg_version}</code>"
            "</blockquote>\n\n"
            "<b>Proxy:</b>\n"
            "<blockquote>"
            "Status: {status}\n"
            "PID: <code>{pid}</code>\n"
            "Port: <code>{port}</code>\n"
            "Domain: <code>{domain}</code>\n"
            "IP: <code>{ip}</code>\n"
            "DoH: <code>{doh}</code>\n"
            "Timeout: <code>{timeout}s</code>"
            "</blockquote>\n\n"
            "<b>Checks:</b>\n"
            "<blockquote>"
            "Port listening: {port_listening}\n"
            "Docker: {docker}\n"
            "Work dir: <code>{work_dir}</code>"
            "</blockquote>"
        ),

        "diagnose_title": "<b>Diagnostics</b>\n\n<blockquote>",
        "diagnose_suffix": "</blockquote>",

        "bot_link_response": (
            "<b>Your MTProxy link</b>\n\n"
            "Press the button below to connect."
        ),
        "bot_not_configured": "<b>Proxy not configured yet</b>",
        "bot_connect_button": "Connect MTProxy",

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
            "<b>MTProxy</b>\n\n"
            "<b>Установка:</b>\n"
            "<blockquote>"
            "<code>{prefix}mtp setup</code> установка mtg + генерация секрета + конфиг\n"
            "<code>{prefix}mtp start</code> / <code>{prefix}mtp stop</code> / <code>{prefix}mtp restart</code>\n"
            "<code>{prefix}mtp status</code> статус и подключения"
            "</blockquote>\n\n"
            "<b>Настройки:</b>\n"
            "<blockquote>"
            "<code>{prefix}mtp port [порт]</code> порт (по умолчанию 443)\n"
            "<code>{prefix}mtp domain [домен]</code> домен fake TLS\n"
            "<code>{prefix}mtp ip [адрес]</code> внешний IP\n"
            "<code>{prefix}mtp doh [ip]</code> DNS-over-HTTPS сервер\n"
            "<code>{prefix}mtp timeout [сек]</code> таймаут (по умолчанию 30)\n"
            "<code>{prefix}mtp secret</code> показать текущий секрет (только в ЛС)\n"
            "<code>{prefix}mtp botinfo</code> инфо для @MTProxybot (только в ЛС)\n"
            "<code>{prefix}mtp overwrite</code> новый секрет + перезапуск"
            "</blockquote>\n\n"
            "<b>Доступ:</b>\n"
            "<blockquote>"
            "<code>{prefix}mtp add</code> ответ на сообщение для добавления пользователя\n"
            "<code>{prefix}mtp rm</code> ответ на сообщение для удаления пользователя\n"
            "<code>{prefix}mtp users</code> список доверенных пользователей"
            "</blockquote>\n\n"
            "<b>Отладка:</b>\n"
            "<blockquote>"
            "<code>{prefix}mtp log</code> / <code>{prefix}mtp log full</code>\n"
            "<code>{prefix}mtp debug</code> отладочная информация\n"
            "<code>{prefix}mtp diagnose</code> диагностика\n"
            "<code>{prefix}mtp ping</code> тест скорости хоста"
            "</blockquote>\n\n"
            "<b>Бот:</b>\n"
            "<blockquote>"
            "/mtp получить ссылку на прокси (только доверенные пользователи)"
            "</blockquote>"
        ),

        "not_installed": "<b>mtg не установлен</b>\n<code>{prefix}mtp setup</code>",
        "setup_progress": "<b>Настройка MTProxy...</b>",
        "setup_installing": "Скачивание mtg...",
        "setup_installed": "mtg установлен: {version}",
        "setup_secret": "Секрет сгенерирован",
        "setup_done": "<b>Настройка завершена</b>\n\nТеперь: <code>{prefix}mtp start</code>",
        "setup_fail": "<b>Настройка не удалась</b>\n\n<code>{error}</code>",
        "setup_docker": (
            "<b>Обнаружен Docker</b>\n\n"
            "Этот модуль не может работать внутри Docker контейнера.\n"
            "Выгрузить: <code>{prefix}ulm MTProxy</code>"
        ),

        "already_running": "<b>Прокси уже запущен</b>",
        "not_running": "<b>Прокси не запущен</b>",
        "starting": "<b>Запуск MTProxy...</b>",
        "started": (
            "<b>MTProxy запущен</b>\n\n"
            "<blockquote>"
            "Порт: <code>{port}</code>\n"
            "Домен: <code>{domain}</code>\n"
            "IP: <code>{ip}</code>"
            "</blockquote>"
        ),
        "start_fail": "<b>Ошибка запуска</b>\n\n<code>{error}</code>",
        "stopped": "<b>Прокси остановлен</b>",
        "restarting": "<b>Перезапуск...</b>",
        "port_busy": (
            "<b>Порт {port} занят</b>\n\n"
            "Сменить порт: <code>{prefix}mtp port [порт]</code>\n"
            "Затем: <code>{prefix}mtp start</code>"
        ),

        "status_on": (
            "<b>Статус MTProxy</b>\n\n"
            "<blockquote>"
            "<b>Состояние:</b> работает\n"
            "<b>PID:</b> <code>{pid}</code>\n"
            "<b>Аптайм:</b> <code>{uptime}</code>"
            "</blockquote>\n\n"
            "<b>Конфиг:</b>\n"
            "<blockquote>"
            "Порт: <code>{port}</code>\n"
            "Домен: <code>{domain}</code>\n"
            "IP: <code>{ip}</code>\n"
            "DoH: <code>{doh}</code>"
            "</blockquote>\n\n"
            "Доверенных пользователей: <code>{trusted_count}</code>"
        ),
        "status_off": "<b>Статус MTProxy</b>\n\n<blockquote><b>Состояние:</b> остановлен</blockquote>",

        "secret_info": (
            "<b>Секрет MTProxy</b>\n\n"
            "<b>Полный секрет (для клиентов):</b>\n"
            "<blockquote><code>{secret}</code></blockquote>\n\n"
            "<b>Базовый секрет (для @MTProxybot):</b>\n"
            "<blockquote><code>{bot_secret}</code></blockquote>\n\n"
            "Домен: <code>{domain}</code>\n"
            "Порт: <code>{port}</code>\n"
            "IP: <code>{ip}</code>"
        ),
        "secret_pm_only": "<b>Секрет только в ЛС</b>",

        "botinfo_title": (
            "<b>Инфо для @MTProxybot</b>\n\n"
            "<b>Шаг 1. Отправьте боту host:port:</b>\n"
            "<blockquote><code>{host_port}</code></blockquote>\n\n"
            "<b>Шаг 2. Отправьте боту секрет:</b>\n"
            "<blockquote><code>{bot_secret}</code></blockquote>\n\n"
            "Отправьте эти значения в @MTProxybot по очереди."
        ),
        "botinfo_pm_only": "<b>Инфо для бота только в ЛС</b>",

        "port_current": "Порт: <code>{port}</code>",
        "port_set": "Порт: <code>{port}</code>\n<code>{prefix}mtp restart</code>",
        "port_invalid": "<b>Порт должен быть 1-65535</b>",

        "domain_set": "Домен: <code>{domain}</code>\n<code>{prefix}mtp restart</code>",
        "domain_current": "Домен: <code>{domain}</code>",

        "ip_set": "IP: <code>{ip}</code>",
        "ip_detected": "IP: <code>{ip}</code>",
        "ip_fail": "<b>IP не определён</b>\n<code>{prefix}mtp ip [адрес]</code>",
        "ip_invalid": "<b>Неверный IP адрес</b>",

        "doh_set": "DoH: <code>{doh}</code>\n<code>{prefix}mtp restart</code>",
        "doh_current": "DoH: <code>{doh}</code>",

        "timeout_set": "Таймаут: <code>{timeout}s</code>\n<code>{prefix}mtp restart</code>",
        "timeout_current": "Таймаут: <code>{timeout}s</code>",

        "user_added": "<b>Добавлен:</b> <blockquote><code>{uid}</code></blockquote>",
        "user_removed": "<b>Удалён:</b> <blockquote><code>{uid}</code></blockquote>",
        "user_not_found": "<b>Не найден</b>",
        "user_need_reply": "<b>Ответьте на сообщение для добавления/удаления пользователя</b>",
        "users_list": "<b>Доверенные пользователи:</b>\n\n<blockquote>{users}</blockquote>",
        "users_empty": "<b>Нет доверенных пользователей</b>",

        "log_empty": "<b>Лог пуст</b>",
        "log_title": "<b>Лог MTProxy:</b>\n\n<blockquote>",
        "log_suffix": "</blockquote>",

        "need_setup": "<b>Сначала настройте</b>\n<code>{prefix}mtp setup</code>",
        "no_config": (
            "<b>Не настроено</b>\n\n"
            "1. <code>{prefix}mtp setup</code>\n"
            "2. <code>{prefix}mtp start</code>"
        ),

        "overwrite_progress": "<b>Перезапись секрета...</b>",
        "overwrite_done": (
            "<b>Секрет перезаписан</b>\n\n"
            "Старые ссылки больше не работают.\n"
            "Получите новую ссылку через /mtp в боте."
        ),
        "overwrite_fail": "<b>Ошибка перезаписи</b>\n\n<code>{error}</code>",

        "debug_info": (
            "<b>Отладка MTProxy</b>\n\n"
            "<b>Система:</b>\n"
            "<blockquote>"
            "ОС: <code>{os_name}</code>\n"
            "Арх: <code>{arch}</code>\n"
            "Python: <code>{python}</code>"
            "</blockquote>\n\n"
            "<b>mtg:</b>\n"
            "<blockquote>"
            "Установлен: {installed}\n"
            "Путь: <code>{mtg_path}</code>\n"
            "Версия: <code>{mtg_version}</code>"
            "</blockquote>\n\n"
            "<b>Прокси:</b>\n"
            "<blockquote>"
            "Статус: {status}\n"
            "PID: <code>{pid}</code>\n"
            "Порт: <code>{port}</code>\n"
            "Домен: <code>{domain}</code>\n"
            "IP: <code>{ip}</code>\n"
            "DoH: <code>{doh}</code>\n"
            "Таймаут: <code>{timeout}s</code>"
            "</blockquote>\n\n"
            "<b>Проверки:</b>\n"
            "<blockquote>"
            "Порт слушает: {port_listening}\n"
            "Docker: {docker}\n"
            "Рабочая директория: <code>{work_dir}</code>"
            "</blockquote>"
        ),

        "diagnose_title": "<b>Диагностика</b>\n\n<blockquote>",
        "diagnose_suffix": "</blockquote>",

        "bot_link_response": (
            "<b>Ваша ссылка MTProxy</b>\n\n"
            "Нажмите кнопку ниже для подключения."
        ),
        "bot_not_configured": "<b>Прокси ещё не настроен</b>",
        "bot_connect_button": "MTProxy",

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
        self._mtg_path = None
        self._log_reader_task = None
        self._log_fd = None
        self._proxy_lock = asyncio.Lock()
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
            tempfile.gettempdir(), f"MTProxy_{tg_user_id}"
        )
        self._mtg_path = os.path.join(self._root, "mtg")

        os.makedirs(self._root, exist_ok=True)

        defaults = {
            "port": 443,
            "domain": "www.google.com",
            "trusted_users": [],
            "external_ip": "",
            "secret": "",
            "doh_ip": "8.8.8.8",
            "timeout": 30,
        }
        for k, v in defaults.items():
            if self._db.get("MTP", k) is None:
                self._db.set("MTP", k, v)

        self._start_log_rotation_scheduler()

        if self._db.get("MTP", "proxy_autostart", False):
            if self._mtg_installed() and self._db.get("MTP", "secret", ""):
                try:
                    await self._do_start_proxy()
                except Exception as e:
                    logger.error("[MTP] Proxy autostart error: %s", e)

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
                    os.killpg(os.getpgid(pid), signal.SIGKILL)
                else:
                    self._proc.kill()
                try:
                    self._proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    self._proc.kill()
                    self._proc.wait(timeout=2)
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
                "pkill", "-9", "-f", f"mtg.*{self._root}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(p.communicate(), timeout=5)
        except Exception:
            pass

        self._db.set("MTP", "proxy_autostart", False)

        if self._root and os.path.exists(self._root):
            try:
                shutil.rmtree(self._root, ignore_errors=True)
            except Exception:
                pass

    def _is_owner(self, uid):
        return uid == self._me.id

    def _is_trusted(self, uid):
        return self._is_owner(uid) or uid in self._db.get(
            "MTP", "trusted_users", []
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

    def _mtg_installed(self):
        return (
            self._mtg_path
            and os.path.isfile(self._mtg_path)
            and os.access(self._mtg_path, os.X_OK)
        )

    def _validate_ip(self, ip_str):
        try:
            import ipaddress
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
        saved = self._db.get("MTP", "external_ip", "")
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
                            self._db.set("MTP", "external_ip", ip)
                            return ip
                except FileNotFoundError:
                    continue
                except Exception:
                    continue
        return ""

    async def _get_mtg_version(self):
        if not self._mtg_installed():
            return "not installed"
        try:
            p = await asyncio.create_subprocess_exec(
                self._mtg_path, "--version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, err = await p.communicate()
            text = (out or err or b"").decode().strip()
            return text[:200] if text else "unknown"
        except Exception:
            return "unknown"

    async def _safe_edit(self, msg, text):
        try:
            if isinstance(msg, list):
                msg = msg[0]
            await msg.edit(text)
        except Exception:
            pass

    def _build_tg_link(self, ip, port, secret):
        return f"tg://proxy?server={ip}&port={port}&secret={secret}"

    def _build_tme_link(self, ip, port, secret):
        return f"https://t.me/proxy?server={ip}&port={port}&secret={secret}"

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

    async def _generate_secret(self):
        domain = self._db.get("MTP", "domain", "www.google.com")
        if not self._mtg_installed():
            return None
        try:
            p = await asyncio.create_subprocess_exec(
                self._mtg_path, "generate-secret", "--hex", domain,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, err = await p.communicate()
            if p.returncode == 0 and out.decode().strip():
                secret = out.decode().strip()
                self._db.set("MTP", "secret", secret)
                return secret
            logger.error("[MTP] generate-secret fail: %s", (err or b"").decode()[:300])
            return None
        except Exception as e:
            logger.error("[MTP] generate-secret error: %s", e)
            return None

    async def _install_mtg(self):
        arch = platform.machine().lower()
        arch_map = {
            "x86_64": "amd64", "amd64": "amd64",
            "aarch64": "arm64", "arm64": "arm64",
            "armv7l": "armv7", "armv6l": "armv6",
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
                "https://api.github.com/repos/9seconds/mtg/releases/latest",
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
                        go_arch in name
                        and "linux" in name
                        and not name.endswith((".sha256", ".md5"))
                    ):
                        download_url = asset["browser_download_url"]
                        break
        except Exception as e:
            logger.warning("[MTP] GitHub API: %s", e)

        if not tag:
            tag = "v2.1.7"
        if not download_url:
            download_url = (
                f"https://github.com/9seconds/mtg/releases/download/{tag}/"
                f"mtg-{tag.lstrip('v')}-linux-{go_arch}.tar.gz"
            )

        tmp = os.path.join(self._root, "tmp_install")
        os.makedirs(tmp, exist_ok=True)

        try:
            is_tar = download_url.endswith((".tar.gz", ".tgz"))
            dl = os.path.join(tmp, "mtg.tar.gz" if is_tar else "mtg")

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

            if is_tar:
                try:
                    p = await asyncio.create_subprocess_exec(
                        "tar", "-xzf", dl, "-C", tmp,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    await p.communicate()
                    if p.returncode != 0:
                        raise RuntimeError("tar failed")
                except (FileNotFoundError, RuntimeError):
                    import tarfile
                    with tarfile.open(dl, "r:gz") as tf:
                        tf.extractall(tmp)

                binary = None
                for root_dir, _, files in os.walk(tmp):
                    for f in files:
                        if f == "mtg" or (
                            f.startswith("mtg")
                            and not f.endswith(
                                (".tar.gz", ".tgz", ".sha256", ".md5", ".txt")
                            )
                        ):
                            candidate = os.path.join(root_dir, f)
                            if os.path.isfile(candidate):
                                binary = candidate
                                break
                    if binary:
                        break
                if not binary:
                    return False, "mtg binary not found in archive"
            else:
                binary = dl

            shutil.copy2(binary, self._mtg_path)
            os.chmod(self._mtg_path, 0o755)

            return True, await self._get_mtg_version()

        except Exception as e:
            return False, str(e)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    async def _do_setup(self, progress_cb=None):
        if _in_docker():
            return False, "docker"

        if not self._mtg_installed():
            if progress_cb:
                await progress_cb(self.strings["setup_installing"])
            ok, res = await self._install_mtg()
            if not ok:
                return False, f"Install failed: {res}"
            if progress_cb:
                await progress_cb(
                    self.strings["setup_installed"].format(
                        version=_escape(res)
                    )
                )

        secret = await self._generate_secret()
        if not secret:
            return False, "Failed to generate secret"

        if progress_cb:
            await progress_cb(self.strings["setup_secret"])

        return True, None

    async def _do_start_proxy(self):
        async with self._proxy_lock:
            if self._proxy_running():
                return False, "already_running"
            if not self._mtg_installed():
                return False, "not_installed"

            secret = self._db.get("MTP", "secret", "")
            if not secret:
                return False, "need_setup"

            port = self._db.get("MTP", "port", 443)
            doh = self._db.get("MTP", "doh_ip", "8.8.8.8")
            timeout = self._db.get("MTP", "timeout", 30)

            listening = await self._check_port_listening(port)
            if listening:
                return False, "port_busy"

            secret = await self._generate_secret()
            if not secret:
                return False, "Secret generation failed"

            self._check_and_rotate_logs()

            bind = f"0.0.0.0:{port}"
            log_path = os.path.join(self._root, "mtg.log")

            try:
                with open(log_path, "w") as f:
                    f.write(
                        f"--- START {time.strftime('%Y-%m-%d %H:%M:%S')} ---\n"
                    )
            except Exception:
                pass

            cmd = [
                self._mtg_path, "simple-run",
                bind,
                secret,
                "-i", "prefer-ipv4",
                "-n", doh,
                "-t", f"{timeout}s",
                "-d",
            ]

            try:
                self._log_fd = open(log_path, "a")
                self._proc = subprocess.Popen(
                    cmd,
                    stdout=self._log_fd,
                    stderr=subprocess.STDOUT,
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

                self._db.set("MTP", "proxy_autostart", True)
                self._start_log_reader(log_path)

                await asyncio.sleep(1)
                port_ok = await self._check_port_listening(port)
                if not port_ok:
                    logger.warning("[MTP] Port %d not listening yet", port)

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

            self._db.set("MTP", "proxy_autostart", False)

    def _check_and_rotate_logs(self):
        for name in ["mtg.log"]:
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
                "[MTP] Trimmed %s: %s -> %s",
                os.path.basename(path),
                self._format_bytes(size),
                self._format_bytes(len(tail)),
            )
        except Exception as e:
            logger.error("[MTP] Failed to trim %s: %s", path, e)
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
        lp = os.path.join(self._root, "mtg.log")
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
        lp = os.path.join(self._root, "mtg.log")
        if not os.path.exists(lp) or os.path.getsize(lp) == 0:
            return False
        txt_path = lp + ".txt"
        shutil.copy2(lp, txt_path)
        try:
            await self._client.send_file(chat_id, txt_path)
            return True
        except Exception:
            return False
        finally:
            try:
                os.remove(txt_path)
            except Exception:
                pass

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

    async def _run_diagnose(self):
        results = []
        prefix = self.get_prefix()

        if self._mtg_installed():
            ver = await self._get_mtg_version()
            results.append(f"OK mtg: <code>{_escape(ver[:100])}</code>")
        else:
            results.append(
                f"FAIL mtg NOT installed, use <code>{prefix}mtp setup</code>"
            )
            return results

        if _in_docker():
            results.append("FAIL Running inside Docker")
        else:
            results.append("OK Not in Docker")

        secret = self._db.get("MTP", "secret", "")
        if secret:
            bot_secret = _extract_bot_secret(secret)
            results.append(
                f"OK Secret: {len(secret)} chars, prefix=<code>{secret[:2]}</code>"
            )
            results.append(
                f"   Bot secret: <code>{bot_secret[:8]}...</code> ({len(bot_secret)} chars)"
            )
        else:
            results.append("FAIL Secret not generated")

        domain = self._db.get("MTP", "domain", "www.google.com")
        results.append(f"OK Domain: <code>{_escape(domain)}</code>")

        try:
            loop = asyncio.get_running_loop()
            info = await loop.getaddrinfo(domain, 443)
            if info:
                ips = list(set(i[4][0] for i in info[:5]))
                results.append(
                    f"OK DNS {domain}: <code>{', '.join(ips[:3])}</code>"
                )
        except Exception as e:
            results.append(f"FAIL DNS {domain}: <code>{e}</code>")

        doh = self._db.get("MTP", "doh_ip", "8.8.8.8")
        results.append(f"DoH DNS: <code>{doh}</code>")
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3)
            r = s.connect_ex((doh, 443))
            s.close()
            if r == 0:
                results.append(f"OK DoH {doh}:443 reachable")
            else:
                results.append(f"FAIL DoH {doh}:443 NOT reachable")
        except Exception as e:
            results.append(f"FAIL DoH {doh}: <code>{e}</code>")

        try:
            p = await asyncio.create_subprocess_exec(
                "curl", "-s", "--max-time", "5", "-o", "/dev/null",
                "-w", "%{http_code}", f"https://{domain}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await p.communicate()
            code = out.decode().strip()
            results.append(f"OK TLS {domain}: HTTP {code}")
        except FileNotFoundError:
            results.append("WARN curl not available")
        except Exception as e:
            results.append(f"WARN TLS {domain}: <code>{e}</code>")

        port = self._db.get("MTP", "port", 443)
        listening = await self._check_port_listening(port)
        results.append(
            f"{'OK' if listening else 'FAIL'} Port {port}: "
            f"{'listening' if listening else 'NOT listening'}"
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

            log_lines = self._read_log_file_sync()
            err_lines = [
                l for l in log_lines[-100:]
                if '"level":"error"' in l or '"level":"fatal"' in l
            ]
            if err_lines:
                results.append(f"FAIL Errors in log: {len(err_lines)}")
                last = err_lines[-1].strip()[:150]
                results.append(
                    f"   Last: <code>{_escape(last)}</code>"
                )
            else:
                results.append("OK No errors in log")
        else:
            results.append("FAIL Process not running")

        results.append(f"Work dir: <code>{_escape(self._root)}</code>")

        return results

    async def _txt_status(self):
        if self._proxy_running():
            port = self._db.get("MTP", "port", 443)
            domain = self._db.get("MTP", "domain", "www.google.com")
            ip = await self._get_external_ip()
            doh = self._db.get("MTP", "doh_ip", "8.8.8.8")
            trusted = self._db.get("MTP", "trusted_users", [])

            return self._s(
                "status_on",
                pid=self._proc.pid,
                uptime=self._get_uptime(),
                port=port,
                domain=_escape(domain),
                ip=ip or "?",
                doh=doh,
                trusted_count=len(trusted),
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
        ver = await self._get_mtg_version()
        port = self._db.get("MTP", "port", 443)
        domain = self._db.get("MTP", "domain", "www.google.com")
        doh = self._db.get("MTP", "doh_ip", "8.8.8.8")
        timeout = self._db.get("MTP", "timeout", 30)
        listening = await self._check_port_listening(port)

        return self.strings["debug_info"].format(
            os_name=platform.system(),
            arch=platform.machine(),
            python=platform.python_version(),
            installed="yes" if self._mtg_installed() else "no",
            mtg_path=_escape(self._mtg_path or "n/a"),
            mtg_version=_escape(ver),
            status="running" if self._proxy_running() else "stopped",
            pid=self._proc.pid if self._proxy_running() else "n/a",
            port=port,
            domain=_escape(domain),
            ip=ip or "?",
            doh=doh,
            timeout=timeout,
            port_listening="yes" if listening else "no",
            docker="yes" if _in_docker() else "no",
            work_dir=_escape(self._root),
        )

    async def aiogram_watcher(self, message: AiogramMessage):
        if not message.text:
            return

        text = message.text.strip()
        if not text.startswith("/mtp"):
            return

        uid = message.from_user.id

        if not self._is_trusted(uid):
            return

        try:
            ip = await self._get_external_ip()
            secret = self._db.get("MTP", "secret", "")
            port = self._db.get("MTP", "port", 443)

            if not ip or not secret:
                await message.answer(
                    self._s("bot_not_configured"),
                    parse_mode="HTML",
                    link_preview_options=LinkPreviewOptions(is_disabled=True),
                )
                return

            link = self._build_tme_link(ip, port, secret)

            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text=self.strings["bot_connect_button"],
                            url=link,
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
            logger.error("[MTP] aiogram_watcher error: %s", e)

    @loader.command(
        ru_doc="Управление MTProxy",
        en_doc="MTProxy management",
    )
    async def mtp(self, message):
        """MTProxy management"""
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
            "port": self._u_port, "domain": self._u_domain,
            "ip": self._u_ip,
            "doh": self._u_doh, "timeout": self._u_timeout,
            "secret": self._u_secret, "botinfo": self._u_botinfo,
            "overwrite": self._u_overwrite,
            "add": self._u_add, "rm": self._u_rm,
            "users": self._u_users,
            "log": self._u_log, "debug": self._u_debug,
            "diagnose": self._u_diagnose,
            "ping": self._u_ping,
        }.get(cmd)

        if h:
            try:
                await h(message, parts)
            except Exception as e:
                logger.error("[MTP] Command %s error: %s", cmd, e)
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
            port = self._db.get("MTP", "port", 443)
            domain = self._db.get("MTP", "domain", "www.google.com")
            ip = await self._get_external_ip()
            t = self._s(
                "started",
                port=port,
                domain=_escape(domain),
                ip=ip or "?",
            )
        elif err == "already_running":
            t = self._s("already_running")
        elif err == "not_installed":
            t = self._s("not_installed")
        elif err == "need_setup":
            t = self._s("need_setup")
        elif err == "port_busy":
            port = self._db.get("MTP", "port", 443)
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
            port = self._db.get("MTP", "port", 443)
            domain = self._db.get("MTP", "domain", "www.google.com")
            ip = await self._get_external_ip()
            t = self._s(
                "started",
                port=port,
                domain=_escape(domain),
                ip=ip or "?",
            )
        else:
            t = self._s("start_fail", error=_escape(str(err)))
        await self._safe_edit(m, t)

    async def _u_status(self, msg, parts):
        await utils.answer(msg, await self._txt_status())

    async def _u_port(self, msg, parts):
        if len(parts) < 2:
            await utils.answer(
                msg,
                self._s("port_current", port=self._db.get("MTP", "port", 443)),
            )
            return
        try:
            p = int(parts[1])
            if not (1 <= p <= 65535):
                raise ValueError
        except ValueError:
            await utils.answer(msg, self._s("port_invalid"))
            return
        self._db.set("MTP", "port", p)
        await utils.answer(msg, self._s("port_set", port=p))

    async def _u_domain(self, msg, parts):
        if len(parts) < 2:
            await utils.answer(
                msg,
                self._s(
                    "domain_current",
                    domain=_escape(self._db.get("MTP", "domain", "www.google.com")),
                ),
            )
            return
        domain = parts[1].strip().lower()
        self._db.set("MTP", "domain", domain)
        await utils.answer(msg, self._s("domain_set", domain=_escape(domain)))

    async def _u_ip(self, msg, parts):
        if len(parts) >= 2:
            ip_str = parts[1].strip()
            if not self._validate_ip(ip_str):
                await utils.answer(msg, self._s("ip_invalid"))
                return
            self._db.set("MTP", "external_ip", ip_str)
            await utils.answer(msg, self._s("ip_set", ip=_escape(ip_str)))
            return
        ip = await self._get_external_ip()
        if ip:
            await utils.answer(msg, self._s("ip_detected", ip=ip))
        else:
            await utils.answer(msg, self._s("ip_fail"))

    async def _u_doh(self, msg, parts):
        if len(parts) < 2:
            await utils.answer(
                msg,
                self._s(
                    "doh_current",
                    doh=self._db.get("MTP", "doh_ip", "8.8.8.8"),
                ),
            )
            return
        doh = parts[1].strip()
        self._db.set("MTP", "doh_ip", doh)
        await utils.answer(msg, self._s("doh_set", doh=_escape(doh)))

    async def _u_timeout(self, msg, parts):
        if len(parts) < 2:
            await utils.answer(
                msg,
                self._s(
                    "timeout_current",
                    timeout=self._db.get("MTP", "timeout", 30),
                ),
            )
            return
        try:
            t = int(parts[1])
            if t < 1:
                t = 30
        except ValueError:
            t = 30
        self._db.set("MTP", "timeout", t)
        await utils.answer(msg, self._s("timeout_set", timeout=t))

    async def _u_secret(self, msg, parts):
        if not msg.is_private:
            await utils.answer(msg, self._s("secret_pm_only"))
            return
        secret = self._db.get("MTP", "secret", "")
        if not secret:
            await utils.answer(msg, self._s("need_setup"))
            return
        bot_secret = _extract_bot_secret(secret)
        domain = self._db.get("MTP", "domain", "www.google.com")
        port = self._db.get("MTP", "port", 443)
        ip = await self._get_external_ip()
        await utils.answer(
            msg,
            self._s(
                "secret_info",
                secret=secret,
                bot_secret=bot_secret,
                domain=_escape(domain),
                port=port,
                ip=ip or "?",
            ),
        )

    async def _u_botinfo(self, msg, parts):
        if not msg.is_private:
            await utils.answer(msg, self._s("botinfo_pm_only"))
            return
        secret = self._db.get("MTP", "secret", "")
        port = self._db.get("MTP", "port", 443)
        ip = await self._get_external_ip()
        if not secret or not ip:
            await utils.answer(msg, self._s("no_config"))
            return
        bot_secret = _extract_bot_secret(secret)
        await utils.answer(
            msg,
            self._s(
                "botinfo_title",
                host_port=f"{ip}:{port}",
                bot_secret=bot_secret,
            ),
        )

    async def _u_overwrite(self, msg, parts):
        if not self._mtg_installed():
            await utils.answer(msg, self._s("not_installed"))
            return

        m = await utils.answer(msg, self._s("overwrite_progress"))

        secret = await self._generate_secret()
        if not secret:
            await self._safe_edit(
                m, self._s("overwrite_fail", error="Secret generation failed")
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
        t = self._db.get("MTP", "trusted_users", [])
        if uid not in t:
            t.append(uid)
            self._db.set("MTP", "trusted_users", t)
        await utils.answer(msg, self._s("user_added", uid=uid))

    async def _u_rm(self, msg, parts):
        reply = await msg.get_reply_message()
        if not reply or not reply.sender_id:
            await utils.answer(msg, self._s("user_need_reply"))
            return
        uid = reply.sender_id
        t = self._db.get("MTP", "trusted_users", [])
        if uid in t:
            t.remove(uid)
            self._db.set("MTP", "trusted_users", t)
            await utils.answer(msg, self._s("user_removed", uid=uid))
        else:
            await utils.answer(msg, self._s("user_not_found"))

    async def _u_users(self, msg, parts):
        t = self._db.get("MTP", "trusted_users", [])
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