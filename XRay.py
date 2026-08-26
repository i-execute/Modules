__version__ = (4, 2, 0)
# meta developer: I_execute.t.me 
# meta banner: https://github.com/i-execute/Modules/raw/main/Storage/XRay/MetaBanner.jpeg

import os
import asyncio
import io
import logging
import signal
import socket
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
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple

from telethon.tl.functions.messages import EditMessageRequest
from telethon.tl.types import InputMediaWebPage

from .. import loader, utils
from ..inline.types import InlineCall

RELOADING_MEDIA_URL = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/XRay/Reloading.jpeg"

logger = logging.getLogger(__name__)

LOG_TRIM_SIZE = 10 * 1024 * 1024
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
    """Multi-user VPN with VLESS+Reality (XHTTP/TCP(RAW)+Vision/WebSocket and post-quantum encryption)"""

    strings = {
        "name": "XRay",
        "reloaded": "<blockquote><b>XRay module successfully reloaded, everything works</b></blockquote>",
        
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
            "<pre><code class=\"language-started\"></code></pre>"
            "<blockquote>"
            "----------------\n"
            "User:      {name}\n"
            "Port:      {port}\n"
            "Transport: {transport}\n"
            "Autostart: {autostart}"
            "</blockquote>"
        ),
        "log_user_stopped": (
            "<pre><code class=\"language-stoped\"></code></pre>"
            "<blockquote>"
            "----------------\n"
            "User:      {name}\n"
            "Port:      {port}\n"
            "Transport: {transport}\n"
            "Reason:    {reason}"
            "</blockquote>"
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
    }

    strings_ru = {
        "name": "XRay",
        "reloaded": "<blockquote><b>Модуль XRay был успешно перезагружен, все воркает</b></blockquote>",
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
            "<pre><code class=\"language-started\"></code></pre>"
            "<blockquote>"
            "----------------\n"
            "User:      {name}\n"
            "Port:      {port}\n"
            "Transport: {transport}\n"
            "Autostart: {autostart}"
            "</blockquote>"
        ),
        "log_user_stopped": (
            "<pre><code class=\"language-stoped\"></code></pre>"
            "<blockquote>"
            "----------------\n"
            "User:      {name}\n"
            "Port:      {port}\n"
            "Transport: {transport}\n"
            "Reason:    {reason}"
            "</blockquote>"
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
            "Evil Cat": "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/XRay/WEB/Evil_Cat.jsx",
        }
        self._logger_topic = None
        self._asset_channel = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._me = await client.get_me()

        tg_user_id = self._me.id
        self._root = os.path.join(os.path.expanduser("~"), ".xray_on_userbot", str(tg_user_id))
        self._xray_path = os.path.join(self._root, "xray")
        self._cloudflared_path = os.path.join(self._root, "cloudflared")
        
        os.makedirs(self._root, mode=0o700, exist_ok=True)
        os.makedirs(os.path.join(self._root, "users"), mode=0o700, exist_ok=True)

        self._users = self._db.get("XR", "users", {})
        self._external_ip = await self._detect_external_ip()
        self._asset_channel = self._db.get("heroku.forums", "channel_id", None)

        if self._asset_channel:
            try:
                self._logger_topic = await utils.asset_forum_topic(
                    self._client,
                    self._db,
                    self._asset_channel,
                    "XRay",
                    description="XRay users and device limit logs",
                    icon_emoji_id=5449413488227166358,
                )
            except Exception as e:
                logger.error(f"[XR] Failed to create/get forum topic: {e}")

        if self._logger_topic and self._asset_channel:
            chat_id = int(f"-100{self._asset_channel}")
            greeting_key = f"xray_greeted_{self._asset_channel}_{self._logger_topic.id}"
            already_greeted = self.get(greeting_key, False)
            if already_greeted:
                await self._send_with_preview(chat_id, self.strings["reloaded"])
            else:
                self.set(greeting_key, True)
        
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

    async def _send_with_preview(self, chat_id, text):
        try:
            msg_text, entities = await self.inline.bot._parse_message_text(text, "html")
            msg = await self.inline.bot.send_message(
                chat_id,
                msg_text,
                parse_mode=None,
                entities=entities,
                message_thread_id=self._logger_topic.id,
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
                            url=RELOADING_MEDIA_URL,
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

    async def on_unload(self):
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        
        for name in list(self._processes.keys()):
            if name in self._users:
                self._users[name]["resume_on_module_load"] = True
            await self._stop_user(name)
        for name in list(self._tunnels.keys()):
            await self._stop_websocket_tunnel(name)
        for name in list(self._site_processes.keys()):
            await self._stop_websocket_site(name)
        self._save_users()

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
            proc = await asyncio.create_subprocess_exec(
                "systemctl", "--user", *args,
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
        with open(os.path.join(self._systemd_user_dir, unit), "w", encoding="utf-8") as f:
            f.write(content)

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

    _WEBSOCKET_SITE_SCRIPT_URL = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/XRay/WEB/websocket_site.py"

    def _websocket_site_script(
        self, path: str, backend_port: int, site_port: int, mask_url: str
    ) -> str:
        try:
            with urllib.request.urlopen(self._WEBSOCKET_SITE_SCRIPT_URL, timeout=15) as response:
                template = response.read().decode("utf-8")
        except Exception:
            template = """
import asyncio
import urllib.request
from aiohttp import web, ClientSession, WSMsgType

PATH = "__PATH__"
BACKEND = "ws://127.0.0.1:__BACKEND_PORT__" + PATH
GATE_JSX = "__MASK_URL__"
LOADING_HTML = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/XRay/WEB/Loading.html?v=loading-v4"

async def fetch_text(url, timeout=10):
    def read():
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.read().decode("utf-8")
    return await asyncio.to_thread(read)

async def index(request):
    try:
        html = await fetch_text(LOADING_HTML)
    except Exception:
        html = "<!doctype html><title>Connecting</title><body style='margin:0;background:#111214;color:#e7e7e8;font:16px system-ui;display:grid;place-items:center;min-height:100vh'>Preparing your connection.</body>"
    return web.Response(text=html, content_type="text/html", headers={"Cache-Control": "no-store"})

async def gate_jsx(request):
    try:
        body = await fetch_text(GATE_JSX)
        return web.Response(text=body, content_type="application/javascript", headers={"Cache-Control": "no-store"})
    except Exception:
        return web.Response(text="window.App=function(){return React.createElement('main',null)};", content_type="application/javascript")

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
app.router.add_get('/', index)
app.router.add_get('/gate.jsx', gate_jsx)
web.run_app(app, host='127.0.0.1', port=__SITE_PORT__)
"""
        return (
            template.replace("__PATH__", path)
            .replace("__BACKEND_PORT__", str(backend_port))
            .replace("__SITE_PORT__", str(site_port))
            .replace("__MASK_URL__", mask_url)
        )

    async def _start_websocket_site(self, name: str, user_dir: str) -> Tuple[bool, str]:
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
        mask_url = self._mask_sites[mask_name]
        user["mask_site"] = mask_name
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
            script_content = self._websocket_site_script(path, user["port"], site_port, mask_url)
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
                    text = open(log_path, "r", errors="replace").read()
                except OSError:
                    text = ""
                match = re.search(r"https://([a-z0-9-]+\.trycloudflare\.com)", text, re.I)
                if match:
                    hostname = match.group(1)
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
        path = os.path.join(tempfile.gettempdir(), f"link_for_{safe_name}.txt")
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(link + "\n")
            link_file = io.BytesIO((link + "\n").encode("utf-8"))
            link_file.name = f"link_for_{safe_name}.txt"
            await self.inline.bot.send_document(
                int(f"-100{self._asset_channel}"),
                link_file,
                caption=(
                    f"<b>WebSocket TLS link refreshed</b>\n"
                    f"<blockquote>User: <code>{_escape(name)}</code>\n"
                    f"TLS: <code>{_escape(user.get('tunnel_host', '?'))}</code></blockquote>"
                ),
                parse_mode="html",
                message_thread_id=self._logger_topic.id,
            )
        except Exception as e:
            logger.error(f"[XR] Failed to send WebSocket link file: {e}")
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass

    async def _send_log(self, text: str):
        if not self._logger_topic or not self._asset_channel:
            return
        try:
            await self.inline.bot.send_message(
                int(f"-100{self._asset_channel}"),
                text,
                disable_web_page_preview=True,
                parse_mode="HTML",
                message_thread_id=self._logger_topic.id,
            )
        except Exception as e:
            logger.error(f"[XR] Failed to send log: {e}")

    def _log_user_data(self, user: Dict) -> Dict:
        return {
            "name": _escape(user.get("name", "unknown")),
            "port": user.get("port", "n/a"),
            "transport": self._transport_label(user.get("transport", "unknown")),
            "autostart": "on" if user.get("autostart") else "off",
        }

    async def _reattach_processes(self):
        """Discover services after a module/userbot reload without owning PIDs."""
        for name, user in self._users.items():
            unit = self._unit_name(name)
            if await self._unit_active(unit):
                self._processes[name] = unit
            if user.get("transport") == "websocket":
                if await self._unit_active(self._unit_name(name, "site")):
                    self._site_processes[name] = self._unit_name(name, "site")
                if await self._unit_active(self._unit_name(name, "cf")):
                    self._tunnels[name] = self._unit_name(name, "cf")

    def _cloudflared_installed(self) -> bool:
        return (
            getattr(self, "_cloudflared_path", None)
            and os.path.isfile(self._cloudflared_path)
            and os.access(self._cloudflared_path, os.X_OK)
        )

    async def _install_cloudflared(self) -> Tuple[bool, str]:
        arch = platform.machine().lower()
        asset = {
            "x86_64": "cloudflared-linux-amd64",
            "amd64": "cloudflared-linux-amd64",
            "aarch64": "cloudflared-linux-arm64",
            "arm64": "cloudflared-linux-arm64",
        }.get(arch)
        if not asset:
            return False, f"Unsupported arch: {arch}"
        tmp_path = f"{self._cloudflared_path}.tmp"
        try:
            process = await asyncio.create_subprocess_exec(
                "curl", "-fL", "--max-time", "120", "-o", tmp_path,
                f"https://github.com/cloudflare/cloudflared/releases/latest/download/{asset}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, err = await process.communicate()
            if process.returncode != 0 or not os.path.isfile(tmp_path):
                return False, err.decode(errors="replace")[:200] or "Download failed"
            os.chmod(tmp_path, 0o755)
            check = await asyncio.create_subprocess_exec(
                tmp_path, "--version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await check.communicate()
            if check.returncode != 0:
                return False, "Cloudflared validation failed"
            os.replace(tmp_path, self._cloudflared_path)
            return True, out.decode(errors="replace").strip()[:80]
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

    async def _get_cloudflared_version(self) -> str:
        if not self._cloudflared_installed():
            return "not installed"
        try:
            p = await asyncio.create_subprocess_exec(
                self._cloudflared_path, "--version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await p.communicate()
            txt = out.decode().strip()
            if not txt:
                return "unknown"
            line = txt.split("\n")[0]
            return line[:80]
        except:
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
        
        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)
        
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
                self.strings["log_user_started"].format(**self._log_user_data(user))
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
                )
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

            if transport == "websocket":
                active_peers = set()
                for line in proc.stdout.splitlines():
                    parts = line.split()
                    if len(parts) < 2:
                        continue
                    peer = parts[-1]
                    peer_ip = peer[1:].split("]", 1)[0] if peer.startswith("[") else peer.rsplit(":", 1)[0]
                    try:
                        if not ipaddress.ip_address(peer_ip).is_loopback:
                            active_peers.add(peer)
                    except ValueError:
                        continue
                return len(active_peers)

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
                    if not (
                        addr.is_private
                        or addr.is_loopback
                        or addr.is_link_local
                        or addr.is_multicast
                        or addr.is_unspecified
                    ):
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
                            )
                        )
                        
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
        cloudflared_status = "Installed" if self._cloudflared_installed() else "Not installed"
        
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
        cf_version = await self._get_cloudflared_version()
        
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
        
        uptime = self._get_user_uptime(name)
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
            self.strings["log_user_deleted"].format(**self._log_user_data(user))
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
                await utils.answer_file(
                    call,
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
            await utils.answer_file(
                call,
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
        while not done_event.is_set():
            try:
                await asyncio.sleep(2)
                if done_event.is_set():
                    break
                lines = [f"<b>{header}</b>"]
                for label in labels:
                    cur, tot = state.get(label, (0, 0))
                    cur_mb = cur / 1024 / 1024
                    tot_mb = tot / 1024 / 1024
                    pct = cur_mb / tot_mb * 100 if tot_mb > 0 else 0.0
                    lines.append(f"  {label}: {pct:.0f}% ({cur_mb:.1f}/{tot_mb:.1f} MB)")
                await call.edit("\n".join(lines))
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

        try:
            for fpath, label in chosen:
                try:
                    await utils.answer_file(
                        call,
                        fpath,
                        caption=f"<b>{kind.title()} {label}:</b> <code>{_escape(name)}</code>",
                        force_document=True,
                    )
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logger.exception(f"[XR] send_file failed for {label}: %s", e)
                    try:
                        await self._client.send_file(
                            call.chat_id,
                            fpath,
                            caption=f"<b>{kind.title()} {label}:</b> <code>{_escape(name)}</code>",
                            parse_mode="html",
                            force_document=True,
                        )
                    except Exception as e2:
                        logger.exception(f"[XR] fallback send_file failed for {label}: %s", e2)
        except Exception as e:
            logger.exception(f"[XR] logs sending failed: %s", e)

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
                cloudflared_version=await self._get_cloudflared_version(),
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