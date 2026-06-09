__version__ = (3, 6, 0)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/XRay/MetaBanner.jpeg

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
from ..inline.types import InlineCall

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
    if os.path.isfile("/.dockerenv"):
        return True

    try:
        with open("/proc/1/cgroup", "rt") as f:
            data = f.read()

        docker_markers = (
            "docker",
            "kubepods",
            "containerd",
            "podman",
            "lxc",
        )

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
    """Run VPN on your VPS server (VLESS + reality)"""

    strings = {
        "name": "XRay",
        
        "main_menu": (
            "<b>XRay VLESS+Reality</b>\n"
            "<blockquote>Status: {status}\n"
            "Port: {port}\n"
            "Uptime: {uptime}</blockquote>"
        ),
        
        "status_menu": (
            "<b>XRay Status</b>\n"
            "<blockquote>State: {state}\n"
            "PID: {pid}\n"
            "Uptime: {uptime}\n"
            "Traffic RX: {rx}\n"
            "Traffic TX: {tx}\n"
            "Active clients: {active}\n"
            "Unique IPs (24h): {unique}\n"
            "Port: {port}</blockquote>"
        ),
        
        "settings_menu": (
            "<b>XRay Settings</b>\n"
            "<blockquote>Port: {port}\n"
            "SNI: {sni}\n"
            "Dest: {dest}\n"
            "IP: {ip}</blockquote>"
        ),
        
        "users_menu": (
            "<b>Trusted Users</b>\n"
            "<blockquote>Total: {count}\n"
            "{users}</blockquote>"
        ),
        
        "cleanup_confirm": (
            "<b>Full Cleanup Warning</b>\n"
            "<blockquote>This will:\n"
            "- Kill all XRay processes\n"
            "- Delete all files and configs\n"
            "- Clear database\n"
            "- Remove all traces\n\n"
            "This action cannot be undone!</blockquote>"
        ),
        
        "cleanup_done": (
            "<b>Cleanup Complete</b>\n"
            "<blockquote>All XRay data removed</blockquote>"
        ),
        
        "btn_status": "Status",
        "btn_start": "Start",
        "btn_stop": "Stop",
        "btn_restart": "Restart",
        "btn_settings": "Settings",
        "btn_users": "Users",
        "btn_get_link": "Get Link",
        "btn_cleanup": "Full Cleanup",
        "btn_back": "Back",
        "btn_close": "Close",
        "btn_confirm_cleanup": "Confirm Cleanup",
        "btn_cancel": "Cancel",
        
        "btn_set_port": "Set Port",
        "btn_set_sni": "Set SNI",
        "btn_set_dest": "Set Dest",
        "btn_detect_ip": "Detect IP",
        
        "input_port": "Enter port (1025-65535):",
        "input_sni": "Enter SNI domain:",
        "input_dest": "Enter destination (domain:port):",
        
        "not_installed": (
            "<b>XRay Not Installed</b>\n"
            "<blockquote>Use setup first</blockquote>"
        ),
        
        "setup_done": (
            "<b>Setup Complete</b>\n"
            "<blockquote>XRay installed and configured</blockquote>"
        ),
        
        "setup_fail": (
            "<b>Setup Failed</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        
        "setup_docker": (
            "<b>Docker Detected</b>\n"
            "<blockquote>This module cannot work in Docker</blockquote>"
        ),
        
        "started": (
            "<b>XRay Started</b>\n"
            "<blockquote>Port: {port}\n"
            "IP: {ip}</blockquote>"
        ),
        
        "stopped": (
            "<b>XRay Stopped</b>\n"
            "<blockquote>Service is offline</blockquote>"
        ),
        
        "already_running": (
            "<b>Already Running</b>\n"
            "<blockquote>XRay is active</blockquote>"
        ),
        
        "not_running": (
            "<b>Not Running</b>\n"
            "<blockquote>Start XRay first</blockquote>"
        ),
        
        "port_set": (
            "<b>Port Updated</b>\n"
            "<blockquote>New port: {port}\n"
            "Restart required</blockquote>"
        ),
        
        "sni_set": (
            "<b>SNI Updated</b>\n"
            "<blockquote>New SNI: {sni}\n"
            "Restart required</blockquote>"
        ),
        
        "dest_set": (
            "<b>Dest Updated</b>\n"
            "<blockquote>New dest: {dest}\n"
            "Restart required</blockquote>"
        ),
        
        "ip_detected": (
            "<b>IP Detected</b>\n"
            "<blockquote>External IP: {ip}</blockquote>"
        ),
        
        "ip_fail": (
            "<b>IP Detection Failed</b>\n"
            "<blockquote>Cannot detect external IP</blockquote>"
        ),
        
        "link_message": (
            "<b>Your VLESS Link</b>\n"
            "<blockquote>Download client:\n"
            "iOS: v2RayTun\n"
            "Android: v2RayTun\n\n"
            "Tap button to copy link</blockquote>"
        ),
        
        "link_not_ready": (
            "<b>Link Not Ready</b>\n"
            "<blockquote>Setup and start XRay first</blockquote>"
        ),
        
        "no_users": "No users added",
        
        "status_running": "Running",
        "status_stopped": "Stopped",
        
        "bot_copy_button": "COPY LINK",
    }

    strings_ru = {
        "main_menu": (
            "<b>XRay VLESS+Reality</b>\n"
            "<blockquote>Статус: {status}\n"
            "Порт: {port}\n"
            "Аптайм: {uptime}</blockquote>"
        ),
        
        "status_menu": (
            "<b>Статус XRay</b>\n"
            "<blockquote>Состояние: {state}\n"
            "PID: {pid}\n"
            "Аптайм: {uptime}\n"
            "Трафик RX: {rx}\n"
            "Трафик TX: {tx}\n"
            "Активные клиенты: {active}\n"
            "Уникальные IP (24ч): {unique}\n"
            "Порт: {port}</blockquote>"
        ),
        
        "settings_menu": (
            "<b>Настройки XRay</b>\n"
            "<blockquote>Порт: {port}\n"
            "SNI: {sni}\n"
            "Dest: {dest}\n"
            "IP: {ip}</blockquote>"
        ),
        
        "users_menu": (
            "<b>Доверенные пользователи</b>\n"
            "<blockquote>Всего: {count}\n"
            "{users}</blockquote>"
        ),
        
        "cleanup_confirm": (
            "<b>Полная очистка - Предупреждение</b>\n"
            "<blockquote>Будет выполнено:\n"
            "- Убийство всех процессов XRay\n"
            "- Удаление всех файлов и конфигов\n"
            "- Очистка базы данных\n"
            "- Удаление всех следов\n\n"
            "Это действие необратимо!</blockquote>"
        ),
        
        "cleanup_done": (
            "<b>Очистка завершена</b>\n"
            "<blockquote>Все данные XRay удалены</blockquote>"
        ),
        
        "btn_status": "Статус",
        "btn_start": "Запустить",
        "btn_stop": "Остановить",
        "btn_restart": "Перезапустить",
        "btn_settings": "Настройки",
        "btn_users": "Пользователи",
        "btn_get_link": "Получить ссылку",
        "btn_cleanup": "Полная очистка",
        "btn_back": "Назад",
        "btn_close": "Закрыть",
        "btn_confirm_cleanup": "Подтвердить очистку",
        "btn_cancel": "Отмена",
        
        "btn_set_port": "Установить порт",
        "btn_set_sni": "Установить SNI",
        "btn_set_dest": "Установить Dest",
        "btn_detect_ip": "Определить IP",
        
        "input_port": "Введите порт (1025-65535):",
        "input_sni": "Введите SNI домен:",
        "input_dest": "Введите назначение (домен:порт):",
        
        "not_installed": (
            "<b>XRay не установлен</b>\n"
            "<blockquote>Сначала выполните установку</blockquote>"
        ),
        
        "setup_done": (
            "<b>Установка завершена</b>\n"
            "<blockquote>XRay установлен и настроен</blockquote>"
        ),
        
        "setup_fail": (
            "<b>Установка не удалась</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        
        "setup_docker": (
            "<b>Обнаружен Docker</b>\n"
            "<blockquote>Модуль не может работать в Docker</blockquote>"
        ),
        
        "started": (
            "<b>XRay запущен</b>\n"
            "<blockquote>Порт: {port}\n"
            "IP: {ip}</blockquote>"
        ),
        
        "stopped": (
            "<b>XRay остановлен</b>\n"
            "<blockquote>Сервис не активен</blockquote>"
        ),
        
        "already_running": (
            "<b>Уже запущен</b>\n"
            "<blockquote>XRay активен</blockquote>"
        ),
        
        "not_running": (
            "<b>Не запущен</b>\n"
            "<blockquote>Сначала запустите XRay</blockquote>"
        ),
        
        "port_set": (
            "<b>Порт обновлен</b>\n"
            "<blockquote>Новый порт: {port}\n"
            "Требуется перезапуск</blockquote>"
        ),
        
        "sni_set": (
            "<b>SNI обновлен</b>\n"
            "<blockquote>Новый SNI: {sni}\n"
            "Требуется перезапуск</blockquote>"
        ),
        
        "dest_set": (
            "<b>Dest обновлен</b>\n"
            "<blockquote>Новый dest: {dest}\n"
            "Требуется перезапуск</blockquote>"
        ),
        
        "ip_detected": (
            "<b>IP определен</b>\n"
            "<blockquote>Внешний IP: {ip}</blockquote>"
        ),
        
        "ip_fail": (
            "<b>Определение IP не удалось</b>\n"
            "<blockquote>Не удается определить внешний IP</blockquote>"
        ),
        
        "link_message": (
            "<b>Ваша VLESS ссылка</b>\n"
            "<blockquote>Загрузите клиент:\n"
            "iOS: v2RayTun\n"
            "Android: v2RayTun\n\n"
            "Нажмите кнопку для копирования</blockquote>"
        ),
        
        "link_not_ready": (
            "<b>Ссылка не готова</b>\n"
            "<blockquote>Сначала установите и запустите XRay</blockquote>"
        ),
        
        "no_users": "Нет добавленных пользователей",
        
        "status_running": "Запущен",
        "status_stopped": "Остановлен",
        
        "bot_copy_button": "КОПИРОВАТЬ ССЫЛКУ",
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
        self._process_marker = None

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
        self._process_marker = f"XRAY_MODULE_{tg_user_id}"

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

        await self._kill_module_processes()

        if self._log_fd:
            try:
                self._log_fd.close()
            except Exception:
                pass
            self._log_fd = None

        self._db.set("XR", "proxy_autostart", False)

    async def _kill_module_processes(self):
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

        if self._process_marker:
            try:
                p = await asyncio.create_subprocess_exec(
                    "pkill", "-f", self._process_marker,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                await asyncio.wait_for(p.communicate(), timeout=5)
            except Exception:
                pass

    async def _full_module_cleanup(self):
        await self._kill_module_processes()

        if self._root and os.path.exists(self._root):
            try:
                shutil.rmtree(self._root, ignore_errors=True)
            except Exception:
                pass

        for key in ["port", "sni", "dest", "trusted_users", "external_ip", 
                    "vless_uuid", "private_key", "public_key", "short_id", 
                    "proxy_autostart"]:
            try:
                self._db.set("XR", key, None)
            except Exception:
                pass

    def _proxy_running(self):
        if not self._proc:
            return False
        
        poll = self._proc.poll()
        if poll is not None:
            self._proc = None
            self._start_time = 0
            return False
        
        try:
            os.kill(self._proc.pid, 0)
            return True
        except (ProcessLookupError, OSError):
            self._proc = None
            self._start_time = 0
            return False

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

    async def _do_setup(self):
        if _in_docker():
            return False, "docker"

        if not self._xray_installed():
            ok, res = await self._install_xray()
            if not ok:
                return False, f"Install failed: {res}"

        private_key, public_key = await self._generate_x25519()
        if not private_key or not public_key:
            return False, "Failed to generate x25519 keys"

        vless_uuid = self._generate_uuid()
        short_id = self._generate_short_id()

        self._db.set("XR", "private_key", private_key)
        self._db.set("XR", "public_key", public_key)
        self._db.set("XR", "vless_uuid", vless_uuid)
        self._db.set("XR", "short_id", short_id)

        try:
            self._write_config()
        except Exception as e:
            return False, f"Config write error: {e}"

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
            env[self._process_marker] = "1"

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

            await self._kill_module_processes()

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

    def _get_main_markup(self):
        is_running = self._proxy_running()
        
        markup = []
        
        markup.append([
            {"text": self.strings["btn_status"], "callback": self._cb_status, "style": "primary"}
        ])
        
        if is_running:
            markup.append([
                {"text": self.strings["btn_stop"], "callback": self._cb_stop, "style": "danger"},
                {"text": self.strings["btn_restart"], "callback": self._cb_restart, "style": "primary"},
            ])
        else:
            markup.append([
                {"text": self.strings["btn_start"], "callback": self._cb_start, "style": "success"}
            ])
        
        markup.append([
            {"text": self.strings["btn_settings"], "callback": self._cb_settings, "style": "primary"},
            {"text": self.strings["btn_users"], "callback": self._cb_users, "style": "primary"},
        ])
        
        markup.append([
            {"text": self.strings["btn_get_link"], "callback": self._cb_get_link, "style": "success"}
        ])
        
        markup.append([
            {"text": self.strings["btn_cleanup"], "callback": self._cb_cleanup_confirm, "style": "danger"}
        ])
        
        markup.append([
            {"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}
        ])
        
        return markup

    def _format_main_text(self):
        status = self.strings["status_running"] if self._proxy_running() else self.strings["status_stopped"]
        port = self._db.get("XR", "port", 8443)
        uptime = self._get_uptime()
        
        return self.strings["main_menu"].format(
            status=status,
            port=port,
            uptime=uptime
        )

    async def _cb_main_menu(self, call: InlineCall):
        await call.edit(
            self._format_main_text(),
            reply_markup=self._get_main_markup()
        )

    async def _cb_status(self, call: InlineCall):
        if self._proxy_running():
            port = self._db.get("XR", "port", 8443)
            active = self._get_active_clients()
            unique_ips = self._get_unique_ips_24h()
            rx = max(0, self._traffic_rx)
            tx = max(0, self._traffic_tx)
            
            text = self.strings["status_menu"].format(
                state=self.strings["status_running"],
                pid=self._proc.pid,
                uptime=self._get_uptime(),
                rx=self._format_bytes(rx),
                tx=self._format_bytes(tx),
                active=active,
                unique=len(unique_ips),
                port=port
            )
        else:
            text = self.strings["status_menu"].format(
                state=self.strings["status_stopped"],
                pid="n/a",
                uptime="n/a",
                rx="n/a",
                tx="n/a",
                active="n/a",
                unique="n/a",
                port=self._db.get("XR", "port", 8443)
            )
        
        await call.edit(
            text,
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
        )

    async def _cb_start(self, call: InlineCall):
        ok, err = await self._do_start_proxy()
        
        if ok:
            port = self._db.get("XR", "port", 8443)
            ip = await self._get_external_ip()
            text = self.strings["started"].format(port=port, ip=ip or "?")
        elif err == "already_running":
            text = self.strings["already_running"]
        elif err == "not_installed":
            text = self.strings["not_installed"]
        else:
            text = self.strings["setup_fail"].format(error=_escape(str(err)[:200]))
        
        await call.edit(
            text,
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
        )

    async def _cb_stop(self, call: InlineCall):
        if not self._proxy_running():
            await call.edit(
                self.strings["not_running"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
            )
            return
        
        await self._do_stop_proxy()
        
        await call.edit(
            self.strings["stopped"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
        )

    async def _cb_restart(self, call: InlineCall):
        await self._do_stop_proxy()
        await asyncio.sleep(1)
        ok, err = await self._do_start_proxy()
        
        if ok:
            port = self._db.get("XR", "port", 8443)
            ip = await self._get_external_ip()
            text = self.strings["started"].format(port=port, ip=ip or "?")
        else:
            text = self.strings["setup_fail"].format(error=_escape(str(err)[:200]))
        
        await call.edit(
            text,
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
        )

    async def _cb_settings(self, call: InlineCall):
        port = self._db.get("XR", "port", 8443)
        sni = self._db.get("XR", "sni", "www.google.com")
        dest = self._db.get("XR", "dest", "www.google.com:443")
        ip = self._db.get("XR", "external_ip", "") or "not set"
        
        text = self.strings["settings_menu"].format(
            port=port,
            sni=_escape(sni),
            dest=_escape(dest),
            ip=ip
        )
        
        markup = [
            [{"text": self.strings["btn_set_port"], "input": self.strings["input_port"], "handler": self._cb_set_port, "style": "primary"}],
            [{"text": self.strings["btn_set_sni"], "input": self.strings["input_sni"], "handler": self._cb_set_sni, "style": "primary"}],
            [{"text": self.strings["btn_set_dest"], "input": self.strings["input_dest"], "handler": self._cb_set_dest, "style": "primary"}],
            [{"text": self.strings["btn_detect_ip"], "callback": self._cb_detect_ip, "style": "success"}],
            [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}],
        ]
        
        await call.edit(text, reply_markup=markup)

    async def _cb_set_port(self, call: InlineCall, port_str: str):
        try:
            port = int(port_str.strip())
            if not (1025 <= port <= 65535):
                raise ValueError
        except ValueError:
            await call.answer("Invalid port number", show_alert=True)
            return
        
        self._db.set("XR", "port", port)
        
        await call.edit(
            self.strings["port_set"].format(port=port),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_settings, "style": "danger"}]]
        )

    async def _cb_set_sni(self, call: InlineCall, sni: str):
        sni = sni.strip().lower()
        self._db.set("XR", "sni", sni)
        
        await call.edit(
            self.strings["sni_set"].format(sni=_escape(sni)),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_settings, "style": "danger"}]]
        )

    async def _cb_set_dest(self, call: InlineCall, dest: str):
        dest = dest.strip()
        if ":" not in dest:
            dest += ":443"
        self._db.set("XR", "dest", dest)
        
        await call.edit(
            self.strings["dest_set"].format(dest=_escape(dest)),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_settings, "style": "danger"}]]
        )

    async def _cb_detect_ip(self, call: InlineCall):
        ip = await self._get_external_ip()
        
        if ip:
            text = self.strings["ip_detected"].format(ip=ip)
        else:
            text = self.strings["ip_fail"]
        
        await call.edit(
            text,
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_settings, "style": "danger"}]]
        )

    async def _cb_users(self, call: InlineCall):
        users = self._db.get("XR", "trusted_users", [])
        
        if users:
            users_text = "\n".join(f"<code>{u}</code>" for u in users)
        else:
            users_text = self.strings["no_users"]
        
        text = self.strings["users_menu"].format(
            count=len(users),
            users=users_text
        )
        
        await call.edit(
            text,
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
        )

    async def _cb_get_link(self, call: InlineCall):
        ip = await self._get_external_ip()
        if not ip:
            await call.edit(
                self.strings["link_not_ready"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
            )
            return
        
        link = self._build_vless_link(ip)
        if not link:
            await call.edit(
                self.strings["link_not_ready"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
            )
            return
        
        markup = [
            [
                {
                    "text": self.strings["bot_copy_button"],
                    "copy": link,
                }
            ],
            [
                {"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}
            ],
        ]
        
        await call.edit(
            self.strings["link_message"],
            reply_markup=markup
        )

    async def _cb_cleanup_confirm(self, call: InlineCall):
        markup = [
            [
                {"text": self.strings["btn_confirm_cleanup"], "callback": self._cb_cleanup_execute, "style": "danger"}
            ],
            [
                {"text": self.strings["btn_cancel"], "callback": self._cb_main_menu, "style": "primary"}
            ],
        ]
        
        await call.edit(
            self.strings["cleanup_confirm"],
            reply_markup=markup
        )

    async def _cb_cleanup_execute(self, call: InlineCall):
        await self._full_module_cleanup()
        
        await call.edit(
            self.strings["cleanup_done"],
            reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]]
        )

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    async def aiogram_watcher(self, message: AiogramMessage):
        if not message.text:
            return

        text = message.text.strip()
        if not text.startswith("/xray"):
            return

        uid = message.from_user.id

        if uid != self._me.id and uid not in self._db.get("XR", "trusted_users", []):
            return

        try:
            ip = await self._get_external_ip()
            if not ip:
                await message.answer(
                    self.strings["link_not_ready"],
                    parse_mode="HTML",
                    link_preview_options=LinkPreviewOptions(is_disabled=True),
                )
                return

            link = self._build_vless_link(ip)
            if not link:
                await message.answer(
                    self.strings["link_not_ready"],
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
                self.strings["link_message"],
                parse_mode="HTML",
                reply_markup=keyboard,
                link_preview_options=LinkPreviewOptions(is_disabled=True),
            )
        except Exception as e:
            logger.error("[XR] aiogram_watcher error: %s", e)

    @loader.command()
    async def xr(self, message):
        """XRay VLESS+Reality VPN management"""
        await self.inline.form(
            text=self._format_main_text(),
            message=message,
            reply_markup=self._get_main_markup(),
            silent=True,
        )