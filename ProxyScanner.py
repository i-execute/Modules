# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (2, 0, 0)
# meta developer: Execute_forge.t.me

import asyncio
import contextlib
import json
import logging
import os
import random
import re
import struct
import tempfile
import time
from typing import Dict, List, Optional, Tuple

import aiohttp
from telethon.tl.types import Message

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

PROXY_SOURCES = [
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/https.txt",
    "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt",
    "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-https.txt",
    "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",
    "https://raw.githubusercontent.com/mmpx12/proxy-list/master/https.txt",
    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
    "https://raw.githubusercontent.com/UserR3X/proxy-list/main/online/http.txt",
    "https://raw.githubusercontent.com/UserR3X/proxy-list/main/online/https.txt",
    "https://raw.githubusercontent.com/proxy4parsing/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/http.txt",
    "https://raw.githubusercontent.com/aslisk/proxyhttps/main/https.txt",
    "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/http/http.txt",
    "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/https/https.txt",
    "https://raw.githubusercontent.com/prxchk/proxy-list/main/http.txt",
    "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/http.txt",
    "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/https.txt",
    "https://api.openproxylist.xyz/http.txt",
    "https://api.proxyscrape.com/?request=displayproxies&proxytype=http",
    "https://proxyspace.pro/http.txt",
    "https://proxyspace.pro/https.txt",
    "https://www.proxy-list.download/api/v1/get?type=http",
    "https://www.proxy-list.download/api/v1/get?type=https",
]

PAGE_SIZE = 5


def _escape(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def _check_tcp_open(host: str, port: int, timeout: float = 3.0) -> bool:
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout,
        )
        writer.close()
        await writer.wait_closed()
        return True
    except Exception:
        return False


async def _check_socks5(host: str, port: int, username: Optional[str] = None, password: Optional[str] = None, timeout: float = 6.0) -> Tuple[bool, bool]:
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout,
        )
        if username and password:
            writer.write(b"\x05\x01\x02")
        else:
            writer.write(b"\x05\x01\x00")
        await writer.drain()

        response = await asyncio.wait_for(reader.read(2), timeout=timeout)
        if len(response) != 2 or response[0] != 0x05:
            writer.close()
            await writer.wait_closed()
            return False, False

        auth_method = response[1]
        if auth_method == 0x00:
            pass
        elif auth_method == 0x02:
            if not username or not password:
                writer.close()
                await writer.wait_closed()
                return True, True
            auth_data = struct.pack("B", 1)
            auth_data += struct.pack("B", len(username)) + username.encode()
            auth_data += struct.pack("B", len(password)) + password.encode()
            writer.write(auth_data)
            await writer.drain()
            auth_response = await asyncio.wait_for(reader.read(2), timeout=timeout)
            if len(auth_response) != 2 or auth_response[1] != 0x00:
                writer.close()
                await writer.wait_closed()
                return True, True
        else:
            writer.close()
            await writer.wait_closed()
            return False, False

        connect_request = b"\x05\x01\x00\x01\x08\x08\x08\x08" + struct.pack(">H", 80)
        writer.write(connect_request)
        await writer.drain()
        connect_response = await asyncio.wait_for(reader.read(10), timeout=timeout)
        writer.close()
        await writer.wait_closed()
        if len(connect_response) >= 2 and connect_response[1] == 0x00:
            return True, False
        return False, False
    except Exception:
        return False, False


async def _check_http_proxy(host: str, port: int, username: Optional[str] = None, password: Optional[str] = None, timeout: float = 6.0) -> Tuple[bool, bool]:
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout,
        )
        request = b"CONNECT example.com:80 HTTP/1.1\r\nHost: example.com:80\r\n"
        if username and password:
            import base64
            auth = base64.b64encode(f"{username}:{password}".encode()).decode()
            request += f"Proxy-Authorization: Basic {auth}\r\n".encode()
        request += b"\r\n"
        writer.write(request)
        await writer.drain()
        response = await asyncio.wait_for(reader.read(1024), timeout=timeout)
        writer.close()
        await writer.wait_closed()
        resp_text = response.decode("latin-1", errors="ignore")
        if " 200 " in resp_text:
            return True, False
        if " 407 " in resp_text:
            return True, True
        return False, False
    except Exception:
        return False, False


async def _detect_proxy_type(host: str, port: int, username: Optional[str] = None, password: Optional[str] = None, timeout: float = 6.0) -> Dict:
    tcp_open = await _check_tcp_open(host, port, timeout=3.0)
    if not tcp_open:
        return {
            "type": "unknown",
            "working": False,
            "auth_required": False,
            "reason": "Port closed or unreachable",
        }
    socks5_ok, socks5_auth = await _check_socks5(host, port, username, password, timeout)
    http_ok, http_auth = await _check_http_proxy(host, port, username, password, timeout)
    if socks5_ok:
        return {
            "type": "SOCKS5",
            "working": True,
            "auth_required": socks5_auth,
            "reason": "SOCKS5 connection successful",
        }
    if http_ok:
        return {
            "type": "HTTP",
            "working": True,
            "auth_required": http_auth,
            "reason": "HTTP CONNECT successful",
        }
    return {
        "type": "unknown",
        "working": False,
        "auth_required": False,
        "reason": "Neither SOCKS5 nor HTTP CONNECT succeeded",
    }


async def _test_proxy(proxy_type: str, host: str, port: int, username: str = None, password: str = None, timeout: int = 6) -> bool:
    try:
        t0 = time.monotonic()
        if proxy_type == "SOCKS5":
            ok, _ = await _check_socks5(host, port, username, password, timeout)
            return ok
        elif proxy_type in ("HTTP", "HTTPS"):
            ok, _ = await _check_http_proxy(host, port, username, password, timeout)
            return ok
        else:
            return await _check_tcp_open(host, port, timeout=3.0)
    except Exception:
        return False


@loader.tds
class ProxyScanner(loader.Module):
    """Proxy manager and tester with auto-detection and public fetch"""

    strings = {
        "name": "ProxyScanner",
        "main_menu": (
            "<b>Proxy Manager</b>\n"
            "<blockquote>Total proxies: {total}</blockquote>"
        ),
        "btn_manage": "Proxy List",
        "btn_fetch": "Fetch Public Proxies",
        "btn_add_manual": "Add Manually",
        "btn_test_all": "Test All",
        "btn_export": "Export JSON",
        "btn_close": "Close",
        "btn_back": "Back",
        "btn_delete": "Delete",
        "btn_test": "Test",
        "fetching_sources": (
            "<b>Fetching public proxy lists...</b>\n"
            "<blockquote>Querying {sources} sources</blockquote>"
        ),
        "validating_proxies": (
            "<b>Validating proxies...</b>\n"
            "<blockquote>Parsed: {parsed}\n"
            "Testing live connections...</blockquote>"
        ),
        "fetch_done": (
            "<b>Fetch Complete</b>\n"
            "<blockquote>Sources: {sources}\n"
            "Parsed unique: {parsed}\n"
            "Added working: {added}\n"
            "Total in pool: {total}</blockquote>"
        ),
        "no_proxies": (
            "<b>No Proxies</b>\n"
            "<blockquote>Add proxies or fetch public lists first</blockquote>"
        ),
        "proxies_list": (
            "<b>Proxy Pool</b>\n"
            "<blockquote>Page {page}/{total_pages} (Total: {total})</blockquote>"
        ),
        "proxy_view": (
            "<b>Proxy Details</b>\n"
            "<blockquote>Type: <b>{type}</b>\n"
            "Host: <code>{ip}</code>\n"
            "Port: <code>{port}</code>\n"
            "Auth: <b>{auth}</b>\n"
            "Status: <b>{status}</b></blockquote>"
        ),
        "test_results": (
            "<b>Test Results</b>\n"
            "<blockquote>Total: {total}\n"
            "Working: {working}\n"
            "Failed: {failed}</blockquote>"
        ),
        "export_done": (
            "<b>Export Complete</b>\n"
            "<blockquote>File <code>proxies.json</code> sent to chat</blockquote>"
        ),
        "add_type_menu": (
            "<b>Add Proxy</b>\n"
            "<blockquote>Select format:</blockquote>"
        ),
        "btn_ip_port": "IP + Port",
        "btn_ip_port_login": "IP + Port + Auth",
        "input_ip": "Enter IP address:",
        "input_port": "Enter port:",
        "input_username": "Enter username:",
        "input_password": "Enter password:",
        "detecting": "<b>Detecting proxy type...</b>",
        "detection_result": (
            "<b>Detection Result</b>\n"
            "<blockquote>IP: <code>{ip}</code>\n"
            "Port: <code>{port}</code>\n"
            "Type: <b>{type}</b>\n"
            "Auth required: <b>{auth}</b>\n"
            "Reason: {reason}</blockquote>"
        ),
        "btn_save": "Save Proxy",
        "proxy_added": (
            "<b>Proxy Saved</b>\n"
            "<blockquote>Type: {type}\n"
            "Host: <code>{ip}:{port}</code></blockquote>"
        ),
        "proxy_deleted": (
            "<b>Proxy Deleted</b>\n"
            "<blockquote>Removed <code>{ip}:{port}</code></blockquote>"
        ),
        "err_invalid_port": "<b>Error:</b>\n<blockquote>Invalid port number</blockquote>",
    }

    strings_ru = {
        "main_menu": (
            "<b>Менеджер прокси</b>\n"
            "<blockquote>Всего прокси: {total}</blockquote>"
        ),
        "btn_manage": "Список прокси",
        "btn_fetch": "Спарсить публичные прокси",
        "btn_add_manual": "Добавить вручную",
        "btn_test_all": "Проверить все",
        "btn_export": "Экспорт JSON",
        "btn_close": "Закрыть",
        "btn_back": "Назад",
        "btn_delete": "Удалить",
        "btn_test": "Проверить",
        "fetching_sources": (
            "<b>Загрузка списков прокси...</b>\n"
            "<blockquote>Опрос {sources} источников</blockquote>"
        ),
        "validating_proxies": (
            "<b>Проверка прокси...</b>\n"
            "<blockquote>Спарсено: {parsed}\n"
            "Тестирование подключений...</blockquote>"
        ),
        "fetch_done": (
            "<b>Парсинг завершен</b>\n"
            "<blockquote>Источников: {sources}\n"
            "Уникальных: {parsed}\n"
            "Добавлено рабочих: {added}\n"
            "Всего в базе: {total}</blockquote>"
        ),
        "no_proxies": (
            "<b>Нет прокси</b>\n"
            "<blockquote>Добавьте прокси вручную или спарсите публичные списки</blockquote>"
        ),
        "proxies_list": (
            "<b>Список прокси</b>\n"
            "<blockquote>Страница {page}/{total_pages} (Всего: {total})</blockquote>"
        ),
        "proxy_view": (
            "<b>Информация о прокси</b>\n"
            "<blockquote>Тип: <b>{type}</b>\n"
            "Хост: <code>{ip}</code>\n"
            "Порт: <code>{port}</code>\n"
            "Авторизация: <b>{auth}</b>\n"
            "Статус: <b>{status}</b></blockquote>"
        ),
        "test_results": (
            "<b>Результаты проверки</b>\n"
            "<blockquote>Всего: {total}\n"
            "Работают: {working}\n"
            "Недоступны: {failed}</blockquote>"
        ),
        "export_done": (
            "<b>Экспорт завершен</b>\n"
            "<blockquote>Файл <code>proxies.json</code> отправлен в чат</blockquote>"
        ),
        "add_type_menu": (
            "<b>Добавить прокси</b>\n"
            "<blockquote>Выберите формат:</blockquote>"
        ),
        "btn_ip_port": "IP + Порт",
        "btn_ip_port_login": "IP + Порт + Авторизация",
        "input_ip": "Введите IP адрес:",
        "input_port": "Введите порт:",
        "input_username": "Введите логин:",
        "input_password": "Введите пароль:",
        "detecting": "<b>Определение типа прокси...</b>",
        "detection_result": (
            "<b>Результат проверки</b>\n"
            "<blockquote>IP: <code>{ip}</code>\n"
            "Порт: <code>{port}</code>\n"
            "Тип: <b>{type}</b>\n"
            "Авторизация: <b>{auth}</b>\n"
            "Причина: {reason}</blockquote>"
        ),
        "btn_save": "Сохранить",
        "proxy_added": (
            "<b>Прокси добавлен</b>\n"
            "<blockquote>Тип: {type}\n"
            "Хост: <code>{ip}:{port}</code></blockquote>"
        ),
        "proxy_deleted": (
            "<b>Прокси удален</b>\n"
            "<blockquote>Удален <code>{ip}:{port}</code></blockquote>"
        ),
        "err_invalid_port": "<b>Ошибка:</b>\n<blockquote>Некорректный номер порта</blockquote>",
    }

    def __init__(self):
        self._proxies = []
        self._temp_data = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._owner = await client.get_me()
        self._tg_id = self._owner.id if self._owner else getattr(client, "tg_id", None)
        self._proxies = self._db.get("ProxyScanner", "proxies", [])

    def _save_proxies(self):
        self._db.set("ProxyScanner", "proxies", self._proxies)

    def _get_tmp_dir(self):
        uid = getattr(self, "_tg_id", None) or (self._owner.id if getattr(self, "_owner", None) else "common")
        path = os.path.join(tempfile.gettempdir(), "ProxyScanner", str(uid))
        os.makedirs(path, exist_ok=True)
        return path

    def _fmt_main(self):
        return self.strings["main_menu"].format(total=len(self._proxies))

    def _markup_main(self):
        return [
            [
                {"text": self.strings["btn_manage"], "callback": self._cb_manage_list, "args": (0,), "style": "primary"},
                {"text": self.strings["btn_fetch"], "callback": self._cb_fetch_public, "style": "success"},
            ],
            [
                {"text": self.strings["btn_add_manual"], "callback": self._cb_add_type_menu, "style": "primary"},
                {"text": self.strings["btn_test_all"], "callback": self._cb_test_all, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_export"], "callback": self._cb_export, "style": "primary"},
                {"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"},
            ],
        ]

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    async def _cb_main_menu(self, call: InlineCall):
        await call.edit(self._fmt_main(), reply_markup=self._markup_main())

    async def _cb_fetch_public(self, call: InlineCall):
        await call.edit(self.strings["fetching_sources"].format(sources=len(PROXY_SOURCES)), reply_markup=[])

        candidates = set()
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for url in PROXY_SOURCES:
                try:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            text = await resp.text()
                            for line in text.splitlines():
                                line = line.strip()
                                m = re.match(r"^(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})$", line)
                                if m:
                                    candidates.add((m.group(1), int(m.group(2))))
                except Exception:
                    continue

        parsed = len(candidates)
        if not parsed:
            await call.edit(
                self.strings["fetch_done"].format(sources=len(PROXY_SOURCES), parsed=0, added=0, total=len(self._proxies)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}]],
            )
            return

        await call.edit(self.strings["validating_proxies"].format(parsed=parsed), reply_markup=[])

        sample_size = min(parsed, 120)
        chosen = random.sample(list(candidates), sample_size)
        sem = asyncio.Semaphore(25)

        async def _test_candidate(host, port):
            async with sem:
                res = await _detect_proxy_type(host, port, timeout=4.0)
                if res.get("working"):
                    return {
                        "type": res.get("type", "HTTP"),
                        "ip": host,
                        "port": port,
                        "username": None,
                        "password": None,
                        "auth_required": res.get("auth_required", False),
                        "status": "active",
                    }
                return None

        results = await asyncio.gather(*(_test_candidate(h, p) for h, p in chosen))
        working_proxies = [r for r in results if r is not None]

        existing = {(p["ip"], p["port"]) for p in self._proxies}
        added = 0
        for wp in working_proxies:
            key = (wp["ip"], wp["port"])
            if key not in existing:
                self._proxies.append(wp)
                existing.add(key)
                added += 1

        if added:
            self._save_proxies()

        await call.edit(
            self.strings["fetch_done"].format(
                sources=len(PROXY_SOURCES),
                parsed=parsed,
                added=added,
                total=len(self._proxies),
            ),
            reply_markup=[
                [{"text": self.strings["btn_manage"], "callback": self._cb_manage_list, "args": (0,), "style": "primary"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}],
            ],
        )

    async def _cb_manage_list(self, call: InlineCall, page: int):
        total = len(self._proxies)
        if total == 0:
            await call.edit(
                self.strings["no_proxies"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}]],
            )
            return

        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        page = max(0, min(page, total_pages - 1))
        start = page * PAGE_SIZE
        chunk = self._proxies[start:start + PAGE_SIZE]

        rows = []
        for idx, p in enumerate(chunk):
            real_idx = start + idx
            proto = p.get("type", "HTTP")
            host_str = f"{proto}://{p['ip']}:{p['port']}"
            rows.append([{
                "text": host_str,
                "callback": self._cb_view_proxy,
                "args": (real_idx, page),
                "style": "primary",
            }])

        nav = []
        if page > 0:
            nav.append({"text": "<", "callback": self._cb_manage_list, "args": (page - 1,), "style": "primary"})
        if page < total_pages - 1:
            nav.append({"text": ">", "callback": self._cb_manage_list, "args": (page + 1,), "style": "primary"})
        if nav:
            rows.append(nav)

        rows.append([{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}])

        await call.edit(
            self.strings["proxies_list"].format(page=page + 1, total_pages=total_pages, total=total),
            reply_markup=rows,
        )

    async def _cb_view_proxy(self, call: InlineCall, idx: int, page: int):
        if idx >= len(self._proxies):
            await self._cb_manage_list(call, page)
            return
        p = self._proxies[idx]
        auth_str = "Yes" if p.get("username") or p.get("auth_required") else "None"
        status_str = p.get("status", "unknown")
        text = self.strings["proxy_view"].format(
            type=p.get("type", "HTTP"),
            ip=_escape(p.get("ip")),
            port=p.get("port"),
            auth=auth_str,
            status=status_str,
        )
        markup = [
            [
                {"text": self.strings["btn_test"], "callback": self._cb_test_single, "args": (idx, page), "style": "primary"},
                {"text": self.strings["btn_delete"], "callback": self._cb_delete_proxy, "args": (idx, page), "style": "danger"},
            ],
            [{"text": self.strings["btn_back"], "callback": self._cb_manage_list, "args": (page,), "style": "primary"}],
        ]
        await call.edit(text, reply_markup=markup)

    async def _cb_test_single(self, call: InlineCall, idx: int, page: int):
        if idx >= len(self._proxies):
            await self._cb_manage_list(call, page)
            return
        p = self._proxies[idx]
        ok = await _test_proxy(
            p.get("type", "HTTP"),
            p.get("ip"),
            p.get("port"),
            p.get("username"),
            p.get("password"),
        )
        p["status"] = "working" if ok else "unreachable"
        self._save_proxies()
        await self._cb_view_proxy(call, idx, page)

    async def _cb_delete_proxy(self, call: InlineCall, idx: int, page: int):
        if idx < len(self._proxies):
            p = self._proxies.pop(idx)
            self._save_proxies()
            await call.edit(
                self.strings["proxy_deleted"].format(ip=_escape(p.get("ip")), port=p.get("port")),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_manage_list, "args": (page,), "style": "primary"}]],
            )
        else:
            await self._cb_manage_list(call, page)

    async def _cb_test_all(self, call: InlineCall):
        if not self._proxies:
            await call.answer(self.strings["no_proxies"], show_alert=True)
            return
        await call.edit(self.strings["validating_proxies"].format(parsed=len(self._proxies)), reply_markup=[])
        sem = asyncio.Semaphore(20)

        async def _check_one(p):
            async with sem:
                ok = await _test_proxy(
                    p.get("type", "HTTP"),
                    p.get("ip"),
                    p.get("port"),
                    p.get("username"),
                    p.get("password"),
                )
                p["status"] = "working" if ok else "unreachable"
                return ok

        results = await asyncio.gather(*(_check_one(p) for p in self._proxies))
        self._save_proxies()
        working = sum(1 for r in results if r)
        failed = len(results) - working
        await call.edit(
            self.strings["test_results"].format(total=len(self._proxies), working=working, failed=failed),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}]],
        )

    async def _cb_export(self, call: InlineCall):
        if not self._proxies:
            await call.answer(self.strings["no_proxies"], show_alert=True)
            return
        tmp_dir = self._get_tmp_dir()
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", prefix="proxies_", dir=tmp_dir, delete=False, encoding="utf-8"
        )
        json.dump(self._proxies, tmp, indent=2, ensure_ascii=False)
        tmp.close()
        try:
            await self._client.send_file(
                call.form["chat"],
                tmp.name,
                force_document=True,
                file_name="proxies.json",
            )
        except Exception as e:
            logger.error("[ProxyScanner] export failed: %s", e)
        finally:
            with contextlib.suppress(Exception):
                os.unlink(tmp.name)
        await call.edit(
            self.strings["export_done"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}]],
        )

    async def _cb_add_type_menu(self, call: InlineCall):
        markup = [
            [
                {"text": self.strings["btn_ip_port"], "input": self.strings["input_ip"], "handler": self._ih_add_ip_simple, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_ip_port_login"], "input": self.strings["input_ip"], "handler": self._ih_add_ip_auth, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"},
            ],
        ]
        await call.edit(self.strings["add_type_menu"], reply_markup=markup)

    async def _ih_add_ip_simple(self, call: InlineCall, ip: str):
        ip = ip.strip()
        await call.edit(
            f"<b>Add Proxy: IP + Port</b>\n<blockquote>IP: <code>{_escape(ip)}</code></blockquote>",
            reply_markup=[
                [{"text": self.strings["input_port"], "input": self.strings["input_port"], "handler": self._ih_add_port_simple, "args": (ip,), "style": "primary"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_add_type_menu, "style": "primary"}],
            ],
        )

    async def _ih_add_port_simple(self, call: InlineCall, port_str: str, ip: str):
        try:
            port = int(port_str.strip())
            if not (1 <= port <= 65535):
                raise ValueError
        except ValueError:
            await call.edit(
                self.strings["err_invalid_port"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_add_type_menu, "style": "primary"}]],
            )
            return
        await self._run_detection(call, ip, port, None, None)

    async def _ih_add_ip_auth(self, call: InlineCall, ip: str):
        ip = ip.strip()
        await call.edit(
            f"<b>Add Proxy: Auth</b>\n<blockquote>IP: <code>{_escape(ip)}</code></blockquote>",
            reply_markup=[
                [{"text": self.strings["input_port"], "input": self.strings["input_port"], "handler": self._ih_add_port_auth, "args": (ip,), "style": "primary"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_add_type_menu, "style": "primary"}],
            ],
        )

    async def _ih_add_port_auth(self, call: InlineCall, port_str: str, ip: str):
        try:
            port = int(port_str.strip())
            if not (1 <= port <= 65535):
                raise ValueError
        except ValueError:
            await call.edit(
                self.strings["err_invalid_port"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_add_type_menu, "style": "primary"}]],
            )
            return
        await call.edit(
            f"<blockquote>IP: <code>{_escape(ip)}</code>\nPort: <code>{port}</code></blockquote>",
            reply_markup=[
                [{"text": self.strings["input_username"], "input": self.strings["input_username"], "handler": self._ih_add_user_auth, "args": (ip, port), "style": "primary"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_add_type_menu, "style": "primary"}],
            ],
        )

    async def _ih_add_user_auth(self, call: InlineCall, username: str, ip: str, port: int):
        username = username.strip()
        await call.edit(
            f"<blockquote>IP: <code>{_escape(ip)}</code>\nPort: <code>{port}</code>\nUser: <code>{_escape(username)}</code></blockquote>",
            reply_markup=[
                [{"text": self.strings["input_password"], "input": self.strings["input_password"], "handler": self._ih_add_pass_auth, "args": (ip, port, username), "style": "primary"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_add_type_menu, "style": "primary"}],
            ],
        )

    async def _ih_add_pass_auth(self, call: InlineCall, password: str, ip: str, port: int, username: str):
        password = password.strip()
        await self._run_detection(call, ip, port, username, password)

    async def _run_detection(self, call: InlineCall, ip: str, port: int, username: Optional[str], password: Optional[str]):
        await call.edit(self.strings["detecting"], reply_markup=[])
        res = await _detect_proxy_type(ip, port, username, password, timeout=6.0)
        proxy_type = res.get("type", "unknown")
        auth_req = res.get("auth_required", False)
        reason = res.get("reason", "")
        text = self.strings["detection_result"].format(
            ip=_escape(ip),
            port=port,
            type=proxy_type,
            auth="Yes" if auth_req else "No",
            reason=_escape(reason),
        )
        markup = []
        if res.get("working"):
            save_key = utils.rand(8)
            self._temp_data[save_key] = {
                "type": proxy_type,
                "ip": ip,
                "port": port,
                "username": username,
                "password": password,
                "auth_required": auth_req,
                "status": "working",
            }
            markup.append([{"text": self.strings["btn_save"], "callback": self._cb_save_detected, "args": (save_key,), "style": "success"}])
        markup.append([{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}])
        await call.edit(text, reply_markup=markup)

    async def _cb_save_detected(self, call: InlineCall, save_key: str):
        data = self._temp_data.pop(save_key, None)
        if not data:
            await self._cb_main_menu(call)
            return
        self._proxies.append(data)
        self._save_proxies()
        await call.edit(
            self.strings["proxy_added"].format(type=data["type"], ip=_escape(data["ip"]), port=data["port"]),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}]],
        )

    @loader.command(
        ru_doc="Менеджер и сканер прокси",
        en_doc="Proxy manager and scanner",
    )
    async def ps(self, message: Message):
        """Proxy manager and scanner"""
        await self.inline.form(
            text=self._fmt_main(),
            message=message,
            reply_markup=self._markup_main(),
            silent=True,
        )