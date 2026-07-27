__version__ = (1, 0, 1)
# meta developer: @I_execute

import logging
import json
import asyncio
import tempfile
import os
import struct
from typing import Dict, List, Optional, Tuple

from telethon.tl.types import Message
from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

try:
    import aiohttp
    AIOHTTP_OK = True
except ImportError:
    AIOHTTP_OK = False


def _escape(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def _check_tcp_open(host: str, port: int, timeout: float = 5.0) -> bool:
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout
        )
        writer.close()
        await writer.wait_closed()
        return True
    except Exception:
        return False


async def _check_socks5(host: str, port: int, username: Optional[str] = None, password: Optional[str] = None, timeout: float = 10.0) -> Tuple[bool, bool]:
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout
        )

        if username and password:
            writer.write(b'\x05\x01\x02')
        else:
            writer.write(b'\x05\x01\x00')
        
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

            auth_data = struct.pack('B', 1)
            auth_data += struct.pack('B', len(username)) + username.encode()
            auth_data += struct.pack('B', len(password)) + password.encode()
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

        connect_request = b'\x05\x01\x00\x01'
        connect_request += b'\x08\x08\x08\x08'
        connect_request += struct.pack('>H', 80)
        
        writer.write(connect_request)
        await writer.drain()

        connect_response = await asyncio.wait_for(reader.read(10), timeout=timeout)
        
        writer.close()
        await writer.wait_closed()

        if len(connect_response) >= 2 and connect_response[1] == 0x00:
            return True, False
        
        return False, False

    except Exception as e:
        logger.debug("_check_socks5 error: %s", e)
        return False, False


async def _check_http_proxy(host: str, port: int, username: Optional[str] = None, password: Optional[str] = None, timeout: float = 10.0) -> Tuple[bool, bool]:
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout
        )

        request = b"CONNECT example.com:80 HTTP/1.1\r\n"
        request += b"Host: example.com:80\r\n"
        
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

        response_str = response.decode('utf-8', errors='ignore')

        if "200" in response_str and "Connection" in response_str:
            return True, False
        elif "407" in response_str:
            return True, True
        
        return False, False

    except Exception as e:
        logger.debug("_check_http_proxy error: %s", e)
        return False, False


async def _detect_proxy_type(host: str, port: int, username: Optional[str] = None, password: Optional[str] = None, timeout: float = 10.0) -> Dict:
    tcp_open = await _check_tcp_open(host, port, timeout=3.0)
    
    if not tcp_open:
        return {
            "type": "unknown",
            "working": False,
            "auth_required": False,
            "reason": "Port closed or unreachable"
        }

    socks5_ok, socks5_auth = await _check_socks5(host, port, username, password, timeout)
    http_ok, http_auth = await _check_http_proxy(host, port, username, password, timeout)

    if socks5_ok and http_ok:
        return {
            "type": "multi-protocol",
            "working": True,
            "auth_required": socks5_auth or http_auth,
            "reason": "Both SOCKS5 and HTTP handshake successful"
        }
    elif socks5_ok:
        return {
            "type": "socks5",
            "working": True,
            "auth_required": socks5_auth,
            "reason": "SOCKS5 handshake successful"
        }
    elif http_ok:
        return {
            "type": "http",
            "working": True,
            "auth_required": http_auth,
            "reason": "HTTP CONNECT successful"
        }
    else:
        return {
            "type": "unknown",
            "working": False,
            "auth_required": False,
            "reason": "No valid proxy protocol detected"
        }


async def _test_proxy(proxy_type: str, host: str, port: int, username: str = None, password: str = None, timeout: int = 10) -> bool:
    if not AIOHTTP_OK:
        return False
    
    try:
        if proxy_type == "http":
            proxy_url = f"http://{host}:{port}"
            if username and password:
                proxy_url = f"http://{username}:{password}@{host}:{port}"
        elif proxy_type == "socks5":
            proxy_url = f"socks5://{host}:{port}"
            if username and password:
                proxy_url = f"socks5://{username}:{password}@{host}:{port}"
        else:
            return False

        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.ipify.org?format=json",
                proxy=proxy_url,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                if resp.status == 200:
                    return True
                return False
    except Exception as e:
        logger.debug("_test_proxy error: %s", e)
        return False


@loader.tds
class ProxyScanner(loader.Module):
    """Proxy manager and tester with auto-detection"""

    strings = {
        "name": "ProxyScanner",

        "main_menu": (
            "<b>Proxy Manager</b>\n"
            "<blockquote>Total proxies: {total}</blockquote>"
        ),

        "add_type_menu": (
            "<b>Add Proxy</b>\n"
            "<blockquote>Select proxy type:</blockquote>"
        ),

        "add_ip_port": (
            "<b>Add Proxy: IP + PORT</b>\n"
            "<blockquote>Enter IP address:</blockquote>"
        ),

        "add_port": (
            "<b>Add Proxy: IP + PORT</b>\n"
            "<blockquote>IP: {ip}\nEnter port:</blockquote>"
        ),

        "add_ip_port_login": (
            "<b>Add Proxy: IP + PORT + LOGIN</b>\n"
            "<blockquote>Enter IP address:</blockquote>"
        ),

        "add_port_login": (
            "<b>Add Proxy: IP + PORT + LOGIN</b>\n"
            "<blockquote>IP: {ip}\nEnter port:</blockquote>"
        ),

        "add_username": (
            "<b>Add Proxy: IP + PORT + LOGIN</b>\n"
            "<blockquote>IP: {ip}\nPort: {port}\nEnter username:</blockquote>"
        ),

        "add_password": (
            "<b>Add Proxy: IP + PORT + LOGIN</b>\n"
            "<blockquote>IP: {ip}\nPort: {port}\nUsername: {username}\nEnter password:</blockquote>"
        ),

        "detecting": "<b>Detecting proxy type...</b>",

        "detection_result": (
            "<b>Detection Result</b>\n"
            "<blockquote>"
            "IP: {ip}\n"
            "Port: {port}\n"
            "Type: {type}\n"
            "Auth required: {auth}\n"
            "Reason: {reason}"
            "</blockquote>"
        ),

        "confirm_menu": (
            "<b>Confirm Proxy</b>\n"
            "<blockquote>"
            "Type: {type}\n"
            "IP: {ip}\n"
            "Port: {port}\n"
            "Username: {username}\n"
            "Password: {password}\n\n"
            "Correct?"
            "</blockquote>"
        ),

        "testing": "<b>Testing proxy...</b>",

        "test_success": (
            "<b>Proxy Test: Success</b>\n"
            "<blockquote>"
            "Type: {type}\n"
            "IP: {ip}\n"
            "Port: {port}\n"
            "Username: {username}\n"
            "Password: {password}\n\n"
            "Proxy is working"
            "</blockquote>"
        ),

        "test_failed": (
            "<b>Proxy Test: Failed</b>\n"
            "<blockquote>"
            "Type: {type}\n"
            "IP: {ip}\n"
            "Port: {port}\n"
            "Username: {username}\n"
            "Password: {password}\n\n"
            "Proxy is not working"
            "</blockquote>"
        ),

        "proxy_added": (
            "<b>Proxy Added</b>\n"
            "<blockquote>{type}://{ip}:{port}</blockquote>"
        ),

        "manage_menu": (
            "<b>Manage Proxies</b>\n"
            "<blockquote>Total: {total}</blockquote>"
        ),

        "testing_all": "<b>Testing all proxies...</b>",

        "test_results": (
            "<b>Test Results</b>\n"
            "<blockquote>"
            "Total: {total}\n"
            "Working: {working}\n"
            "Failed: {failed}"
            "</blockquote>"
        ),

        "export_done": (
            "<b>Export Complete</b>\n"
            "<blockquote>proxies.json sent to chat</blockquote>"
        ),

        "no_proxies": (
            "<b>No Proxies</b>\n"
            "<blockquote>Add proxies first</blockquote>"
        ),

        "btn_ip_port": "IP + PORT",
        "btn_ip_port_login": "IP + PORT + LOGIN",
        "btn_manage": "Manage",
        "btn_back": "Back",
        "btn_test": "Test",
        "btn_success": "Success",
        "btn_test_all": "Test All",
        "btn_export": "Export",
        "btn_auto": "Auto-detect",

        "input_ip": "Enter IP:",
        "input_port": "Enter port:",
        "input_username": "Enter username:",
        "input_password": "Enter password:",

        "err_invalid_port": (
            "<b>Error</b>\n"
            "<blockquote>Invalid port number</blockquote>"
        ),

        "err_detection_failed": (
            "<b>Detection Failed</b>\n"
            "<blockquote>Could not determine proxy type</blockquote>"
        ),
    }

    strings_ru = {
        "main_menu": (
            "<b>Менеджер прокси</b>\n"
            "<blockquote>Всего прокси: {total}</blockquote>"
        ),

        "add_type_menu": (
            "<b>Добавить прокси</b>\n"
            "<blockquote>Выберите тип прокси:</blockquote>"
        ),

        "add_ip_port": (
            "<b>Добавить прокси: IP + ПОРТ</b>\n"
            "<blockquote>Введите IP адрес:</blockquote>"
        ),

        "add_port": (
            "<b>Добавить прокси: IP + ПОРТ</b>\n"
            "<blockquote>IP: {ip}\nВведите порт:</blockquote>"
        ),

        "add_ip_port_login": (
            "<b>Добавить прокси: IP + ПОРТ + ЛОГИН</b>\n"
            "<blockquote>Введите IP адрес:</blockquote>"
        ),

        "add_port_login": (
            "<b>Добавить прокси: IP + ПОРТ + ЛОГИН</b>\n"
            "<blockquote>IP: {ip}\nВведите порт:</blockquote>"
        ),

        "add_username": (
            "<b>Добавить прокси: IP + ПОРТ + ЛОГИН</b>\n"
            "<blockquote>IP: {ip}\nПорт: {port}\nВведите логин:</blockquote>"
        ),

        "add_password": (
            "<b>Добавить прокси: IP + ПОРТ + ЛОГИН</b>\n"
            "<blockquote>IP: {ip}\nПорт: {port}\nЛогин: {username}\nВведите пароль:</blockquote>"
        ),

        "detecting": "<b>Определение типа прокси...</b>",

        "detection_result": (
            "<b>Результат определения</b>\n"
            "<blockquote>"
            "IP: {ip}\n"
            "Порт: {port}\n"
            "Тип: {type}\n"
            "Требуется авторизация: {auth}\n"
            "Причина: {reason}"
            "</blockquote>"
        ),

        "confirm_menu": (
            "<b>Подтверждение прокси</b>\n"
            "<blockquote>"
            "Тип: {type}\n"
            "IP: {ip}\n"
            "Порт: {port}\n"
            "Логин: {username}\n"
            "Пароль: {password}\n\n"
            "Правильно?"
            "</blockquote>"
        ),

        "testing": "<b>Тестирование прокси...</b>",

        "test_success": (
            "<b>Тест прокси: Успешно</b>\n"
            "<blockquote>"
            "Тип: {type}\n"
            "IP: {ip}\n"
            "Порт: {port}\n"
            "Логин: {username}\n"
            "Пароль: {password}\n\n"
            "Прокси работает"
            "</blockquote>"
        ),

        "test_failed": (
            "<b>Тест прокси: Ошибка</b>\n"
            "<blockquote>"
            "Тип: {type}\n"
            "IP: {ip}\n"
            "Порт: {port}\n"
            "Логин: {username}\n"
            "Пароль: {password}\n\n"
            "Прокси не работает"
            "</blockquote>"
        ),

        "proxy_added": (
            "<b>Прокси добавлен</b>\n"
            "<blockquote>{type}://{ip}:{port}</blockquote>"
        ),

        "manage_menu": (
            "<b>Управление прокси</b>\n"
            "<blockquote>Всего: {total}</blockquote>"
        ),

        "testing_all": "<b>Тестирование всех прокси...</b>",

        "test_results": (
            "<b>Результаты теста</b>\n"
            "<blockquote>"
            "Всего: {total}\n"
            "Работает: {working}\n"
            "Не работает: {failed}"
            "</blockquote>"
        ),

        "export_done": (
            "<b>Экспорт завершён</b>\n"
            "<blockquote>proxies.json отправлен в чат</blockquote>"
        ),

        "no_proxies": (
            "<b>Нет прокси</b>\n"
            "<blockquote>Сначала добавьте прокси</blockquote>"
        ),

        "btn_ip_port": "IP + ПОРТ",
        "btn_ip_port_login": "IP + ПОРТ + ЛОГИН",
        "btn_manage": "Управление",
        "btn_back": "Назад",
        "btn_test": "Тест",
        "btn_success": "Готово",
        "btn_test_all": "Тест всех",
        "btn_export": "Экспорт",
        "btn_auto": "Авто-определение",

        "input_ip": "Введите IP:",
        "input_port": "Введите порт:",
        "input_username": "Введите логин:",
        "input_password": "Введите пароль:",

        "err_invalid_port": (
            "<b>Ошибка</b>\n"
            "<blockquote>Неверный номер порта</blockquote>"
        ),

        "err_detection_failed": (
            "<b>Определение не удалось</b>\n"
            "<blockquote>Не удалось определить тип прокси</blockquote>"
        ),
    }

    def __init__(self):
        self._proxies: List[Dict] = []

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._proxies = self._db.get("ProxyScanner", "proxies", [])

    def _save_proxies(self):
        self._db.set("ProxyScanner", "proxies", self._proxies)

    async def _cb_main_menu(self, call: InlineCall):
        text = self.strings["main_menu"].format(total=len(self._proxies))

        markup = [
            [{"text": self.strings["btn_manage"], "callback": self._cb_manage_menu, "style": "primary"}],
            [{"text": self.strings["btn_ip_port"], "callback": self._cb_add_ip_simple, "style": "primary"}],
            [{"text": self.strings["btn_ip_port_login"], "callback": self._cb_add_ip_auth, "style": "primary"}],
        ]

        await call.edit(text, reply_markup=markup)

    async def _cb_add_ip_simple(self, call: InlineCall):
        text = self.strings["add_ip_port"]

        markup = [[
            {
                "text": self.strings["input_ip"],
                "input": self.strings["input_ip"],
                "handler": self._cb_add_port_simple,
                "style": "primary",
            }
        ]]

        await call.edit(text, reply_markup=markup)

    async def _cb_add_port_simple(self, call: InlineCall, ip: str):
        text = self.strings["add_port"].format(ip=_escape(ip))

        markup = [[
            {
                "text": self.strings["input_port"],
                "input": self.strings["input_port"],
                "handler": self._cb_detect_type,
                "args": (ip.strip(), None, None),
                "style": "primary",
            }
        ]]

        await call.edit(text, reply_markup=markup)

    async def _cb_add_ip_auth(self, call: InlineCall):
        text = self.strings["add_ip_port_login"]

        markup = [[
            {
                "text": self.strings["input_ip"],
                "input": self.strings["input_ip"],
                "handler": self._cb_add_port_auth,
                "style": "primary",
            }
        ]]

        await call.edit(text, reply_markup=markup)

    async def _cb_add_port_auth(self, call: InlineCall, ip: str):
        text = self.strings["add_port_login"].format(ip=_escape(ip))

        markup = [[
            {
                "text": self.strings["input_port"],
                "input": self.strings["input_port"],
                "handler": self._cb_add_username,
                "args": (ip.strip(),),
                "style": "primary",
            }
        ]]

        await call.edit(text, reply_markup=markup)

    async def _cb_add_username(self, call: InlineCall, port_str: str, ip: str):
        try:
            port = int(port_str.strip())
            if port <= 0 or port > 65535:
                raise ValueError
        except ValueError:
            await call.answer(self.strings["err_invalid_port"], show_alert=True)
            return

        text = self.strings["add_username"].format(ip=_escape(ip), port=port)

        markup = [[
            {
                "text": self.strings["input_username"],
                "input": self.strings["input_username"],
                "handler": self._cb_add_password,
                "args": (ip, port),
                "style": "primary",
            }
        ]]

        await call.edit(text, reply_markup=markup)

    async def _cb_add_password(self, call: InlineCall, username: str, ip: str, port: int):
        text = self.strings["add_password"].format(
            ip=_escape(ip),
            port=port,
            username=_escape(username),
        )

        markup = [[
            {
                "text": self.strings["input_password"],
                "input": self.strings["input_password"],
                "handler": self._cb_detect_type,
                "args": (ip, port, username.strip()),
                "style": "primary",
            }
        ]]

        await call.edit(text, reply_markup=markup)

    async def _cb_detect_type(self, call: InlineCall, port_or_pass: str, ip: str, port: Optional[int], username: Optional[str]):
        if port is None:
            try:
                port = int(port_or_pass.strip())
                if port <= 0 or port > 65535:
                    raise ValueError
            except ValueError:
                await call.answer(self.strings["err_invalid_port"], show_alert=True)
                return
            password = None
        else:
            password = port_or_pass.strip()

        await call.edit(self.strings["detecting"], reply_markup=[])

        result = await _detect_proxy_type(ip, port, username, password)

        if not result["working"]:
            text = self.strings["detection_result"].format(
                ip=_escape(ip),
                port=port,
                type=_escape(result["type"]),
                auth="Yes" if result["auth_required"] else "No",
                reason=_escape(result["reason"]),
            )

            markup = [[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
            await call.edit(text, reply_markup=markup)
            return

        proxy_type = result["type"]
        if proxy_type == "multi-protocol":
            proxy_type = "socks5"

        text = self.strings["confirm_menu"].format(
            type=proxy_type.upper(),
            ip=_escape(ip),
            port=port,
            username=_escape(username) if username else "N/A",
            password=_escape(password) if password else "N/A",
        )

        markup = [
            [{"text": self.strings["btn_test"], "callback": self._cb_test_proxy, "args": (proxy_type, ip, port, username, password), "style": "success"}],
            [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}],
        ]

        await call.edit(text, reply_markup=markup)

    async def _cb_test_proxy(self, call: InlineCall, proxy_type: str, ip: str, port: int, username: Optional[str], password: Optional[str]):
        await call.edit(self.strings["testing"], reply_markup=[])

        result = await _test_proxy(proxy_type, ip, port, username, password)

        if result:
            text = self.strings["test_success"].format(
                type=proxy_type.upper(),
                ip=_escape(ip),
                port=port,
                username=_escape(username) if username else "N/A",
                password=_escape(password) if password else "N/A",
            )

            markup = [
                [{"text": self.strings["btn_success"], "callback": self._cb_save_proxy, "args": (proxy_type, ip, port, username, password), "style": "success"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}],
            ]
        else:
            text = self.strings["test_failed"].format(
                type=proxy_type.upper(),
                ip=_escape(ip),
                port=port,
                username=_escape(username) if username else "N/A",
                password=_escape(password) if password else "N/A",
            )

            markup = [[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]

        await call.edit(text, reply_markup=markup)

    async def _cb_save_proxy(self, call: InlineCall, proxy_type: str, ip: str, port: int, username: Optional[str], password: Optional[str]):
        proxy = {
            "type": proxy_type,
            "ip": ip,
            "port": port,
            "username": username,
            "password": password,
        }

        self._proxies.append(proxy)
        self._save_proxies()

        text = self.strings["proxy_added"].format(type=proxy_type, ip=_escape(ip), port=port)

        markup = [[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}]]

        await call.edit(text, reply_markup=markup)

    async def _cb_manage_menu(self, call: InlineCall):
        if not self._proxies:
            text = self.strings["no_proxies"]
            markup = [[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}]]
            await call.edit(text, reply_markup=markup)
            return

        text = self.strings["manage_menu"].format(total=len(self._proxies))

        markup = [
            [{"text": self.strings["btn_test_all"], "callback": self._cb_test_all, "style": "primary"}],
            [{"text": self.strings["btn_export"], "callback": self._cb_export, "style": "primary"}],
            [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}],
        ]

        await call.edit(text, reply_markup=markup)

    async def _cb_test_all(self, call: InlineCall):
        await call.edit(self.strings["testing_all"], reply_markup=[])

        tasks = []
        for proxy in self._proxies:
            tasks.append(_test_proxy(
                proxy["type"],
                proxy["ip"],
                proxy["port"],
                proxy.get("username"),
                proxy.get("password"),
            ))

        results = await asyncio.gather(*tasks)

        working = sum(1 for r in results if r)
        failed = len(results) - working

        text = self.strings["test_results"].format(
            total=len(self._proxies),
            working=working,
            failed=failed,
        )

        markup = [[{"text": self.strings["btn_back"], "callback": self._cb_manage_menu, "style": "primary"}]]

        await call.edit(text, reply_markup=markup)

    async def _cb_export(self, call: InlineCall):
        if not self._proxies:
            await call.answer(self.strings["no_proxies"], show_alert=True)
            return

        tmp = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            prefix="proxies_",
            delete=False,
        )

        json.dump(self._proxies, tmp, indent=2)
        tmp.close()

        try:
            await self._client.send_file(
                call.form["chat"],
                tmp.name,
                force_document=True,
                file_name="proxies.json",
            )
        except Exception as e:
            logger.exception("send_file failed: %s", e)
        finally:
            os.unlink(tmp.name)

        text = self.strings["export_done"]
        markup = [[{"text": self.strings["btn_back"], "callback": self._cb_manage_menu, "style": "primary"}]]

        await call.edit(text, reply_markup=markup)

    @loader.command(
        ru_doc="Менеджер прокси с авто-определением",
        en_doc="Proxy manager with auto-detection",
    )
    async def ps(self, message: Message):
        """Proxy manager with auto-detection"""
        await self.inline.form(
            text=self.strings["main_menu"].format(total=len(self._proxies)),
            message=message,
            reply_markup=[
                [{"text": self.strings["btn_manage"], "callback": self._cb_manage_menu, "style": "primary"}],
                [{"text": self.strings["btn_ip_port"], "callback": self._cb_add_ip_simple, "style": "primary"}],
                [{"text": self.strings["btn_ip_port_login"], "callback": self._cb_add_ip_auth, "style": "primary"}],
            ],
            silent=True,
        )