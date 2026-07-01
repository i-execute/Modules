__version__ = (2, 3, 4)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/Session/MetaBanner.jpeg

import logging
import asyncio
import time
import base64
import ipaddress
import struct
import sqlite3
import os
import re
import tempfile
import shutil
import io
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl import types
from telethon.tl.types import Message
from telethon.errors import (
    SessionPasswordNeededError,
    AuthKeyUnregisteredError,
    UserDeactivatedBanError
)
from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

ACTION_DELAY = 1.7
STRING_SESSION_PATTERN = re.compile(r'1[A-Za-z0-9_-]{200,}={0,2}')
HEX_KEY_PATTERN = re.compile(r'[0-9a-fA-F]{512}')


def _safe_disconnect(client):
    if client:
        try:
            asyncio.ensure_future(client.disconnect())
        except Exception:
            pass


@loader.tds
class Session(loader.Module):
    """Create new session files/strings or HEX. Need unexternal flag for sending .session files"""

    strings = {
        "name": "Session",
        "line": "--------------------",
        "main_menu_create": (
            "<b>Create New Session</b>\n"
            "<blockquote>Select output format:</blockquote>"
        ),
        "main_menu_convert_hex": (
            "<b>Convert Session</b>\n"
            "<blockquote>Reply contains: HEX\n"
            "Select action:</blockquote>"
        ),
        "main_menu_convert_format": (
            "<b>Convert Session</b>\n"
            "<blockquote>Reply contains: {input_type}\n"
            "Select action:</blockquote>"
        ),
        "select_dc_hex": (
            "<b>Select Data Center</b>\n"
            "<blockquote>Choose DC ID (1-5):</blockquote>"
        ),
        "btn_string": "String",
        "btn_file": "File",
        "btn_hex": "HEX",
        "btn_dc_1": "DC 1",
        "btn_dc_2": "DC 2",
        "btn_dc_3": "DC 3",
        "btn_dc_4": "DC 4",
        "btn_dc_5": "DC 5",
        "btn_test": "Test Session",
        "btn_convert": "Convert",
        "btn_close": "Close",
        "btn_kill": "Kill Process",
        "btn_back": "Back",
        "select_format": (
            "<b>Select Output Format</b>\n"
            "<blockquote>Choose format:</blockquote>"
        ),
        "select_dc_for_test": (
            "<b>Select Data Center for Test</b>\n"
            "<blockquote>Choose DC ID (1-5):</blockquote>"
        ),
        "creating": (
            "<b>Creating Session</b>\n"
            "<blockquote>"
            "Output: {output_type}\n"
            "Status: {status}"
            "</blockquote>"
        ),
        "converting": (
            "<b>Converting Session</b>\n"
            "<blockquote>"
            "From: {input_type}\n"
            "To: {output_type}\n"
            "Status: {status}"
            "</blockquote>"
        ),
        "testing": "<b>Testing session...</b>",
        "input_phone": "Enter phone number:",
        "input_code": "Enter verification code:",
        "input_password": "Enter 2FA password:",
        "result_string": (
            "<b>Session Created</b>\n"
            "<blockquote><code>{string}</code></blockquote>"
        ),
        "result_hex": (
            "<b>Session Created</b>\n"
            "<blockquote>"
            "DC: <code>{dc_id}</code>\n"
            "HEX:\n<code>{hex_key}</code>"
            "</blockquote>"
        ),
        "result_file": (
            "<b>Session Created</b>\n"
            "<blockquote>File sent to chat</blockquote>"
        ),
        "converted_string": (
            "<b>Session Converted</b>\n"
            "<blockquote><code>{string}</code></blockquote>"
        ),
        "converted_hex": (
            "<b>Session Converted</b>\n"
            "<blockquote>"
            "DC: <code>{dc_id}</code>\n"
            "HEX:\n<code>{hex_key}</code>"
            "</blockquote>"
        ),
        "converted_file": (
            "<b>Session Converted</b>\n"
            "<blockquote>File sent to chat</blockquote>"
        ),
        "test_success": (
            "<b>Session Valid</b>\n"
            "{line}\n"
            "<blockquote>"
            "User: {user_link}\n"
            "ID: <code>{user_id}</code>\n"
            "DC: <code>{dc_id}</code>\n"
            "Server IP: <code>{ip}</code>\n"
            "Port: <code>{port}</code>\n"
            "Auth Key: <code>{key_len} bytes</code>\n"
            "Premium: {premium}\n"
            "Status: <b>OK</b>"
            "</blockquote>\n"
            "{line}"
        ),
        "test_fail": (
            "<b>Session Invalid</b>\n"
            "{line}\n"
            "<blockquote>Reason: {reason}</blockquote>\n"
            "{line}"
        ),
        "test_info_only": (
            "<b>Session Info</b> (not authorized)\n"
            "{line}\n"
            "<blockquote>"
            "DC ID: <code>{dc_id}</code>\n"
            "Server IP: <code>{ip}</code>\n"
            "Port: <code>{port}</code>\n"
            "Auth Key: <code>{key_len} bytes</code>"
            "</blockquote>\n"
            "{line}"
        ),
        "err_no_reply": "<b>Error:</b> No session in reply",
        "err_invalid": "<b>Error:</b> Invalid session data",
        "err_connection": "<b>Error:</b> Connection failed",
        "status_connecting": "Connecting...",
        "status_waiting_phone": "Waiting for phone...",
        "status_waiting_code": "Waiting for code...",
        "status_waiting_password": "Waiting for password...",
        "status_processing": "Processing...",
        "status_done": "Done",
        "not_authorized": "Session not authorized",
        "process_killed": "<b>Process killed</b>",
    }

    strings_ru = {
        "line": "--------------------",
        "main_menu_create": (
            "<b>Создание новой сессии</b>\n"
            "<blockquote>Выберите формат вывода:</blockquote>"
        ),
        "main_menu_convert_hex": (
            "<b>Конвертация сессии</b>\n"
            "<blockquote>В реплае находится: HEX\n"
            "Выберите действие:</blockquote>"
        ),
        "main_menu_convert_format": (
            "<b>Конвертация сессии</b>\n"
            "<blockquote>В реплае находится: {input_type}\n"
            "Выберите действие:</blockquote>"
        ),
        "select_dc_hex": (
            "<b>Выбор дата-центра</b>\n"
            "<blockquote>Выберите DC ID (1-5):</blockquote>"
        ),
        "btn_string": "String",
        "btn_file": "Файл",
        "btn_hex": "HEX",
        "btn_dc_1": "DC 1",
        "btn_dc_2": "DC 2",
        "btn_dc_3": "DC 3",
        "btn_dc_4": "DC 4",
        "btn_dc_5": "DC 5",
        "btn_test": "Проверить сессию",
        "btn_convert": "Конвертировать",
        "btn_close": "Закрыть",
        "btn_kill": "Убить процесс",
        "btn_back": "Назад",
        "select_format": (
            "<b>Выбор формата вывода</b>\n"
            "<blockquote>Выберите формат:</blockquote>"
        ),
        "select_dc_for_test": (
            "<b>Выбор дата-центра для теста</b>\n"
            "<blockquote>Выберите DC ID (1-5):</blockquote>"
        ),
        "creating": (
            "<b>Создание сессии</b>\n"
            "<blockquote>"
            "Формат: {output_type}\n"
            "Статус: {status}"
            "</blockquote>"
        ),
        "converting": (
            "<b>Конвертация сессии</b>\n"
            "<blockquote>"
            "Из: {input_type}\n"
            "В: {output_type}\n"
            "Статус: {status}"
            "</blockquote>"
        ),
        "testing": "<b>Проверка сессии...</b>",
        "input_phone": "Введите номер телефона:",
        "input_code": "Введите код подтверждения:",
        "input_password": "Введите 2FA пароль:",
        "result_string": (
            "<b>Сессия создана</b>\n"
            "<blockquote><code>{string}</code></blockquote>"
        ),
        "result_hex": (
            "<b>Сессия создана</b>\n"
            "<blockquote>"
            "DC: <code>{dc_id}</code>\n"
            "HEX:\n<code>{hex_key}</code>"
            "</blockquote>"
        ),
        "result_file": (
            "<b>Сессия создана</b>\n"
            "<blockquote>Файл отправлен в чат</blockquote>"
        ),
        "converted_string": (
            "<b>Сессия конвертирована</b>\n"
            "<blockquote><code>{string}</code></blockquote>"
        ),
        "converted_hex": (
            "<b>Сессия конвертирована</b>\n"
            "<blockquote>"
            "DC: <code>{dc_id}</code>\n"
            "HEX:\n<code>{hex_key}</code>"
            "</blockquote>"
        ),
        "converted_file": (
            "<b>Сессия конвертирована</b>\n"
            "<blockquote>Файл отправлен в чат</blockquote>"
        ),
        "test_success": (
            "<b>Сессия валидна</b>\n"
            "{line}\n"
            "<blockquote>"
            "Юзер: {user_link}\n"
            "ID: <code>{user_id}</code>\n"
            "DC: <code>{dc_id}</code>\n"
            "IP сервера: <code>{ip}</code>\n"
            "Порт: <code>{port}</code>\n"
            "Auth Key: <code>{key_len} байт</code>\n"
            "Премиум: {premium}\n"
            "Статус: <b>OK</b>"
            "</blockquote>\n"
            "{line}"
        ),
        "test_fail": (
            "<b>Сессия невалидна</b>\n"
            "{line}\n"
            "<blockquote>Причина: {reason}</blockquote>\n"
            "{line}"
        ),
        "test_info_only": (
            "<b>Информация о сессии</b> (не авторизована)\n"
            "{line}\n"
            "<blockquote>"
            "DC ID: <code>{dc_id}</code>\n"
            "IP сервера: <code>{ip}</code>\n"
            "Порт: <code>{port}</code>\n"
            "Auth Key: <code>{key_len} байт</code>"
            "</blockquote>\n"
            "{line}"
        ),
        "err_no_reply": "<b>Ошибка:</b> Нет сессии в реплае",
        "err_invalid": "<b>Ошибка:</b> Невалидные данные сессии",
        "err_connection": "<b>Ошибка:</b> Ошибка подключения",
        "status_connecting": "Подключение...",
        "status_waiting_phone": "Ожидание телефона...",
        "status_waiting_code": "Ожидание кода...",
        "status_waiting_password": "Ожидание пароля...",
        "status_processing": "Обработка...",
        "status_done": "Готово",
        "not_authorized": "Сессия не авторизована",
        "process_killed": "<b>Процесс завершён</b>",
    }

    _DC_IP_MAP = {
        1: "149.154.175.53",
        2: "149.154.167.51",
        3: "149.154.175.100",
        4: "149.154.167.91",
        5: "91.108.56.130"
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "API_ID",
                2040,
                "Telegram API ID",
                validator=loader.validators.Integer(),
            ),
            loader.ConfigValue(
                "API_HASH",
                "b18441a1ff607e10a989891a5462e627",
                "Telegram API Hash",
                validator=loader.validators.Hidden(),
            ),
        )
        self._sessions = {}
        self._temp_dir = None

    async def client_ready(self, client, db):
        self._client = client
        self._temp_dir = os.path.join(tempfile.gettempdir(), "session_module")
        if os.path.exists(self._temp_dir):
            shutil.rmtree(self._temp_dir)
        os.makedirs(self._temp_dir, exist_ok=True)

    async def on_unload(self):
        if self._temp_dir and os.path.exists(self._temp_dir):
            try:
                shutil.rmtree(self._temp_dir)
            except:
                pass

    def _find_string_session(self, text):
        if not text:
            return None
        match = STRING_SESSION_PATTERN.search(text)
        return match.group(0) if match else None

    def _find_hex_key(self, text):
        if not text:
            return None
        match = HEX_KEY_PATTERN.search(text)
        return match.group(0) if match else None

    def _parse_string_session(self, session_str):
        try:
            if not session_str:
                return None
            session_str = session_str.strip()
            if not session_str.startswith('1'):
                return None
            string = session_str[1:]
            string_padded = string + '=' * (-len(string) % 4)
            try:
                data = base64.urlsafe_b64decode(string_padded)
            except:
                return None
            if len(data) == 263:
                dc_id, ip_bytes, port, auth_key = struct.unpack('>B4sH256s', data)
                ip = str(ipaddress.IPv4Address(ip_bytes))
            elif len(data) == 275:
                dc_id, ip_bytes, port, auth_key = struct.unpack('>B16sH256s', data)
                ip = str(ipaddress.IPv6Address(ip_bytes))
            else:
                return None
            return {
                'dc_id': dc_id,
                'ip': ip,
                'port': port,
                'auth_key': auth_key
            }
        except:
            return None

    def _build_string_session(self, dc_id, auth_key):
        try:
            if dc_id not in self._DC_IP_MAP:
                return None
            if auth_key is None:
                return None
            if isinstance(auth_key, str):
                auth_key = auth_key.encode('latin-1')
            elif not isinstance(auth_key, bytes):
                auth_key = bytes(auth_key)
            if len(auth_key) != 256:
                return None
            ip_str = self._DC_IP_MAP[dc_id]
            ip = ipaddress.IPv4Address(ip_str)
            port = 443
            data = struct.pack(
                '>B4sH256s',
                dc_id,
                ip.packed,
                port,
                auth_key
            )
            encoded = base64.urlsafe_b64encode(data).decode('ascii')
            return '1' + encoded
        except Exception as e:
            logger.error(f"[SESSION] Build error: {e}")
            return None

    def _auth_key_to_hex(self, auth_key):
        if isinstance(auth_key, str):
            auth_key = auth_key.encode('latin-1')
        return auth_key.hex()

    def _hex_to_auth_key(self, hex_str):
        return bytes.fromhex(hex_str)

    async def _read_session_file(self, file_path):
        try:
            conn = sqlite3.connect(file_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sessions'")
            if not cursor.fetchone():
                conn.close()
                return None
            cursor.execute("SELECT dc_id, auth_key FROM sessions LIMIT 1")
            row = cursor.fetchone()
            conn.close()
            if row:
                dc_id = row[0]
                auth_key = row[1]
                if isinstance(auth_key, str):
                    auth_key = auth_key.encode('latin-1')
                if not auth_key or len(auth_key) != 256:
                    return None
                return {'dc_id': dc_id, 'auth_key': auth_key}
            return None
        except:
            return None

    def _create_session_file(self, dc_id, auth_key):
        try:
            file_path = os.path.join(self._temp_dir, "file.session")
            if os.path.exists(file_path):
                os.remove(file_path)
            if isinstance(auth_key, str):
                auth_key = auth_key.encode('latin-1')
            if len(auth_key) != 256:
                return None
            conn = sqlite3.connect(file_path)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    dc_id INTEGER PRIMARY KEY,
                    server_address TEXT,
                    port INTEGER,
                    auth_key BLOB
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS entities (
                    id INTEGER PRIMARY KEY,
                    hash INTEGER NOT NULL,
                    username TEXT,
                    phone INTEGER,
                    name TEXT,
                    date INTEGER
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sent_files (
                    md5_digest BLOB,
                    file_size INTEGER,
                    type INTEGER,
                    id INTEGER,
                    hash INTEGER,
                    PRIMARY KEY (md5_digest, file_size, type)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS update_state (
                    id INTEGER PRIMARY KEY,
                    pts INTEGER,
                    qts INTEGER,
                    date INTEGER,
                    seq INTEGER
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS version (
                    version INTEGER PRIMARY KEY
                )
            """)
            cursor.execute("INSERT OR REPLACE INTO version VALUES (7)")
            ip = self._DC_IP_MAP.get(dc_id, self._DC_IP_MAP[2])
            cursor.execute(
                "INSERT OR REPLACE INTO sessions VALUES (?, ?, ?, ?)",
                (dc_id, ip, 443, auth_key)
            )
            conn.commit()
            conn.close()
            return file_path
        except Exception as e:
            logger.error(f"[SESSION] Create file error: {e}")
            return None

    async def _send_session_file(self, chat_id, dc_id, auth_key, topic_id=None):
        file_path = None
        try:
            file_path = self._create_session_file(dc_id, auth_key)
            if not file_path or not os.path.exists(file_path):
                logger.error("[SESSION] File creation failed")
                return False
            with open(file_path, "rb") as f:
                file_bytes = f.read()
            memory_file = io.BytesIO(file_bytes)
            memory_file.name = "database_data.db"
            attributes = [
                types.DocumentAttributeFilename(file_name="file.session")
            ]
            await self._client.send_file(
                chat_id,
                memory_file,
                force_document=True,
                attributes=attributes,
                reply_to=topic_id
            )
            return True
        except Exception as e:
            logger.error(f"[SESSION] Send file error: {e}")
            return False
        finally:
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except:
                    pass

    async def _test_session_data(self, call: InlineCall, session_data: dict, input_type: str, dc_id: int = None):
        await call.edit(self.strings["testing"])
        
        try:
            string_session = None
            
            if input_type == 'string':
                string_session = session_data
            elif input_type == 'file':
                string_session = self._build_string_session(
                    session_data['dc_id'], 
                    session_data['auth_key']
                )
            elif input_type == 'hex':
                if dc_id is None:
                    await call.edit(self.strings["err_invalid"])
                    return
                auth_key = self._hex_to_auth_key(session_data)
                string_session = self._build_string_session(dc_id, auth_key)
            
            if not string_session:
                await call.edit(self.strings["err_invalid"])
                return
            
            parsed = self._parse_string_session(string_session)
            test_client = None
            
            try:
                test_client = TelegramClient(
                    StringSession(string_session),
                    int(self.config["API_ID"]),
                    self.config["API_HASH"],
                    device_model="SessionTest",
                    system_version="By @I_execute",
                    app_version=f"v{'.'.join(map(str, __version__))}",
                )
                await asyncio.wait_for(test_client.connect(), timeout=15)
                me = await asyncio.wait_for(test_client.get_me(), timeout=10)
                
                if me is None:
                    if parsed:
                        result = self.strings["test_info_only"].format(
                            line=self.strings["line"],
                            dc_id=parsed['dc_id'],
                            ip=parsed['ip'],
                            port=parsed['port'],
                            key_len=len(parsed['auth_key']),
                        )
                    else:
                        result = self.strings["test_fail"].format(
                            line=self.strings["line"],
                            reason=self.strings["not_authorized"],
                        )
                    await call.edit(result)
                    return
                
                username = None
                if hasattr(me, 'usernames') and me.usernames:
                    username = me.usernames[0].username
                elif hasattr(me, 'username') and me.username:
                    username = me.username
                
                first_name = getattr(me, 'first_name', '') or ''
                last_name = getattr(me, 'last_name', '') or ''
                full_name = f"{first_name} {last_name}".strip() or "Unknown"
                
                if username:
                    user_link = f"<a href='tg://resolve?domain={username}'>{full_name}</a>"
                else:
                    user_link = f"<a href='tg://user?id={me.id}'>{full_name}</a>"
                
                dc_id_display = parsed['dc_id'] if parsed else "?"
                ip = parsed['ip'] if parsed else "?"
                port = parsed['port'] if parsed else "?"
                key_len = len(parsed['auth_key']) if parsed else "?"
                
                result = self.strings["test_success"].format(
                    line=self.strings["line"],
                    user_link=user_link,
                    user_id=me.id,
                    dc_id=dc_id_display,
                    ip=ip,
                    port=port,
                    key_len=key_len,
                    premium="Yes" if getattr(me, 'premium', False) else "No"
                )
                await call.edit(result)
                
            except asyncio.TimeoutError:
                result = self.strings["test_fail"].format(
                    line=self.strings["line"],
                    reason="Connection timeout"
                )
                await call.edit(result)
            except AuthKeyUnregisteredError:
                if parsed:
                    result = self.strings["test_info_only"].format(
                        line=self.strings["line"],
                        dc_id=parsed['dc_id'],
                        ip=parsed['ip'],
                        port=parsed['port'],
                        key_len=len(parsed['auth_key']),
                    )
                    result += "\n" + self.strings["test_fail"].format(
                        line=self.strings["line"],
                        reason="Session Revoked"
                    )
                else:
                    result = self.strings["test_fail"].format(
                        line=self.strings["line"],
                        reason="Session Revoked"
                    )
                await call.edit(result)
            except UserDeactivatedBanError:
                result = self.strings["test_fail"].format(
                    line=self.strings["line"],
                    reason="Account Banned"
                )
                await call.edit(result)
            except Exception as e:
                result = self.strings["test_fail"].format(
                    line=self.strings["line"],
                    reason=str(e)
                )
                await call.edit(result)
            finally:
                _safe_disconnect(test_client)
                
        except Exception as e:
            await call.edit(f"<b>Error:</b>\n<blockquote>{str(e)[:200]}</blockquote>")

    async def _cb_test_session(self, call: InlineCall, session_id: str):
        session = self._sessions.get(session_id)
        if not session:
            return
        
        await self._test_session_data(
            call,
            session['input_data'],
            session['input_type']
        )

    async def _cb_test_hex_select_dc(self, call: InlineCall, session_id: str):
        await call.edit(
            self.strings["select_dc_for_test"],
            reply_markup=[
                [
                    {"text": self.strings["btn_dc_1"], "callback": self._cb_test_hex_with_dc, "args": (session_id, 1), "style": "primary"},
                    {"text": self.strings["btn_dc_2"], "callback": self._cb_test_hex_with_dc, "args": (session_id, 2), "style": "primary"},
                    {"text": self.strings["btn_dc_3"], "callback": self._cb_test_hex_with_dc, "args": (session_id, 3), "style": "primary"},
                ],
                [
                    {"text": self.strings["btn_dc_4"], "callback": self._cb_test_hex_with_dc, "args": (session_id, 4), "style": "primary"},
                    {"text": self.strings["btn_dc_5"], "callback": self._cb_test_hex_with_dc, "args": (session_id, 5), "style": "primary"},
                ]
            ]
        )

    async def _cb_test_hex_with_dc(self, call: InlineCall, session_id: str, dc_id: int):
        session = self._sessions.get(session_id)
        if not session:
            return
        
        await self._test_session_data(
            call,
            session['input_data'],
            session['input_type'],
            dc_id=dc_id
        )

    async def _cb_convert_hex(self, call: InlineCall, session_id: str):
        await call.edit(
            self.strings["select_dc_hex"],
            reply_markup=[
                [
                    {"text": self.strings["btn_dc_1"], "callback": self._cb_dc_select_then_format, "args": (session_id, 1), "style": "primary"},
                    {"text": self.strings["btn_dc_2"], "callback": self._cb_dc_select_then_format, "args": (session_id, 2), "style": "primary"},
                    {"text": self.strings["btn_dc_3"], "callback": self._cb_dc_select_then_format, "args": (session_id, 3), "style": "primary"},
                ],
                [
                    {"text": self.strings["btn_dc_4"], "callback": self._cb_dc_select_then_format, "args": (session_id, 4), "style": "primary"},
                    {"text": self.strings["btn_dc_5"], "callback": self._cb_dc_select_then_format, "args": (session_id, 5), "style": "primary"},
                ]
            ]
        )

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    async def _cb_kill(self, call: InlineCall, session_id: str):
        session = self._sessions.get(session_id)
        if session:
            client = session.get('client')
            if client:
                _safe_disconnect(client)
            del self._sessions[session_id]
        
        await call.edit(
            self.strings["process_killed"],
            reply_markup=[]
        )

    async def _cb_create_string(self, call: InlineCall):
        session_id = utils.rand(16)
        self._sessions[session_id] = {
            'mode': 'create',
            'output_type': 'string',
            'chat_id': call.form["chat"],
            'topic_id': None,
            'step': 'phone'
        }
        
        await call.edit(
            self.strings["creating"].format(
                output_type="String",
                status=self.strings["status_waiting_phone"]
            ),
            reply_markup=[
                [
                    {
                        "text": self.strings["input_phone"],
                        "input": self.strings["input_phone"],
                        "handler": self._input_phone,
                        "args": (session_id,),
                        "style": "primary",
                    }
                ],
                [
                    {"text": self.strings["btn_kill"], "callback": self._cb_kill, "args": (session_id,), "style": "danger"}
                ]
            ]
        )

    async def _cb_create_file(self, call: InlineCall):
        session_id = utils.rand(16)
        self._sessions[session_id] = {
            'mode': 'create',
            'output_type': 'file',
            'chat_id': call.form["chat"],
            'topic_id': None,
            'step': 'phone'
        }
        
        await call.edit(
            self.strings["creating"].format(
                output_type="File",
                status=self.strings["status_waiting_phone"]
            ),
            reply_markup=[
                [
                    {
                        "text": self.strings["input_phone"],
                        "input": self.strings["input_phone"],
                        "handler": self._input_phone,
                        "args": (session_id,),
                        "style": "primary",
                    }
                ],
                [
                    {"text": self.strings["btn_kill"], "callback": self._cb_kill, "args": (session_id,), "style": "danger"}
                ]
            ]
        )

    async def _cb_create_hex(self, call: InlineCall):
        session_id = utils.rand(16)
        self._sessions[session_id] = {
            'mode': 'create',
            'output_type': 'hex',
            'chat_id': call.form["chat"],
            'topic_id': None,
            'step': 'phone'
        }
        
        await call.edit(
            self.strings["creating"].format(
                output_type="HEX",
                status=self.strings["status_waiting_phone"]
            ),
            reply_markup=[
                [
                    {
                        "text": self.strings["input_phone"],
                        "input": self.strings["input_phone"],
                        "handler": self._input_phone,
                        "args": (session_id,),
                        "style": "primary",
                    }
                ],
                [
                    {"text": self.strings["btn_kill"], "callback": self._cb_kill, "args": (session_id,), "style": "danger"}
                ]
            ]
        )

    async def _input_phone(self, call: InlineCall, phone: str, session_id: str):
        session = self._sessions.get(session_id)
        if not session:
            return
        
        try:
            client = TelegramClient(
                StringSession(),
                int(self.config["API_ID"]),
                self.config["API_HASH"],
                device_model="SessionManager",
                system_version="By @I_execute",
                app_version=f"v{'.'.join(map(str, __version__))}",
            )
            await client.connect()
            res = await client.send_code_request(phone)
            
            session['client'] = client
            session['phone'] = phone
            session['hash'] = res.phone_code_hash
            session['step'] = 'code'
            
            await call.edit(
                self.strings["creating"].format(
                    output_type=session['output_type'].upper(),
                    status=self.strings["status_waiting_code"]
                ),
                reply_markup=[
                    [
                        {
                            "text": self.strings["input_code"],
                            "input": self.strings["input_code"],
                            "handler": self._input_code,
                            "args": (session_id,),
                            "style": "primary",
                        }
                    ],
                    [
                        {"text": self.strings["btn_kill"], "callback": self._cb_kill, "args": (session_id,), "style": "danger"}
                    ]
                ]
            )
        except Exception as e:
            await call.edit(f"<b>Error:</b> {str(e)}")

    async def _input_code(self, call: InlineCall, code: str, session_id: str):
        session = self._sessions.get(session_id)
        if not session:
            return
        
        try:
            client = session['client']
            await client.sign_in(
                session['phone'], code, phone_code_hash=session['hash']
            )
            await self._finalize_create(call, session_id)
        except SessionPasswordNeededError:
            session['step'] = 'password'
            await call.edit(
                self.strings["creating"].format(
                    output_type=session['output_type'].upper(),
                    status=self.strings["status_waiting_password"]
                ),
                reply_markup=[
                    [
                        {
                            "text": self.strings["input_password"],
                            "input": self.strings["input_password"],
                            "handler": self._input_password,
                            "args": (session_id,),
                            "style": "primary",
                        }
                    ],
                    [
                        {"text": self.strings["btn_kill"], "callback": self._cb_kill, "args": (session_id,), "style": "danger"}
                    ]
                ]
            )
        except Exception as e:
            await call.edit(f"<b>Error:</b> {str(e)}")

    async def _input_password(self, call: InlineCall, password: str, session_id: str):
        session = self._sessions.get(session_id)
        if not session:
            return
        
        try:
            client = session['client']
            await client.sign_in(password=password)
            await self._finalize_create(call, session_id)
        except Exception as e:
            await call.edit(f"<b>Error:</b> {str(e)}")

    async def _finalize_create(self, call: InlineCall, session_id: str):
        session = self._sessions.get(session_id)
        if not session:
            return
        
        try:
            client = session['client']
            string_session = client.session.save()
            parsed = self._parse_string_session(string_session)
            
            if session['output_type'] == 'string':
                await call.edit(
                    self.strings["result_string"].format(string=string_session)
                )
            elif session['output_type'] == 'hex':
                if parsed:
                    hex_key = self._auth_key_to_hex(parsed['auth_key'])
                    await call.edit(
                        self.strings["result_hex"].format(
                            dc_id=parsed['dc_id'],
                            hex_key=hex_key
                        )
                    )
                else:
                    await call.edit(self.strings["err_invalid"])
            elif session['output_type'] == 'file':
                if parsed:
                    await self._send_session_file(
                        session['chat_id'],
                        parsed['dc_id'],
                        parsed['auth_key'],
                        session.get('topic_id')
                    )
                    await call.edit(self.strings["result_file"])
                else:
                    await call.edit(self.strings["err_invalid"])
            
            _safe_disconnect(client)
            del self._sessions[session_id]
        except Exception as e:
            logger.error(f"[SESSION] Finalize error: {e}")
            await call.edit(f"<b>Error:</b> {str(e)}")

    async def _cb_dc_select_then_format(self, call: InlineCall, session_id: str, dc_id: int):
        session = self._sessions.get(session_id)
        if not session:
            return
        
        session['dc_id'] = dc_id
        
        await call.edit(
            self.strings["select_format"],
            reply_markup=[
                [
                    {"text": self.strings["btn_string"], "callback": self._cb_convert_string_from_hex, "args": (session_id,), "style": "primary"},
                    {"text": self.strings["btn_file"], "callback": self._cb_convert_file_from_hex, "args": (session_id,), "style": "primary"},
                ]
            ]
        )

    async def _cb_convert_string_from_hex(self, call: InlineCall, session_id: str):
        session = self._sessions.get(session_id)
        if not session:
            return
        
        hex_key = session['input_data']
        dc_id = session['dc_id']
        auth_key = self._hex_to_auth_key(hex_key)
        
        string_session = self._build_string_session(dc_id, auth_key)
        if string_session:
            await call.edit(
                self.strings["converted_string"].format(string=string_session)
            )
        else:
            await call.edit(self.strings["err_invalid"])
        
        del self._sessions[session_id]

    async def _cb_convert_file_from_hex(self, call: InlineCall, session_id: str):
        session = self._sessions.get(session_id)
        if not session:
            return
        
        hex_key = session['input_data']
        dc_id = session['dc_id']
        auth_key = self._hex_to_auth_key(hex_key)
        
        await self._send_session_file(
            session['chat_id'],
            dc_id,
            auth_key,
            session.get('topic_id')
        )
        await call.edit(self.strings["converted_file"])
        
        del self._sessions[session_id]

    async def _cb_convert_string(self, call: InlineCall, session_id: str):
        session = self._sessions.get(session_id)
        if not session:
            return
        
        input_type = session['input_type']
        input_data = session['input_data']
        
        if input_type == 'string':
            parsed = self._parse_string_session(input_data)
            if parsed:
                string_session = self._build_string_session(parsed['dc_id'], parsed['auth_key'])
                await call.edit(
                    self.strings["converted_string"].format(string=string_session)
                )
            else:
                await call.edit(self.strings["err_invalid"])
            del self._sessions[session_id]
        elif input_type == 'file':
            if input_data:
                string_session = self._build_string_session(input_data['dc_id'], input_data['auth_key'])
                await call.edit(
                    self.strings["converted_string"].format(string=string_session)
                )
            else:
                await call.edit(self.strings["err_invalid"])
            del self._sessions[session_id]

    async def _cb_convert_hex_from_other(self, call: InlineCall, session_id: str):
        session = self._sessions.get(session_id)
        if not session:
            return
        
        input_type = session['input_type']
        input_data = session['input_data']
        
        if input_type == 'string':
            parsed = self._parse_string_session(input_data)
            if parsed:
                hex_key = self._auth_key_to_hex(parsed['auth_key'])
                await call.edit(
                    self.strings["converted_hex"].format(
                        dc_id=parsed['dc_id'],
                        hex_key=hex_key
                    )
                )
            else:
                await call.edit(self.strings["err_invalid"])
            del self._sessions[session_id]
        elif input_type == 'file':
            if input_data:
                hex_key = self._auth_key_to_hex(input_data['auth_key'])
                await call.edit(
                    self.strings["converted_hex"].format(
                        dc_id=input_data['dc_id'],
                        hex_key=hex_key
                    )
                )
            else:
                await call.edit(self.strings["err_invalid"])
            del self._sessions[session_id]

    async def _cb_convert_file(self, call: InlineCall, session_id: str):
        session = self._sessions.get(session_id)
        if not session:
            return
        
        input_type = session['input_type']
        input_data = session['input_data']
        
        if input_type == 'string':
            parsed = self._parse_string_session(input_data)
            if parsed:
                await self._send_session_file(
                    session['chat_id'],
                    parsed['dc_id'],
                    parsed['auth_key'],
                    session.get('topic_id')
                )
                await call.edit(self.strings["converted_file"])
            else:
                await call.edit(self.strings["err_invalid"])
            del self._sessions[session_id]
        elif input_type == 'file':
            if input_data:
                await self._send_session_file(
                    session['chat_id'],
                    input_data['dc_id'],
                    input_data['auth_key'],
                    session.get('topic_id')
                )
                await call.edit(self.strings["converted_file"])
            else:
                await call.edit(self.strings["err_invalid"])
            del self._sessions[session_id]

    @loader.command(
        ru_doc="Менеджер сессий",
        en_doc="Session manager",
    )
    async def ses(self, message: Message):
        """Session manager"""
        reply = await message.get_reply_message()
        
        if reply:
            input_data = None
            input_type = None
            
            if reply.file and reply.file.name and reply.file.name.endswith('.session'):
                file_path = os.path.join(self._temp_dir, "temp.session")
                try:
                    await reply.download_media(file_path)
                    data = await self._read_session_file(file_path)
                    if data:
                        input_data = data
                        input_type = 'file'
                except:
                    pass
                finally:
                    if os.path.exists(file_path):
                        os.remove(file_path)
            
            if not input_data:
                text = reply.text or ""
                
                hex_key = self._find_hex_key(text)
                if hex_key:
                    input_data = hex_key
                    input_type = 'hex'
                else:
                    string_session = self._find_string_session(text)
                    if string_session:
                        input_data = string_session
                        input_type = 'string'
            
            if input_data and input_type:
                session_id = utils.rand(16)
                self._sessions[session_id] = {
                    'mode': 'convert',
                    'input_type': input_type,
                    'input_data': input_data,
                    'chat_id': message.chat_id,
                    'topic_id': None
                }
                
                buttons = []
                
                if input_type == 'hex':
                    buttons.append([
                        {"text": self.strings["btn_test"], "callback": self._cb_test_hex_select_dc, "args": (session_id,), "style": "primary"}
                    ])
                    buttons.append([
                        {"text": self.strings["btn_convert"], "callback": self._cb_convert_hex, "args": (session_id,), "style": "primary"}
                    ])
                else:
                    buttons.append([
                        {"text": self.strings["btn_test"], "callback": self._cb_test_session, "args": (session_id,), "style": "primary"}
                    ])
                    
                    if input_type == 'string':
                        buttons.append([
                            {"text": self.strings["btn_hex"], "callback": self._cb_convert_hex_from_other, "args": (session_id,), "style": "primary"},
                            {"text": self.strings["btn_file"], "callback": self._cb_convert_file, "args": (session_id,), "style": "primary"},
                        ])
                    elif input_type == 'file':
                        buttons.append([
                            {"text": self.strings["btn_string"], "callback": self._cb_convert_string, "args": (session_id,), "style": "primary"},
                            {"text": self.strings["btn_hex"], "callback": self._cb_convert_hex_from_other, "args": (session_id,), "style": "primary"},
                        ])
                
                await self.inline.form(
                    text=self.strings["main_menu_convert_format" if input_type != 'hex' else "main_menu_convert_hex"].format(
                        input_type=input_type.upper()
                    ),
                    message=message,
                    reply_markup=buttons,
                    silent=True
                )
                return
        
        await self.inline.form(
            text=self.strings["main_menu_create"],
            message=message,
            reply_markup=[
                [
                    {"text": self.strings["btn_string"], "callback": self._cb_create_string, "style": "primary"},
                ],
                [
                    {"text": self.strings["btn_file"], "callback": self._cb_create_file, "style": "primary"},
                ],
                [
                    {"text": self.strings["btn_hex"], "callback": self._cb_create_hex, "style": "primary"},
                ],
                [
                    {"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}
                ]
            ],
            silent=True
        )