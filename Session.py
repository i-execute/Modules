__version__ = (2, 2, 0)
# meta developer: I_execute.t.me

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
        "usage": (
            "<b>Session Manager</b>\n"
            "<b>Create:</b>\n"
            "<blockquote><code>{prefix}ss create string</code>\n"
            "<code>{prefix}ss create file</code>\n"
            "<code>{prefix}ss create hex</code>\n"
            "<code>{prefix}ss phone/code/password</code></blockquote>\n"
            "<b>Convert:</b>\n"
            "<blockquote><code>{prefix}ss convert hex_to_string</code>\n"
            "<code>{prefix}ss convert hex_to_file</code>\n"
            "<code>{prefix}ss convert string_to_hex</code>\n"
            "<code>{prefix}ss convert string_to_file</code>\n"
            "<code>{prefix}ss convert file_to_string</code>\n"
            "<code>{prefix}ss convert file_to_hex</code>\n"
            "<code>{prefix}ss convert dc [id]</code></blockquote>\n"
            "<b>Test:</b>\n"
            "<blockquote><code>{prefix}ss test</code>\n"
            "<code>{prefix}ss test dc [id]</code>\n\n"
            "<code>{prefix}ss terminate</code></blockquote>"
        ),
        "create_status": (
            "<b>{status_text}</b>\n"
            "{line}\n"
            "<blockquote>"
            "Output: {output_type}\n"
            "API ID: <code>{api_id_st}</code>\n"
            "API HASH: <code>{api_hash_st}</code>\n"
            "Phone: {phone_st}\n"
            "Code: {code_st}\n"
            "Password: {pass_st}"
            "</blockquote>\n"
            "<b>Result:</b>\n"
            "<blockquote>{result_st}</blockquote>\n"
            "{line}\n"
            "Execution time: {exec_time} sec"
        ),
        "convert_status": (
            "<b>{status_text}</b>\n"
            "{line}\n"
            "<blockquote>"
            "Mode: {mode}\n"
            "Input: {input_st}\n"
            "DC ID: {dc_st}"
            "</blockquote>\n"
            "<b>Result:</b>\n"
            "<blockquote>{result_st}</blockquote>\n"
            "{line}\n"
            "Execution time: {exec_time} sec"
        ),
        "test_status": (
            "<b>{status_text}</b>\n"
            "{line}\n"
            "<blockquote>"
            "Input: {input_st}\n"
            "DC ID: {dc_st}\n"
            "Connection: {conn_st}"
            "</blockquote>\n"
            "{line}\n"
            "Execution time: {exec_time} sec"
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
        "caption_create_file": "<b>New Session → File</b>",
        "caption_create_string": "<b>New Session → File</b>",
        "caption_create_hex": "<b>New Session → File</b>",
        "caption_string_to_file": "<b>String → File</b>",
        "caption_hex_to_file": "<b>HEX → File</b>",
        "caption_generic": "<b>Session File</b>",
        "err_running": "<b>Error:</b> Process running. Use <code>{prefix}ss terminate</code>",
        "err_no_process": "<b>Error:</b> No active process.",
        "err_wrong_step": "<b>Error:</b> Wrong step. Current: <b>{step}</b>",
        "err_no_file": "<b>Error:</b> Reply to .session file.",
        "err_invalid_file": "<b>Error:</b> Invalid .session file.",
        "err_no_hex": "<b>Error:</b> No valid HEX (512 chars).",
        "err_no_string": "<b>Error:</b> No valid StringSession.",
        "err_invalid_dc": "<b>Error:</b> DC ID must be 1-5.",
        "err_file_create": "<b>Error:</b> Failed to create/send file.",
        "terminated": "<b>Process terminated.</b>",
        "success": "Successfully completed",
        "creating": "Creating new session...",
        "converting": "Converting session...",
        "checking": "Checking session...",
        "wait": "wait",
        "now_waiting": "now waiting...",
        "done": "Done",
        "ok": "OK",
        "provide_phone": "<b>Provide phone number.</b>",
        "provide_code": "<b>Provide code.</b>",
        "provide_password": "<b>Provide password.</b>",
        "not_authorized": "Session not authorized",
    }

    strings_ru = {
        "line": "--------------------",
        "usage": (
            "<b>Session Manager</b>\n"
            "<b>Создание:</b>\n"
            "<blockquote><code>{prefix}ss create string</code>\n"
            "<code>{prefix}ss create file</code>\n"
            "<code>{prefix}ss create hex</code>\n"
            "<code>{prefix}ss phone/code/password</code></blockquote>\n"
            "<b>Конвертация:</b>\n"
            "<blockquote><code>{prefix}ss convert hex_to_string</code>\n"
            "<code>{prefix}ss convert hex_to_file</code>\n"
            "<code>{prefix}ss convert string_to_hex</code>\n"
            "<code>{prefix}ss convert string_to_file</code>\n"
            "<code>{prefix}ss convert file_to_string</code>\n"
            "<code>{prefix}ss convert file_to_hex</code>\n"
            "<code>{prefix}ss convert dc [id]</code></blockquote>\n"
            "<b>Тест:</b>\n"
            "<blockquote><code>{prefix}ss test</code>\n"
            "<code>{prefix}ss test dc [id]</code>\n\n"
            "<code>{prefix}ss terminate</code></blockquote>"
        ),
        "create_status": (
            "<b>{status_text}</b>\n"
            "{line}\n"
            "<blockquote>"
            "Формат: {output_type}\n"
            "API ID: <code>{api_id_st}</code>\n"
            "API HASH: <code>{api_hash_st}</code>\n"
            "Телефон: {phone_st}\n"
            "Код: {code_st}\n"
            "Пароль: {pass_st}"
            "</blockquote>\n"
            "<b>Результат:</b>\n"
            "<blockquote>{result_st}</blockquote>\n"
            "{line}\n"
            "Время выполнения: {exec_time} сек"
        ),
        "convert_status": (
            "<b>{status_text}</b>\n"
            "{line}\n"
            "<blockquote>"
            "Режим: {mode}\n"
            "Ввод: {input_st}\n"
            "DC ID: {dc_st}"
            "</blockquote>\n"
            "<b>Результат:</b>\n"
            "<blockquote>{result_st}</blockquote>\n"
            "{line}\n"
            "Время выполнения: {exec_time} сек"
        ),
        "test_status": (
            "<b>{status_text}</b>\n"
            "{line}\n"
            "<blockquote>"
            "Ввод: {input_st}\n"
            "DC ID: {dc_st}\n"
            "Соединение: {conn_st}"
            "</blockquote>\n"
            "{line}\n"
            "Время выполнения: {exec_time} сек"
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
        "caption_create_file": "<b>Новая сессия → Файл</b>",
        "caption_create_string": "<b>Новая сессия → Файл</b>",
        "caption_create_hex": "<b>Новая сессия → Файл</b>",
        "caption_string_to_file": "<b>String → Файл</b>",
        "caption_hex_to_file": "<b>HEX → Файл</b>",
        "caption_generic": "<b>Файл сессии</b>",
        "err_running": "<b>Ошибка:</b> Процесс запущен. Используй <code>{prefix}ss terminate</code>",
        "err_no_process": "<b>Ошибка:</b> Нет активного процесса.",
        "err_wrong_step": "<b>Ошибка:</b> Неверный шаг. Текущий: <b>{step}</b>",
        "err_no_file": "<b>Ошибка:</b> Ответь на .session файл.",
        "err_invalid_file": "<b>Ошибка:</b> Невалидный .session файл.",
        "err_no_hex": "<b>Ошибка:</b> Нет валидного HEX (512 символов).",
        "err_no_string": "<b>Ошибка:</b> Нет валидной StringSession.",
        "err_invalid_dc": "<b>Ошибка:</b> DC ID должен быть 1-5.",
        "err_file_create": "<b>Ошибка:</b> Не удалось создать/отправить файл.",
        "terminated": "<b>Процесс завершён.</b>",
        "success": "Успешно завершено",
        "creating": "Создание новой сессии...",
        "converting": "Конвертация сессии...",
        "checking": "Проверка сессии...",
        "wait": "ожидание",
        "now_waiting": "ожидание ввода...",
        "done": "Готово",
        "ok": "OK",
        "provide_phone": "<b>Укажите номер телефона.</b>",
        "provide_code": "<b>Укажите код.</b>",
        "provide_password": "<b>Укажите пароль.</b>",
        "not_authorized": "Сессия не авторизована",
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
        self._active = False
        self._session_client = None
        self._status_msg = None
        self._chat_id = None
        self._topic_id = None
        self._origin_message = None
        self._start_time = 0
        self._step = "none"
        self._mode = None
        self._output_type = None
        self._data = {}
        self._loop_task = None
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

    def _get_device_model(self, output_type=None):
        """Возвращает device_model в зависимости от типа сессии"""
        if output_type == "string":
            return "StringSession"
        elif output_type == "file":
            return "FileSession"
        elif output_type == "hex":
            return "HEXSession"
        return "SessionManager"

    def _get_api_id_display(self):
        """Возвращает реальный API ID"""
        return str(self.config["API_ID"])

    def _get_api_hash_display(self):
        """Возвращает первые 3 символа API HASH + ..."""
        api_hash = str(self.config["API_HASH"])
        if len(api_hash) >= 3:
            return f"{api_hash[:3]}..."
        return api_hash

    def _get_topic_id(self, message: Message):
        reply_to = getattr(message, 'reply_to', None)
        if reply_to:
            return getattr(reply_to, 'reply_to_top_id', None) or getattr(reply_to, 'reply_to_msg_id', None)
        return None

    def _get_exec_time(self):
        return round(time.perf_counter() - self._start_time, 2)

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

    async def _send_session_file(self, chat_id, dc_id, auth_key, caption=None, topic_id=None):
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
                caption=caption or self.strings["caption_generic"],
                force_document=True,
                attributes=attributes,
                parse_mode="html",
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

    async def _update_loop(self):
        while self._active:
            await self._update_status()
            await asyncio.sleep(ACTION_DELAY)

    async def _update_status(self):
        if not self._status_msg:
            return
        try:
            if self._mode == "create":
                await self._update_create_status()
            elif self._mode == "convert":
                await self._update_convert_status()
            elif self._mode == "test":
                await self._update_test_status()
        except:
            pass

    async def _update_create_status(self):
        status_text = self.strings["success"] if self._step == "done" else self.strings["creating"]
        phone_st = self.strings["done"] if self._step in ["code", "password", "done"] else (
            self.strings["now_waiting"] if self._step == "phone" else self.strings["wait"])
        code_st = self.strings["done"] if self._step in ["password", "done"] else (
            self.strings["now_waiting"] if self._step == "code" else self.strings["wait"])
        pass_st = self.strings["done"] if self._step == "done" else (
            self.strings["now_waiting"] if self._step == "password" else self.strings["wait"])
        result_st = self._data.get('result_display', self.strings["wait"])
        msg_text = self.strings["create_status"].format(
            status_text=status_text, line=self.strings["line"],
            output_type=self._output_type.upper() if self._output_type else "STRING",
            api_id_st=self._get_api_id_display(),
            api_hash_st=self._get_api_hash_display(),
            phone_st=phone_st, code_st=code_st, pass_st=pass_st,
            result_st=result_st, exec_time=self._get_exec_time()
        )
        try:
            self._status_msg = await utils.answer(self._status_msg, msg_text)
        except:
            pass

    async def _update_convert_status(self):
        status_text = self.strings["success"] if self._step == "done" else self.strings["converting"]
        input_st = self.strings["done"] if self._data.get('input_ready') else self.strings["now_waiting"]
        dc_st = (f"<b>{self._data.get('dc_id')}</b>" if self._data.get('dc_id') else
            (self.strings["now_waiting"] if self._step == "dc" else self.strings["wait"]))
        result_st = self._data.get('result_display', self.strings["wait"])
        msg_text = self.strings["convert_status"].format(
            status_text=status_text, line=self.strings["line"],
            mode=self._data.get('convert_mode', 'unknown'),
            input_st=input_st, dc_st=dc_st, result_st=result_st,
            exec_time=self._get_exec_time()
        )
        try:
            self._status_msg = await utils.answer(self._status_msg, msg_text)
        except:
            pass

    async def _update_test_status(self):
        status_text = self.strings["success"] if self._step == "done" else self.strings["checking"]
        input_st = self.strings["done"] if self._data.get('input_ready') else self.strings["now_waiting"]
        dc_st = (f"<b>{self._data.get('dc_id')}</b>" if self._data.get('dc_id') else
            (self.strings["now_waiting"] if self._step == "dc" else self.strings["wait"]))
        conn_st = self._data.get('conn_status', self.strings["wait"])
        msg_text = self.strings["test_status"].format(
            status_text=status_text, line=self.strings["line"],
            input_st=input_st, dc_st=dc_st, conn_st=conn_st,
            exec_time=self._get_exec_time()
        )
        try:
            self._status_msg = await utils.answer(self._status_msg, msg_text)
        except:
            pass

    @loader.command(
        ru_doc="Менеджер сессий",
        en_doc="Session manager",
    )
    async def ss(self, message: Message):
        """Session manager"""
        args = utils.get_args_raw(message).split()
        prefix = self.get_prefix()
        if not args:
            await utils.answer(message, self.strings["usage"].format(prefix=prefix))
            return
        cmd = args[0].lower()
        if cmd == "create":
            await self._handle_create(message, args)
        elif cmd == "phone":
            await self._handle_phone(message, args)
        elif cmd == "code":
            await self._handle_code(message, args)
        elif cmd == "password":
            await self._handle_password(message, args)
        elif cmd == "convert":
            await self._handle_convert(message, args)
        elif cmd == "test":
            await self._handle_test(message, args)
        elif cmd == "terminate":
            await self._cleanup()
            await utils.answer(message, self.strings["terminated"])
        else:
            await utils.answer(message, self.strings["usage"].format(prefix=prefix))

    async def _handle_create(self, message: Message, args):
        if self._active:
            prefix = self.get_prefix()
            return await utils.answer(message, self.strings["err_running"].format(prefix=prefix))
        output_type = args[1].lower() if len(args) > 1 else "string"
        if output_type not in ["string", "file", "hex"]:
            output_type = "string"
        self._active = True
        self._start_time = time.perf_counter()
        self._step = "phone"
        self._mode = "create"
        self._output_type = output_type
        self._data = {}
        self._chat_id = message.chat_id
        self._topic_id = self._get_topic_id(message)
        self._origin_message = message
        try:
            device_model = self._get_device_model(output_type)
            self._session_client = TelegramClient(
                StringSession(),
                int(self.config["API_ID"]),
                self.config["API_HASH"],
                device_model=device_model,
                system_version="By @FireJester",
                app_version=f"v{'.'.join(map(str, __version__))}",
            )
            await self._session_client.connect()
        except Exception as e:
            self._active = False
            return await utils.answer(message, f"<b>Error:</b> {str(e)}")
        self._status_msg = await utils.answer(message, self.strings["creating"])
        self._loop_task = asyncio.create_task(self._update_loop())

    async def _handle_phone(self, message: Message, args):
        if not self._active:
            return await utils.answer(message, self.strings["err_no_process"])
        if self._step != "phone":
            return await utils.answer(message, self.strings["err_wrong_step"].format(step=self._step))
        phone = args[1] if len(args) > 1 else None
        if not phone:
            return await utils.answer(message, self.strings["provide_phone"])
        try:
            res = await self._session_client.send_code_request(phone)
            self._data['phone'] = phone
            self._data['hash'] = res.phone_code_hash
            self._step = "code"
            await message.delete()
        except Exception as e:
            await utils.answer(message, f"<b>Error:</b> {str(e)}")

    async def _handle_code(self, message: Message, args):
        if not self._active:
            return await utils.answer(message, self.strings["err_no_process"])
        if self._step != "code":
            return await utils.answer(message, self.strings["err_wrong_step"].format(step=self._step))
        code = args[1] if len(args) > 1 else None
        if not code:
            return await utils.answer(message, self.strings["provide_code"])
        try:
            await self._session_client.sign_in(
                self._data['phone'], code, phone_code_hash=self._data['hash']
            )
            await self._finalize_create()
            await message.delete()
        except SessionPasswordNeededError:
            self._step = "password"
            await message.delete()
        except Exception as e:
            await utils.answer(message, f"<b>Error:</b> {str(e)}")

    async def _handle_password(self, message: Message, args):
        if not self._active:
            return await utils.answer(message, self.strings["err_no_process"])
        if self._step != "password":
            return await utils.answer(message, self.strings["err_wrong_step"].format(step=self._step))
        pwd = args[1] if len(args) > 1 else None
        if not pwd:
            return await utils.answer(message, self.strings["provide_password"])
        try:
            await self._session_client.sign_in(password=pwd)
            await self._finalize_create()
            await message.delete()
        except Exception as e:
            await utils.answer(message, f"<b>Error:</b> {str(e)}")

    def _get_create_file_caption(self):
        """Возвращает правильную подпись для файла в зависимости от output_type при создании"""
        if self._output_type == "file":
            return self.strings["caption_create_file"]
        elif self._output_type == "string":
            return self.strings["caption_create_string"]
        elif self._output_type == "hex":
            return self.strings["caption_create_hex"]
        return self.strings["caption_generic"]

    async def _finalize_create(self):
        try:
            string_session = self._session_client.session.save()
            parsed = self._parse_string_session(string_session)
            self._step = "done"
            if self._loop_task:
                self._loop_task.cancel()
                try:
                    await self._loop_task
                except asyncio.CancelledError:
                    pass
            self._active = False
            if self._output_type == "string":
                self._data['result_display'] = f"<code>{string_session}</code>"
                await self._update_status()
            elif self._output_type == "hex":
                if parsed:
                    hex_key = self._auth_key_to_hex(parsed['auth_key'])
                    self._data['result_display'] = f"DC: <code>{parsed['dc_id']}</code>\nHEX:\n<code>{hex_key}</code>"
                else:
                    self._data['result_display'] = "<b>Error parsing</b>"
                await self._update_status()
            elif self._output_type == "file":
                self._data['result_display'] = self.strings["done"]
                await self._update_status()
                if parsed:
                    caption = self._get_create_file_caption()
                    success = await self._send_session_file(
                        self._chat_id,
                        parsed['dc_id'],
                        parsed['auth_key'],
                        caption=caption,
                        topic_id=self._topic_id
                    )
                    if not success:
                        await self._client.send_message(
                            self._chat_id,
                            self.strings["err_file_create"],
                            parse_mode="html",
                            reply_to=self._topic_id
                        )
            _safe_disconnect(self._session_client)
            self._session_client = None
        except Exception as e:
            logger.error(f"[SESSION] Finalize error: {e}")
            self._active = False

    async def _handle_convert(self, message: Message, args):
        prefix = self.get_prefix()
        if len(args) < 2:
            return await utils.answer(message, self.strings["usage"].format(prefix=prefix))
        sub_cmd = args[1].lower()
        if sub_cmd == "dc":
            if not self._active or self._mode != "convert":
                return await utils.answer(message, self.strings["err_no_process"])
            try:
                dc_id = int(args[2])
                if dc_id not in self._DC_IP_MAP:
                    raise ValueError
                self._data['dc_id'] = dc_id
                await message.delete()
                await self._finalize_convert()
            except (ValueError, IndexError):
                await utils.answer(message, self.strings["err_invalid_dc"])
            return
        if self._active:
            return await utils.answer(message, self.strings["err_running"].format(prefix=prefix))
        reply = await message.get_reply_message()
        self._chat_id = message.chat_id
        self._topic_id = self._get_topic_id(message)
        self._origin_message = message
        if sub_cmd == "hex_to_string":
            await self._convert_hex_to_string(message, args, reply)
        elif sub_cmd == "hex_to_file":
            await self._convert_hex_to_file(message, args, reply)
        elif sub_cmd == "string_to_hex":
            await self._convert_string_to_hex(message, args, reply)
        elif sub_cmd == "string_to_file":
            await self._convert_string_to_file(message, args, reply)
        elif sub_cmd == "file_to_string":
            await self._convert_file_to_string(message, reply)
        elif sub_cmd == "file_to_hex":
            await self._convert_file_to_hex(message, reply)
        else:
            await utils.answer(message, self.strings["usage"].format(prefix=prefix))

    async def _convert_hex_to_string(self, message: Message, args, reply):
        hex_key = None
        if len(args) > 2:
            hex_key = self._find_hex_key(args[2])
        if not hex_key and reply:
            hex_key = self._find_hex_key(reply.text or "")
        if not hex_key:
            return await utils.answer(message, self.strings["err_no_hex"])
        self._active = True
        self._start_time = time.perf_counter()
        self._mode = "convert"
        self._step = "dc"
        self._data = {
            'convert_mode': 'HEX → String',
            'hex_key': hex_key,
            'input_ready': True,
            'target': 'string'
        }
        self._status_msg = await utils.answer(message, self.strings["converting"])
        self._loop_task = asyncio.create_task(self._update_loop())

    async def _convert_hex_to_file(self, message: Message, args, reply):
        hex_key = None
        if len(args) > 2:
            hex_key = self._find_hex_key(args[2])
        if not hex_key and reply:
            hex_key = self._find_hex_key(reply.text or "")
        if not hex_key:
            return await utils.answer(message, self.strings["err_no_hex"])
        self._active = True
        self._start_time = time.perf_counter()
        self._mode = "convert"
        self._step = "dc"
        self._data = {
            'convert_mode': 'HEX → File',
            'hex_key': hex_key,
            'input_ready': True,
            'target': 'file'
        }
        self._status_msg = await utils.answer(message, self.strings["converting"])
        self._loop_task = asyncio.create_task(self._update_loop())

    async def _convert_string_to_hex(self, message: Message, args, reply):
        string_session = None
        if len(args) > 2:
            string_session = self._find_string_session(" ".join(args[2:]))
        if not string_session and reply:
            string_session = self._find_string_session(reply.text or "")
        if not string_session:
            return await utils.answer(message, self.strings["err_no_string"])
        parsed = self._parse_string_session(string_session)
        if not parsed:
            return await utils.answer(message, self.strings["err_no_string"])
        hex_key = self._auth_key_to_hex(parsed['auth_key'])
        result = (
            f"<b>String → HEX</b>\n"
            f"{self.strings['line']}\n"
            f"<blockquote>"
            f"DC: <code>{parsed['dc_id']}</code>\n"
            f"HEX:\n<code>{hex_key}</code>"
            f"</blockquote>"
        )
        await utils.answer(message, result)

    async def _convert_string_to_file(self, message: Message, args, reply):
        string_session = None
        if len(args) > 2:
            string_session = self._find_string_session(" ".join(args[2:]))
        if not string_session and reply:
            string_session = self._find_string_session(reply.text or "")
        if not string_session:
            return await utils.answer(message, self.strings["err_no_string"])
        parsed = self._parse_string_session(string_session)
        if not parsed:
            return await utils.answer(message, self.strings["err_no_string"])
        topic_id = self._get_topic_id(message)
        await message.delete()
        success = await self._send_session_file(
            message.chat_id,
            parsed['dc_id'],
            parsed['auth_key'],
            caption=self.strings["caption_string_to_file"],
            topic_id=topic_id
        )
        if not success:
            await self._client.send_message(
                message.chat_id,
                self.strings["err_file_create"],
                parse_mode="html",
                reply_to=topic_id
            )

    async def _convert_file_to_string(self, message: Message, reply):
        if not reply or not reply.file:
            return await utils.answer(message, self.strings["err_no_file"])
        if not reply.file.name or not reply.file.name.endswith('.session'):
            return await utils.answer(message, self.strings["err_invalid_file"])
        file_path = os.path.join(self._temp_dir, "download.session")
        try:
            await reply.download_media(file_path)
            data = await self._read_session_file(file_path)
            if not data:
                return await utils.answer(message, self.strings["err_invalid_file"])
            string_session = self._build_string_session(data['dc_id'], data['auth_key'])
            if not string_session:
                return await utils.answer(message, self.strings["err_invalid_file"])
            result = (
                f"<b>File → String</b>\n"
                f"{self.strings['line']}\n"
                f"<blockquote><code>{string_session}</code></blockquote>"
            )
            await utils.answer(message, result)
        except Exception as e:
            await utils.answer(message, f"<b>Error:</b> {str(e)}")
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    async def _convert_file_to_hex(self, message: Message, reply):
        if not reply or not reply.file:
            return await utils.answer(message, self.strings["err_no_file"])
        if not reply.file.name or not reply.file.name.endswith('.session'):
            return await utils.answer(message, self.strings["err_invalid_file"])
        file_path = os.path.join(self._temp_dir, "download.session")
        try:
            await reply.download_media(file_path)
            data = await self._read_session_file(file_path)
            if not data:
                return await utils.answer(message, self.strings["err_invalid_file"])
            hex_key = self._auth_key_to_hex(data['auth_key'])
            result = (
                f"<b>File → HEX</b>\n"
                f"{self.strings['line']}\n"
                f"<blockquote>"
                f"DC: <code>{data['dc_id']}</code>\n"
                f"HEX:\n<code>{hex_key}</code>"
                f"</blockquote>"
            )
            await utils.answer(message, result)
        except Exception as e:
            await utils.answer(message, f"<b>Error:</b> {str(e)}")
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

    async def _finalize_convert(self):
        try:
            self._step = "done"
            if self._loop_task:
                self._loop_task.cancel()
                try:
                    await self._loop_task
                except asyncio.CancelledError:
                    pass
            self._active = False
            dc_id = self._data['dc_id']
            hex_key = self._data['hex_key']
            auth_key = self._hex_to_auth_key(hex_key)
            target = self._data.get('target', 'string')
            if target == 'string':
                string_session = self._build_string_session(dc_id, auth_key)
                if string_session:
                    self._data['result_display'] = f"<code>{string_session}</code>"
                else:
                    self._data['result_display'] = "<b>Error building session</b>"
                await self._update_status()
            elif target == 'file':
                self._data['result_display'] = self.strings["done"]
                await self._update_status()
                success = await self._send_session_file(
                    self._chat_id,
                    dc_id,
                    auth_key,
                    caption=self.strings["caption_hex_to_file"],
                    topic_id=self._topic_id
                )
                if not success:
                    await self._client.send_message(
                        self._chat_id,
                        self.strings["err_file_create"],
                        parse_mode="html",
                        reply_to=self._topic_id
                    )
        except:
            self._active = False

    async def _handle_test(self, message: Message, args):
        if len(args) > 1 and args[1].lower() == "dc":
            if not self._active or self._mode != "test":
                return await utils.answer(message, self.strings["err_no_process"])
            try:
                dc_id = int(args[2])
                if dc_id not in self._DC_IP_MAP:
                    raise ValueError
                self._data['dc_id'] = dc_id
                await message.delete()
                await self._finalize_test()
            except (ValueError, IndexError):
                await utils.answer(message, self.strings["err_invalid_dc"])
            return
        if self._active:
            prefix = self.get_prefix()
            return await utils.answer(message, self.strings["err_running"].format(prefix=prefix))
        reply = await message.get_reply_message()

        # Сначала проверяем файл в реплае
        if reply and reply.file:
            file_name = getattr(reply.file, 'name', None) or ""
            if file_name.endswith('.session'):
                file_path = os.path.join(self._temp_dir, "test.session")
                try:
                    await reply.download_media(file_path)
                    data = await self._read_session_file(file_path)
                    if data:
                        string_session = self._build_string_session(data['dc_id'], data['auth_key'])
                        if string_session:
                            await self._test_string_session(message, string_session)
                            return
                    await utils.answer(message, self.strings["err_invalid_file"])
                    return
                except Exception as e:
                    await utils.answer(message, f"<b>Error:</b> {str(e)}")
                    return
                finally:
                    if os.path.exists(file_path):
                        os.remove(file_path)

        # Затем проверяем текст
        input_text = " ".join(args[1:]) if len(args) > 1 else ""
        if not input_text and reply:
            input_text = reply.text or ""

        string_session = self._find_string_session(input_text)
        if string_session:
            await self._test_string_session(message, string_session)
            return

        hex_key = self._find_hex_key(input_text)
        if hex_key:
            self._active = True
            self._start_time = time.perf_counter()
            self._mode = "test"
            self._step = "dc"
            self._chat_id = message.chat_id
            self._topic_id = self._get_topic_id(message)
            self._data = {'hex_key': hex_key, 'input_ready': True, 'conn_status': self.strings["wait"]}
            self._status_msg = await utils.answer(message, self.strings["checking"])
            self._loop_task = asyncio.create_task(self._update_loop())
            return

        await utils.answer(message, self.strings["err_no_string"])

    async def _test_string_session(self, message: Message, string_session):
        parsed = self._parse_string_session(string_session)
        test_client = None
        try:
            test_client = TelegramClient(
                StringSession(string_session),
                int(self.config["API_ID"]),
                self.config["API_HASH"],
                device_model="SessionTest",
                system_version="By @FireJester",
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
                await utils.answer(message, result)
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
            dc_id = parsed['dc_id'] if parsed else "?"
            ip = parsed['ip'] if parsed else "?"
            port = parsed['port'] if parsed else "?"
            key_len = len(parsed['auth_key']) if parsed else "?"
            result = self.strings["test_success"].format(
                line=self.strings["line"], user_link=user_link, user_id=me.id,
                dc_id=dc_id, ip=ip, port=port, key_len=key_len,
                premium="Yes" if getattr(me, 'premium', False) else "No"
            )
            await utils.answer(message, result)
        except asyncio.TimeoutError:
            result = self.strings["test_fail"].format(
                line=self.strings["line"], reason="Connection timeout"
            )
            await utils.answer(message, result)
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
                    line=self.strings["line"], reason="Session Revoked"
                )
            else:
                result = self.strings["test_fail"].format(line=self.strings["line"], reason="Session Revoked")
            await utils.answer(message, result)
        except UserDeactivatedBanError:
            result = self.strings["test_fail"].format(line=self.strings["line"], reason="Account Banned")
            await utils.answer(message, result)
        except Exception as e:
            result = self.strings["test_fail"].format(line=self.strings["line"], reason=str(e))
            await utils.answer(message, result)
        finally:
            _safe_disconnect(test_client)

    async def _finalize_test(self):
        try:
            self._step = "done"
            if self._loop_task:
                self._loop_task.cancel()
                try:
                    await self._loop_task
                except asyncio.CancelledError:
                    pass
            self._active = False
            dc_id = self._data['dc_id']
            hex_key = self._data['hex_key']
            auth_key = self._hex_to_auth_key(hex_key)
            string_session = self._build_string_session(dc_id, auth_key)
            if not string_session:
                await utils.answer(self._status_msg, "<b>Error building session</b>")
                return
            parsed = self._parse_string_session(string_session)
            test_client = None
            try:
                test_client = TelegramClient(
                    StringSession(string_session),
                    int(self.config["API_ID"]),
                    self.config["API_HASH"],
                    device_model="SessionTest",
                    system_version="By @FireJester",
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
                    await utils.answer(self._status_msg, result)
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
                ip = parsed['ip'] if parsed else "?"
                port = parsed['port'] if parsed else "?"
                key_len = len(parsed['auth_key']) if parsed else "?"
                result = self.strings["test_success"].format(
                    line=self.strings["line"], user_link=user_link, user_id=me.id,
                    dc_id=dc_id, ip=ip, port=port, key_len=key_len,
                    premium="Yes" if getattr(me, 'premium', False) else "No"
                )
                await utils.answer(self._status_msg, result)
            except asyncio.TimeoutError:
                result = self.strings["test_fail"].format(
                    line=self.strings["line"], reason="Connection timeout"
                )
                await utils.answer(self._status_msg, result)
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
                        line=self.strings["line"], reason="Session Revoked"
                    )
                else:
                    result = self.strings["test_fail"].format(line=self.strings["line"], reason="Session Revoked")
                await utils.answer(self._status_msg, result)
            except UserDeactivatedBanError:
                result = self.strings["test_fail"].format(line=self.strings["line"], reason="Account Banned")
                await utils.answer(self._status_msg, result)
            except Exception as e:
                result = self.strings["test_fail"].format(line=self.strings["line"], reason=str(e))
                await utils.answer(self._status_msg, result)
            finally:
                _safe_disconnect(test_client)
        except:
            self._active = False

    async def _cleanup(self):
        self._active = False
        self._step = "none"
        self._mode = None
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except:
                pass
        _safe_disconnect(self._session_client)
        self._session_client = None
        self._data = {}
        self._status_msg = None
        self._chat_id = None
        self._topic_id = None
        self._origin_message = None