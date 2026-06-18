__version__ = (3, 0, 5)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/QRAuthDumper/MetaBanner.jpeg

import io
import logging
import asyncio
import hashlib
import base64
import struct
import ipaddress
import sys
import tempfile
import os

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Message
from telethon.errors import SessionPasswordNeededError, PasswordHashInvalidError

from .. import loader, utils

logger = logging.getLogger(__name__)

QR_REFRESH = 15
DEPS = ["qrcode[pil]", "Pillow"]


def _install_deps():
    import importlib
    import subprocess
    
    pip = os.path.join(os.path.dirname(sys.executable), "pip")
    if not os.path.exists(pip):
        pip = "pip"
    
    in_venv = sys.prefix != sys.base_prefix
    imp_map = {"qrcode[pil]": "qrcode", "Pillow": "PIL"}
    lines = [f"venv: {'yes' if in_venv else 'no'} ({sys.prefix})"]
    
    for pkg in DEPS:
        try:
            subprocess.run(
                [pip, "install", "-U", pkg, "--break-system-packages", "-q"],
                capture_output=True,
                text=True,
                timeout=120
            )
            try:
                imp_name = imp_map.get(pkg, pkg)
                mod = importlib.import_module(imp_name)
                ver = getattr(mod, "__version__", "?")
                lines.append(f"{pkg}: OK ({ver})")
            except ImportError:
                lines.append(f"{pkg}: FAIL (import error)")
        except Exception as e:
            lines.append(f"{pkg}: FAIL ({e})")
    return lines


def escape_html(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def upload_to_x0(data: bytes, filename: str, content_type: str = "image/png") -> str:
    try:
        import aiohttp
        form = aiohttp.FormData()
        form.add_field("file", data, filename=filename, content_type=content_type)
        async with aiohttp.ClientSession() as s:
            async with s.post("https://x0.at", data=form, timeout=aiohttp.ClientTimeout(total=60)) as r:
                text = (await r.text()).strip()
                if text.startswith("http"):
                    return text
    except Exception:
        pass
    return ""


@loader.tds
class QRAuthDumper(loader.Module):
    """QR code authentication session dumper"""

    strings = {
        "name": "QRAuthDumper",
        "menu_title": "<b>QRAuthDumper</b>\nSelect action:",
        "usage_title": (
            "<b>QRAuthDumper</b>\n"
            "<blockquote>"
            "This module allows you to obtain Telegram session data via QR code authorization.\n"
            "How it works:\n"
            "1. Generate QR code\n"
            "2. Scan it with your phone (Settings - Devices - Link Desktop Device)\n"
            "3. Get auth key and session data\n"
            "Requirements: API_ID and API_HASH in module config"
            "</blockquote>"
        ),
        "no_config": (
            "<b>Config not set</b>\n"
            "API_ID and API_HASH required.\n"
            "Configure them in module settings."
        ),
        "qr_prompt": (
            "<b>Scan this QR code</b>\n"
            "1. Open Telegram on phone\n"
            "2. Settings - Devices - Link Desktop Device\n"
            "3. Point camera at QR\n"
            "Time left: <b>{timeout} sec</b>"
        ),
        "qr_refreshed": (
            "<b>QR refreshed</b>\n"
            "Old one expired, scan new one.\n"
            "Time left: <b>{time_left} sec</b>"
        ),
        "auth_success": (
            "<b>Auth Success</b>\n"
            "Name: {name}\n"
            "ID: <code>{user_id}</code>\n"
            "Username: {username}\n"
            "DC: <code>{dc_id}</code>\n\n"
            "<b>Auth Key (HEX):</b>\n"
            "<code>{auth_key_hex}</code>\n\n"
            "<b>Auth Key SHA256:</b>\n"
            "<code>{auth_key_sha}</code>\n\n"
            "<b>Save this and delete this message.</b>"
        ),
        "auth_timeout": "<b>Timeout</b>\nQR expired.",
        "auth_error": "<b>Error</b>\nDetails: <code>{error}</code>",
        "already_running": "<b>Wait</b>\nAuth already running.",
        "generating": "<b>Generating QR...</b>",
        "password_needed": (
            "<b>2FA Password Required</b>\n"
            "Enter password below.\n"
            "Attempts left: <b>{attempts}</b>"
        ),
        "wrong_password": (
            "<b>Wrong password!</b>\n"
            "Attempts left: <b>{attempts}</b>\n"
            "Enter password below."
        ),
        "attempts_exhausted": (
            "<b>All password attempts used.</b>\n"
            "Process terminated."
        ),
        "session_cancelled": "<b>Auth cancelled.</b>",
        "btn_start": "Start",
        "btn_usage": "Usage",
        "btn_back": "Back",
        "btn_cancel": "Cancel",
        "btn_kill": "Kill",
        "btn_retry": "Retry",
        "btn_enter_pass": "Enter Password",
        "input_password": "Enter your 2FA password:",
    }

    strings_ru = {
        "menu_title": "<b>QRAuthDumper</b>\nВыберите действие:",
        "usage_title": (
            "<b>QRAuthDumper</b>\n"
            "<blockquote>"
            "Этот модуль позволяет получить данные сессии Telegram через QR авторизацию.\n"
            "Как работает:\n"
            "1. Генерируется QR код\n"
            "2. Сканируете его телефоном (Настройки - Устройства - Подключить устройство)\n"
            "3. Получаете auth key и данные сессии\n"
            "Требования: API_ID и API_HASH в конфиге модуля"
            "</blockquote>"
        ),
        "no_config": (
            "<b>Настройки не заданы</b>\n"
            "Требуются API_ID и API_HASH.\n"
            "Настройте их в настройках модуля."
        ),
        "qr_prompt": (
            "<b>Отсканируйте этот QR код</b>\n"
            "1. Откройте Telegram на телефоне\n"
            "2. Настройки - Устройства - Подключить устройство\n"
            "3. Наведите камеру на QR\n"
            "Осталось: <b>{timeout} сек</b>"
        ),
        "qr_refreshed": (
            "<b>QR обновлён</b>\n"
            "Старый истёк, сканируйте новый.\n"
            "Осталось: <b>{time_left} сек</b>"
        ),
        "auth_success": (
            "<b>Авторизация успешна</b>\n"
            "Имя: {name}\n"
            "ID: <code>{user_id}</code>\n"
            "Юзернейм: {username}\n"
            "DC: <code>{dc_id}</code>\n\n"
            "<b>Auth Key (HEX):</b>\n"
            "<code>{auth_key_hex}</code>\n\n"
            "<b>Auth Key SHA256:</b>\n"
            "<code>{auth_key_sha}</code>\n\n"
            "<b>Сохраните это и удалите сообщение.</b>"
        ),
        "auth_timeout": "<b>Таймаут</b>\nQR истёк.",
        "auth_error": "<b>Ошибка</b>\nДетали: <code>{error}</code>",
        "already_running": "<b>Подождите</b>\nАвторизация уже запущена.",
        "generating": "<b>Генерация QR...</b>",
        "password_needed": (
            "<b>Требуется 2FA пароль</b>\n"
            "Введите пароль ниже.\n"
            "Осталось попыток: <b>{attempts}</b>"
        ),
        "wrong_password": (
            "<b>Неверный пароль!</b>\n"
            "Осталось попыток: <b>{attempts}</b>\n"
            "Введите пароль ниже."
        ),
        "attempts_exhausted": (
            "<b>Все попытки пароля исчерпаны.</b>\n"
            "Процесс завершён."
        ),
        "session_cancelled": "<b>Авторизация отменена.</b>",
        "btn_start": "Старт",
        "btn_usage": "Описание",
        "btn_back": "Назад",
        "btn_cancel": "Отмена",
        "btn_kill": "Остановить",
        "btn_retry": "Повторить",
        "btn_enter_pass": "Ввести пароль",
        "input_password": "Введите ваш 2FA пароль:",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue("API_ID", 2040, "Telegram API ID", validator=loader.validators.Integer(minimum=1)),
            loader.ConfigValue("API_HASH", "b18441a1ff607e10a989891a5462e627", "Telegram API Hash", validator=loader.validators.String()),
            loader.ConfigValue("QR_TIMEOUT", 60, "QR scan timeout in seconds", validator=loader.validators.Integer(minimum=10, maximum=300)),
            loader.ConfigValue("MAX_PASSWORD_ATTEMPTS", 3, "Max 2FA password attempts", validator=loader.validators.Integer(minimum=1, maximum=10)),
        )
        self._owner_id = None
        self._active_sessions = {}
        self._pending_2fa = {}
        self._session_data = {}
        self._tasks = set()
        self._tmp_dir = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        me = await client.get_me()
        self._owner_id = me.id
        self._tmp_dir = tempfile.mkdtemp(prefix="qrauth_")
        logger.info("[QRAuth] Module loaded, owner id=%d", self._owner_id)
        
        try:
            status_lines = _install_deps()
            logger.info("[QRAuth] Dependencies check:\n" + "\n".join(status_lines))
        except Exception as e:
            logger.error("[QRAuth] Dependency installation error: %s", e)

    async def _is_forum_chat(self, message):
        try:
            chat = await message.get_chat()
            return getattr(chat, "forum", False)
        except Exception:
            return False

    def _get_topic_id(self, message):
        try:
            if message.reply_to and hasattr(message.reply_to, "reply_to_top_id"):
                return message.reply_to.reply_to_top_id or message.reply_to.reply_to_msg_id
        except Exception:
            pass
        return None

    def _make_qr(self, url: str) -> bytes:
        import qrcode
        qr = qrcode.QRCode(version=1, error_correction=qrcode.constants.ERROR_CORRECT_L, box_size=10, border=4)
        qr.add_data(url)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()

    def _parse_string_session(self, session_str):
        try:
            if not session_str or not session_str.startswith("1"):
                return None
            string = session_str[1:]
            padded = string + "=" * (-len(string) % 4)
            data = base64.urlsafe_b64decode(padded)
            if len(data) == 263:
                dc_id, ip_bytes, port, auth_key = struct.unpack(">B4sH256s", data)
            elif len(data) == 275:
                dc_id, ip_bytes, port, auth_key = struct.unpack(">B16sH256s", data)
            else:
                return None
            return {"dc_id": dc_id, "auth_key": auth_key}
        except Exception as e:
            logger.error("[QRAuth] parse error: %s", e)
            return None

    def _extract_hex(self, client: TelegramClient) -> tuple:
        try:
            session = client.session
            if hasattr(session, "_auth_key") and session._auth_key:
                key_data = session._auth_key.key
                dc = getattr(session, "_dc_id", None)
                return key_data.hex(), dc
            saved = session.save()
            if saved:
                parsed = self._parse_string_session(saved)
                if parsed:
                    return parsed["auth_key"].hex(), parsed["dc_id"]
            return "FAILED_TO_EXTRACT", None
        except Exception as e:
            logger.error("[QRAuth] extract error: %s", e)
            return f"ERROR: {e}", None

    def _format_result(self, user, hex_key, dc_id, sha):
        fn = getattr(user, "first_name", "") or ""
        ln = getattr(user, "last_name", "") or ""
        name = f"{fn} {ln}".strip() or "Unknown"
        uname = getattr(user, "username", None)
        uname_s = f"@{uname}" if uname else "---"
        uid = getattr(user, "id", 0)
        return self.strings["auth_success"].format(
            name=escape_html(name),
            user_id=uid,
            username=escape_html(uname_s),
            dc_id=dc_id,
            auth_key_hex=hex_key,
            auth_key_sha=sha,
        )

    async def _run_qr(self, api_id, api_hash, uid, form_call):
        timeout = int(self.config["QR_TIMEOUT"])
        refresh = QR_REFRESH
        
        session_data = self._session_data.get(uid, {})
        chat_id = session_data.get("chat_id")
        topic_id = session_data.get("topic_id")
        is_forum = session_data.get("is_forum", False)

        tc = TelegramClient(
            StringSession(),
            api_id,
            api_hash,
            device_model="QRAuthDumper",
            system_version="By @i_execute",
            app_version=f"v{'.'.join(map(str, __version__))}",
        )

        try:
            await tc.connect()
            logger.info("[QRAuth] Temp client connected")
            qr = await tc.qr_login()
            logger.info("[QRAuth] QR login initiated")

            qr_bytes = self._make_qr(qr.url)
            qr_url = await upload_to_x0(qr_bytes, "qr_auth.png", "image/png")
            
            edit_kwargs = {
                "text": self.strings["qr_prompt"].format(timeout=timeout),
                "reply_markup": [[{"text": self.strings["btn_kill"], "callback": self._qr_cancel, "args": (uid,), "style": "danger"}]],
            }
            if qr_url:
                edit_kwargs["photo"] = qr_url
            
            await form_call.edit(**edit_kwargs)
            logger.info("[QRAuth] QR image sent")

            user = None
            elapsed = 0
            need_2fa = False

            while elapsed < timeout:
                wt = min(refresh, timeout - elapsed)
                try:
                    user = await asyncio.wait_for(qr.wait(), timeout=wt)
                    logger.info("[QRAuth] QR scanned!")
                    break
                except SessionPasswordNeededError:
                    need_2fa = True
                    break
                except asyncio.TimeoutError:
                    elapsed += wt
                    if elapsed >= timeout:
                        break
                    logger.info("[QRAuth] QR expired, recreating, elapsed=%d", elapsed)
                    try:
                        await qr.recreate()
                        new_qr_bytes = self._make_qr(qr.url)
                        new_qr_url = await upload_to_x0(new_qr_bytes, "qr_auth.png", "image/png")
                        tl = timeout - elapsed
                        edit_kwargs = {
                            "text": self.strings["qr_refreshed"].format(time_left=tl),
                            "reply_markup": [[{"text": self.strings["btn_kill"], "callback": self._qr_cancel, "args": (uid,), "style": "danger"}]],
                        }
                        if new_qr_url:
                            edit_kwargs["photo"] = new_qr_url
                        await form_call.edit(**edit_kwargs)
                    except Exception as e:
                        logger.warning("[QRAuth] QR recreate failed: %s", e)

            if need_2fa:
                max_attempts = int(self.config["MAX_PASSWORD_ATTEMPTS"])
                self._pending_2fa[uid] = {
                    "client": tc,
                    "attempts_left": max_attempts,
                    "form_call": form_call,
                }
                await form_call.edit(
                    self.strings["password_needed"].format(attempts=max_attempts),
                    reply_markup=[[
                        {"text": self.strings["btn_enter_pass"], "input": self.strings["input_password"], "handler": self._password_input, "args": (uid,), "style": "primary"},
                        {"text": self.strings["btn_kill"], "callback": self._qr_cancel, "args": (uid,), "style": "danger"},
                    ]]
                )
                return

            if user is None:
                try:
                    await tc.disconnect()
                except Exception:
                    pass
                logger.info("[QRAuth] Timeout")
                try:
                    await form_call.delete()
                except Exception:
                    pass
                if chat_id:
                    reply_to = topic_id if is_forum and topic_id else None
                    await self._client.send_message(chat_id, self.strings["auth_timeout"], parse_mode="html", reply_to=reply_to)
                return

            await self._finalize_auth(tc, user, form_call)

        except Exception as e:
            logger.error("[QRAuth] QR error: %s", e, exc_info=True)
            try:
                await tc.disconnect()
            except Exception:
                pass
            try:
                try:
                    await form_call.delete()
                except Exception:
                    pass
                if chat_id:
                    reply_to = topic_id if is_forum and topic_id else None
                    await self._client.send_message(chat_id, self.strings["auth_error"].format(error=escape_html(str(e))), parse_mode="html", reply_to=reply_to)
            except Exception as ex:
                logger.error("[QRAuth] Failed to send error message: %s", ex)

    async def _finalize_auth(self, tc, user, form_call):
        try:
            logger.info("[QRAuth] Success: %s id=%d", getattr(user, "first_name", "?"), getattr(user, "id", 0))
            hex_key, dc_id = self._extract_hex(tc)
            if dc_id is None:
                ss = tc.session.save()
                parsed = self._parse_string_session(ss)
                dc_id = parsed["dc_id"] if parsed else "?"
            try:
                kb = bytes.fromhex(hex_key)
                sha = hashlib.sha256(kb).hexdigest()
            except Exception:
                sha = "N/A"
            result = self._format_result(user, hex_key, dc_id, sha)
            await form_call.edit(result, reply_markup=[])
        finally:
            try:
                await tc.disconnect()
                logger.info("[QRAuth] Temp client disconnected")
            except Exception:
                pass

    async def _password_input(self, call, password: str, uid: int):
        pending = self._pending_2fa.get(uid)
        if not pending:
            await call.answer("No active process", show_alert=True)
            return

        tc = pending["client"]
        form_call = pending["form_call"]
        
        session_data = self._session_data.get(uid, {})
        chat_id = session_data.get("chat_id")
        topic_id = session_data.get("topic_id")
        is_forum = session_data.get("is_forum", False)

        try:
            await tc.sign_in(password=password)
            user = await tc.get_me()
            del self._pending_2fa[uid]
            await self._finalize_auth(tc, user, form_call)
        except PasswordHashInvalidError:
            pending["attempts_left"] -= 1
            if pending["attempts_left"] <= 0:
                try:
                    await tc.disconnect()
                except Exception:
                    pass
                del self._pending_2fa[uid]
                await form_call.edit(self.strings["attempts_exhausted"], reply_markup=[])
                return
            await form_call.edit(
                self.strings["wrong_password"].format(attempts=pending["attempts_left"]),
                reply_markup=[[
                    {"text": self.strings["btn_enter_pass"], "input": self.strings["input_password"], "handler": self._password_input, "args": (uid,), "style": "primary"},
                    {"text": self.strings["btn_kill"], "callback": self._qr_cancel, "args": (uid,), "style": "danger"},
                ]]
            )
        except Exception as e:
            logger.error("[QRAuth] 2FA error: %s", e, exc_info=True)
            try:
                await tc.disconnect()
            except Exception:
                pass
            if uid in self._pending_2fa:
                del self._pending_2fa[uid]
            try:
                await form_call.delete()
            except Exception:
                pass
            if chat_id:
                reply_to = topic_id if is_forum and topic_id else None
                await self._client.send_message(chat_id, self.strings["auth_error"].format(error=escape_html(str(e))), parse_mode="html", reply_to=reply_to)

    async def _show_menu(self, call, uid):
        await call.edit(
            self.strings["menu_title"],
            reply_markup=[
                [{"text": self.strings["btn_start"], "callback": self._start_qr, "args": (uid,), "style": "success"}],
                [{"text": self.strings["btn_usage"], "callback": self._show_usage, "args": (uid,), "style": "primary"}],
            ]
        )

    async def _show_usage(self, call, uid):
        await call.edit(
            self.strings["usage_title"],
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._show_menu, "args": (uid,), "style": "primary"}]]
        )

    async def _start_qr(self, call, uid):
        api_id = self.config["API_ID"]
        api_hash = self.config["API_HASH"]

        if not api_id or not api_hash:
            await call.edit(
                self.strings["no_config"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._show_menu, "args": (uid,), "style": "primary"}]]
            )
            return

        self._active_sessions[uid] = True
        await call.edit(self.strings["generating"], reply_markup=[])
        
        task = asyncio.create_task(self._run_qr(int(api_id), str(api_hash), uid, call))
        self._tasks.add(task)
        task.add_done_callback(lambda t: (self._tasks.discard(t), self._active_sessions.pop(uid, None)))

    async def _qr_cancel(self, call, uid: int):
        pending = self._pending_2fa.pop(uid, None)
        if pending:
            try:
                await pending["client"].disconnect()
            except Exception:
                pass
        self._active_sessions.pop(uid, None)
        self._session_data.pop(uid, None)
        await call.delete()

    async def _cleanup_session(self, uid):
        pending = self._pending_2fa.pop(uid, None)
        if pending:
            try:
                await pending["client"].disconnect()
            except Exception:
                pass
        self._active_sessions.pop(uid, None)
        self._session_data.pop(uid, None)

    @loader.command(ru_doc="Запуск QR авторизации", en_doc="Start QR authentication")
    async def dumpqr(self, message: Message):
        """Start QR authentication"""
        uid = self._owner_id

        if uid in self._pending_2fa:
            await self._cleanup_session(uid)

        if self._active_sessions.get(uid):
            await utils.answer(message, self.strings["already_running"])
            return

        is_forum = await self._is_forum_chat(message)
        topic_id = self._get_topic_id(message) if is_forum else None
        chat_id = message.chat_id

        self._session_data[uid] = {
            "chat_id": chat_id,
            "topic_id": topic_id,
            "is_forum": is_forum
        }

        await self.inline.form(
            text=self.strings["menu_title"],
            message=message,
            reply_markup=[
                [{"text": self.strings["btn_start"], "callback": self._start_qr, "args": (uid,), "style": "success"}],
                [{"text": self.strings["btn_usage"], "callback": self._show_usage, "args": (uid,), "style": "primary"}],
            ],
            silent=True,
        )

    async def on_unload(self):
        for task in list(self._tasks):
            if not task.done():
                task.cancel()
        for uid in list(self._pending_2fa.keys()):
            await self._cleanup_session(uid)
        if self._tmp_dir and os.path.exists(self._tmp_dir):
            import shutil
            shutil.rmtree(self._tmp_dir, ignore_errors=True)
        logger.info("[QRAuth] Module unloaded, cleaned up tasks and sessions")