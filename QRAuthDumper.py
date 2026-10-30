# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (3, 0, 1)
# meta developer: Execute_forge.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/QRAuthDumper/MetaBanner.jpeg

import io
import logging
import asyncio
import hashlib
import base64
import struct
import ipaddress
import sys

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Message
from telethon.errors import SessionPasswordNeededError, PasswordHashInvalidError

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

QR_REFRESH = 15

DEPS = ["qrcode[pil]", "Pillow"]


def _install_deps():
    import subprocess

    pip = __import__('os').path.join(__import__('os').path.dirname(sys.executable), "pip")
    if not __import__('os').path.exists(pip):
        pip = "pip"

    for pkg in DEPS:
        try:
            subprocess.run(
                [pip, "install", "-U", pkg, "--break-system-packages", "-q"],
                capture_output=True,
                text=True,
                timeout=120,
            )
        except Exception:
            pass



def _escape(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")



@loader.tds
class QRAuthDumper(loader.Module):
    """QR code authentication session dumper"""

    strings = {
        "name": "QRAuthDumper",
        "main_menu": (
            "<b>QRAuthDumper</b>\n"
            "<blockquote>"
            "Status: {status}\n"
            "API_ID: {api_id}\n"
            "API_HASH: {api_hash}"
            "</blockquote>"
        ),
        "status_ready": "ready",
        "status_running": "running",
        "status_stopped": "stopped",
        "status_2fa": "waiting 2FA",
        "btn_start": "Start QR Auth",
        "btn_stop": "Stop",
        "btn_api": "API Settings",
        "btn_back": "Back",
        "btn_close": "Close",
        "btn_set_id": "Set API_ID",
        "btn_set_hash": "Set API_HASH",
        "btn_submit_pass": "Submit Password",
        "input_api_id": "Enter API_ID:",
        "input_api_hash": "Enter API_HASH:",
        "input_password": "Enter 2FA password:",
        "api_menu": (
            "<b>API Settings</b>\n"
            "<blockquote>"
            "API_ID: {api_id}\n"
            "API_HASH: {api_hash}"
            "</blockquote>"
        ),
        "generating": "<b>Generating QR code...</b>",
        "qr_prompt": (
            "<b>Scan this QR code</b>\n"
            "<blockquote>"
            "1. Open Telegram on phone\n"
            "2. Settings → Devices → Link Desktop Device\n"
            "3. Point camera at QR\n"
            "Time left: {timeout} sec"
            "</blockquote>"
        ),
        "qr_refreshed": (
            "<b>QR refreshed</b>\n"
            "<blockquote>"
            "Old one expired, scan new one.\n"
            "Time left: {time_left} sec"
            "</blockquote>"
        ),
        "auth_success": (
            "<b>Auth Success</b>\n"
            "<blockquote>"
            "Name: {name}\n"
            "ID: {user_id}\n"
            "Username: {username}\n"
            "DC: {dc_id}"
            "</blockquote>\n"
            "<b>Auth Key (HEX):</b>\n"
            "<code>{auth_key_hex}</code>\n"
            "<b>Auth Key SHA256:</b>\n"
            "<code>{auth_key_sha}</code>\n"
            "<blockquote>Save this and delete this message.</blockquote>"
        ),
        "auth_timeout": (
            "<b>Timeout</b>\n"
            "<blockquote>QR expired. Try again.</blockquote>"
        ),
        "auth_error": (
            "<b>Error</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "already_running": (
            "<b>Auth already running.</b>\n"
            "<blockquote>Wait or stop current session.</blockquote>"
        ),
        "password_needed": (
            "<b>2FA Password Required</b>\n"
            "<blockquote>Attempts left: {attempts}</blockquote>"
        ),
        "wrong_password": (
            "<b>Wrong password!</b>\n"
            "<blockquote>Attempts left: {attempts}</blockquote>"
        ),
        "attempts_exhausted": (
            "<b>All password attempts used.</b>\n"
            "<blockquote>Process terminated. Try again.</blockquote>"
        ),
        "no_config": (
            "<b>API not configured</b>\n"
            "<blockquote>Set API_ID and API_HASH first.</blockquote>"
        ),
        "invalid_value": "<b>Invalid value.</b>",
        "upload_failed": "<b>QR upload failed.</b>",
    }

    strings_ru = {
        "main_menu": (
            "<b>QRAuthDumper</b>\n"
            "<blockquote>"
            "Статус: {status}\n"
            "API_ID: {api_id}\n"
            "API_HASH: {api_hash}"
            "</blockquote>"
        ),
        "status_ready": "готов",
        "status_running": "запущен",
        "status_stopped": "остановлен",
        "status_2fa": "ожидание 2FA",
        "btn_start": "Запустить QR Auth",
        "btn_stop": "Остановить",
        "btn_api": "Настройки API",
        "btn_back": "Назад",
        "btn_close": "Закрыть",
        "btn_set_id": "Задать API_ID",
        "btn_set_hash": "Задать API_HASH",
        "btn_submit_pass": "Ввести пароль",
        "input_api_id": "Введите API_ID:",
        "input_api_hash": "Введите API_HASH:",
        "input_password": "Введите 2FA пароль:",
        "api_menu": (
            "<b>Настройки API</b>\n"
            "<blockquote>"
            "API_ID: {api_id}\n"
            "API_HASH: {api_hash}"
            "</blockquote>"
        ),
        "generating": "<b>Генерация QR кода...</b>",
        "qr_prompt": (
            "<b>Отсканируйте QR код</b>\n"
            "<blockquote>"
            "1. Откройте Telegram на телефоне\n"
            "2. Настройки → Устройства → Подключить устройство\n"
            "3. Наведите камеру на QR\n"
            "Осталось: {timeout} сек"
            "</blockquote>"
        ),
        "qr_refreshed": (
            "<b>QR обновлён</b>\n"
            "<blockquote>"
            "Старый истёк, сканируйте новый.\n"
            "Осталось: {time_left} сек"
            "</blockquote>"
        ),
        "auth_success": (
            "<b>Авторизация успешна</b>\n"
            "<blockquote>"
            "Имя: {name}\n"
            "ID: {user_id}\n"
            "Юзернейм: {username}\n"
            "DC: {dc_id}"
            "</blockquote>\n"
            "<b>Auth Key (HEX):</b>\n"
            "<code>{auth_key_hex}</code>\n"
            "<b>Auth Key SHA256:</b>\n"
            "<code>{auth_key_sha}</code>\n"
            "<blockquote>Сохраните это и удалите сообщение.</blockquote>"
        ),
        "auth_timeout": (
            "<b>Таймаут</b>\n"
            "<blockquote>QR истёк. Попробуйте снова.</blockquote>"
        ),
        "auth_error": (
            "<b>Ошибка</b>\n"
            "<blockquote>{error}</blockquote>"
        ),
        "already_running": (
            "<b>Авторизация уже запущена.</b>\n"
            "<blockquote>Подождите или остановите текущую сессию.</blockquote>"
        ),
        "password_needed": (
            "<b>Требуется 2FA пароль</b>\n"
            "<blockquote>Осталось попыток: {attempts}</blockquote>"
        ),
        "wrong_password": (
            "<b>Неверный пароль!</b>\n"
            "<blockquote>Осталось попыток: {attempts}</blockquote>"
        ),
        "attempts_exhausted": (
            "<b>Все попытки исчерпаны.</b>\n"
            "<blockquote>Процесс завершён. Попробуйте снова.</blockquote>"
        ),
        "no_config": (
            "<b>API не настроен</b>\n"
            "<blockquote>Сначала задайте API_ID и API_HASH.</blockquote>"
        ),
        "invalid_value": "<b>Некорректное значение.</b>",
        "upload_failed": "<b>Не удалось загрузить QR.</b>",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "API_ID",
                2040,
                "Telegram API ID",
                validator=loader.validators.Integer(minimum=1),
            ),
            loader.ConfigValue(
                "API_HASH",
                "b18441a1ff607e10a989891a5462e627",
                "Telegram API Hash",
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "QR_TIMEOUT",
                60,
                "QR scan timeout in seconds",
                validator=loader.validators.Integer(minimum=10, maximum=300),
            ),
            loader.ConfigValue(
                "MAX_PASSWORD_ATTEMPTS",
                3,
                "Max 2FA password attempts",
                validator=loader.validators.Integer(minimum=1, maximum=10),
            ),
        )
        self._owner_id = None
        self._active_sessions = {}
        self._pending_2fa = {}
        self._tasks = set()

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        me = await client.get_me()
        self._owner_id = me.id
        _install_deps()
        logger.info("[QRAuth] ready, owner=%d", self._owner_id)

    def _fmt_status(self, uid):
        if self._active_sessions.get(uid):
            return self.strings["status_running"]
        if uid in self._pending_2fa:
            return self.strings["status_2fa"]
        if self.config["API_ID"] and self.config["API_HASH"]:
            return self.strings["status_ready"]
        return self.strings["status_stopped"]

    def _fmt_hash(self):
        h = str(self.config["API_HASH"])
        if len(h) > 8:
            return h[:4] + "..." + h[-4:]
        return "***" if h else "not set"

    def _fmt_menu(self, uid):
        return self.strings["main_menu"].format(
            status=self._fmt_status(uid),
            api_id=self.config["API_ID"] or "not set",
            api_hash=self._fmt_hash(),
        )

    def _main_markup(self, uid):
        running = self._active_sessions.get(uid)
        waiting_2fa = uid in self._pending_2fa
        rows = []

        if waiting_2fa:
            rows.append([{
                "text": self.strings["btn_submit_pass"],
                "input": self.strings["input_password"],
                "handler": self._cb_password,
                "style": "primary",
            }])

        if running or waiting_2fa:
            rows.append([{
                "text": self.strings["btn_stop"],
                "callback": self._cb_stop,
                "style": "danger",
            }])
        else:
            rows.append([{
                "text": self.strings["btn_start"],
                "callback": self._cb_start,
                "style": "success",
            }])

        rows.append([
            {"text": self.strings["btn_api"], "callback": self._cb_api_menu, "style": "primary"},
            {"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"},
        ])

        return rows

    def _api_markup(self):
        return [
            [
                {
                    "text": self.strings["btn_set_id"],
                    "input": self.strings["input_api_id"],
                    "handler": self._cb_set_api_id,
                    "style": "primary",
                },
                {
                    "text": self.strings["btn_set_hash"],
                    "input": self.strings["input_api_hash"],
                    "handler": self._cb_set_api_hash,
                    "style": "primary",
                },
            ],
            [{"text": self.strings["btn_back"], "callback": self._cb_back_main, "style": "danger"}],
        ]

    def _make_qr_bytes(self, url: str) -> bytes:
        import qrcode
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_L,
            box_size=10,
            border=4,
        )
        qr.add_data(url)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()

    def _upload_qr(self, url: str, attempt: int = 0) -> io.BytesIO:
        buf = io.BytesIO(self._make_qr_bytes(url))
        buf.name = "qr.png"
        return buf

    def _parse_string_session(self, session_str):
        try:
            if not session_str or not session_str.startswith("1"):
                return None
            string = session_str[1:]
            padded = string + "=" * (-len(string) % 4)
            data = base64.urlsafe_b64decode(padded)
            if len(data) == 263:
                dc_id, ip_bytes, port, auth_key = struct.unpack(">B4sH256s", data)
                ip = str(ipaddress.IPv4Address(ip_bytes))
            elif len(data) == 275:
                dc_id, ip_bytes, port, auth_key = struct.unpack(">B16sH256s", data)
                ip = str(ipaddress.IPv6Address(ip_bytes))
            else:
                return None
            return {"dc_id": dc_id, "ip": ip, "port": port, "auth_key": auth_key}
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
            name=_escape(name),
            user_id=uid,
            username=_escape(uname_s),
            dc_id=dc_id,
            auth_key_hex=hex_key,
            auth_key_sha=sha,
        )

    async def _finalize_auth(self, tc, user):
        try:
            ss = tc.session.save()
            hex_key, dc_id = self._extract_hex(tc)
            if dc_id is None:
                parsed = self._parse_string_session(ss)
                dc_id = parsed["dc_id"] if parsed else "?"
            try:
                kb = bytes.fromhex(hex_key)
                sha = hashlib.sha256(kb).hexdigest()
            except Exception:
                sha = "N/A"
            return self._format_result(user, hex_key, dc_id, sha)
        finally:
            try:
                await tc.disconnect()
            except Exception:
                pass

    async def _cleanup_session(self, uid):
        pending = self._pending_2fa.pop(uid, None)
        if pending:
            try:
                await pending["client"].disconnect()
            except Exception:
                pass
        self._active_sessions.pop(uid, None)

    @loader.command(
        ru_doc="Панель управления QRAuthDumper",
        en_doc="QRAuthDumper control panel",
    )
    async def qrd(self, message: Message):
        """QRAuthDumper control panel"""
        uid = self._owner_id
        await self.inline.form(
            text=self._fmt_menu(uid),
            message=message,
            reply_markup=self._main_markup(uid),
            silent=True,
        )

    async def _cb_back_main(self, call: InlineCall):
        uid = self._owner_id
        await call.edit(
            text=self._fmt_menu(uid),
            reply_markup=self._main_markup(uid),
        )

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    async def _cb_stop(self, call: InlineCall):
        uid = self._owner_id
        await self._cleanup_session(uid)
        await call.edit(
            text=self._fmt_menu(uid),
            reply_markup=self._main_markup(uid),
        )

    async def _cb_api_menu(self, call: InlineCall):
        await call.edit(
            text=self.strings["api_menu"].format(
                api_id=self.config["API_ID"] or "not set",
                api_hash=self._fmt_hash(),
            ),
            reply_markup=self._api_markup(),
        )

    async def _cb_set_api_id(self, call: InlineCall, value: str):
        value = value.strip()
        try:
            self.config["API_ID"] = int(value)
        except ValueError:
            await call.answer(self.strings["invalid_value"], show_alert=True)
            return
        await call.edit(
            text=self.strings["api_menu"].format(
                api_id=self.config["API_ID"],
                api_hash=self._fmt_hash(),
            ),
            reply_markup=self._api_markup(),
        )

    async def _cb_set_api_hash(self, call: InlineCall, value: str):
        value = value.strip()
        if not value:
            await call.answer(self.strings["invalid_value"], show_alert=True)
            return
        self.config["API_HASH"] = value
        await call.edit(
            text=self.strings["api_menu"].format(
                api_id=self.config["API_ID"] or "not set",
                api_hash=self._fmt_hash(),
            ),
            reply_markup=self._api_markup(),
        )

    async def _cb_start(self, call: InlineCall):
        uid = self._owner_id

        if self._active_sessions.get(uid):
            await call.answer(self.strings["already_running"], show_alert=True)
            return

        api_id = self.config["API_ID"]
        api_hash = self.config["API_HASH"]

        if not api_id or not api_hash:
            await call.answer(self.strings["no_config"], show_alert=True)
            return

        self._active_sessions[uid] = True

        await call.edit(
            text=self.strings["generating"],
            reply_markup=[],
        )

        task = asyncio.create_task(
            self._run_qr_task(uid, int(api_id), str(api_hash), call)
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _cb_password(self, call: InlineCall, value: str):
        uid = self._owner_id
        value = value.strip()

        if not value:
            await call.answer(self.strings["invalid_value"], show_alert=True)
            return

        if uid not in self._pending_2fa:
            await call.edit(
                text=self._fmt_menu(uid),
                reply_markup=self._main_markup(uid),
            )
            return

        pending = self._pending_2fa[uid]
        tc = pending["client"]

        try:
            await tc.sign_in(password=value)
            user = await tc.get_me()
            del self._pending_2fa[uid]
            result = await self._finalize_auth(tc, user)
            self._active_sessions.pop(uid, None)
            await call.edit(
                text=result,
                reply_markup=[[{
                    "text": self.strings["btn_close"],
                    "callback": self._cb_close,
                    "style": "danger",
                }]],
            )
        except PasswordHashInvalidError:
            pending["attempts_left"] -= 1
            if pending["attempts_left"] <= 0:
                try:
                    await tc.disconnect()
                except Exception:
                    pass
                del self._pending_2fa[uid]
                self._active_sessions.pop(uid, None)
                await call.edit(
                    text=self.strings["attempts_exhausted"],
                    reply_markup=[[{
                        "text": self.strings["btn_back"],
                        "callback": self._cb_back_main,
                        "style": "primary",
                    }]],
                )
            else:
                await call.edit(
                    text=self.strings["wrong_password"].format(
                        attempts=pending["attempts_left"]
                    ),
                    reply_markup=self._main_markup(uid),
                )
        except Exception as e:
            logger.error("[QRAuth] 2FA error: %s", e, exc_info=True)
            await self._cleanup_session(uid)
            await call.edit(
                text=self.strings["auth_error"].format(error=_escape(str(e))),
                reply_markup=[[{
                    "text": self.strings["btn_back"],
                    "callback": self._cb_back_main,
                    "style": "danger",
                }]],
            )

    async def _run_qr_task(self, uid, api_id, api_hash, call: InlineCall):
        timeout = int(self.config["QR_TIMEOUT"])
        max_attempts = int(self.config["MAX_PASSWORD_ATTEMPTS"])

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
            qr = await tc.qr_login()

            attempt = 0
            qr_photo = self._upload_qr(qr.url, attempt=attempt)
            if not qr_photo:
                self._active_sessions.pop(uid, None)
                await call.edit(
                    text=self.strings["upload_failed"],
                    reply_markup=self._main_markup(uid),
                )
                try:
                    await tc.disconnect()
                except Exception:
                    pass
                return

            await call.edit(
                text=self.strings["qr_prompt"].format(timeout=timeout),
                photo=qr_photo,
                reply_markup=self._main_markup(uid),
            )

            user = None
            elapsed = 0
            need_2fa = False

            while elapsed < timeout:
                wt = min(QR_REFRESH, timeout - elapsed)
                try:
                    user = await asyncio.wait_for(qr.wait(), timeout=wt)
                    break
                except SessionPasswordNeededError:
                    need_2fa = True
                    break
                except asyncio.TimeoutError:
                    elapsed += wt
                    if elapsed >= timeout:
                        break
                    try:
                        attempt += 1
                        await qr.recreate()
                        new_photo = self._upload_qr(qr.url, attempt=attempt)
                        tl = timeout - elapsed
                        if new_photo:
                            await call.edit(
                                text=self.strings["qr_refreshed"].format(time_left=tl),
                                photo=new_photo,
                                reply_markup=self._main_markup(uid),
                            )
                        else:
                            await call.edit(
                                text=self.strings["qr_refreshed"].format(time_left=tl),
                                reply_markup=self._main_markup(uid),
                            )
                    except Exception as e:
                        logger.warning("[QRAuth] recreate failed: %s", e)
                except Exception as e:
                    logger.error("[QRAuth] wait error: %s", e, exc_info=True)
                    raise

            if need_2fa:
                self._pending_2fa[uid] = {
                    "client": tc,
                    "attempts_left": max_attempts,
                }
                await call.edit(
                    text=self.strings["password_needed"].format(attempts=max_attempts),
                    reply_markup=self._main_markup(uid),
                )
                return

            if user is None:
                self._active_sessions.pop(uid, None)
                await call.edit(
                    text=self.strings["auth_timeout"],
                    reply_markup=[[{
                        "text": self.strings["btn_back"],
                        "callback": self._cb_back_main,
                        "style": "primary",
                    }]],
                )
                try:
                    await tc.disconnect()
                except Exception:
                    pass
                return

            result = await self._finalize_auth(tc, user)
            self._active_sessions.pop(uid, None)

            await call.edit(
                text=result,
                reply_markup=[[{
                    "text": self.strings["btn_close"],
                    "callback": self._cb_close,
                    "style": "danger",
                }]],
            )

        except Exception as e:
            logger.error("[QRAuth] task error: %s", e, exc_info=True)
            try:
                await tc.disconnect()
            except Exception:
                pass
            await self._cleanup_session(uid)
            try:
                await call.edit(
                    text=self.strings["auth_error"].format(error=_escape(str(e))),
                    reply_markup=[[{
                        "text": self.strings["btn_back"],
                        "callback": self._cb_back_main,
                        "style": "danger",
                    }]],
                )
            except Exception:
                pass

    async def on_unload(self):
        for task in list(self._tasks):
            if not task.done():
                task.cancel()
        for uid in list(self._pending_2fa.keys()):
            await self._cleanup_session(uid)
        logger.info("[QRAuth] unloaded")