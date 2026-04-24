__version__ = (2, 0, 2)
# meta developer: I_execute.t.me
# requires: qrcode[pil] Pillow

import io
import logging
import asyncio
import hashlib
import base64
import struct
import ipaddress

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Message
from telethon.errors import SessionPasswordNeededError, PasswordHashInvalidError

from .. import loader, utils

logger = logging.getLogger(__name__)

QR_REFRESH = 15


def _escape(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


@loader.tds
class QRAuthDumper(loader.Module):
    """QR code authentication session dumper"""

    strings = {
        "name": "QRAuthDumper",
        "line": "--------------------",
        "help": (
            "<b>QR Auth Dumper v{ver}</b>\n\n"
            "<b>Commands:</b>\n"
            "<code>{prefix}dumpqr</code> — start QR auth\n"
            "<code>{prefix}dumpqr pass [password]</code> — provide 2FA password\n\n"
            "<b>Config:</b>\n"
            "<code>{prefix}qrauth</code> — this help\n"
            "<code>{prefix}qrauth status</code> — status\n"
            "<code>{prefix}qrauth id [val]</code> — set API_ID\n"
            "<code>{prefix}qrauth hash [val]</code> — set API_HASH\n"
            "<code>{prefix}qrauth timeout [sec]</code> — QR timeout\n"
        ),
        "status": (
            "<b>QRAuthDumper Status</b>\n"
            "{line}\n"
            "API_ID: <code>{api_id}</code>\n"
            "API_HASH: <code>{api_hash}</code>\n"
            "Timeout: <code>{timeout}</code> sec\n"
            "Refresh: <code>{refresh}</code> sec\n"
            "Password attempts: <code>{max_attempts}</code>\n"
            "Status: {status}\n"
            "{line}"
        ),
        "no_config": (
            "<b>Config not set</b>\n"
            "{line}\n"
            "API_ID and API_HASH required.\n"
            "<code>{prefix}qrauth id YOUR_API_ID</code>\n"
            "<code>{prefix}qrauth hash YOUR_API_HASH</code>\n"
            "{line}"
        ),
        "qr_prompt": (
            "<b>Scan this QR code</b>\n"
            "{line}\n"
            "1. Open Telegram on phone\n"
            "2. Settings - Devices - Link Desktop Device\n"
            "3. Point camera at QR\n\n"
            "Time left: <b>{timeout} sec</b>\n"
            "{line}"
        ),
        "qr_refreshed": (
            "<b>QR refreshed</b>\n"
            "{line}\n"
            "Old one expired, scan new one.\n"
            "Time left: <b>{time_left} sec</b>\n"
            "{line}"
        ),
        "auth_success": (
            "<b>Auth Success</b>\n"
            "{line}\n"
            "Name: {name}\n"
            "ID: <code>{user_id}</code>\n"
            "Username: {username}\n"
            "DC: <code>{dc_id}</code>\n"
            "{line}\n"
            "<b>Auth Key (HEX):</b>\n"
            "<code>{auth_key_hex}</code>\n"
            "{line}\n"
            "<b>Auth Key SHA256:</b>\n"
            "<code>{auth_key_sha}</code>\n"
            "{line}\n"
            "<b>Save this and delete this message.</b>"
        ),
        "auth_timeout": (
            "<b>Timeout</b>\n{line}\n"
            "QR expired. Try again.\n{line}"
        ),
        "auth_error": (
            "<b>Error</b>\n{line}\n"
            "Details: <code>{error}</code>\n"
            "Try again.\n{line}"
        ),
        "already_running": (
            "<b>Wait</b>\n{line}\n"
            "Auth already running. Wait.\n{line}"
        ),
        "generating": "<b>Generating QR...</b>",
        "config_updated": "<b>{key} updated.</b>",
        "provide_value": "<b>Provide value.</b>",
        "password_needed": (
            "<b>2FA Password Required</b>\n"
            "{line}\n"
            "Use: <code>{prefix}dumpqr pass YOUR_PASSWORD</code>\n"
            "Attempts left: <b>{attempts}</b>\n"
            "{line}"
        ),
        "wrong_password": (
            "<b>Wrong password!</b>\n"
            "{line}\n"
            "Attempts left: <b>{attempts}</b>\n"
            "Use: <code>{prefix}dumpqr pass YOUR_PASSWORD</code>\n"
            "{line}"
        ),
        "attempts_exhausted": (
            "<b>All password attempts used.</b>\n"
            "{line}\n"
            "Process terminated. Try again.\n"
            "{line}"
        ),
        "provide_password": "<b>Provide password.</b>",
        "no_active_process": "<b>No active QR auth process.</b>",
    }

    strings_ru = {
        "line": "--------------------",
        "help": (
            "<b>QR Auth Dumper v{ver}</b>\n\n"
            "<b>Команды:</b>\n"
            "<code>{prefix}dumpqr</code> — запустить QR авторизацию\n"
            "<code>{prefix}dumpqr pass [пароль]</code> — ввести 2FA пароль\n\n"
            "<b>Настройки:</b>\n"
            "<code>{prefix}qrauth</code> — эта справка\n"
            "<code>{prefix}qrauth status</code> — статус\n"
            "<code>{prefix}qrauth id [значение]</code> — установить API_ID\n"
            "<code>{prefix}qrauth hash [значение]</code> — установить API_HASH\n"
            "<code>{prefix}qrauth timeout [сек]</code> — таймаут QR\n"
        ),
        "status": (
            "<b>Статус QRAuthDumper</b>\n"
            "{line}\n"
            "API_ID: <code>{api_id}</code>\n"
            "API_HASH: <code>{api_hash}</code>\n"
            "Таймаут: <code>{timeout}</code> сек\n"
            "Обновление: <code>{refresh}</code> сек\n"
            "Попытки пароля: <code>{max_attempts}</code>\n"
            "Статус: {status}\n"
            "{line}"
        ),
        "no_config": (
            "<b>Настройки не заданы</b>\n"
            "{line}\n"
            "Требуются API_ID и API_HASH.\n"
            "<code>{prefix}qrauth id ВАШ_API_ID</code>\n"
            "<code>{prefix}qrauth hash ВАШ_API_HASH</code>\n"
            "{line}"
        ),
        "qr_prompt": (
            "<b>Отсканируйте этот QR код</b>\n"
            "{line}\n"
            "1. Откройте Telegram на телефоне\n"
            "2. Настройки - Устройства - Подключить устройство\n"
            "3. Наведите камеру на QR\n\n"
            "Осталось: <b>{timeout} сек</b>\n"
            "{line}"
        ),
        "qr_refreshed": (
            "<b>QR обновлён</b>\n"
            "{line}\n"
            "Старый истёк, сканируйте новый.\n"
            "Осталось: <b>{time_left} сек</b>\n"
            "{line}"
        ),
        "auth_success": (
            "<b>Авторизация успешна</b>\n"
            "{line}\n"
            "Имя: {name}\n"
            "ID: <code>{user_id}</code>\n"
            "Юзернейм: {username}\n"
            "DC: <code>{dc_id}</code>\n"
            "{line}\n"
            "<b>Auth Key (HEX):</b>\n"
            "<code>{auth_key_hex}</code>\n"
            "{line}\n"
            "<b>Auth Key SHA256:</b>\n"
            "<code>{auth_key_sha}</code>\n"
            "{line}\n"
            "<b>Сохраните это и удалите сообщение.</b>"
        ),
        "auth_timeout": (
            "<b>Таймаут</b>\n{line}\n"
            "QR истёк. Попробуйте снова.\n{line}"
        ),
        "auth_error": (
            "<b>Ошибка</b>\n{line}\n"
            "Детали: <code>{error}</code>\n"
            "Попробуйте снова.\n{line}"
        ),
        "already_running": (
            "<b>Подождите</b>\n{line}\n"
            "Авторизация уже запущена. Ожидайте.\n{line}"
        ),
        "generating": "<b>Генерация QR...</b>",
        "config_updated": "<b>{key} обновлён.</b>",
        "provide_value": "<b>Укажите значение.</b>",
        "password_needed": (
            "<b>Требуется 2FA пароль</b>\n"
            "{line}\n"
            "Используйте: <code>{prefix}dumpqr pass ВАШ_ПАРОЛЬ</code>\n"
            "Осталось попыток: <b>{attempts}</b>\n"
            "{line}"
        ),
        "wrong_password": (
            "<b>Неверный пароль!</b>\n"
            "{line}\n"
            "Осталось попыток: <b>{attempts}</b>\n"
            "Используйте: <code>{prefix}dumpqr pass ВАШ_ПАРОЛЬ</code>\n"
            "{line}"
        ),
        "attempts_exhausted": (
            "<b>Все попытки пароля исчерпаны.</b>\n"
            "{line}\n"
            "Процесс завершён. Попробуйте снова.\n"
            "{line}"
        ),
        "provide_password": "<b>Укажите пароль.</b>",
        "no_active_process": "<b>Нет активного процесса QR авторизации.</b>",
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

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        me = await client.get_me()
        self._owner_id = me.id
        logger.info("[QRAuth] Module loaded, owner id=%d", self._owner_id)

    def _get_topic_id(self, message: Message):
        reply_to = getattr(message, "reply_to", None)
        if reply_to:
            return getattr(reply_to, "reply_to_top_id", None) or getattr(
                reply_to, "reply_to_msg_id", None
            )
        return None

    def _make_qr(self, url: str) -> io.BytesIO:
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
        buf.seek(0)
        buf.name = "qr_auth.png"
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
        line = self.strings["line"]
        fn = getattr(user, "first_name", "") or ""
        ln = getattr(user, "last_name", "") or ""
        name = f"{fn} {ln}".strip() or "Unknown"
        uname = getattr(user, "username", None)
        uname_s = f"@{uname}" if uname else "---"
        uid = getattr(user, "id", 0)
        return self.strings["auth_success"].format(
            line=line,
            name=_escape(name),
            user_id=uid,
            username=_escape(uname_s),
            dc_id=dc_id,
            auth_key_hex=hex_key,
            auth_key_sha=sha,
        )

    async def _run_qr(self, api_id, api_hash, send_func, delete_func, uid):
        timeout = int(self.config["QR_TIMEOUT"])
        refresh = QR_REFRESH
        line = self.strings["line"]
        max_attempts = int(self.config["MAX_PASSWORD_ATTEMPTS"])
        prefix = self.get_prefix()

        tc = TelegramClient(
            StringSession(),
            api_id,
            api_hash,
            device_model="QRAuthDumper",
            system_version="By @FireJester",
            app_version=f"v{'.'.join(map(str, __version__))}",
        )

        try:
            await tc.connect()
            logger.info("[QRAuth] Temp client connected")

            qr = await tc.qr_login()
            logger.info("[QRAuth] QR login initiated")

            img = self._make_qr(qr.url)
            qr_msg = await send_func(
                img,
                self.strings["qr_prompt"].format(line=line, timeout=timeout),
            )
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
                    logger.info(
                        "[QRAuth] QR expired, recreating, elapsed=%d", elapsed
                    )
                    try:
                        await qr.recreate()
                        new_img = self._make_qr(qr.url)
                        try:
                            await delete_func(qr_msg)
                        except Exception:
                            pass
                        tl = timeout - elapsed
                        qr_msg = await send_func(
                            new_img,
                            self.strings["qr_refreshed"].format(
                                line=line, time_left=tl
                            ),
                        )
                    except Exception as e:
                        logger.warning("[QRAuth] QR recreate failed: %s", e)
                except Exception as e:
                    logger.error("[QRAuth] Wait error: %s", e, exc_info=True)
                    raise

            try:
                await delete_func(qr_msg)
            except Exception:
                pass

            if need_2fa:
                self._pending_2fa[uid] = {
                    "client": tc,
                    "attempts_left": max_attempts,
                }
                return self.strings["password_needed"].format(
                    line=line, attempts=max_attempts, prefix=prefix
                )

            if user is None:
                try:
                    await tc.disconnect()
                except Exception:
                    pass
                logger.info("[QRAuth] Timeout")
                return self.strings["auth_timeout"].format(line=line)

            return await self._finalize_auth(tc, user)

        except Exception:
            try:
                await tc.disconnect()
            except Exception:
                pass
            raise

    async def _finalize_auth(self, tc, user):
        try:
            logger.info(
                "[QRAuth] Success: %s id=%d",
                getattr(user, "first_name", "?"),
                getattr(user, "id", 0),
            )

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
                logger.info("[QRAuth] Temp client disconnected")
            except Exception:
                pass

    async def _handle_2fa(self, uid, password):
        pending = self._pending_2fa.get(uid)
        if not pending:
            return None, False

        tc = pending["client"]
        line = self.strings["line"]
        prefix = self.get_prefix()

        try:
            await tc.sign_in(password=password)
            user = await tc.get_me()
            del self._pending_2fa[uid]
            result = await self._finalize_auth(tc, user)
            return result, True
        except PasswordHashInvalidError:
            pending["attempts_left"] -= 1
            if pending["attempts_left"] <= 0:
                try:
                    await tc.disconnect()
                except Exception:
                    pass
                del self._pending_2fa[uid]
                return self.strings["attempts_exhausted"].format(line=line), True
            return (
                self.strings["wrong_password"].format(
                    line=line, attempts=pending["attempts_left"], prefix=prefix
                ),
                False,
            )
        except Exception:
            try:
                await tc.disconnect()
            except Exception:
                pass
            if uid in self._pending_2fa:
                del self._pending_2fa[uid]
            raise

    async def _cleanup_session(self, uid):
        pending = self._pending_2fa.pop(uid, None)
        if pending:
            try:
                await pending["client"].disconnect()
            except Exception:
                pass
        self._active_sessions.pop(uid, None)

    @loader.command(
        ru_doc="QR Auth Dumper — справка и настройки",
        en_doc="QR Auth Dumper — help and config",
    )
    async def qrauth(self, message: Message):
        """QR Auth Dumper help and configuration"""
        args = utils.get_args_raw(message).split()
        prefix = self.get_prefix()
        if not args:
            ver = ".".join(map(str, __version__))
            await utils.answer(
                message,
                self.strings["help"].format(ver=ver, prefix=prefix),
            )
            return

        cmd = args[0].lower()

        if cmd == "status":
            await self._cmd_status(message)
        elif cmd == "id":
            await self._cmd_set(message, args, "API_ID", is_int=True)
        elif cmd == "hash":
            await self._cmd_set(message, args, "API_HASH", is_int=False)
        elif cmd == "timeout":
            await self._cmd_set(message, args, "QR_TIMEOUT", is_int=True)
        else:
            ver = ".".join(map(str, __version__))
            await utils.answer(
                message,
                self.strings["help"].format(ver=ver, prefix=prefix),
            )

    async def _cmd_status(self, message: Message):
        api_id = self.config["API_ID"]
        api_hash = str(self.config["API_HASH"])

        if api_id and api_hash:
            masked = (
                api_hash[:4] + "..." + api_hash[-4:]
                if len(api_hash) > 8
                else "***"
            )
            status = "Ready"
        else:
            masked = "Not set"
            status = "Not configured"

        await utils.answer(
            message,
            self.strings["status"].format(
                line=self.strings["line"],
                api_id=api_id or "Not set",
                api_hash=masked,
                timeout=self.config["QR_TIMEOUT"],
                refresh=QR_REFRESH,
                max_attempts=self.config["MAX_PASSWORD_ATTEMPTS"],
                status=status,
            ),
        )

    async def _cmd_set(self, message: Message, args, key, is_int=False):
        val = args[1] if len(args) > 1 else None
        if not val:
            reply = await message.get_reply_message()
            if reply:
                val = (reply.text or "").strip()
        if not val:
            await utils.answer(message, self.strings["provide_value"])
            return
        try:
            if is_int:
                val = int(val)
            self.config[key] = val
            try:
                await message.delete()
            except Exception:
                pass
            topic_id = self._get_topic_id(message)
            await self._client.send_message(
                message.chat_id,
                self.strings["config_updated"].format(key=key),
                parse_mode="html",
                reply_to=topic_id,
            )
        except ValueError:
            await utils.answer(message, "<b>Error:</b> Invalid value")

    @loader.command(
        ru_doc="Запуск QR авторизации / ввод 2FA пароля",
        en_doc="Start QR auth / provide 2FA password",
    )
    async def dumpqr(self, message: Message):
        """Start QR authentication or provide 2FA password"""
        args = utils.get_args_raw(message).split()
        line = self.strings["line"]
        prefix = self.get_prefix()
        uid = self._owner_id
        peer = message.peer_id
        topic_id = self._get_topic_id(message)

        if args and args[0].lower() == "pass":
            pwd = args[1] if len(args) > 1 else None
            if not pwd:
                await utils.answer(message, self.strings["provide_password"])
                return
            if uid not in self._pending_2fa:
                await utils.answer(message, self.strings["no_active_process"])
                return
            try:
                await message.delete()
            except Exception:
                pass
            try:
                result_text, is_final = await self._handle_2fa(uid, pwd)
                await self._client.send_message(
                    peer,
                    result_text,
                    parse_mode="html",
                    reply_to=topic_id,
                )
                if is_final:
                    self._active_sessions.pop(uid, None)
            except Exception as e:
                logger.error("[QRAuth] 2FA error: %s", e, exc_info=True)
                await self._client.send_message(
                    peer,
                    self.strings["auth_error"].format(
                        line=line, error=_escape(str(e))
                    ),
                    parse_mode="html",
                    reply_to=topic_id,
                )
                await self._cleanup_session(uid)
            return

        if uid in self._pending_2fa:
            await self._cleanup_session(uid)

        api_id = self.config["API_ID"]
        api_hash = self.config["API_HASH"]

        if not api_id or not api_hash:
            await utils.answer(
                message,
                self.strings["no_config"].format(line=line, prefix=prefix),
            )
            return

        if self._active_sessions.get(uid):
            await utils.answer(
                message, self.strings["already_running"].format(line=line)
            )
            return

        await utils.answer(message, self.strings["generating"])
        try:
            await message.delete()
        except Exception:
            pass

        asyncio.create_task(
            self._dumpqr_task(uid, peer, topic_id, api_id, api_hash)
        )

    async def _dumpqr_task(self, uid, peer, topic_id, api_id, api_hash):
        line = self.strings["line"]
        self._active_sessions[uid] = True

        try:

            async def send_func(file_obj, caption):
                if file_obj:
                    return await self._client.send_file(
                        peer,
                        file_obj,
                        caption=caption,
                        parse_mode="html",
                        reply_to=topic_id,
                    )
                else:
                    return await self._client.send_message(
                        peer,
                        caption,
                        parse_mode="html",
                        reply_to=topic_id,
                    )

            async def delete_func(msg):
                try:
                    await msg.delete()
                except Exception:
                    pass

            result_text = await self._run_qr(
                int(api_id), str(api_hash), send_func, delete_func, uid
            )

            await self._client.send_message(
                peer,
                result_text,
                parse_mode="html",
                reply_to=topic_id,
            )

        except Exception as e:
            logger.error("[QRAuth] dumpqr task error: %s", e, exc_info=True)
            try:
                await self._client.send_message(
                    peer,
                    self.strings["auth_error"].format(
                        line=line, error=_escape(str(e))
                    ),
                    parse_mode="html",
                    reply_to=topic_id,
                )
            except Exception:
                pass
            await self._cleanup_session(uid)
        finally:
            if uid not in self._pending_2fa:
                self._active_sessions.pop(uid, None)
