__version__ = (1, 0, 5)
# meta developer: FireJester.t.me

import io
import logging
import asyncio
import hashlib
import base64
import struct
import ipaddress
import subprocess
import sys

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Message

from aiogram.types import Message as AiogramMessage

from .. import loader, utils

logger = logging.getLogger(__name__)


def _ensure_libs():
    libs = {"qrcode": "qrcode[pil]", "PIL": "Pillow"}
    for mod, pkg in libs.items():
        try:
            __import__(mod)
        except ImportError:
            logger.info("[QRAuth] Installing %s...", pkg)
            try:
                subprocess.check_call(
                    [sys.executable, "-m", "pip", "install", pkg, "-q"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                logger.info("[QRAuth] %s installed", pkg)
            except Exception as e:
                logger.error("[QRAuth] Failed to install %s: %s", pkg, e)


_ensure_libs()


def _escape(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


@loader.tds
class QRAuthDumper(loader.Module):
    """QR auth dumper. Send /dumpqr to ur inline bot, scan QR, get session."""

    strings = {
        "name": "QRAuthDumper",
        "line": "--------------------",
        "help": (
            "<b>QR Auth Dumper v{ver}</b>\n\n"
            "<b>Via bot:</b>\n"
            "Send <code>/dumpqr</code> to inline bot DM\n\n"
            "<b>Via userbot:</b>\n"
            "<code>.qrstart</code> -- fallback command\n\n"
            "<b>Config:</b>\n"
            "<code>.qrauth</code> -- this help\n"
            "<code>.qrauth status</code> -- status\n"
            "<code>.qrauth id [val]</code> -- set API_ID\n"
            "<code>.qrauth hash [val]</code> -- set API_HASH\n"
            "<code>.qrauth timeout [sec]</code> -- QR timeout\n"
            "<code>.qrauth refresh [sec]</code> -- QR refresh interval\n"
        ),
        "status": (
            "<b>QRAuthDumper Status</b>\n"
            "{line}\n"
            "API_ID: <code>{api_id}</code>\n"
            "API_HASH: <code>{api_hash}</code>\n"
            "Timeout: <code>{timeout}</code> sec\n"
            "Refresh: <code>{refresh}</code> sec\n"
            "Bot: <code>{bot_status}</code>\n"
            "Status: {status}\n"
            "{line}"
        ),
        "no_config": (
            "<b>Config not set</b>\n"
            "{line}\n"
            "API_ID and API_HASH required.\n"
            "<code>.qrauth id YOUR_API_ID</code>\n"
            "<code>.qrauth hash YOUR_API_HASH</code>\n"
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
            "<b>String Session:</b>\n"
            "<code>{string_session}</code>\n"
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
        "not_owner": "<b>Access denied.</b>",
        "config_updated": "<b>{key} updated:</b> <code>{value}</code>",
        "provide_value": "<b>Provide value.</b>",
    }

    _DC_IP_MAP = {
        1: "149.154.175.53",
        2: "149.154.167.51",
        3: "149.154.175.100",
        4: "149.154.167.91",
        5: "91.108.56.130",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            "API_ID", 2040, "Telegram API ID",
            "API_HASH", "b18441a1ff607e10a989891a5462e627", "Telegram API Hash",
            "QR_TIMEOUT", 60, "QR scan timeout in seconds",
            "QR_REFRESH", 15, "QR refresh interval in seconds",
        )
        self._owner_id = None
        self._active_sessions = {}
        self.inline_bot = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        me = await client.get_me()
        self._owner_id = me.id
        logger.info("[QRAuth] Module loaded, owner id=%d", self._owner_id)

        if hasattr(self, "inline") and hasattr(self.inline, "bot"):
            self.inline_bot = self.inline.bot
            logger.info("[QRAuth] Inline bot found")
        else:
            logger.warning("[QRAuth] No inline bot, /dumpqr unavailable")

    def _get_topic_id(self, message: Message):
        reply_to = getattr(message, 'reply_to', None)
        if reply_to:
            return getattr(reply_to, 'reply_to_top_id', None) or getattr(reply_to, 'reply_to_msg_id', None)
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
            if not session_str or not session_str.startswith('1'):
                return None
            string = session_str[1:]
            padded = string + '=' * (-len(string) % 4)
            data = base64.urlsafe_b64decode(padded)
            if len(data) == 263:
                dc_id, ip_bytes, port, auth_key = struct.unpack('>B4sH256s', data)
                ip = str(ipaddress.IPv4Address(ip_bytes))
            elif len(data) == 275:
                dc_id, ip_bytes, port, auth_key = struct.unpack('>B16sH256s', data)
                ip = str(ipaddress.IPv6Address(ip_bytes))
            else:
                return None
            return {'dc_id': dc_id, 'ip': ip, 'port': port, 'auth_key': auth_key}
        except Exception as e:
            logger.error("[QRAuth] parse error: %s", e)
            return None

    def _extract_hex(self, client: TelegramClient) -> tuple:
        try:
            session = client.session
            if hasattr(session, '_auth_key') and session._auth_key:
                key_data = session._auth_key.key
                dc = getattr(session, '_dc_id', None)
                return key_data.hex(), dc
            saved = session.save()
            if saved:
                parsed = self._parse_string_session(saved)
                if parsed:
                    return parsed['auth_key'].hex(), parsed['dc_id']
            return "FAILED_TO_EXTRACT", None
        except Exception as e:
            logger.error("[QRAuth] extract error: %s", e)
            return f"ERROR: {e}", None

    def _format_result(self, user, ss, hex_key, dc_id, sha):
        line = self.strings["line"]
        fn = getattr(user, 'first_name', '') or ''
        ln = getattr(user, 'last_name', '') or ''
        name = f"{fn} {ln}".strip() or "Unknown"
        uname = getattr(user, 'username', None)
        uname_s = f"@{uname}" if uname else "---"
        uid = getattr(user, 'id', 0)
        return self.strings["auth_success"].format(
            line=line, name=_escape(name), user_id=uid,
            username=_escape(uname_s), dc_id=dc_id,
            auth_key_hex=hex_key, string_session=ss,
            auth_key_sha=sha,
        )

    async def _run_qr(self, api_id: int, api_hash: str, send_func, delete_func):
        timeout = int(self.config["QR_TIMEOUT"])
        refresh = int(self.config["QR_REFRESH"])
        line = self.strings["line"]

        tc = TelegramClient(
            StringSession(), api_id, api_hash,
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

            while elapsed < timeout:
                wt = min(refresh, timeout - elapsed)
                try:
                    user = await asyncio.wait_for(qr.wait(), timeout=wt)
                    logger.info("[QRAuth] QR scanned!")
                    break
                except asyncio.TimeoutError:
                    elapsed += wt
                    if elapsed >= timeout:
                        break
                    logger.info("[QRAuth] QR expired, recreating, elapsed=%d", elapsed)
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

            if user is None:
                logger.info("[QRAuth] Timeout")
                return self.strings["auth_timeout"].format(line=line)

            logger.info(
                "[QRAuth] Success: %s id=%d",
                getattr(user, 'first_name', '?'),
                getattr(user, 'id', 0),
            )

            ss = tc.session.save()
            hex_key, dc_id = self._extract_hex(tc)

            if dc_id is None:
                parsed = self._parse_string_session(ss)
                dc_id = parsed['dc_id'] if parsed else "?"

            try:
                kb = bytes.fromhex(hex_key)
                sha = hashlib.sha256(kb).hexdigest()
            except Exception:
                sha = "N/A"

            return self._format_result(user, ss, hex_key, dc_id, sha)

        finally:
            try:
                await tc.disconnect()
                logger.info("[QRAuth] Temp client disconnected")
            except Exception:
                pass

    async def aiogram_watcher(self, message: AiogramMessage):

        if not message.text:
            return
        if not self.inline_bot:
            return

        text = message.text.strip()
        if not text.startswith("/dumpqr"):
            return

        uid = message.from_user.id
        cid = message.chat.id
        logger.info("[QRAuth] /dumpqr from uid=%d cid=%d", uid, cid)

        if uid != self._owner_id:
            await message.answer(
                self.strings["not_owner"], parse_mode="HTML"
            )
            return

        api_id = self.config["API_ID"]
        api_hash = self.config["API_HASH"]
        line = self.strings["line"]

        if not api_id or not api_hash:
            await message.answer(
                self.strings["no_config"].format(line=line),
                parse_mode="HTML",
            )
            return

        if self._active_sessions.get(uid):
            await message.answer(
                self.strings["already_running"].format(line=line),
                parse_mode="HTML",
            )
            return

        self._active_sessions[uid] = True

        try:
            status_msg = await message.answer(
                self.strings["generating"], parse_mode="HTML"
            )

            async def send_func(file_obj, caption):
                if file_obj:
                    from aiogram.types import BufferedInputFile
                    file_obj.seek(0)
                    data = file_obj.read()
                    input_file = BufferedInputFile(data, filename="qr.png")
                    return await self.inline_bot.send_photo(
                        cid, photo=input_file,
                        caption=caption, parse_mode="HTML",
                    )
                else:
                    return await self.inline_bot.send_message(
                        cid, caption, parse_mode="HTML",
                    )

            async def delete_func(msg):
                try:
                    await msg.delete()
                except Exception:
                    pass

            try:
                await status_msg.delete()
            except Exception:
                pass

            result_text = await self._run_qr(
                int(api_id), str(api_hash), send_func, delete_func
            )

            await self.inline_bot.send_message(
                cid, result_text, parse_mode="HTML"
            )

        except Exception as e:
            logger.error("[QRAuth] /dumpqr error: %s", e, exc_info=True)
            try:
                await self.inline_bot.send_message(
                    cid,
                    self.strings["auth_error"].format(
                        line=line, error=_escape(str(e))
                    ),
                    parse_mode="HTML",
                )
            except Exception:
                pass
        finally:
            self._active_sessions.pop(uid, None)

    @loader.command(ru_doc="QR Auth Dumper - help and config")
    async def qrauth(self, message: Message):
        args = utils.get_args_raw(message).split()
        if not args:
            ver = '.'.join(map(str, __version__))
            await utils.answer(message, self.strings["help"].format(ver=ver))
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
        elif cmd == "refresh":
            await self._cmd_set(message, args, "QR_REFRESH", is_int=True)
        else:
            ver = '.'.join(map(str, __version__))
            await utils.answer(message, self.strings["help"].format(ver=ver))

    async def _cmd_status(self, message: Message):
        api_id = self.config["API_ID"]
        api_hash = str(self.config["API_HASH"])

        if api_id and api_hash:
            masked = api_hash[:4] + "..." + api_hash[-4:] if len(api_hash) > 8 else "***"
            status = "Ready"
        else:
            masked = "Not set"
            status = "Not configured"

        bot_status = "Available" if self.inline_bot else "Not found"

        await utils.answer(message, self.strings["status"].format(
            line=self.strings["line"],
            api_id=api_id or "Not set",
            api_hash=masked,
            timeout=self.config["QR_TIMEOUT"],
            refresh=self.config["QR_REFRESH"],
            bot_status=bot_status,
            status=status,
        ))

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
            await utils.answer(message, self.strings["config_updated"].format(
                key=key, value=val
            ))
        except ValueError:
            await utils.answer(message, "<b>Error:</b> Invalid value")

    @loader.command(ru_doc="QR auth fallback via userbot")
    async def qrstart(self, message: Message):
        """Fallback QR auth through userbot messages"""
        api_id = self.config["API_ID"]
        api_hash = self.config["API_HASH"]
        line = self.strings["line"]

        if not api_id or not api_hash:
            await utils.answer(message, self.strings["no_config"].format(line=line))
            return

        uid = self._owner_id
        if self._active_sessions.get(uid):
            await utils.answer(message, self.strings["already_running"].format(line=line))
            return

        peer = message.peer_id
        topic_id = self._get_topic_id(message)

        await utils.answer(message, self.strings["generating"])
        try:
            await message.delete()
        except Exception:
            pass

        asyncio.create_task(self._qrstart_task(uid, peer, topic_id, api_id, api_hash))

    async def _qrstart_task(self, uid, peer, topic_id, api_id, api_hash):
        line = self.strings["line"]
        self._active_sessions[uid] = True

        try:
            async def send_func(file_obj, caption):
                if file_obj:
                    return await self._client.send_file(
                        peer, file_obj,
                        caption=caption, parse_mode="html",
                        reply_to=topic_id,
                    )
                else:
                    return await self._client.send_message(
                        peer, caption, parse_mode="html",
                        reply_to=topic_id,
                    )

            async def delete_func(msg):
                try:
                    await msg.delete()
                except Exception:
                    pass

            result_text = await self._run_qr(
                int(api_id), str(api_hash), send_func, delete_func
            )

            await self._client.send_message(
                peer, result_text, parse_mode="html",
                reply_to=topic_id,
            )

        except Exception as e:
            logger.error("[QRAuth] qrstart task error: %s", e, exc_info=True)
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
        finally:
            self._active_sessions.pop(uid, None)