__version__ = (2, 0, 0)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/Robber/MetaBanner.jpeg

import logging
import asyncio
import os
import re
import tempfile
import shutil
import time
import zipfile
import io
import base64
import ipaddress
import struct
import sqlite3

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl import functions, types
from telethon.tl.types import (
    MessageMediaWebPage,
    MessageMediaContact,
    MessageMediaGeo,
    MessageMediaGeoLive,
    MessageMediaPoll,
)
from telethon.errors import FloodWaitError, AuthKeyUnregisteredError, UserDeactivatedBanError
from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

STORY_LINK_RE = re.compile(r"t\.me/([^/]+)/s/(\d+)")
POST_LINK_RE = re.compile(r"t\.me/(?:c/)?([^/]+)/(\d+)")
STRING_SESSION_PATTERN = re.compile(r"1[A-Za-z0-9_-]{200,}={0,2}")
HEX_KEY_PATTERN = re.compile(r"[0-9a-fA-F]{512}")

SKIP_MEDIA_TYPES = (
    MessageMediaContact,
    MessageMediaGeo,
    MessageMediaGeoLive,
    MessageMediaPoll,
)

BATCH_SIZE = 100
FLOOD_EXTRA = 5
PAUSE_EVERY = 100
PAUSE_DURATION = 5
MAX_ACCOUNTS = 5

DC_IP_MAP = {
    1: "149.154.175.53",
    2: "149.154.167.51",
    3: "149.154.175.100",
    4: "149.154.167.91",
    5: "91.108.56.130",
}


def _safe_disconnect(client):
    if client:
        try:
            asyncio.ensure_future(client.disconnect())
        except Exception:
            pass


@loader.tds
class Robber(loader.Module):
    """Fully steal channels and stories, support for private channels and multi-account"""

    strings = {
        "name": "Robber",
        "main_menu": "<b>Robber - Channel & Content Stealer</b>\nSelect operation:",
        "account_select": "<b>Choose account to work with:</b>",
        "connected_select": "<b>Select connected account:</b>",
        "btn_double": "Copy Channel",
        "btn_steal": "Steal Post",
        "btn_sdl": "Download Story",
        "btn_back": "Back to Menu",
        "btn_close": "Close",
        "btn_owner": "Owner",
        "btn_connected": "Connected",
        "connect_menu": "<b>Connect Session</b>\nDetected: <code>{type}</code>\nConnect this session?",
        "connect_select_dc": "<b>Select DC for HEX session:</b>",
        "connecting": "<b>Connecting session...</b>",
        "connect_ok": "<b>Connected!</b>\nSlot: <code>{slot}</code>\nUser: {user}\nID: <code>{uid}</code>",
        "connect_fail_invalid": "<b>Error:</b> Session is invalid or not authorized",
        "connect_fail_banned": "<b>Error:</b> Account is banned",
        "connect_fail_revoked": "<b>Error:</b> Session revoked",
        "connect_fail_error": "<b>Error:</b> <code>{err}</code>",
        "connect_slots_full": "<b>Error:</b> Max 5 accounts connected",
        "slot_info": "Slot {slot} | {name} | ID: {uid}",
        "double_start": "<b>Starting copy...</b>\nFetching messages...",
        "double_progress": "<b>Copying...</b>\nProcessed: {done} / {total}\nSkipped: {skipped}\nAlbums: {albums}",
        "double_done": "<b>Copy complete!</b>\nProcessed: {done}\nSkipped: {skipped}\nAlbums: {albums}\nLink: {link}",
        "double_no_id": "<b>Error:</b> Provide channel ID or username",
        "double_error": "<b>Error:</b> <code>{err}</code>",
        "double_send_error": "<b>Copying...</b>\nProcessed: {done} / {total}\nSkipped: {skipped}\nAlbums: {albums}\n<b>Send error on msg {msg_id}:</b>\n<code>{err}</code>",
        "input_channel": "Enter channel ID or username:",
        "input_post": "Enter post link (e.g., https://t.me/c/3872609933/8):",
        "input_story": "Enter username, user ID or story link (e.g., https://t.me/username/s/53):",
        "steal_bad_link": "<b>Error:</b> Cannot parse post link",
        "steal_not_found": "<b>Error:</b> Post not found",
        "steal_error": "<b>Error:</b> <code>{err}</code>",
        "steal_processing": "<b>Stealing post...</b>",
        "steal_done": "<b>Post stolen successfully!</b>",
        "sdl_bad_input": "<b>Error:</b> Cannot parse input",
        "sdl_not_found": "<b>Error:</b> Story not found or has no media",
        "sdl_download_fail": "<b>Error:</b> Download failed",
        "sdl_processing": "<b>Downloading story...</b>",
        "sdl_done": "<b>Story downloaded successfully!</b>",
        "sdl_select_mode": "<b>Select download mode:</b>\nUser: {user}",
        "sdl_select_export": "<b>Select export format:</b>",
        "btn_sdl_one": "One Story (by link)",
        "btn_sdl_all": "All Stories",
        "btn_sdl_export_one": "Send one by one",
        "btn_sdl_export_zip": "ZIP archive",
        "sdl_progress_dl": "<b>Downloading stories...</b>\n{done} / {total} downloaded",
        "sdl_progress_zip": "<b>Creating ZIP archive...</b>\n{done} / {total} files",
        "sdl_progress_upload": "<b>Uploading to Telegram...</b>\n{cur:.1f} / {total:.1f} MB",
        "sdl_done_all": "<b>Done!</b>\n{count} stories sent",
        "sdl_done_zip": "<b>Done!</b>\nArchive sent: {name}",
        "sdl_input_story_link": "Enter story link:",
        "sdl_no_stories": "<b>No stories found</b>",
        "btn_dc_1": "DC 1",
        "btn_dc_2": "DC 2",
        "btn_dc_3": "DC 3",
        "btn_dc_4": "DC 4",
        "btn_dc_5": "DC 5",
    }

    strings_ru = {
        "main_menu": "<b>Robber - Копировщик каналов и контента</b>\nВыберите операцию:",
        "account_select": "<b>Выберите аккаунт для работы:</b>",
        "connected_select": "<b>Выберите подключённый аккаунт:</b>",
        "btn_double": "Копировать канал",
        "btn_steal": "Украсть пост",
        "btn_sdl": "Скачать историю",
        "btn_back": "Назад в меню",
        "btn_close": "Закрыть",
        "btn_owner": "Владелец",
        "btn_connected": "Подключённые",
        "connect_menu": "<b>Подключение сессии</b>\nОбнаружено: <code>{type}</code>\nПодключить эту сессию?",
        "connect_select_dc": "<b>Выберите DC для HEX сессии:</b>",
        "connecting": "<b>Подключение сессии...</b>",
        "connect_ok": "<b>Подключено!</b>\nСлот: <code>{slot}</code>\nПользователь: {user}\nID: <code>{uid}</code>",
        "connect_fail_invalid": "<b>Ошибка:</b> Сессия невалидна или не авторизована",
        "connect_fail_banned": "<b>Ошибка:</b> Аккаунт заблокирован",
        "connect_fail_revoked": "<b>Ошибка:</b> Сессия отозвана",
        "connect_fail_error": "<b>Ошибка:</b> <code>{err}</code>",
        "connect_slots_full": "<b>Ошибка:</b> Максимум 5 аккаунтов подключено",
        "slot_info": "Слот {slot} | {name} | ID: {uid}",
        "double_start": "<b>Начинаю копирование...</b>\nПолучаю сообщения...",
        "double_progress": "<b>Копирую...</b>\nОбработано: {done} / {total}\nПропущено: {skipped}\nАльбомы: {albums}",
        "double_done": "<b>Копирование завершено!</b>\nОбработано: {done}\nПропущено: {skipped}\nАльбомы: {albums}\nСсылка: {link}",
        "double_no_id": "<b>Ошибка:</b> Укажите ID или юзернейм канала",
        "double_error": "<b>Ошибка:</b> <code>{err}</code>",
        "double_send_error": "<b>Копирую...</b>\nОбработано: {done} / {total}\nПропущено: {skipped}\nАльбомы: {albums}\n<b>Ошибка отправки сообщения {msg_id}:</b>\n<code>{err}</code>",
        "input_channel": "Введите ID или юзернейм канала:",
        "input_post": "Введите ссылку на пост (например, https://t.me/c/3872609933/8):",
        "input_story": "Введите юзернейм, ID пользователя или ссылку на историю:",
        "steal_bad_link": "<b>Ошибка:</b> Не могу распарсить ссылку на пост",
        "steal_not_found": "<b>Ошибка:</b> Пост не найден",
        "steal_error": "<b>Ошибка:</b> <code>{err}</code>",
        "steal_processing": "<b>Крадем пост...</b>",
        "steal_done": "<b>Пост успешно украден!</b>",
        "sdl_bad_input": "<b>Ошибка:</b> Не могу распарсить ввод",
        "sdl_not_found": "<b>Ошибка:</b> История не найдена или без медиа",
        "sdl_download_fail": "<b>Ошибка:</b> Не удалось скачать",
        "sdl_processing": "<b>Скачиваю историю...</b>",
        "sdl_done": "<b>История успешно скачана!</b>",
        "sdl_select_mode": "<b>Выберите режим скачивания:</b>\nПользователь: {user}",
        "sdl_select_export": "<b>Выберите формат экспорта:</b>",
        "btn_sdl_one": "Одна история (по ссылке)",
        "btn_sdl_all": "Все истории",
        "btn_sdl_export_one": "Отправить по одной",
        "btn_sdl_export_zip": "ZIP архив",
        "sdl_progress_dl": "<b>Скачиваю истории...</b>\n{done} / {total} скачано",
        "sdl_progress_zip": "<b>Создаю ZIP архив...</b>\n{done} / {total} файлов",
        "sdl_progress_upload": "<b>Выгружаю в Telegram...</b>\n{cur:.1f} / {total:.1f} МБ",
        "sdl_done_all": "<b>Готово!</b>\n{count} историй отправлено",
        "sdl_done_zip": "<b>Готово!</b>\nАрхив отправлен: {name}",
        "sdl_input_story_link": "Введите ссылку на историю:",
        "sdl_no_stories": "<b>Историй не найдено</b>",
        "btn_dc_1": "DC 1",
        "btn_dc_2": "DC 2",
        "btn_dc_3": "DC 3",
        "btn_dc_4": "DC 4",
        "btn_dc_5": "DC 5",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "CREATE_CHANNEL",
                True,
                "If True creates channel, if False creates group",
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "API_ID",
                2040,
                "Telegram API ID for session connections",
                validator=loader.validators.Integer(),
            ),
            loader.ConfigValue(
                "API_HASH",
                "b18441a1ff607e10a989891a5462e627",
                "Telegram API Hash",
                validator=loader.validators.Hidden(),
            ),
        )
        self._temp_dir = None
        self._client = None
        self._db = None
        self._pending_connect = {}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._temp_dir = os.path.join(tempfile.gettempdir(), "robber_tmp")
        os.makedirs(self._temp_dir, exist_ok=True)

    async def on_unload(self):
        self._wipe_temp()
        slots = self._load_slots()
        for slot_data in slots.values():
            client = slot_data.get("_client")
            if client:
                _safe_disconnect(client)

    def _wipe_temp(self):
        if self._temp_dir and os.path.exists(self._temp_dir):
            try:
                shutil.rmtree(self._temp_dir)
            except Exception:
                pass

    def _tmp_path(self, name):
        return os.path.join(self._temp_dir, name)

    def _load_slots(self) -> dict:
        raw = self._db.get("Robber", "slots", {})
        return raw if isinstance(raw, dict) else {}

    def _save_slots(self, slots: dict):
        save = {k: {sk: sv for sk, sv in v.items() if sk != "_client"} for k, v in slots.items()}
        self._db.set("Robber", "slots", save)

    def _next_slot(self, slots: dict) -> int | None:
        for i in range(1, MAX_ACCOUNTS + 1):
            if str(i) not in slots:
                return i
        return None

    def _find_string_session(self, text):
        if not text:
            return None
        m = STRING_SESSION_PATTERN.search(text)
        return m.group(0) if m else None

    def _find_hex_key(self, text):
        if not text:
            return None
        m = HEX_KEY_PATTERN.search(text)
        return m.group(0) if m else None

    def _parse_string_session(self, s):
        try:
            s = s.strip()
            if not s.startswith("1"):
                return None
            raw = s[1:]
            raw += "=" * (-len(raw) % 4)
            data = base64.urlsafe_b64decode(raw)
            if len(data) == 263:
                dc_id, ip_b, port, auth_key = struct.unpack(">B4sH256s", data)
                ip = str(ipaddress.IPv4Address(ip_b))
            elif len(data) == 275:
                dc_id, ip_b, port, auth_key = struct.unpack(">B16sH256s", data)
                ip = str(ipaddress.IPv6Address(ip_b))
            else:
                return None
            return {"dc_id": dc_id, "ip": ip, "port": port, "auth_key": auth_key}
        except Exception:
            return None

    def _build_string_session(self, dc_id, auth_key):
        try:
            if dc_id not in DC_IP_MAP:
                return None
            if isinstance(auth_key, str):
                auth_key = auth_key.encode("latin-1")
            if len(auth_key) != 256:
                return None
            ip = ipaddress.IPv4Address(DC_IP_MAP[dc_id])
            data = struct.pack(">B4sH256s", dc_id, ip.packed, 443, auth_key)
            return "1" + base64.urlsafe_b64encode(data).decode()
        except Exception:
            return None

    async def _read_session_file(self, path):
        try:
            conn = sqlite3.connect(path)
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sessions'")
            if not cur.fetchone():
                conn.close()
                return None
            cur.execute("SELECT dc_id, auth_key FROM sessions LIMIT 1")
            row = cur.fetchone()
            conn.close()
            if row:
                dc_id, auth_key = row
                if isinstance(auth_key, str):
                    auth_key = auth_key.encode("latin-1")
                if not auth_key or len(auth_key) != 256:
                    return None
                return {"dc_id": dc_id, "auth_key": auth_key}
            return None
        except Exception:
            return None

    async def _connect_and_test(self, string_session: str):
        client = TelegramClient(
            StringSession(string_session),
            int(self.config["API_ID"]),
            self.config["API_HASH"],
            device_model="RobberModule",
            system_version="By @I_execute",
            app_version=f"v{'.'.join(map(str, __version__))}",
        )
        await asyncio.wait_for(client.connect(), timeout=15)
        me = await asyncio.wait_for(client.get_me(), timeout=10)
        return client, me

    async def _get_slot_client(self, slot: str):
        slots = self._load_slots()
        data = slots.get(slot)
        if not data:
            return None
        if data.get("_client"):
            return data["_client"]
        string_session = data.get("string")
        if not string_session:
            return None
        try:
            client, me = await self._connect_and_test(string_session)
            slots[slot]["_client"] = client
            return client
        except Exception:
            return None

    def _get_working_client(self, slot: str | None):
        if slot is None:
            return self._client
        slots = self._load_slots()
        data = slots.get(slot)
        if data and data.get("_client"):
            return data["_client"]
        return self._client

    async def _safe(self, coro, client=None):
        c = client or self._client
        while True:
            try:
                return await coro
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds + FLOOD_EXTRA)
            except Exception:
                raise

    def _get_html(self, msg):
        if not msg.message:
            return None
        try:
            if msg.entities:
                from telethon.extensions import html
                return html.unparse(msg.message, msg.entities)
        except Exception:
            pass
        return msg.message

    async def _dl(self, media, fname, client=None):
        c = client or self._client
        path = self._tmp_path(fname)
        try:
            result = await c.download_media(media, file=path)
            if result and os.path.exists(result):
                return result
        except Exception as e:
            logger.warning(f"[Robber] dl failed: {e}")
        return None

    async def _create_target(self, title, about, client=None):
        c = client or self._client
        as_channel = self.config["CREATE_CHANNEL"]
        result = await self._safe(
            c(functions.channels.CreateChannelRequest(
                title=title, about=about,
                broadcast=as_channel, megagroup=not as_channel,
            )),
            client=c,
        )
        return result.chats[0]

    async def _set_avatar(self, channel, src_entity, client=None):
        c = client or self._client
        try:
            av_path = self._tmp_path(f"av_{channel.id}")
            downloaded = await c.download_profile_photo(src_entity, file=av_path)
            if downloaded and os.path.exists(downloaded):
                try:
                    photo = await self._safe(c.upload_file(downloaded), client=c)
                    await self._safe(
                        c(functions.channels.EditPhotoRequest(
                            channel=channel,
                            photo=types.InputChatUploadedPhoto(file=photo),
                        )),
                        client=c,
                    )
                finally:
                    try:
                        os.remove(downloaded)
                    except Exception:
                        pass
        except Exception as e:
            logger.warning(f"[Robber] avatar failed: {e}")

    async def _send_msg(self, target, msg, id_map, client=None):
        c = client or self._client
        if msg.action is not None:
            return None
        media = msg.media
        text = self._get_html(msg)
        reply_to = None
        if msg.reply_to and hasattr(msg.reply_to, "reply_to_msg_id"):
            reply_to = id_map.get(msg.reply_to.reply_to_msg_id)
        if media and isinstance(media, SKIP_MEDIA_TYPES):
            label = type(media).__name__.replace("MessageMedia", "")
            fallback = f"[{label}]"
            if text:
                fallback = f"{text}\n\n[{label}]"
            return await self._safe(c.send_message(target, fallback, reply_to=reply_to), client=c)
        if media and isinstance(media, MessageMediaWebPage):
            media = None
        if media is not None:
            fname = f"media_{msg.id}"
            path = await self._dl(media, fname, client=c)
            if path:
                try:
                    return await self._safe(
                        c.send_file(target, path, caption=text, parse_mode="html", reply_to=reply_to, force_document=False),
                        client=c,
                    )
                finally:
                    try:
                        os.remove(path)
                    except Exception:
                        pass
            else:
                if text:
                    return await self._safe(c.send_message(target, text, parse_mode="html", reply_to=reply_to), client=c)
                return None
        if text:
            return await self._safe(c.send_message(target, text, parse_mode="html", reply_to=reply_to), client=c)
        return None

    async def _send_album(self, target, album_msgs, id_map, client=None):
        c = client or self._client
        first = album_msgs[0]
        reply_to = None
        if first.reply_to and hasattr(first.reply_to, "reply_to_msg_id"):
            reply_to = id_map.get(first.reply_to.reply_to_msg_id)
        paths = []
        captions = []
        try:
            for i, am in enumerate(album_msgs):
                if am.media and not isinstance(am.media, (MessageMediaWebPage,) + SKIP_MEDIA_TYPES):
                    path = await self._dl(am.media, f"album_{am.id}_{i}", client=c)
                    if path:
                        paths.append(path)
                        captions.append(self._get_html(am) or "")
            if not paths:
                return []
            sent = await self._safe(
                c.send_file(target, paths, caption=captions, parse_mode="html", reply_to=reply_to),
                client=c,
            )
            if not isinstance(sent, list):
                sent = [sent]
            return sent
        finally:
            for p in paths:
                try:
                    os.remove(p)
                except Exception:
                    pass

    async def _resolve_post_link(self, link, client=None):
        c = client or self._client
        m = POST_LINK_RE.search(link)
        if not m:
            return None, None
        chat_part = m.group(1)
        msg_id = int(m.group(2))
        try:
            if chat_part.isdigit():
                entity = await c.get_entity(int(f"-100{chat_part}"))
            else:
                entity = await c.get_entity(chat_part)
        except Exception as e:
            raise Exception(f"Cannot resolve chat: {e}")
        return entity, msg_id

    async def _fetch_album(self, entity, anchor_msg, client=None):
        c = client or self._client
        if not anchor_msg.grouped_id:
            return [anchor_msg]
        group_id = anchor_msg.grouped_id
        album = []
        async for msg in c.iter_messages(entity, min_id=anchor_msg.id - 20, max_id=anchor_msg.id + 20):
            if msg.grouped_id == group_id:
                album.append(msg)
        if not album:
            album = [anchor_msg]
        album.sort(key=lambda m: m.id)
        return album

    async def _resolve_story_user(self, inp: str, client=None):
        c = client or self._client
        inp = inp.strip()
        m = STORY_LINK_RE.search(inp)
        if m:
            username = m.group(1)
            story_id = int(m.group(2))
            peer = (await c(functions.contacts.ResolveUsernameRequest(username))).peer
            return peer, story_id
        if inp.lstrip("-").isdigit():
            uid = int(inp)
            try:
                entity = await c.get_entity(uid)
                peer = await c.get_input_entity(entity)
                return peer, None
            except Exception as e:
                raise Exception(f"Cannot resolve user ID: {e}")
        try:
            peer = (await c(functions.contacts.ResolveUsernameRequest(inp.lstrip("@")))).peer
            return peer, None
        except Exception as e:
            raise Exception(f"Cannot resolve username: {e}")

    async def _fetch_all_stories(self, peer, client=None):
        c = client or self._client
        seen_ids = set()
        stories = []

        try:
            result = await c(functions.stories.GetPeerStoriesRequest(peer=peer))
            for s in result.stories.stories:
                if s.id not in seen_ids:
                    seen_ids.add(s.id)
                    stories.append(s)
        except Exception as e:
            logger.warning(f"[Robber] GetPeerStoriesRequest failed: {e}")

        offset_id = 0
        while True:
            try:
                result = await c(functions.stories.GetPinnedStoriesRequest(
                    peer=peer, offset_id=offset_id, limit=100
                ))
                if not result.stories:
                    break
                for s in result.stories:
                    if s.id not in seen_ids:
                        seen_ids.add(s.id)
                        stories.append(s)
                if len(result.stories) < 100:
                    break
                offset_id = result.stories[-1].id
            except Exception as e:
                logger.warning(f"[Robber] GetPinnedStoriesRequest failed: {e}")
                break

        return stories

    async def _download_story_media(self, story, idx: int, client=None):
        c = client or self._client
        media = getattr(story, "media", None)
        if not media:
            return None
        is_video = isinstance(media, types.MessageMediaDocument)
        ext = ".mp4" if is_video else ".jpg"
        fname = f"story_{idx}{ext}"
        path = self._tmp_path(fname)
        if os.path.exists(path):
            os.remove(path)
        try:
            downloaded = await c.download_media(media, file=path)
            if downloaded and os.path.exists(downloaded):
                return downloaded
        except Exception as e:
            logger.warning(f"[Robber] story dl failed: {e}")
        return None

    def _get_main_markup(self, slot: str | None = None):
        return [
            [{"text": self.strings["btn_double"], "callback": self._cb_double_menu, "args": (slot,), "style": "primary"}],
            [{"text": self.strings["btn_steal"], "callback": self._cb_steal_menu, "args": (slot,), "style": "primary"}],
            [{"text": self.strings["btn_sdl"], "callback": self._cb_sdl_menu, "args": (slot,), "style": "primary"}],
            [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
        ]

    def _slot_label(self, slot: str, data: dict) -> str:
        name = data.get("name", "?")
        uid = data.get("uid", "?")
        return f"{slot}. {name} | {uid}"

    def _get_account_select_markup(self, slots: dict):
        connected_btn = [{"text": self.strings["btn_connected"], "callback": self._cb_connected_select, "style": "success"}]
        markup = [
            [{"text": self.strings["btn_owner"], "callback": self._cb_owner_selected, "style": "primary"}],
            connected_btn,
            [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
        ]
        return markup

    def _get_connected_markup(self, slots: dict):
        markup = []
        styles = ["primary", "success", "primary", "success", "primary"]
        for i, (slot, data) in enumerate(slots.items()):
            label = self._slot_label(slot, data)
            markup.append([{
                "text": label,
                "callback": self._cb_slot_selected,
                "args": (slot,),
                "style": styles[i % len(styles)],
            }])
        markup.append([{"text": self.strings["btn_back"], "callback": self._cb_account_select, "style": "danger"}])
        return markup

    async def _cb_account_select(self, call: InlineCall):
        slots = self._load_slots()
        if not slots:
            await call.edit(self.strings["main_menu"], reply_markup=self._get_main_markup(None))
            return
        await call.edit(self.strings["account_select"], reply_markup=self._get_account_select_markup(slots))

    async def _cb_owner_selected(self, call: InlineCall):
        await call.edit(self.strings["main_menu"], reply_markup=self._get_main_markup(None))

    async def _cb_connected_select(self, call: InlineCall):
        slots = self._load_slots()
        if not slots:
            await call.edit(self.strings["main_menu"], reply_markup=self._get_main_markup(None))
            return
        await call.edit(self.strings["connected_select"], reply_markup=self._get_connected_markup(slots))

    async def _cb_slot_selected(self, call: InlineCall, slot: str):
        slots = self._load_slots()
        data = slots.get(slot, {})
        name = data.get("name", "?")
        uid = data.get("uid", "?")
        text = f"{self.strings['main_menu']}\n<blockquote>Account: {name} | ID: <code>{uid}</code></blockquote>"
        await call.edit(text, reply_markup=self._get_main_markup(slot))

    async def _cb_connect_string(self, call: InlineCall, string_session: str):
        await call.edit(self.strings["connecting"])
        slots = self._load_slots()
        slot = self._next_slot(slots)
        if slot is None:
            await call.edit(self.strings["connect_slots_full"])
            return
        try:
            client, me = await self._connect_and_test(string_session)
            if me is None:
                _safe_disconnect(client)
                await call.edit(self.strings["connect_fail_invalid"])
                return
            first = getattr(me, "first_name", "") or ""
            last = getattr(me, "last_name", "") or ""
            name = f"{first} {last}".strip() or "Unknown"
            slots[str(slot)] = {
                "string": string_session,
                "name": name,
                "uid": me.id,
                "_client": client,
            }
            self._save_slots(slots)
            await call.edit(self.strings["connect_ok"].format(slot=slot, user=name, uid=me.id))
        except AuthKeyUnregisteredError:
            await call.edit(self.strings["connect_fail_revoked"])
        except UserDeactivatedBanError:
            await call.edit(self.strings["connect_fail_banned"])
        except Exception as e:
            await call.edit(self.strings["connect_fail_error"].format(err=str(e)))

    async def _cb_connect_hex_dc(self, call: InlineCall, hex_key: str, dc_id: int):
        auth_key = bytes.fromhex(hex_key)
        string_session = self._build_string_session(dc_id, auth_key)
        if not string_session:
            await call.edit(self.strings["connect_fail_invalid"])
            return
        await self._cb_connect_string(call, string_session)

    async def _cb_connect_hex_select_dc(self, call: InlineCall, hex_key: str):
        await call.edit(
            self.strings["connect_select_dc"],
            reply_markup=[
                [
                    {"text": self.strings["btn_dc_1"], "callback": self._cb_connect_hex_dc, "args": (hex_key, 1), "style": "primary"},
                    {"text": self.strings["btn_dc_2"], "callback": self._cb_connect_hex_dc, "args": (hex_key, 2), "style": "primary"},
                    {"text": self.strings["btn_dc_3"], "callback": self._cb_connect_hex_dc, "args": (hex_key, 3), "style": "primary"},
                ],
                [
                    {"text": self.strings["btn_dc_4"], "callback": self._cb_connect_hex_dc, "args": (hex_key, 4), "style": "primary"},
                    {"text": self.strings["btn_dc_5"], "callback": self._cb_connect_hex_dc, "args": (hex_key, 5), "style": "primary"},
                ],
            ],
        )

    async def _cb_main_menu(self, call: InlineCall, slot: str | None = None):
        slots = self._load_slots()
        if slots:
            await call.edit(self.strings["account_select"], reply_markup=self._get_account_select_markup(slots))
        else:
            await call.edit(self.strings["main_menu"], reply_markup=self._get_main_markup(None))

    async def _cb_double_menu(self, call: InlineCall, slot: str | None = None):
        await call.edit(
            self.strings["main_menu"],
            reply_markup=[
                [{"text": self.strings["btn_double"], "input": self.strings["input_channel"], "handler": self._cb_double_execute, "args": (slot,), "style": "success"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "args": (slot,), "style": "danger"}],
            ],
        )

    async def _cb_steal_menu(self, call: InlineCall, slot: str | None = None):
        await call.edit(
            self.strings["main_menu"],
            reply_markup=[
                [{"text": self.strings["btn_steal"], "input": self.strings["input_post"], "handler": self._cb_steal_execute, "args": (slot,), "style": "success"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "args": (slot,), "style": "danger"}],
            ],
        )

    async def _cb_sdl_menu(self, call: InlineCall, slot: str | None = None):
        await call.edit(
            self.strings["main_menu"],
            reply_markup=[
                [{"text": self.strings["btn_sdl"], "input": self.strings["input_story"], "handler": self._cb_sdl_input, "args": (slot,), "style": "success"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "args": (slot,), "style": "danger"}],
            ],
        )

    async def _cb_double_execute(self, call: InlineCall, channel_input: str, slot: str | None = None):
        channel_input = channel_input.strip()
        if not channel_input:
            await call.answer(self.strings["double_no_id"], show_alert=True)
            return

        client = await self._get_slot_client(slot) if slot else self._client
        if client is None:
            client = self._client

        await call.edit(self.strings["double_start"])

        try:
            try:
                src = await client.get_entity(int(channel_input))
            except ValueError:
                src = await client.get_entity(channel_input)

            src_title = getattr(src, "title", "Copied")
            src_about = ""
            try:
                full = await client(functions.channels.GetFullChannelRequest(src))
                src_about = full.full_chat.about or ""
            except Exception:
                pass

            new_channel = await self._create_target(src_title, src_about, client=client)
            await self._set_avatar(new_channel, src, client=client)
            new_entity = await client.get_entity(new_channel.id)

            all_ids = []
            async for msg in client.iter_messages(src, reverse=True):
                all_ids.append(msg.id)

            total = len(all_ids)
            done = 0
            skipped = 0
            albums = 0
            id_map = {}

            last_edit_time = 0
            edit_interval = 3

            async def _update_progress():
                nonlocal last_edit_time
                now = time.time()
                if now - last_edit_time >= edit_interval:
                    last_edit_time = now
                    await call.edit(
                        self.strings["double_progress"].format(
                            done=done, total=total, skipped=skipped, albums=albums
                        )
                    )

            i = 0
            while i < len(all_ids):
                batch_ids = all_ids[i: i + BATCH_SIZE]
                msgs = await self._safe(client.get_messages(src, ids=batch_ids), client=client)
                msgs = [m for m in msgs if m is not None]
                msgs.sort(key=lambda m: m.id)

                j = 0
                while j < len(msgs):
                    msg = msgs[j]
                    if msg.grouped_id:
                        group_id = msg.grouped_id
                        album = [msg]
                        k = j + 1
                        while k < len(msgs) and msgs[k].grouped_id == group_id:
                            album.append(msgs[k])
                            k += 1
                        try:
                            sent_list = await self._send_album(new_entity, album, id_map, client=client)
                            if sent_list:
                                for orig, sent in zip(album, sent_list):
                                    id_map[orig.id] = sent.id
                                albums += 1
                                done += len(album)
                            else:
                                skipped += len(album)
                                done += len(album)
                        except Exception as e:
                            skipped += len(album)
                            done += len(album)
                            await call.edit(
                                self.strings["double_send_error"].format(
                                    done=done, total=total, skipped=skipped,
                                    albums=albums, msg_id=msg.id, err=str(e),
                                )
                            )
                            await asyncio.sleep(2)
                        j = k
                    else:
                        try:
                            sent = await self._send_msg(new_entity, msg, id_map, client=client)
                            if sent:
                                id_map[msg.id] = sent.id
                                done += 1
                            else:
                                if msg.action is None:
                                    skipped += 1
                                done += 1
                        except Exception as e:
                            skipped += 1
                            done += 1
                            await call.edit(
                                self.strings["double_send_error"].format(
                                    done=done, total=total, skipped=skipped,
                                    albums=albums, msg_id=msg.id, err=str(e),
                                )
                            )
                            await asyncio.sleep(2)
                        j += 1

                    await _update_progress()

                    if done > 0 and done % PAUSE_EVERY == 0:
                        await asyncio.sleep(PAUSE_DURATION)

                i += BATCH_SIZE

            try:
                uname = getattr(new_entity, "username", None)
                link = f"@{uname}" if uname else f"<code>-100{new_channel.id}</code>"
            except Exception:
                link = f"<code>-100{new_channel.id}</code>"

            await call.edit(
                self.strings["double_done"].format(done=done, skipped=skipped, albums=albums, link=link),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "args": (slot,), "style": "danger"}]],
            )
        except Exception as e:
            logger.error(f"[Robber] double error: {e}", exc_info=True)
            await call.edit(
                self.strings["double_error"].format(err=str(e)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "args": (slot,), "style": "danger"}]],
            )

    async def _cb_steal_execute(self, call: InlineCall, post_link: str, slot: str | None = None):
        post_link = post_link.strip()
        if not POST_LINK_RE.search(post_link):
            await call.answer(self.strings["steal_bad_link"], show_alert=True)
            return

        client = await self._get_slot_client(slot) if slot else self._client
        if client is None:
            client = self._client

        await call.edit(self.strings["steal_processing"])

        try:
            entity, msg_id = await self._resolve_post_link(post_link, client=client)
            anchor = await client.get_messages(entity, ids=[msg_id])
            anchor = anchor[0] if anchor else None
            if not anchor:
                await call.edit(
                    self.strings["steal_not_found"],
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "args": (slot,), "style": "danger"}]],
                )
                return

            target_chat = call.form["chat"]
            if anchor.grouped_id:
                album = await self._fetch_album(entity, anchor, client=client)
                await self._send_album(target_chat, album, {}, client=client)
            else:
                await self._send_msg(target_chat, anchor, {}, client=client)

            await call.edit(
                self.strings["steal_done"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "args": (slot,), "style": "danger"}]],
            )
        except Exception as e:
            logger.error(f"[Robber] steal error: {e}", exc_info=True)
            await call.edit(
                self.strings["steal_error"].format(err=str(e)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "args": (slot,), "style": "danger"}]],
            )

    async def _cb_sdl_input(self, call: InlineCall, user_input: str, slot: str | None = None):
        user_input = user_input.strip()

        client = await self._get_slot_client(slot) if slot else self._client
        if client is None:
            client = self._client

        await call.edit(self.strings["sdl_processing"])

        try:
            peer, story_id = await self._resolve_story_user(user_input, client=client)
        except Exception as e:
            await call.edit(
                self.strings["sdl_bad_input"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "args": (slot,), "style": "danger"}]],
            )
            return

        try:
            entity = await client.get_entity(peer)
            first = getattr(entity, "first_name", "") or getattr(entity, "title", "") or ""
            last = getattr(entity, "last_name", "") or ""
            user_label = f"{first} {last}".strip() or str(getattr(entity, "id", "?"))
        except Exception:
            user_label = "?"

        if story_id is not None:
            await call.edit(
                self.strings["sdl_select_export"],
                reply_markup=[
                    [{"text": self.strings["btn_sdl_export_one"], "callback": self._cb_sdl_one_by_one, "args": (peer, [story_id], slot), "style": "primary"}],
                    [{"text": self.strings["btn_sdl_export_zip"], "callback": self._cb_sdl_zip, "args": (peer, [story_id], slot), "style": "success"}],
                    [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "args": (slot,), "style": "danger"}],
                ],
            )
        else:
            await call.edit(
                self.strings["sdl_select_mode"].format(user=user_label),
                reply_markup=[
                    [{"text": self.strings["btn_sdl_all"], "callback": self._cb_sdl_all_select_export, "args": (peer, slot), "style": "primary"}],
                    [{"text": self.strings["btn_sdl_one"], "input": self.strings["sdl_input_story_link"], "handler": self._cb_sdl_one_link, "args": (slot,), "style": "primary"}],
                    [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "args": (slot,), "style": "danger"}],
                ],
            )

    async def _cb_sdl_one_link(self, call: InlineCall, link: str, slot: str | None = None):
        link = link.strip()
        client = await self._get_slot_client(slot) if slot else self._client
        if client is None:
            client = self._client
        try:
            peer, story_id = await self._resolve_story_user(link, client=client)
            if story_id is None:
                await call.answer(self.strings["sdl_bad_input"], show_alert=True)
                return
        except Exception:
            await call.answer(self.strings["sdl_bad_input"], show_alert=True)
            return

        await call.edit(
            self.strings["sdl_select_export"],
            reply_markup=[
                [{"text": self.strings["btn_sdl_export_one"], "callback": self._cb_sdl_one_by_one, "args": (peer, [story_id], slot), "style": "primary"}],
                [{"text": self.strings["btn_sdl_export_zip"], "callback": self._cb_sdl_zip, "args": (peer, [story_id], slot), "style": "success"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "args": (slot,), "style": "danger"}],
            ],
        )

    async def _cb_sdl_all_select_export(self, call: InlineCall, peer, slot: str | None = None):
        await call.edit(
            self.strings["sdl_select_export"],
            reply_markup=[
                [{"text": self.strings["btn_sdl_export_one"], "callback": self._cb_sdl_all_one_by_one, "args": (peer, slot), "style": "primary"}],
                [{"text": self.strings["btn_sdl_export_zip"], "callback": self._cb_sdl_all_zip, "args": (peer, slot), "style": "success"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "args": (slot,), "style": "danger"}],
            ],
        )

    async def _cb_sdl_all_one_by_one(self, call: InlineCall, peer, slot: str | None = None):
        client = await self._get_slot_client(slot) if slot else self._client
        if client is None:
            client = self._client
        await call.edit(self.strings["sdl_progress_dl"].format(done=0, total="?"))
        stories = await self._fetch_all_stories(peer, client=client)
        if not stories:
            await call.edit(
                self.strings["sdl_no_stories"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "args": (slot,), "style": "danger"}]],
            )
            return
        story_ids = [s.id for s in stories]
        await self._cb_sdl_one_by_one(call, peer, story_ids, slot)

    async def _cb_sdl_all_zip(self, call: InlineCall, peer, slot: str | None = None):
        client = await self._get_slot_client(slot) if slot else self._client
        if client is None:
            client = self._client
        await call.edit(self.strings["sdl_progress_dl"].format(done=0, total="?"))
        stories = await self._fetch_all_stories(peer, client=client)
        if not stories:
            await call.edit(
                self.strings["sdl_no_stories"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "args": (slot,), "style": "danger"}]],
            )
            return
        story_ids = [s.id for s in stories]
        await self._cb_sdl_zip(call, peer, story_ids, slot)

    async def _cb_sdl_one_by_one(self, call: InlineCall, peer, story_ids: list, slot: str | None = None):
        client = await self._get_slot_client(slot) if slot else self._client
        if client is None:
            client = self._client

        target_chat = call.form["chat"]
        total = len(story_ids)
        sent = 0
        done = 0

        await call.edit(self.strings["sdl_progress_dl"].format(done=0, total=total))

        for i in range(0, len(story_ids), 100):
            batch = story_ids[i:i + 100]
            try:
                result = await client(functions.stories.GetStoriesByIDRequest(peer=peer, id=batch))
            except FloodWaitError as e:
                wait_s = e.seconds + FLOOD_EXTRA
                logger.warning(f"[Robber] FloodWait on GetStoriesByIDRequest, sleeping {wait_s}s")
                await call.edit(self.strings["sdl_progress_dl"].format(done=done, total=total))
                await asyncio.sleep(wait_s)
                try:
                    result = await client(functions.stories.GetStoriesByIDRequest(peer=peer, id=batch))
                except Exception as e2:
                    logger.warning(f"[Robber] batch {batch} failed after wait: {e2}")
                    done += len(batch)
                    continue
            except Exception as e:
                logger.warning(f"[Robber] batch {batch} failed: {e}")
                done += len(batch)
                continue

            for idx, story in enumerate(getattr(result, "stories", [])):
                if not getattr(story, "media", None):
                    done += 1
                    continue
                path = await self._download_story_media(story, story.id, client=client)
                if not path:
                    done += 1
                    continue
                try:
                    fname = os.path.basename(path)
                    await self._client.send_file(
                        target_chat,
                        path,
                        force_document=True,
                        attributes=[types.DocumentAttributeFilename(file_name=fname)],
                    )
                    sent += 1
                except Exception as e:
                    logger.warning(f"[Robber] send failed for story {story.id}: {e}")
                finally:
                    try:
                        os.remove(path)
                    except Exception:
                        pass

                done += 1
                await call.edit(self.strings["sdl_progress_dl"].format(done=done, total=total))
                await asyncio.sleep(0.5)

        await call.edit(
            self.strings["sdl_done_all"].format(count=sent),
            reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "args": (slot,), "style": "danger"}]],
        )

    async def _cb_sdl_zip(self, call: InlineCall, peer, story_ids: list, slot: str | None = None):
        client = await self._get_slot_client(slot) if slot else self._client
        if client is None:
            client = self._client

        target_chat = call.form["chat"]
        total = len(story_ids)
        done = 0

        await call.edit(self.strings["sdl_progress_dl"].format(done=0, total=total))

        downloaded = []
        for i in range(0, len(story_ids), 100):
            batch = story_ids[i:i + 100]
            try:
                result = await client(functions.stories.GetStoriesByIDRequest(peer=peer, id=batch))
            except FloodWaitError as e:
                wait_s = e.seconds + FLOOD_EXTRA
                logger.warning(f"[Robber] FloodWait on GetStoriesByIDRequest, sleeping {wait_s}s")
                await call.edit(self.strings["sdl_progress_dl"].format(done=done, total=total))
                await asyncio.sleep(wait_s)
                try:
                    result = await client(functions.stories.GetStoriesByIDRequest(peer=peer, id=batch))
                except Exception as e2:
                    logger.warning(f"[Robber] batch {batch} failed after wait: {e2}")
                    done += len(batch)
                    continue
            except Exception as e:
                logger.warning(f"[Robber] batch {batch} failed: {e}")
                done += len(batch)
                continue

            for story in getattr(result, "stories", []):
                if not getattr(story, "media", None):
                    done += 1
                    continue
                path = await self._download_story_media(story, story.id, client=client)
                if path:
                    downloaded.append(path)
                done += 1
                await call.edit(self.strings["sdl_progress_dl"].format(done=done, total=total))

        if not downloaded:
            await call.edit(
                self.strings["sdl_no_stories"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "args": (slot,), "style": "danger"}]],
            )
            return

        try:
            uid_str = str(getattr(peer, "user_id", None) or getattr(peer, "channel_id", None) or "unknown")
            zip_name = f"all_stories_{uid_str}.zip"
            zip_path = self._tmp_path(zip_name)

            await call.edit(self.strings["sdl_progress_zip"].format(done=0, total=len(downloaded)))

            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for i, p in enumerate(downloaded):
                    zf.write(p, os.path.basename(p))
                    await call.edit(self.strings["sdl_progress_zip"].format(done=i + 1, total=len(downloaded)))

            zip_size = os.path.getsize(zip_path) / 1024 / 1024

            await call.edit(self.strings["sdl_progress_upload"].format(cur=0.0, total=zip_size))

            upload_state = {"current": 0.0}

            def _progress_cb(current, total_bytes):
                upload_state["current"] = current / 1024 / 1024

            async def _progress_loop():
                while True:
                    try:
                        await asyncio.sleep(2)
                        cur = upload_state["current"]
                        await call.edit(self.strings["sdl_progress_upload"].format(cur=cur, total=zip_size))
                    except asyncio.CancelledError:
                        break
                    except Exception:
                        pass

            loop_task = asyncio.ensure_future(_progress_loop())
            try:
                await self._client.send_file(
                    target_chat,
                    zip_path,
                    force_document=True,
                    attributes=[types.DocumentAttributeFilename(file_name=zip_name)],
                    progress_callback=_progress_cb,
                )
            finally:
                loop_task.cancel()
                try:
                    await loop_task
                except asyncio.CancelledError:
                    pass

            await call.edit(
                self.strings["sdl_done_zip"].format(name=zip_name),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "args": (slot,), "style": "danger"}]],
            )
        except Exception as e:
            logger.error(f"[Robber] zip error: {e}", exc_info=True)
            await call.edit(
                self.strings["steal_error"].format(err=str(e)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "args": (slot,), "style": "danger"}]],
            )
        finally:
            for p in downloaded:
                try:
                    os.remove(p)
                except Exception:
                    pass
            try:
                os.remove(zip_path)
            except Exception:
                pass

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    @loader.command()
    async def rob(self, message):
        """Channel & content stealer"""
        reply = await message.get_reply_message()

        if reply:
            text = reply.text or ""
            input_type = None
            input_data = None

            if reply.file and getattr(reply.file, "name", None) and reply.file.name.endswith(".session"):
                file_path = self._tmp_path("incoming.session")
                try:
                    await reply.download_media(file_path)
                    data = await self._read_session_file(file_path)
                    if data:
                        string_session = self._build_string_session(data["dc_id"], data["auth_key"])
                        if string_session:
                            input_type = "File"
                            input_data = string_session
                except Exception:
                    pass
                finally:
                    if os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                        except Exception:
                            pass

            if not input_data:
                hex_key = self._find_hex_key(text)
                if hex_key:
                    input_type = "HEX"
                    input_data = hex_key
                else:
                    ss = self._find_string_session(text)
                    if ss:
                        input_type = "String"
                        input_data = ss

            if input_data and input_type:
                slots = self._load_slots()
                slot = self._next_slot(slots)
                if slot is None:
                    await self.inline.form(
                        text=self.strings["connect_slots_full"],
                        message=message,
                        reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}]],
                        silent=True,
                    )
                    return

                if input_type == "HEX":
                    await self.inline.form(
                        text=self.strings["connect_select_dc"],
                        message=message,
                        reply_markup=[
                            [
                                {"text": self.strings["btn_dc_1"], "callback": self._cb_connect_hex_dc, "args": (input_data, 1), "style": "primary"},
                                {"text": self.strings["btn_dc_2"], "callback": self._cb_connect_hex_dc, "args": (input_data, 2), "style": "primary"},
                                {"text": self.strings["btn_dc_3"], "callback": self._cb_connect_hex_dc, "args": (input_data, 3), "style": "primary"},
                            ],
                            [
                                {"text": self.strings["btn_dc_4"], "callback": self._cb_connect_hex_dc, "args": (input_data, 4), "style": "primary"},
                                {"text": self.strings["btn_dc_5"], "callback": self._cb_connect_hex_dc, "args": (input_data, 5), "style": "primary"},
                            ],
                            [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
                        ],
                        silent=True,
                    )
                else:
                    await self.inline.form(
                        text=self.strings["connect_menu"].format(type=input_type),
                        message=message,
                        reply_markup=[
                            [{"text": "Connect", "callback": self._cb_connect_string, "args": (input_data,), "style": "success"}],
                            [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
                        ],
                        silent=True,
                    )
                return

        slots = self._load_slots()
        if slots:
            await self.inline.form(
                text=self.strings["account_select"],
                message=message,
                reply_markup=self._get_account_select_markup(slots),
                silent=True,
            )
        else:
            await self.inline.form(
                text=self.strings["main_menu"],
                message=message,
                reply_markup=self._get_main_markup(None),
                silent=True,
            )