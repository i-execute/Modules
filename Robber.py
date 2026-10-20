# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (3, 1, 0)
# meta developer: Execute_forge.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/Robber/MetaBanner.jpeg

import asyncio
import base64
import ipaddress
import logging
import os
import re
import shutil
import sqlite3
import struct
import subprocess
import sys
import tempfile
import time
import zipfile

from telethon import TelegramClient, functions, types
from telethon import utils as tl_utils
from telethon.errors import (
    AuthKeyUnregisteredError,
    FloodWaitError,
    MessageNotModifiedError,
    UserDeactivatedBanError,
)
from telethon.sessions import StringSession
from telethon.tl.types import (
    DocumentAttributeFilename,
    DocumentAttributeVideo,
    InputMediaWebPage,
    MessageMediaContact,
    MessageMediaGeo,
    MessageMediaGeoLive,
    MessageMediaPoll,
    MessageMediaWebPage,
)

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

MB = 1024 * 1024
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

RELOADING_MEDIA_URL = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/Robber/Reloading.jpeg"
TOPIC_TITLE = "Robber"
TOPIC_ICON = 5379758279851289726


def _ensure_deps():
    for mod, pkg in (("imageio_ffmpeg", "imageio-ffmpeg"),):
        try:
            __import__(mod)
        except ImportError:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", pkg, "-q"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )


_ensure_deps()


def _safe_disconnect(client):
    if client:
        try:
            asyncio.ensure_future(client.disconnect())
        except Exception:
            pass


def _fmt_speed(b):
    if not b:
        return "..."
    if b < 1024:
        return f"{b:.0f} B/s"
    if b < MB:
        return f"{b / 1024:.1f} KB/s"
    return f"{b / MB:.1f} MB/s"


def _fmt_elapsed(s):
    s = int(s or 0)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        m, sec = divmod(s, 60)
        return f"{m}m {sec}s" if sec else f"{m}m"
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h}h {m}m {sec}s"


@loader.tds
class Robber(loader.Module):
    """Channel and content stealer with auto ffmpeg re-encode and multi-account support"""

    strings = {
        "name": "Robber",
        "main_menu": (
            "<blockquote><b>Robber</b> | Channel and Content Stealer</blockquote>\n"
            "<blockquote>Select operation:</blockquote>"
        ),
        "account_select": (
            "<blockquote><b>Robber</b> | Account Selection</blockquote>\n"
            "<blockquote>Choose account to work with:</blockquote>"
        ),
        "connected_select": (
            "<blockquote><b>Robber</b> | Connected Accounts</blockquote>\n"
            "<blockquote>Select connected slot:</blockquote>"
        ),
        "mode_select": (
            "<blockquote><b>Robber</b> | Send Mode</blockquote>\n"
            "<blockquote>Target: <code>{target}</code></blockquote>\n"
            "<blockquote>Choose media delivery format:</blockquote>"
        ),
        "btn_double": "Copy Channel",
        "btn_steal": "Steal Post",
        "btn_sdl": "Download Story",
        "btn_back": "Back",
        "btn_close": "Close",
        "btn_owner": "Owner Account",
        "btn_connected": "Connected Slots",
        "btn_mode_media": "Media (Video/Photo)",
        "btn_mode_doc": "File (Document)",
        "btn_sdl_one": "One Story",
        "btn_sdl_all": "All Stories",
        "btn_sdl_zip": "ZIP Archive",
        "btn_sdl_media": "All (Media)",
        "btn_sdl_doc": "All (File)",
        "btn_connect": "Connect",
        "connect_menu": (
            "<blockquote><b>Robber</b> | Connect Session</blockquote>\n"
            "<blockquote>Detected: <code>{type}</code></blockquote>\n"
            "<blockquote>Connect this session?</blockquote>"
        ),
        "connect_select_dc": (
            "<blockquote><b>Robber</b> | Connect HEX Session</blockquote>\n"
            "<blockquote>Select DC for this key:</blockquote>"
        ),
        "connecting": "<blockquote><b>Connecting session...</b></blockquote>",
        "connect_ok": (
            "<blockquote><b>Connected successfully!</b></blockquote>\n"
            "<blockquote>Slot: <code>{slot}</code> | User: <b>{user}</b> | ID: <code>{uid}</code></blockquote>"
        ),
        "connect_fail_invalid": "<blockquote><b>Error:</b> Session is invalid or not authorized</blockquote>",
        "connect_fail_banned": "<blockquote><b>Error:</b> Account is banned</blockquote>",
        "connect_fail_revoked": "<blockquote><b>Error:</b> Session revoked</blockquote>",
        "connect_fail_error": "<blockquote><b>Error:</b> <code>{err}</code></blockquote>",
        "connect_slots_full": "<blockquote><b>Error:</b> Maximum 5 accounts connected</blockquote>",
        "slot_info": "Slot {slot} | {name} | {uid}",
        "input_channel": "Enter channel ID or username:",
        "input_post": "Enter post link (e.g. https://t.me/c/3872609933/8):",
        "input_story": "Enter username, user ID or story link:",
        "input_story_link": "Enter single story link:",
        "double_start": (
            "<blockquote><b>Robber</b> | Channel Copy</blockquote>\n"
            "<blockquote>Initializing target channel and messages...</blockquote>"
        ),
        "double_progress": (
            "<blockquote><b>Robber</b> | Copying Channel</blockquote>\n"
            "<blockquote>Processed: <b>{done}</b> / <b>{total}</b> | Skipped: <b>{skipped}</b> | Albums: <b>{albums}</b></blockquote>\n"
            "<pre><code class=\"language-robber\">{body}</code></pre>"
        ),
        "double_done": (
            "<blockquote><b>Robber</b> | Copy Complete!</blockquote>\n"
            "<blockquote>Processed: <b>{done}</b> | Skipped: <b>{skipped}</b> | Albums: <b>{albums}</b></blockquote>\n"
            "<blockquote>Target: {link}</blockquote>"
        ),
        "double_no_id": "Provide channel ID or username",
        "double_error": (
            "<blockquote><b>Robber</b> | Copy Error</blockquote>\n"
            "<blockquote><code>{err}</code></blockquote>"
        ),
        "double_send_error": (
            "<blockquote><b>Robber</b> | Send Error on msg {msg_id}</blockquote>\n"
            "<blockquote><code>{err}</code></blockquote>"
        ),
        "steal_bad_link": "Cannot parse post link",
        "steal_not_found": (
            "<blockquote><b>Robber</b> | Steal Post</blockquote>\n"
            "<blockquote>Post not found or inaccessible</blockquote>"
        ),
        "steal_processing": (
            "<blockquote><b>Robber</b> | Stealing Post</blockquote>\n"
            "<pre><code class=\"language-robber\">{body}</code></pre>"
        ),
        "steal_done": (
            "<blockquote><b>Robber</b> | Steal Complete</blockquote>\n"
            "<blockquote>Post stolen successfully!</blockquote>"
        ),
        "steal_error": (
            "<blockquote><b>Robber</b> | Steal Error</blockquote>\n"
            "<blockquote><code>{err}</code></blockquote>"
        ),
        "sdl_bad_input": "Cannot parse user or story input",
        "sdl_not_found": (
            "<blockquote><b>Robber</b> | Stories</blockquote>\n"
            "<blockquote>Story not found or has no media</blockquote>"
        ),
        "sdl_no_stories": (
            "<blockquote><b>Robber</b> | Stories</blockquote>\n"
            "<blockquote>No stories found for this user</blockquote>"
        ),
        "sdl_processing": (
            "<blockquote><b>Robber</b> | Story Download</blockquote>\n"
            "<pre><code class=\"language-robber\">{body}</code></pre>"
        ),
        "sdl_done_all": (
            "<blockquote><b>Robber</b> | Stories Complete</blockquote>\n"
            "<blockquote>Sent <b>{count}</b> stories!</blockquote>"
        ),
        "sdl_done_zip": (
            "<blockquote><b>Robber</b> | Archive Complete</blockquote>\n"
            "<blockquote>Archive sent: <code>{name}</code> ({count} files)</blockquote>"
        ),
        "btn_dc_1": "DC 1",
        "btn_dc_2": "DC 2",
        "btn_dc_3": "DC 3",
        "btn_dc_4": "DC 4",
        "btn_dc_5": "DC 5",
        "greeting_first": (
            "<blockquote><b>Robber</b> installed successfully!</blockquote>\n"
            "<blockquote>Version: <b>{ver}</b></blockquote>\n"
            "<blockquote>Ready to copy channels, steal posts and download stories.</blockquote>"
        ),
        "reloaded": (
            "<blockquote><b>Robber</b> reloaded successfully!</blockquote>\n"
            "<blockquote>Version: <b>{ver}</b></blockquote>\n"
            "<blockquote>All systems active and ready.</blockquote>"
        ),
    }

    strings_ru = {
        "main_menu": (
            "<blockquote><b>Robber</b> | Копировщик каналов и контента</blockquote>\n"
            "<blockquote>Выберите операцию:</blockquote>"
        ),
        "account_select": (
            "<blockquote><b>Robber</b> | Выбор аккаунта</blockquote>\n"
            "<blockquote>Выберите аккаунт для работы:</blockquote>"
        ),
        "connected_select": (
            "<blockquote><b>Robber</b> | Подключенные аккаунты</blockquote>\n"
            "<blockquote>Выберите подключенный слот:</blockquote>"
        ),
        "mode_select": (
            "<blockquote><b>Robber</b> | Режим отправки</blockquote>\n"
            "<blockquote>Цель: <code>{target}</code></blockquote>\n"
            "<blockquote>Выберите формат отправки медиа:</blockquote>"
        ),
        "btn_double": "Копировать канал",
        "btn_steal": "Украсть пост",
        "btn_sdl": "Скачать историю",
        "btn_back": "Назад",
        "btn_close": "Закрыть",
        "btn_owner": "Аккаунт владельца",
        "btn_connected": "Подключенные слоты",
        "btn_mode_media": "Медиа (Видео/Фото)",
        "btn_mode_doc": "Файл (Документ)",
        "btn_sdl_one": "Одна история",
        "btn_sdl_all": "Все истории",
        "btn_sdl_zip": "ZIP архив",
        "btn_sdl_media": "Все (Медиа)",
        "btn_sdl_doc": "Все (Файлом)",
        "btn_connect": "Подключить",
        "connect_menu": (
            "<blockquote><b>Robber</b> | Подключение сессии</blockquote>\n"
            "<blockquote>Обнаружено: <code>{type}</code></blockquote>\n"
            "<blockquote>Подключить эту сессию?</blockquote>"
        ),
        "connect_select_dc": (
            "<blockquote><b>Robber</b> | Выбор DC для HEX</blockquote>\n"
            "<blockquote>Выберите DC для этого ключа:</blockquote>"
        ),
        "connecting": "<blockquote><b>Подключение сессии...</b></blockquote>",
        "connect_ok": (
            "<blockquote><b>Успешно подключено!</b></blockquote>\n"
            "<blockquote>Слот: <code>{slot}</code> | Юзер: <b>{user}</b> | ID: <code>{uid}</code></blockquote>"
        ),
        "connect_fail_invalid": "<blockquote><b>Ошибка:</b> Сессия невалидна или не авторизована</blockquote>",
        "connect_fail_banned": "<blockquote><b>Ошибка:</b> Аккаунт заблокирован</blockquote>",
        "connect_fail_revoked": "<blockquote><b>Ошибка:</b> Сессия отозвана</blockquote>",
        "connect_fail_error": "<blockquote><b>Ошибка:</b> <code>{err}</code></blockquote>",
        "connect_slots_full": "<blockquote><b>Ошибка:</b> Достигнут лимит 5 аккаунтов</blockquote>",
        "slot_info": "Слот {slot} | {name} | {uid}",
        "input_channel": "Введите ID или юзернейм канала:",
        "input_post": "Введите ссылку на пост (напр. https://t.me/c/3872609933/8):",
        "input_story": "Введите юзернейм, ID пользователя или ссылку на историю:",
        "input_story_link": "Введите ссылку на историю:",
        "double_start": (
            "<blockquote><b>Robber</b> | Копирование канала</blockquote>\n"
            "<blockquote>Инициализация целевого канала и сообщений...</blockquote>"
        ),
        "double_progress": (
            "<blockquote><b>Robber</b> | Копирование канала</blockquote>\n"
            "<blockquote>Обработано: <b>{done}</b> / <b>{total}</b> | Пропущено: <b>{skipped}</b> | Альбомы: <b>{albums}</b></blockquote>\n"
            "<pre><code class=\"language-robber\">{body}</code></pre>"
        ),
        "double_done": (
            "<blockquote><b>Robber</b> | Копирование завершено!</blockquote>\n"
            "<blockquote>Обработано: <b>{done}</b> | Пропущено: <b>{skipped}</b> | Альбомы: <b>{albums}</b></blockquote>\n"
            "<blockquote>Ссылка: {link}</blockquote>"
        ),
        "double_no_id": "Укажите ID или юзернейм канала",
        "double_error": (
            "<blockquote><b>Robber</b> | Ошибка копирования</blockquote>\n"
            "<blockquote><code>{err}</code></blockquote>"
        ),
        "double_send_error": (
            "<blockquote><b>Robber</b> | Ошибка отправки на сообщении {msg_id}</blockquote>\n"
            "<blockquote><code>{err}</code></blockquote>"
        ),
        "steal_bad_link": "Не удалось распознать ссылку на пост",
        "steal_not_found": (
            "<blockquote><b>Robber</b> | Кража поста</blockquote>\n"
            "<blockquote>Пост не найден или недоступен</blockquote>"
        ),
        "steal_processing": (
            "<blockquote><b>Robber</b> | Кража поста</blockquote>\n"
            "<pre><code class=\"language-robber\">{body}</code></pre>"
        ),
        "steal_done": (
            "<blockquote><b>Robber</b> | Пост украден!</blockquote>\n"
            "<blockquote>Пост успешно украден и отправлен!</blockquote>"
        ),
        "steal_error": (
            "<blockquote><b>Robber</b> | Ошибка кражи</blockquote>\n"
            "<blockquote><code>{err}</code></blockquote>"
        ),
        "sdl_bad_input": "Не удалось распознать ввод пользователя или истории",
        "sdl_not_found": (
            "<blockquote><b>Robber</b> | Истории</blockquote>\n"
            "<blockquote>История не найдена или не содержит медиа</blockquote>"
        ),
        "sdl_no_stories": (
            "<blockquote><b>Robber</b> | Истории</blockquote>\n"
            "<blockquote>У пользователя нет доступных историй</blockquote>"
        ),
        "sdl_processing": (
            "<blockquote><b>Robber</b> | Скачивание историй</blockquote>\n"
            "<pre><code class=\"language-robber\">{body}</code></pre>"
        ),
        "sdl_done_all": (
            "<blockquote><b>Robber</b> | Истории отправлены</blockquote>\n"
            "<blockquote>Отправлено историй: <b>{count}</b>!</blockquote>"
        ),
        "sdl_done_zip": (
            "<blockquote><b>Robber</b> | Архив готов</blockquote>\n"
            "<blockquote>Архив отправлен: <code>{name}</code> ({count} файлов)</blockquote>"
        ),
        "btn_dc_1": "DC 1",
        "btn_dc_2": "DC 2",
        "btn_dc_3": "DC 3",
        "btn_dc_4": "DC 4",
        "btn_dc_5": "DC 5",
        "greeting_first": (
            "<blockquote><b>Robber</b> успешно установлен!</blockquote>\n"
            "<blockquote>Версия: <b>{ver}</b></blockquote>\n"
            "<blockquote>Готов к копированию каналов, краже постов и историй.</blockquote>"
        ),
        "reloaded": (
            "<blockquote><b>Robber</b> успешно перезагружен!</blockquote>\n"
            "<blockquote>Версия: <b>{ver}</b></blockquote>\n"
            "<blockquote>Все системы в боевой готовности.</blockquote>"
        ),
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
        self._ffmpeg = None
        self._topic_id = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        me = None
        try:
            me = await client.get_me()
        except Exception:
            pass
        uid = getattr(me, "id", None) or getattr(client, "tg_id", None) or "common"
        self._temp_dir = os.path.join(tempfile.gettempdir(), "Robber", str(uid))
        os.makedirs(self._temp_dir, exist_ok=True)
        asyncio.ensure_future(self._notify_ready())

    async def on_unload(self):
        self._wipe_temp()
        slots = self._load_slots()
        for slot_data in slots.values():
            cl = slot_data.get("_client")
            if cl:
                _safe_disconnect(cl)

    def _wipe_temp(self):
        if self._temp_dir and os.path.exists(self._temp_dir):
            try:
                shutil.rmtree(self._temp_dir)
            except Exception:
                pass

    def _tmp_path(self, name):
        return os.path.join(self._temp_dir, name)

    def _asset_channel(self):
        cid = self._db.get("feroku.forums", "channel_id", None) or self._db.get(
            "heroku.forums", "channel_id", None
        )
        if not cid:
            return None
        cid = abs(int(cid))
        text = str(cid)
        if text.startswith("100") and len(text) > 12:
            cid = int(text[3:])
        return cid

    async def _ensure_topic(self, fresh=False):
        if self._topic_id and not fresh:
            return self._topic_id
        cid = self._asset_channel()
        if not cid:
            return None
        try:
            topic = await utils.asset_forum_topic(
                self._client,
                self._db,
                cid,
                TOPIC_TITLE,
                icon_emoji_id=TOPIC_ICON,
            )
            self._topic_id = topic.id
            if getattr(topic, "icon_emoji_id", None) != TOPIC_ICON:
                try:
                    await self._client(
                        functions.channels.EditForumTopicRequest(
                            channel=cid,
                            topic_id=topic.id,
                            icon_emoji_id=TOPIC_ICON,
                        )
                    )
                except Exception:
                    pass
            return self._topic_id
        except Exception as e:
            logger.error(f"[Robber] topic creation failed: {e}")
            return None

    async def _send_with_preview(self, chat_id, topic_id, text):
        inline = getattr(self, "inline", None)
        bot = getattr(inline, "bot", None) or getattr(inline, "_bot_client", None)
        if not bot:
            logger.error("[Robber] Inline bot is unavailable; reload notification was not sent")
            return
        try:
            msg_text, entities = await bot._parse_message_text(text, "html")
            msg = await bot.send_message(
                chat_id,
                text,
                message_thread_id=topic_id,
            )
            try:
                peer = await bot.get_input_entity(chat_id)
                current_msg = await bot.get_messages(chat_id, ids=msg.id)
                reply_markup = current_msg.reply_markup if current_msg else None
                await bot(
                    functions.messages.EditMessageRequest(
                        peer=peer,
                        id=msg.id,
                        message=msg_text,
                        media=InputMediaWebPage(
                            url=RELOADING_MEDIA_URL,
                            optional=True,
                            force_large_media=True,
                        ),
                        invert_media=True,
                        reply_markup=reply_markup,
                        entities=entities,
                        no_webpage=False,
                    )
                )
            except Exception as e:
                logger.error(f"[Robber] banner preview attach failed: {e}")
        except Exception as e:
            logger.error(f"[Robber] greeting message send failed: {e}")

    async def _notify_ready(self):
        await asyncio.sleep(2)
        cid = None
        topic_id = None
        for attempt in range(6):
            cid = self._asset_channel()
            if cid:
                topic_id = await self._ensure_topic()
                if topic_id:
                    break
            else:
                logger.warning("[Robber] Asset forum channel_id is unavailable; retrying notification setup")
            if attempt < 5:
                await asyncio.sleep(min(5 * (attempt + 1), 20))
        if not topic_id:
            logger.error("[Robber] Could not set up the log topic; reload notification was not sent")
            return
        chat_id = int(f"-100{cid}")
        greet_key = f"robber_greeted_{cid}_{topic_id}"
        already = self.get(greet_key, False)
        ver_str = ".".join(map(str, __version__))
        if already:
            text = self.strings["reloaded"].format(ver=ver_str)
            await self._send_with_preview(chat_id, topic_id, text)
        else:
            self.set(greet_key, True)
            text = self.strings["greeting_first"].format(ver=ver_str)
            await self._send_with_preview(chat_id, topic_id, text)

    def _ffmpeg_bin(self):
        if self._ffmpeg and os.path.exists(self._ffmpeg):
            return self._ffmpeg
        path = shutil.which("ffmpeg")
        if not path:
            try:
                import imageio_ffmpeg

                exe = imageio_ffmpeg.get_ffmpeg_exe()
                bin_dir = os.path.join(self._temp_dir, "bin")
                os.makedirs(bin_dir, exist_ok=True)
                path = os.path.join(bin_dir, "ffmpeg")
                if os.path.realpath(path) != os.path.realpath(exe):
                    try:
                        if os.path.lexists(path):
                            os.remove(path)
                        os.symlink(exe, path)
                    except OSError:
                        path = exe
            except Exception:
                path = "ffmpeg"
        self._ffmpeg = path
        return path

    async def _run_ff(self, args, duration=0, state=None):
        proc = await asyncio.create_subprocess_exec(
            self._ffmpeg_bin(),
            "-hide_banner",
            "-nostdin",
            "-y",
            "-loglevel",
            "error",
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        err_task = asyncio.ensure_future(proc.stderr.read())
        try:
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                if state is None or not duration:
                    continue
                key, _, value = line.decode(errors="ignore").strip().partition("=")
                if key in ("out_time_us", "out_time_ms") and value.isdigit():
                    state["ff_pct"] = min(int(value) / 1e6 / duration * 100, 100.0)
            await proc.wait()
        except asyncio.CancelledError:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            err_task.cancel()
            raise
        err = (await err_task).decode(errors="ignore").strip()
        if proc.returncode != 0:
            raise RuntimeError(f"ffmpeg: {err[-400:] or proc.returncode}")
        return err

    async def _probe(self, path):
        proc = await asyncio.create_subprocess_exec(
            self._ffmpeg_bin(),
            "-hide_banner",
            "-nostdin",
            "-i",
            path,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, err = await proc.communicate()
        text = err.decode(errors="ignore")
        info = {
            "vcodec": None,
            "pix_fmt": None,
            "width": 0,
            "height": 0,
            "duration": 0.0,
            "acodec": None,
        }
        m = re.search(r"Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)", text)
        if m:
            info["duration"] = (
                int(m.group(1)) * 3600 + int(m.group(2)) * 60 + float(m.group(3))
            )
        video = re.search(r"Stream #\d+:\d+[^\n]*?: Video: (\w+)([^\n]*)", text)
        if video:
            info["vcodec"] = video.group(1)
            rest = video.group(2)
            pix = re.search(r"\),\s*(\w+)|^[^,]*,\s*(\w+)", rest)
            if pix:
                info["pix_fmt"] = pix.group(1) or pix.group(2)
            dims = re.search(r",\s*(\d{2,5})x(\d{2,5})", rest)
            if dims:
                info["width"], info["height"] = int(dims.group(1)), int(dims.group(2))
            tail = text[video.end() :]
            next_stream = tail.find("Stream #")
            block = tail if next_stream < 0 else tail[:next_stream]
            rot = re.search(
                r"rotation of (-?\d+(?:\.\d+)?) degrees|rotate\s*:\s*(-?\d+)", block
            )
            if rot:
                angle = abs(int(float(rot.group(1) or rot.group(2)))) % 180
                if angle == 90:
                    info["width"], info["height"] = info["height"], info["width"]
        audio = re.search(r"Stream #\d+:\d+[^\n]*?: Audio: (\w+)", text)
        if audio:
            info["acodec"] = audio.group(1)
        return info

    async def _normalize(self, src, dst, state=None):
        probe = await self._probe(src)
        if not probe["vcodec"]:
            shutil.copyfile(src, dst)
            return dst
        copy_video = probe["vcodec"] == "h264" and probe["pix_fmt"] in (
            "yuv420p",
            "yuvj420p",
        )
        copy_audio = probe["acodec"] in (None, "aac")
        args = [
            "-progress",
            "pipe:1",
            "-nostats",
            "-i",
            src,
            "-map",
            "0:v:0",
            "-map",
            "0:a:0?",
            "-sn",
            "-dn",
        ]
        if copy_video:
            args += ["-c:v", "copy"]
        else:
            args += [
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-crf",
                "21",
                "-profile:v",
                "high",
                "-pix_fmt",
                "yuv420p",
                "-vf",
                "scale=trunc(iw/2)*2:trunc(ih/2)*2",
            ]
        args += ["-c:a", "copy"] if copy_audio else ["-c:a", "aac", "-b:a", "192k", "-ac", "2"]
        args += ["-movflags", "+faststart", "-f", "mp4", dst]
        if state:
            state["ff"] = "run"
            state["ff_pct"] = 0.0
        await self._run_ff(args, probe["duration"], state)
        if state:
            state["ff"] = "done"
        return dst

    async def _make_thumb(self, src, dst, at=1.0):
        args = []
        if at:
            args += ["-ss", f"{at:.2f}"]
        args += [
            "-i",
            src,
            "-frames:v",
            "1",
            "-vf",
            "scale='if(gt(iw,ih),320,-2)':'if(gt(iw,ih),-2,320)'",
            "-q:v",
            "3",
            dst,
        ]
        try:
            await self._run_ff(args)
        except Exception:
            return None
        return dst if os.path.isfile(dst) and os.path.getsize(dst) else None

    def _load_slots(self) -> dict:
        raw = self._db.get("Robber", "slots", {})
        return raw if isinstance(raw, dict) else {}

    def _save_slots(self, slots: dict):
        save = {
            k: {sk: sv for sk, sv in v.items() if sk != "_client"}
            for k, v in slots.items()
        }
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

    async def _get_slot_client(self, slot: str | None):
        if not slot:
            return self._client
        slots = self._load_slots()
        data = slots.get(str(slot))
        if not data:
            return self._client
        if data.get("_client"):
            return data["_client"]
        string_session = data.get("string")
        if not string_session:
            return self._client
        try:
            client, me = await self._connect_and_test(string_session)
            slots[str(slot)]["_client"] = client
            return client
        except Exception:
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

    async def _is_forum_chat(self, message):
        if getattr(message, "is_private", False):
            return False
        try:
            chat = await message.get_chat()
            return getattr(chat, "forum", False)
        except Exception:
            return False

    def _get_topic_id(self, message):
        reply_to = getattr(message, "reply_to", None)
        if reply_to:
            top_id = getattr(reply_to, "reply_to_top_id", None)
            if top_id:
                return top_id
            msg_id = getattr(reply_to, "reply_to_msg_id", None)
            if msg_id and getattr(reply_to, "forum_topic", False):
                return msg_id
        return None

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

    def _make_progress_cb(self, state, phase="dl"):
        def _cb(current, total):
            state[f"{phase}_cur"] = current
            if total:
                state[f"{phase}_total"] = total
            if not state.get(f"{phase}_t0"):
                state[f"{phase}_t0"] = time.time()
            spent = max(time.time() - state.get(f"{phase}_t0", time.time()), 0.001)
            state[f"{phase}_speed"] = current / spent

        return _cb

    def _render_progress_body(self, state):
        lines = []
        dl_cur = state.get("dl_cur", 0)
        dl_total = state.get("dl_total", 0)
        dl_speed = state.get("dl_speed", 0)
        if state.get("dl_done"):
            lines.append(f"Download: done {dl_cur / MB:.1f} MB")
        elif dl_total:
            pct = min(dl_cur / dl_total * 100, 100.0)
            lines.append(
                f"Download: {pct:.1f}% ({dl_cur / MB:.1f}/{dl_total / MB:.1f} MB) | {_fmt_speed(dl_speed)}"
            )
        elif dl_cur:
            lines.append(f"Download: {dl_cur / MB:.1f} MB | {_fmt_speed(dl_speed)}")
        else:
            lines.append("Download: waiting...")

        ff_state = state.get("ff")
        if ff_state == "run":
            lines.append(f"FFmpeg: H.264 {state.get('ff_pct', 0.0):.1f}%")
        elif ff_state == "done":
            lines.append("FFmpeg: done")
        elif ff_state == "skip":
            lines.append("FFmpeg: not needed")
        else:
            lines.append("FFmpeg: waiting...")

        up_cur = state.get("up_cur", 0)
        up_total = state.get("up_total", 0)
        up_speed = state.get("up_speed", 0)
        if state.get("up_done"):
            lines.append(f"Telegram: done {up_cur / MB:.1f} MB")
        elif up_total:
            pct = min(up_cur / up_total * 100, 100.0)
            lines.append(
                f"Telegram: {pct:.1f}% ({up_cur / MB:.1f}/{up_total / MB:.1f} MB) | {_fmt_speed(up_speed)}"
            )
        elif up_cur:
            lines.append(f"Telegram: {up_cur / MB:.1f} MB | {_fmt_speed(up_speed)}")
        else:
            lines.append("Telegram: waiting...")

        elapsed = _fmt_elapsed(time.time() - state.get("t0", time.time()))
        stage = state.get("stage", "processing")
        lines.append(f"Time: {elapsed} | {stage}")
        return "\n".join(lines)

    async def _progress_loop(self, call: InlineCall, title_key: str, state: dict, done_event: asyncio.Event):
        last_text = ""
        while not done_event.is_set():
            try:
                await asyncio.wait_for(done_event.wait(), timeout=2)
            except asyncio.TimeoutError:
                pass
            if done_event.is_set():
                break
            body = self._render_progress_body(state)
            text = self.strings[title_key].format(body=body)
            if text != last_text:
                last_text = text
                try:
                    await call.edit(text)
                except Exception:
                    pass

    async def _dl(self, media, fname, client=None, progress_cb=None):
        c = client or self._client
        path = self._tmp_path(fname)
        try:
            result = await c.download_media(media, file=path, progress_callback=progress_cb)
            if result and os.path.exists(result):
                return result
        except Exception as e:
            logger.warning(f"[Robber] dl failed: {e}")
        return None

    async def _send_single_file(
        self,
        target,
        path,
        caption=None,
        reply_to=None,
        as_doc=False,
        client=None,
        state=None,
    ):
        c = client or self._client
        probe = await self._probe(path)
        is_video = bool(probe.get("vcodec"))
        norm_path = None
        thumb_path = None
        try:
            if state:
                state["stage"] = "upload"
                state["up_t0"] = time.time()
                state["up_total"] = os.path.getsize(path)
            up_cb = self._make_progress_cb(state, "up") if state else None

            if is_video and not as_doc:
                if state:
                    state["stage"] = "ffmpeg"
                norm_path = self._tmp_path(f"norm_{os.path.basename(path)}.mp4")
                await self._normalize(path, norm_path, state)
                use_path = norm_path
                probe_norm = await self._probe(norm_path)
                thumb_path = self._tmp_path(f"th_{os.path.basename(path)}.jpg")
                await self._make_thumb(
                    norm_path, thumb_path, min(1.0, probe_norm["duration"] / 2)
                )
                thumb = thumb_path if os.path.exists(thumb_path) else None
                attrs = [
                    DocumentAttributeVideo(
                        duration=int(probe_norm["duration"]),
                        w=probe_norm["width"],
                        h=probe_norm["height"],
                        supports_streaming=True,
                    ),
                    DocumentAttributeFilename(os.path.basename(norm_path)),
                ]
                if state:
                    state["stage"] = "upload"
                    state["up_total"] = os.path.getsize(norm_path)
                sent = await self._safe(
                    c.send_file(
                        target,
                        use_path,
                        caption=caption,
                        parse_mode="html",
                        reply_to=reply_to,
                        attributes=attrs,
                        thumb=thumb,
                        supports_streaming=True,
                        force_document=False,
                        progress_callback=up_cb,
                    ),
                    client=c,
                )
            else:
                if state:
                    state["ff"] = "skip"
                fname = os.path.basename(path)
                attrs = [DocumentAttributeFilename(fname)] if as_doc else None
                sent = await self._safe(
                    c.send_file(
                        target,
                        path,
                        caption=caption,
                        parse_mode="html",
                        reply_to=reply_to,
                        force_document=as_doc,
                        attributes=attrs,
                        progress_callback=up_cb,
                    ),
                    client=c,
                )
            if state:
                state["up_done"] = True
            return sent
        finally:
            for p in (norm_path, thumb_path):
                if p and os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass

    async def _send_msg(self, target, msg, id_map, as_doc=False, client=None, state=None):
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
            fname = f"msg_{msg.id}"
            if state:
                state["stage"] = "download"
            dl_cb = self._make_progress_cb(state, "dl") if state else None
            path = await self._dl(media, fname, client=c, progress_cb=dl_cb)
            if path:
                if state:
                    state["dl_done"] = True
                try:
                    return await self._send_single_file(
                        target,
                        path,
                        caption=text,
                        reply_to=reply_to,
                        as_doc=as_doc,
                        client=c,
                        state=state,
                    )
                finally:
                    try:
                        os.remove(path)
                    except Exception:
                        pass
            else:
                if text:
                    return await self._safe(
                        c.send_message(target, text, parse_mode="html", reply_to=reply_to),
                        client=c,
                    )
                return None
        if text:
            return await self._safe(
                c.send_message(target, text, parse_mode="html", reply_to=reply_to), client=c
            )
        return None

    async def _send_album(
        self, target, album_msgs, id_map, as_doc=False, client=None, state=None
    ):
        c = client or self._client
        first = album_msgs[0]
        reply_to = None
        if first.reply_to and hasattr(first.reply_to, "reply_to_msg_id"):
            reply_to = id_map.get(first.reply_to.reply_to_msg_id)
        paths = []
        captions = []
        temp_cleanup = []
        try:
            if state:
                state["stage"] = "download"
            for i, am in enumerate(album_msgs):
                if am.media and not isinstance(
                    am.media, (MessageMediaWebPage,) + SKIP_MEDIA_TYPES
                ):
                    dl_cb = self._make_progress_cb(state, "dl") if state else None
                    path = await self._dl(
                        am.media, f"alb_{am.id}_{i}", client=c, progress_cb=dl_cb
                    )
                    if path:
                        paths.append(path)
                        temp_cleanup.append(path)
                        captions.append(self._get_html(am) or "")
            if not paths:
                return []
            if state:
                state["dl_done"] = True

            use_paths = []
            for p in paths:
                probe = await self._probe(p)
                if probe.get("vcodec") and not as_doc:
                    if state:
                        state["stage"] = "ffmpeg"
                    norm = self._tmp_path(f"alb_norm_{os.path.basename(p)}.mp4")
                    await self._normalize(p, norm, state)
                    use_paths.append(norm)
                    temp_cleanup.append(norm)
                else:
                    use_paths.append(p)

            if state:
                state["stage"] = "upload"
                state["up_total"] = sum(os.path.getsize(p) for p in use_paths)
            up_cb = self._make_progress_cb(state, "up") if state else None

            sent = await self._safe(
                c.send_file(
                    target,
                    use_paths,
                    caption=captions,
                    parse_mode="html",
                    reply_to=reply_to,
                    force_document=as_doc,
                    progress_callback=up_cb,
                ),
                client=c,
            )
            if state:
                state["up_done"] = True
            if not isinstance(sent, list):
                sent = [sent]
            return sent
        finally:
            for p in temp_cleanup:
                if p and os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass

    async def _create_target(self, title, about, client=None):
        c = client or self._client
        as_channel = self.config["CREATE_CHANNEL"]
        result = await self._safe(
            c(
                functions.channels.CreateChannelRequest(
                    title=title,
                    about=about,
                    broadcast=as_channel,
                    megagroup=not as_channel,
                )
            ),
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
                        c(
                            functions.channels.EditPhotoRequest(
                                channel=channel,
                                photo=types.InputChatUploadedPhoto(file=photo),
                            )
                        ),
                        client=c,
                    )
                finally:
                    try:
                        os.remove(downloaded)
                    except Exception:
                        pass
        except Exception as e:
            logger.warning(f"[Robber] avatar failed: {e}")

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
        async for msg in c.iter_messages(
            entity, min_id=anchor_msg.id - 20, max_id=anchor_msg.id + 20
        ):
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
                result = await c(
                    functions.stories.GetPinnedStoriesRequest(
                        peer=peer, offset_id=offset_id, limit=100
                    )
                )
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

    async def _download_story_media(self, story, idx: int, client=None, progress_cb=None):
        c = client or self._client
        media = getattr(story, "media", None)
        if not media:
            return None
        is_video = isinstance(media, types.MessageMediaDocument)
        ext = ".mp4" if is_video else ".jpg"
        fname = f"story_{idx}{ext}"
        path = self._tmp_path(fname)
        if os.path.exists(path):
            try:
                os.remove(path)
            except Exception:
                pass
        try:
            downloaded = await c.download_media(media, file=path, progress_callback=progress_cb)
            if downloaded and os.path.exists(downloaded):
                return downloaded
        except Exception as e:
            logger.warning(f"[Robber] story dl failed: {e}")
        return None

    def _get_main_markup(self, slot: str | None = None):
        return [
            [
                {
                    "text": self.strings["btn_double"],
                    "input": self.strings["input_channel"],
                    "handler": self._cb_double_prompt_mode,
                    "args": (slot,),
                    "style": "primary",
                }
            ],
            [
                {
                    "text": self.strings["btn_steal"],
                    "input": self.strings["input_post"],
                    "handler": self._cb_steal_prompt_mode,
                    "args": (slot,),
                    "style": "primary",
                }
            ],
            [
                {
                    "text": self.strings["btn_sdl"],
                    "input": self.strings["input_story"],
                    "handler": self._cb_sdl_input,
                    "args": (slot,),
                    "style": "primary",
                }
            ],
            [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
        ]

    def _get_account_select_markup(self, slots: dict):
        return [
            [
                {
                    "text": self.strings["btn_owner"],
                    "callback": self._cb_owner_selected,
                    "style": "primary",
                }
            ],
            [
                {
                    "text": self.strings["btn_connected"],
                    "callback": self._cb_connected_select,
                    "style": "success",
                }
            ],
            [{"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"}],
        ]

    def _get_connected_markup(self, slots: dict):
        markup = []
        styles = ["primary", "success", "primary", "success", "primary"]
        for i, (slot, data) in enumerate(slots.items()):
            label = self.strings["slot_info"].format(
                slot=slot,
                name=data.get("name", "?"),
                uid=data.get("uid", "?"),
            )
            markup.append(
                [
                    {
                        "text": label,
                        "callback": self._cb_slot_selected,
                        "args": (slot,),
                        "style": styles[i % len(styles)],
                    }
                ]
            )
        markup.append(
            [
                {
                    "text": self.strings["btn_back"],
                    "callback": self._cb_account_select,
                    "style": "danger",
                }
            ]
        )
        return markup

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    async def _cb_account_select(self, call: InlineCall):
        slots = self._load_slots()
        if not slots:
            await call.edit(self.strings["main_menu"], reply_markup=self._get_main_markup(None))
            return
        await call.edit(
            self.strings["account_select"], reply_markup=self._get_account_select_markup(slots)
        )

    async def _cb_owner_selected(self, call: InlineCall):
        await call.edit(self.strings["main_menu"], reply_markup=self._get_main_markup(None))

    async def _cb_connected_select(self, call: InlineCall):
        slots = self._load_slots()
        if not slots:
            await call.edit(self.strings["main_menu"], reply_markup=self._get_main_markup(None))
            return
        await call.edit(
            self.strings["connected_select"], reply_markup=self._get_connected_markup(slots)
        )

    async def _cb_slot_selected(self, call: InlineCall, slot: str):
        slots = self._load_slots()
        data = slots.get(str(slot), {})
        name = data.get("name", "?")
        uid = data.get("uid", "?")
        text = (
            f"<blockquote><b>Robber</b> | Slot {slot}</blockquote>\n"
            f"<blockquote>Account: <b>{utils.escape_html(name)}</b> | ID: <code>{uid}</code></blockquote>"
        )
        await call.edit(text, reply_markup=self._get_main_markup(str(slot)))

    async def _cb_main_menu(self, call: InlineCall, slot: str | None = None):
        slots = self._load_slots()
        if slots:
            await call.edit(
                self.strings["account_select"], reply_markup=self._get_account_select_markup(slots)
            )
        else:
            await call.edit(self.strings["main_menu"], reply_markup=self._get_main_markup(None))

    async def _cb_connect_string(self, call: InlineCall, string_session: str):
        await call.edit(self.strings["connecting"])
        slots = self._load_slots()
        slot = self._next_slot(slots)
        if slot is None:
            await call.edit(
                self.strings["connect_slots_full"],
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_close"],
                            "callback": self._cb_close,
                            "style": "danger",
                        }
                    ]
                ],
            )
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
            await call.edit(
                self.strings["connect_ok"].format(
                    slot=slot, user=utils.escape_html(name), uid=me.id
                ),
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_main_menu,
                            "args": (slot,),
                            "style": "primary",
                        }
                    ]
                ],
            )
        except AuthKeyUnregisteredError:
            await call.edit(self.strings["connect_fail_revoked"])
        except UserDeactivatedBanError:
            await call.edit(self.strings["connect_fail_banned"])
        except Exception as e:
            await call.edit(self.strings["connect_fail_error"].format(err=utils.escape_html(str(e))))

    async def _cb_connect_hex_dc(self, call: InlineCall, hex_key: str, dc_id: int):
        auth_key = bytes.fromhex(hex_key)
        string_session = self._build_string_session(dc_id, auth_key)
        if not string_session:
            await call.edit(self.strings["connect_fail_invalid"])
            return
        await self._cb_connect_string(call, string_session)

    async def _cb_steal_prompt_mode(
        self, call: InlineCall, post_link: str, slot: str | None = None
    ):
        post_link = post_link.strip()
        if not POST_LINK_RE.search(post_link):
            await call.answer(self.strings["steal_bad_link"], show_alert=True)
            return
        text = self.strings["mode_select"].format(target=utils.escape_html(post_link))
        markup = [
            [
                {
                    "text": self.strings["btn_mode_media"],
                    "callback": self._cb_steal_execute,
                    "args": (post_link, False, slot),
                    "style": "success",
                }
            ],
            [
                {
                    "text": self.strings["btn_mode_doc"],
                    "callback": self._cb_steal_execute,
                    "args": (post_link, True, slot),
                    "style": "primary",
                }
            ],
            [
                {
                    "text": self.strings["btn_back"],
                    "callback": self._cb_main_menu,
                    "args": (slot,),
                    "style": "danger",
                }
            ],
        ]
        await call.edit(text, reply_markup=markup)

    async def _cb_steal_execute(
        self, call: InlineCall, post_link: str, as_doc: bool = False, slot: str | None = None
    ):
        post_link = post_link.strip()
        client = await self._get_slot_client(slot)

        state = {
            "t0": time.time(),
            "stage": "download",
            "dl_cur": 0,
            "dl_total": 0,
            "dl_speed": 0,
            "dl_done": False,
            "ff": "wait",
            "ff_pct": 0.0,
            "up_cur": 0,
            "up_total": 0,
            "up_speed": 0,
            "up_done": False,
        }
        done_event = asyncio.Event()
        loop_task = asyncio.create_task(
            self._progress_loop(call, "steal_processing", state, done_event)
        )

        try:
            entity, msg_id = await self._resolve_post_link(post_link, client=client)
            anchor = await client.get_messages(entity, ids=[msg_id])
            anchor = anchor[0] if anchor else None
            if not anchor:
                done_event.set()
                loop_task.cancel()
                await call.edit(
                    self.strings["steal_not_found"],
                    reply_markup=[
                        [
                            {
                                "text": self.strings["btn_back"],
                                "callback": self._cb_main_menu,
                                "args": (slot,),
                                "style": "danger",
                            }
                        ]
                    ],
                )
                return

            target_chat = call.form["chat"]
            if anchor.grouped_id:
                album = await self._fetch_album(entity, anchor, client=client)
                await self._send_album(
                    target_chat, album, {}, as_doc=as_doc, client=client, state=state
                )
            else:
                await self._send_msg(
                    target_chat, anchor, {}, as_doc=as_doc, client=client, state=state
                )

            done_event.set()
            loop_task.cancel()
            await call.edit(
                self.strings["steal_done"],
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_main_menu,
                            "args": (slot,),
                            "style": "primary",
                        }
                    ]
                ],
            )
        except Exception as e:
            done_event.set()
            loop_task.cancel()
            logger.error(f"[Robber] steal error: {e}", exc_info=True)
            await call.edit(
                self.strings["steal_error"].format(err=utils.escape_html(str(e))),
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_main_menu,
                            "args": (slot,),
                            "style": "danger",
                        }
                    ]
                ],
            )

    async def _cb_sdl_input(self, call: InlineCall, user_input: str, slot: str | None = None):
        user_input = user_input.strip()
        client = await self._get_slot_client(slot)

        await call.edit(self.strings["connecting"])
        try:
            peer, story_id = await self._resolve_story_user(user_input, client=client)
        except Exception:
            await call.edit(
                self.strings["sdl_bad_input"],
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_main_menu,
                            "args": (slot,),
                            "style": "danger",
                        }
                    ]
                ],
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
            text = self.strings["mode_select"].format(
                target=f"{user_label} (Story #{story_id})"
            )
            markup = [
                [
                    {
                        "text": self.strings["btn_mode_media"],
                        "callback": self._cb_sdl_execute_stories,
                        "args": (peer, [story_id], False, slot),
                        "style": "success",
                    }
                ],
                [
                    {
                        "text": self.strings["btn_mode_doc"],
                        "callback": self._cb_sdl_execute_stories,
                        "args": (peer, [story_id], True, slot),
                        "style": "primary",
                    }
                ],
                [
                    {
                        "text": self.strings["btn_sdl_zip"],
                        "callback": self._cb_sdl_zip,
                        "args": (peer, [story_id], slot),
                        "style": "primary",
                    }
                ],
                [
                    {
                        "text": self.strings["btn_back"],
                        "callback": self._cb_main_menu,
                        "args": (slot,),
                        "style": "danger",
                    }
                ],
            ]
        else:
            text = self.strings["mode_select"].format(target=user_label)
            markup = [
                [
                    {
                        "text": self.strings["btn_sdl_media"],
                        "callback": self._cb_sdl_all,
                        "args": (peer, False, slot),
                        "style": "success",
                    }
                ],
                [
                    {
                        "text": self.strings["btn_sdl_doc"],
                        "callback": self._cb_sdl_all,
                        "args": (peer, True, slot),
                        "style": "primary",
                    }
                ],
                [
                    {
                        "text": self.strings["btn_sdl_zip"],
                        "callback": self._cb_sdl_all_zip,
                        "args": (peer, slot),
                        "style": "primary",
                    }
                ],
                [
                    {
                        "text": self.strings["btn_sdl_one"],
                        "input": self.strings["input_story_link"],
                        "handler": self._cb_sdl_one_link,
                        "args": (slot,),
                        "style": "primary",
                    }
                ],
                [
                    {
                        "text": self.strings["btn_back"],
                        "callback": self._cb_main_menu,
                        "args": (slot,),
                        "style": "danger",
                    }
                ],
            ]
        await call.edit(text, reply_markup=markup)

    async def _cb_sdl_one_link(self, call: InlineCall, link: str, slot: str | None = None):
        link = link.strip()
        client = await self._get_slot_client(slot)
        try:
            peer, story_id = await self._resolve_story_user(link, client=client)
            if story_id is None:
                await call.answer(self.strings["sdl_bad_input"], show_alert=True)
                return
            await self._cb_sdl_input(call, link, slot)
        except Exception:
            await call.answer(self.strings["sdl_bad_input"], show_alert=True)

    async def _cb_sdl_all(self, call: InlineCall, peer, as_doc: bool, slot: str | None = None):
        client = await self._get_slot_client(slot)
        await call.edit(self.strings["connecting"])
        stories = await self._fetch_all_stories(peer, client=client)
        if not stories:
            await call.edit(
                self.strings["sdl_no_stories"],
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_main_menu,
                            "args": (slot,),
                            "style": "danger",
                        }
                    ]
                ],
            )
            return
        story_ids = [s.id for s in stories]
        await self._cb_sdl_execute_stories(call, peer, story_ids, as_doc, slot)

    async def _cb_sdl_all_zip(self, call: InlineCall, peer, slot: str | None = None):
        client = await self._get_slot_client(slot)
        await call.edit(self.strings["connecting"])
        stories = await self._fetch_all_stories(peer, client=client)
        if not stories:
            await call.edit(
                self.strings["sdl_no_stories"],
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_main_menu,
                            "args": (slot,),
                            "style": "danger",
                        }
                    ]
                ],
            )
            return
        story_ids = [s.id for s in stories]
        await self._cb_sdl_zip(call, peer, story_ids, slot)

    async def _cb_sdl_execute_stories(
        self,
        call: InlineCall,
        peer,
        story_ids: list,
        as_doc: bool = False,
        slot: str | None = None,
    ):
        client = await self._get_slot_client(slot)
        target_chat = call.form["chat"]
        total = len(story_ids)
        sent = 0

        state = {
            "t0": time.time(),
            "stage": "stories",
            "dl_cur": 0,
            "dl_total": 0,
            "dl_speed": 0,
            "dl_done": False,
            "ff": "wait",
            "ff_pct": 0.0,
            "up_cur": 0,
            "up_total": 0,
            "up_speed": 0,
            "up_done": False,
        }
        done_event = asyncio.Event()
        loop_task = asyncio.create_task(
            self._progress_loop(call, "sdl_processing", state, done_event)
        )

        try:
            for i in range(0, len(story_ids), 100):
                batch = story_ids[i : i + 100]
                try:
                    result = await client(
                        functions.stories.GetStoriesByIDRequest(peer=peer, id=batch)
                    )
                except FloodWaitError as e:
                    await asyncio.sleep(e.seconds + FLOOD_EXTRA)
                    try:
                        result = await client(
                            functions.stories.GetStoriesByIDRequest(peer=peer, id=batch)
                        )
                    except Exception:
                        continue
                except Exception:
                    continue

                for story in getattr(result, "stories", []):
                    if not getattr(story, "media", None):
                        continue
                    state["stage"] = f"story {sent + 1}/{total}"
                    state["dl_done"] = False
                    dl_cb = self._make_progress_cb(state, "dl")
                    path = await self._download_story_media(
                        story, story.id, client=client, progress_cb=dl_cb
                    )
                    if not path:
                        continue
                    state["dl_done"] = True
                    try:
                        await self._send_single_file(
                            target_chat,
                            path,
                            caption=None,
                            reply_to=None,
                            as_doc=as_doc,
                            client=self._client,
                            state=state,
                        )
                        sent += 1
                    except Exception as e:
                        logger.warning(f"[Robber] story send failed: {e}")
                    finally:
                        try:
                            os.remove(path)
                        except Exception:
                            pass
                    await asyncio.sleep(0.3)

            done_event.set()
            loop_task.cancel()
            await call.edit(
                self.strings["sdl_done_all"].format(count=sent),
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_main_menu,
                            "args": (slot,),
                            "style": "primary",
                        }
                    ]
                ],
            )
        except Exception as e:
            done_event.set()
            loop_task.cancel()
            logger.error(f"[Robber] stories error: {e}", exc_info=True)
            await call.edit(
                self.strings["steal_error"].format(err=utils.escape_html(str(e))),
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_main_menu,
                            "args": (slot,),
                            "style": "danger",
                        }
                    ]
                ],
            )

    async def _cb_sdl_zip(
        self, call: InlineCall, peer, story_ids: list, slot: str | None = None
    ):
        client = await self._get_slot_client(slot)
        target_chat = call.form["chat"]
        total = len(story_ids)

        state = {
            "t0": time.time(),
            "stage": "download",
            "dl_cur": 0,
            "dl_total": 0,
            "dl_speed": 0,
            "dl_done": False,
            "ff": "skip",
            "up_cur": 0,
            "up_total": 0,
            "up_speed": 0,
            "up_done": False,
        }
        done_event = asyncio.Event()
        loop_task = asyncio.create_task(
            self._progress_loop(call, "sdl_processing", state, done_event)
        )

        downloaded = []
        try:
            for i in range(0, len(story_ids), 100):
                batch = story_ids[i : i + 100]
                try:
                    result = await client(
                        functions.stories.GetStoriesByIDRequest(peer=peer, id=batch)
                    )
                except FloodWaitError as e:
                    await asyncio.sleep(e.seconds + FLOOD_EXTRA)
                    try:
                        result = await client(
                            functions.stories.GetStoriesByIDRequest(peer=peer, id=batch)
                        )
                    except Exception:
                        continue
                except Exception:
                    continue

                for story in getattr(result, "stories", []):
                    if not getattr(story, "media", None):
                        continue
                    state["stage"] = f"download {len(downloaded) + 1}/{total}"
                    dl_cb = self._make_progress_cb(state, "dl")
                    path = await self._download_story_media(
                        story, story.id, client=client, progress_cb=dl_cb
                    )
                    if path:
                        downloaded.append(path)

            state["dl_done"] = True
            if not downloaded:
                done_event.set()
                loop_task.cancel()
                await call.edit(
                    self.strings["sdl_not_found"],
                    reply_markup=[
                        [
                            {
                                "text": self.strings["btn_back"],
                                "callback": self._cb_main_menu,
                                "args": (slot,),
                                "style": "danger",
                            }
                        ]
                    ],
                )
                return

            state["stage"] = "zipping"
            zip_name = f"stories_{int(time.time())}.zip"
            zip_path = self._tmp_path(zip_name)
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for p in downloaded:
                    zf.write(p, os.path.basename(p))

            state["stage"] = "upload"
            state["up_total"] = os.path.getsize(zip_path)
            up_cb = self._make_progress_cb(state, "up")
            await self._safe(
                self._client.send_file(
                    target_chat,
                    zip_path,
                    force_document=True,
                    attributes=[DocumentAttributeFilename(file_name=zip_name)],
                    progress_callback=up_cb,
                ),
                client=self._client,
            )
            state["up_done"] = True

            done_event.set()
            loop_task.cancel()
            await call.edit(
                self.strings["sdl_done_zip"].format(name=zip_name, count=len(downloaded)),
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_main_menu,
                            "args": (slot,),
                            "style": "primary",
                        }
                    ]
                ],
            )
        except Exception as e:
            done_event.set()
            loop_task.cancel()
            logger.error(f"[Robber] zip error: {e}", exc_info=True)
            await call.edit(
                self.strings["steal_error"].format(err=utils.escape_html(str(e))),
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_main_menu,
                            "args": (slot,),
                            "style": "danger",
                        }
                    ]
                ],
            )
        finally:
            for p in downloaded:
                if os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass
            if os.path.exists(zip_path):
                try:
                    os.remove(zip_path)
                except Exception:
                    pass

    async def _cb_double_prompt_mode(
        self, call: InlineCall, channel_input: str, slot: str | None = None
    ):
        channel_input = channel_input.strip()
        if not channel_input:
            await call.answer(self.strings["double_no_id"], show_alert=True)
            return
        text = self.strings["mode_select"].format(target=utils.escape_html(channel_input))
        markup = [
            [
                {
                    "text": self.strings["btn_mode_media"],
                    "callback": self._cb_double_execute,
                    "args": (channel_input, False, slot),
                    "style": "success",
                }
            ],
            [
                {
                    "text": self.strings["btn_mode_doc"],
                    "callback": self._cb_double_execute,
                    "args": (channel_input, True, slot),
                    "style": "primary",
                }
            ],
            [
                {
                    "text": self.strings["btn_back"],
                    "callback": self._cb_main_menu,
                    "args": (slot,),
                    "style": "danger",
                }
            ],
        ]
        await call.edit(text, reply_markup=markup)

    async def _cb_double_execute(
        self,
        call: InlineCall,
        channel_input: str,
        as_doc: bool = False,
        slot: str | None = None,
    ):
        channel_input = channel_input.strip()
        client = await self._get_slot_client(slot)

        await call.edit(self.strings["double_start"])

        try:
            try:
                src = await client.get_entity(int(channel_input))
            except ValueError:
                src = await client.get_entity(channel_input)

            src_title = getattr(src, "title", "Copied Channel")
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

            state = {
                "t0": time.time(),
                "stage": "copying",
                "dl_cur": 0,
                "dl_total": 0,
                "dl_speed": 0,
                "dl_done": False,
                "ff": "wait",
                "ff_pct": 0.0,
                "up_cur": 0,
                "up_total": 0,
                "up_speed": 0,
                "up_done": False,
            }

            last_edit = 0
            edit_interval = 2.5

            async def _update_progress():
                nonlocal last_edit
                now = time.time()
                if now - last_edit >= edit_interval:
                    last_edit = now
                    body = self._render_progress_body(state)
                    try:
                        await call.edit(
                            self.strings["double_progress"].format(
                                done=done,
                                total=total,
                                skipped=skipped,
                                albums=albums,
                                body=body,
                            )
                        )
                    except Exception:
                        pass

            i = 0
            while i < len(all_ids):
                batch_ids = all_ids[i : i + BATCH_SIZE]
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
                            state["stage"] = f"album {albums + 1} ({len(album)} msgs)"
                            sent_list = await self._send_album(
                                new_entity,
                                album,
                                id_map,
                                as_doc=as_doc,
                                client=client,
                                state=state,
                            )
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
                            try:
                                await call.edit(
                                    self.strings["double_send_error"].format(
                                        msg_id=msg.id, err=utils.escape_html(str(e))
                                    )
                                )
                            except Exception:
                                pass
                            await asyncio.sleep(2)
                        j = k
                    else:
                        try:
                            state["stage"] = f"msg {done + 1}/{total}"
                            sent = await self._send_msg(
                                new_entity,
                                msg,
                                id_map,
                                as_doc=as_doc,
                                client=client,
                                state=state,
                            )
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
                            try:
                                await call.edit(
                                    self.strings["double_send_error"].format(
                                        msg_id=msg.id, err=utils.escape_html(str(e))
                                    )
                                )
                            except Exception:
                                pass
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
                self.strings["double_done"].format(
                    done=done, skipped=skipped, albums=albums, link=link
                ),
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_main_menu,
                            "args": (slot,),
                            "style": "primary",
                        }
                    ]
                ],
            )
        except Exception as e:
            logger.error(f"[Robber] double error: {e}", exc_info=True)
            await call.edit(
                self.strings["double_error"].format(err=utils.escape_html(str(e))),
                reply_markup=[
                    [
                        {
                            "text": self.strings["btn_back"],
                            "callback": self._cb_main_menu,
                            "args": (slot,),
                            "style": "danger",
                        }
                    ]
                ],
            )

    @loader.command(
        ru_doc="Копировщик каналов, кража постов и скачивание историй с поддержкой мультиаккаунтов",
        en_doc="Channel copy, post stealer and story downloader with multi-account support",
    )
    async def rob(self, message):
        """Channel copy, post stealer and story downloader with multi-account support"""
        reply = await message.get_reply_message()
        is_forum = await self._is_forum_chat(message)
        topic_id = self._get_topic_id(message) if is_forum else None

        if reply:
            text = getattr(reply, "raw_text", None) or reply.message or ""
            input_type = None
            input_data = None

            if (
                reply.file
                and getattr(reply.file, "name", None)
                and reply.file.name.endswith(".session")
            ):
                file_path = self._tmp_path("incoming.session")
                try:
                    await reply.download_media(file_path)
                    data = await self._read_session_file(file_path)
                    if data:
                        string_session = self._build_string_session(
                            data["dc_id"], data["auth_key"]
                        )
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
                        reply_markup=[
                            [
                                {
                                    "text": self.strings["btn_close"],
                                    "callback": self._cb_close,
                                    "style": "danger",
                                }
                            ]
                        ],
                        reply_to=topic_id,
                        silent=True,
                    )
                    return

                if input_type == "HEX":
                    await self.inline.form(
                        text=self.strings["connect_select_dc"],
                        message=message,
                        reply_markup=[
                            [
                                {
                                    "text": self.strings["btn_dc_1"],
                                    "callback": self._cb_connect_hex_dc,
                                    "args": (input_data, 1),
                                    "style": "primary",
                                },
                                {
                                    "text": self.strings["btn_dc_2"],
                                    "callback": self._cb_connect_hex_dc,
                                    "args": (input_data, 2),
                                    "style": "primary",
                                },
                                {
                                    "text": self.strings["btn_dc_3"],
                                    "callback": self._cb_connect_hex_dc,
                                    "args": (input_data, 3),
                                    "style": "primary",
                                },
                            ],
                            [
                                {
                                    "text": self.strings["btn_dc_4"],
                                    "callback": self._cb_connect_hex_dc,
                                    "args": (input_data, 4),
                                    "style": "primary",
                                },
                                {
                                    "text": self.strings["btn_dc_5"],
                                    "callback": self._cb_connect_hex_dc,
                                    "args": (input_data, 5),
                                    "style": "primary",
                                },
                            ],
                            [
                                {
                                    "text": self.strings["btn_close"],
                                    "callback": self._cb_close,
                                    "style": "danger",
                                }
                            ],
                        ],
                        reply_to=topic_id,
                        silent=True,
                    )
                else:
                    await self.inline.form(
                        text=self.strings["connect_menu"].format(type=input_type),
                        message=message,
                        reply_markup=[
                            [
                                {
                                    "text": self.strings["btn_connect"],
                                    "callback": self._cb_connect_string,
                                    "args": (input_data,),
                                    "style": "success",
                                }
                            ],
                            [
                                {
                                    "text": self.strings["btn_close"],
                                    "callback": self._cb_close,
                                    "style": "danger",
                                }
                            ],
                        ],
                        reply_to=topic_id,
                        silent=True,
                    )
                return

        slots = self._load_slots()
        if slots:
            await self.inline.form(
                text=self.strings["account_select"],
                message=message,
                reply_markup=self._get_account_select_markup(slots),
                reply_to=topic_id,
                silent=True,
            )
        else:
            await self.inline.form(
                text=self.strings["main_menu"],
                message=message,
                reply_markup=self._get_main_markup(None),
                reply_to=topic_id,
                silent=True,
            )