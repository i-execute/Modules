__version__ = (1, 1, 0)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/Robber/MetaBanner.jpeg

import logging
import asyncio
import os
import re
import tempfile
import shutil
from telethon.tl import functions, types
from telethon.tl.types import (
    MessageMediaWebPage,
    MessageMediaContact,
    MessageMediaGeo,
    MessageMediaGeoLive,
    MessageMediaPoll,
)
from telethon.errors import FloodWaitError
from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

STORY_LINK_RE = re.compile(r"t\.me/([^/]+)/s/(\d+)")
POST_LINK_RE = re.compile(r"t\.me/(?:c/)?([^/]+)/(\d+)")

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


@loader.tds
class Robber(loader.Module):
    """Fully steal channels and stories, support for private channels"""

    strings = {
        "name": "Robber",
        
        "main_menu": (
            "<b>Robber - Channel & Content Stealer</b>\n\n"
            "Select operation:"
        ),
        
        "btn_double": "Copy Channel",
        "btn_steal": "Steal Post",
        "btn_sdl": "Download Story",
        "btn_back": "Back to Menu",
        "btn_close": "Close",
        
        "double_start": "<b>Starting copy...</b>\nFetching messages...",
        "double_progress": (
            "<b>Copying...</b>\n"
            "Processed: {done} / {total}\n"
            "Skipped: {skipped}\n"
            "Albums: {albums}"
        ),
        "double_done": (
            "<b>Copy complete!</b>\n"
            "Processed: {done}\n"
            "Skipped: {skipped}\n"
            "Albums: {albums}\n"
            "Link: {link}"
        ),
        "double_no_id": "<b>Error:</b> Provide channel ID or username",
        "double_error": "<b>Error:</b> <code>{err}</code>",
        "double_send_error": (
            "<b>Copying...</b>\n"
            "Processed: {done} / {total}\n"
            "Skipped: {skipped}\n"
            "Albums: {albums}\n\n"
            "<b>Send error on msg {msg_id}:</b>\n<code>{err}</code>"
        ),
        
        "input_channel": "Enter channel ID or username:",
        "input_post": "Enter post link (e.g., https://t.me/c/3872609933/8):",
        "input_story": "Enter story link (e.g., https://t.me/username/s/53):",
        
        "steal_bad_link": "<b>Error:</b> Cannot parse post link",
        "steal_not_found": "<b>Error:</b> Post not found",
        "steal_error": "<b>Error:</b> <code>{err}</code>",
        "steal_processing": "<b>Stealing post...</b>",
        "steal_done": "<b>Post stolen successfully!</b>",
        
        "sdl_bad_link": "<b>Error:</b> Cannot parse story link",
        "sdl_not_found": "<b>Error:</b> Story not found or has no media",
        "sdl_download_fail": "<b>Error:</b> Download failed",
        "sdl_processing": "<b>Downloading story...</b>",
        "sdl_done": "<b>Story downloaded successfully!</b>",
    }

    strings_ru = {
        "main_menu": (
            "<b>Robber - Копировщик каналов и контента</b>\n\n"
            "Выберите операцию:"
        ),
        
        "btn_double": "Копировать канал",
        "btn_steal": "Украсть пост",
        "btn_sdl": "Скачать историю",
        "btn_back": "Назад в меню",
        "btn_close": "Закрыть",
        
        "double_start": "<b>Начинаю копирование...</b>\nПолучаю сообщения...",
        "double_progress": (
            "<b>Копирую...</b>\n"
            "Обработано: {done} / {total}\n"
            "Пропущено: {skipped}\n"
            "Альбомы: {albums}"
        ),
        "double_done": (
            "<b>Копирование завершено!</b>\n"
            "Обработано: {done}\n"
            "Пропущено: {skipped}\n"
            "Альбомы: {albums}\n"
            "Ссылка: {link}"
        ),
        "double_no_id": "<b>Ошибка:</b> Укажите ID или юзернейм канала",
        "double_error": "<b>Ошибка:</b> <code>{err}</code>",
        "double_send_error": (
            "<b>Копирую...</b>\n"
            "Обработано: {done} / {total}\n"
            "Пропущено: {skipped}\n"
            "Альбомы: {albums}\n\n"
            "<b>Ошибка отправки сообщения {msg_id}:</b>\n<code>{err}</code>"
        ),
        
        "input_channel": "Введите ID или юзернейм канала:",
        "input_post": "Введите ссылку на пост (например, https://t.me/c/3872609933/8):",
        "input_story": "Введите ссылку на историю (например, https://t.me/username/s/53):",
        
        "steal_bad_link": "<b>Ошибка:</b> Не могу распарсить ссылку на пост",
        "steal_not_found": "<b>Ошибка:</b> Пост не найден",
        "steal_error": "<b>Ошибка:</b> <code>{err}</code>",
        "steal_processing": "<b>Крадем пост...</b>",
        "steal_done": "<b>Пост успешно украден!</b>",
        
        "sdl_bad_link": "<b>Ошибка:</b> Не могу распарсить ссылку на историю",
        "sdl_not_found": "<b>Ошибка:</b> История не найдена или без медиа",
        "sdl_download_fail": "<b>Ошибка:</b> Не удалось скачать",
        "sdl_processing": "<b>Скачиваю историю...</b>",
        "sdl_done": "<b>История успешно скачана!</b>",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "CREATE_CHANNEL",
                True,
                "If True creates channel, if False creates group",
                validator=loader.validators.Boolean(),
            )
        )
        self._temp_dir = None
        self._client = None

    async def client_ready(self, client, _):
        self._client = client
        self._temp_dir = os.path.join(tempfile.gettempdir(), "robber_tmp")
        os.makedirs(self._temp_dir, exist_ok=True)

    async def on_unload(self):
        self._wipe_temp()

    def _wipe_temp(self):
        if self._temp_dir and os.path.exists(self._temp_dir):
            try:
                shutil.rmtree(self._temp_dir)
            except Exception:
                pass

    def _tmp_path(self, name):
        return os.path.join(self._temp_dir, name)

    async def _safe(self, coro):
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

    async def _dl(self, media, fname):
        path = self._tmp_path(fname)
        try:
            result = await self._client.download_media(media, file=path)
            if result and os.path.exists(result):
                return result
        except Exception as e:
            logger.warning(f"[Robber] dl failed: {e}")
        return None

    async def _create_target(self, title, about):
        as_channel = self.config["CREATE_CHANNEL"]
        result = await self._safe(
            self._client(
                functions.channels.CreateChannelRequest(
                    title=title,
                    about=about,
                    broadcast=as_channel,
                    megagroup=not as_channel,
                )
            )
        )
        return result.chats[0]

    async def _set_avatar(self, channel, src_entity):
        try:
            av_path = self._tmp_path(f"av_{channel.id}")
            downloaded = await self._client.download_profile_photo(src_entity, file=av_path)
            if downloaded and os.path.exists(downloaded):
                try:
                    photo = await self._safe(self._client.upload_file(downloaded))
                    await self._safe(
                        self._client(
                            functions.channels.EditPhotoRequest(
                                channel=channel,
                                photo=types.InputChatUploadedPhoto(file=photo),
                            )
                        )
                    )
                finally:
                    try:
                        os.remove(downloaded)
                    except Exception:
                        pass
        except Exception as e:
            logger.warning(f"[Robber] avatar failed: {e}")

    async def _send_msg(self, target, msg, id_map):
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
            return await self._safe(
                self._client.send_message(target, fallback, reply_to=reply_to)
            )

        if media and isinstance(media, MessageMediaWebPage):
            media = None

        if media is not None:
            fname = f"media_{msg.id}"
            path = await self._dl(media, fname)
            if path:
                try:
                    return await self._safe(
                        self._client.send_file(
                            target,
                            path,
                            caption=text,
                            parse_mode="html",
                            reply_to=reply_to,
                            force_document=False,
                        )
                    )
                finally:
                    try:
                        os.remove(path)
                    except Exception:
                        pass
            else:
                if text:
                    return await self._safe(
                        self._client.send_message(
                            target, text, parse_mode="html", reply_to=reply_to
                        )
                    )
                return None

        if text:
            return await self._safe(
                self._client.send_message(
                    target, text, parse_mode="html", reply_to=reply_to
                )
            )

        return None

    async def _send_album(self, target, album_msgs, id_map):
        first = album_msgs[0]
        reply_to = None
        if first.reply_to and hasattr(first.reply_to, "reply_to_msg_id"):
            reply_to = id_map.get(first.reply_to.reply_to_msg_id)

        paths = []
        captions = []
        try:
            for i, am in enumerate(album_msgs):
                if am.media and not isinstance(
                    am.media, (MessageMediaWebPage,) + SKIP_MEDIA_TYPES
                ):
                    path = await self._dl(am.media, f"album_{am.id}_{i}")
                    if path:
                        paths.append(path)
                        captions.append(self._get_html(am) or "")

            if not paths:
                return []

            sent = await self._safe(
                self._client.send_file(
                    target,
                    paths,
                    caption=captions,
                    parse_mode="html",
                    reply_to=reply_to,
                )
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

    async def _resolve_post_link(self, link):
        m = POST_LINK_RE.search(link)
        if not m:
            return None, None

        chat_part = m.group(1)
        msg_id = int(m.group(2))

        try:
            if chat_part.isdigit():
                entity = await self._client.get_entity(int(f"-100{chat_part}"))
            else:
                entity = await self._client.get_entity(chat_part)
        except Exception as e:
            raise Exception(f"Cannot resolve chat: {e}")

        return entity, msg_id

    async def _fetch_album(self, entity, anchor_msg):
        if not anchor_msg.grouped_id:
            return [anchor_msg]

        group_id = anchor_msg.grouped_id
        album = []

        async for msg in self._client.iter_messages(
            entity,
            min_id=anchor_msg.id - 20,
            max_id=anchor_msg.id + 20,
        ):
            if msg.grouped_id == group_id:
                album.append(msg)

        if not album:
            album = [anchor_msg]

        album.sort(key=lambda m: m.id)
        return album

    def _get_main_markup(self):
        return [
            [
                {"text": self.strings["btn_double"], "callback": self._cb_double_menu, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_steal"], "callback": self._cb_steal_menu, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_sdl"], "callback": self._cb_sdl_menu, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"},
            ],
        ]

    async def _cb_main_menu(self, call: InlineCall):
        await call.edit(
            self.strings["main_menu"],
            reply_markup=self._get_main_markup()
        )

    async def _cb_double_menu(self, call: InlineCall):
        await call.edit(
            self.strings["main_menu"],
            reply_markup=[
                [
                    {"text": self.strings["btn_double"], "input": self.strings["input_channel"], "handler": self._cb_double_execute, "style": "success"}
                ],
                [
                    {"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}
                ],
            ]
        )

    async def _cb_steal_menu(self, call: InlineCall):
        await call.edit(
            self.strings["main_menu"],
            reply_markup=[
                [
                    {"text": self.strings["btn_steal"], "input": self.strings["input_post"], "handler": self._cb_steal_execute, "style": "success"}
                ],
                [
                    {"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}
                ],
            ]
        )

    async def _cb_sdl_menu(self, call: InlineCall):
        await call.edit(
            self.strings["main_menu"],
            reply_markup=[
                [
                    {"text": self.strings["btn_sdl"], "input": self.strings["input_story"], "handler": self._cb_sdl_execute, "style": "success"}
                ],
                [
                    {"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}
                ],
            ]
        )

    async def _cb_double_execute(self, call: InlineCall, channel_input: str):
        channel_input = channel_input.strip()
        if not channel_input:
            await call.answer(self.strings["double_no_id"], show_alert=True)
            return

        await call.edit(self.strings["double_start"])

        try:
            try:
                src = await self._client.get_entity(int(channel_input))
            except ValueError:
                src = await self._client.get_entity(channel_input)

            src_title = getattr(src, "title", "Copied")
            src_about = ""
            try:
                full = await self._client(
                    functions.channels.GetFullChannelRequest(src)
                )
                src_about = full.full_chat.about or ""
            except Exception:
                pass

            new_channel = await self._create_target(src_title, src_about)
            await self._set_avatar(new_channel, src)
            new_entity = await self._client.get_entity(new_channel.id)

            all_ids = []
            async for msg in self._client.iter_messages(src, reverse=True):
                all_ids.append(msg.id)

            total = len(all_ids)
            done = 0
            skipped = 0
            albums = 0
            id_map = {}

            i = 0
            while i < len(all_ids):
                batch_ids = all_ids[i: i + BATCH_SIZE]
                msgs = await self._safe(
                    self._client.get_messages(src, ids=batch_ids)
                )
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
                            sent_list = await self._send_album(new_entity, album, id_map)
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
                                    done=done, total=total,
                                    skipped=skipped, albums=albums,
                                    msg_id=msg.id, err=str(e),
                                )
                            )
                            await asyncio.sleep(2)

                        j = k
                    else:
                        try:
                            sent = await self._send_msg(new_entity, msg, id_map)
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
                                    done=done, total=total,
                                    skipped=skipped, albums=albums,
                                    msg_id=msg.id, err=str(e),
                                )
                            )
                            await asyncio.sleep(2)

                        j += 1

                    if done > 0 and done % PAUSE_EVERY == 0:
                        await call.edit(
                            self.strings["double_progress"].format(
                                done=done, total=total,
                                skipped=skipped, albums=albums,
                            )
                        )
                        await asyncio.sleep(PAUSE_DURATION)

                i += BATCH_SIZE

                await call.edit(
                    self.strings["double_progress"].format(
                        done=done, total=total,
                        skipped=skipped, albums=albums,
                    )
                )

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
                    [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]
                ]
            )

        except Exception as e:
            logger.error(f"[Robber] double error: {e}", exc_info=True)
            await call.edit(
                self.strings["double_error"].format(err=str(e)),
                reply_markup=[
                    [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]
                ]
            )

    async def _cb_steal_execute(self, call: InlineCall, post_link: str):
        post_link = post_link.strip()
        
        if not POST_LINK_RE.search(post_link):
            await call.answer(self.strings["steal_bad_link"], show_alert=True)
            return

        await call.edit(self.strings["steal_processing"])

        try:
            entity, msg_id = await self._resolve_post_link(post_link)

            anchor = await self._client.get_messages(entity, ids=[msg_id])
            anchor = anchor[0] if anchor else None

            if not anchor:
                await call.edit(
                    self.strings["steal_not_found"],
                    reply_markup=[
                        [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]
                    ]
                )
                return

            target_chat = call.form["chat"]

            if anchor.grouped_id:
                album = await self._fetch_album(entity, anchor)
                await self._send_album(target_chat, album, {})
            else:
                await self._send_msg(target_chat, anchor, {})

            await call.edit(
                self.strings["steal_done"],
                reply_markup=[
                    [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]
                ]
            )

        except Exception as e:
            logger.error(f"[Robber] steal error: {e}", exc_info=True)
            await call.edit(
                self.strings["steal_error"].format(err=str(e)),
                reply_markup=[
                    [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]
                ]
            )

    async def _cb_sdl_execute(self, call: InlineCall, story_link: str):
        story_link = story_link.strip()
        
        m = STORY_LINK_RE.search(story_link)
        if not m:
            await call.answer(self.strings["sdl_bad_link"], show_alert=True)
            return

        username = m.group(1)
        story_id = int(m.group(2))

        await call.edit(self.strings["sdl_processing"])

        try:
            peer = (
                await self._client(
                    functions.contacts.ResolveUsernameRequest(username)
                )
            ).peer

            stories_result = await self._client(
                functions.stories.GetStoriesByIDRequest(peer=peer, id=[story_id])
            )

            stories = stories_result.stories
            if not stories or not getattr(stories[0], "media", None):
                await call.edit(
                    self.strings["sdl_not_found"],
                    reply_markup=[
                        [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]
                    ]
                )
                return

            media = stories[0].media
            is_video = isinstance(media, types.MessageMediaDocument)
            ext = ".mp4" if is_video else ".jpg"
            fname = f"story{ext}"
            path = self._tmp_path(fname)

            if os.path.exists(path):
                os.remove(path)

            downloaded = await self._client.download_media(media, file=path)

            if not downloaded or not os.path.exists(downloaded):
                await call.edit(
                    self.strings["sdl_download_fail"],
                    reply_markup=[
                        [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]
                    ]
                )
                return

            target_chat = call.form["chat"]

            try:
                await self._client.send_file(
                    target_chat,
                    downloaded,
                    force_document=True,
                    attributes=[
                        types.DocumentAttributeFilename(file_name=fname)
                    ],
                )
                await call.edit(
                    self.strings["sdl_done"],
                    reply_markup=[
                        [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]
                    ]
                )
            finally:
                try:
                    os.remove(downloaded)
                except Exception:
                    pass

        except Exception as e:
            logger.error(f"[Robber] sdl error: {e}", exc_info=True)
            await call.edit(
                self.strings["steal_error"].format(err=str(e)),
                reply_markup=[
                    [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]
                ]
            )

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    @loader.command()
    async def rob(self, message):
        """Channel & content stealer"""
        await self.inline.form(
            text=self.strings["main_menu"],
            message=message,
            reply_markup=self._get_main_markup(),
            silent=True,
        )