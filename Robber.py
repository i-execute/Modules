__version__ = (1, 0, 0)
# meta developer: FireJester.t.me

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
    """Fully steal channels and stories, support for private channels, look config"""

    strings = {
        "name": "Robber",
        "help": (
            "<b>Robber Commands</b>\n\n"
            "<code>.double [channel_id]</code> — fully copy a channel\n"
            "<code>.steal [post_link]</code> — steal a post to current chat\n"
            "<code>.sdl [story_link]</code> — download a story as file\n\n"
            "Post link example: <code>https://t.me/c/3872609933/8</code>\n"
            "Story link example: <code>https://t.me/username/s/53</code>"
        ),
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
            "<b>⚠️ Send error on msg {msg_id}:</b>\n<code>{err}</code>"
        ),
        "steal_no_link": "<b>Error:</b> Provide post link\nExample: <code>https://t.me/c/3872609933/8</code>",
        "steal_bad_link": "<b>Error:</b> Cannot parse post link",
        "steal_not_found": "<b>Error:</b> Post not found",
        "steal_error": "<b>Error:</b> <code>{err}</code>",
        "sdl_no_link": "<b>Error:</b> Provide story link\nExample: <code>https://t.me/username/s/53</code>",
        "sdl_bad_link": "<b>Error:</b> Cannot parse story link",
        "sdl_not_found": "<b>Error:</b> Story not found or has no media",
        "sdl_download_fail": "<b>Error:</b> Download failed",
        "sdl_done": "<b>Done!</b>",
    }

    strings_ru = {
        "help": (
            "<b>Команды Robber</b>\n\n"
            "<code>.double [id канала]</code> — полностью скопировать канал\n"
            "<code>.steal [ссылка на пост]</code> — украсть пост в текущий чат\n"
            "<code>.sdl [ссылка на историю]</code> — скачать историю как файл\n\n"
            "Пример поста: <code>https://t.me/c/3872609933/8</code>\n"
            "Пример истории: <code>https://t.me/username/s/53</code>"
        ),
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
        "double_no_id": "<b>Ошибка:</b> Укажи ID или юзернейм канала",
        "double_error": "<b>Ошибка:</b> <code>{err}</code>",
        "double_send_error": (
            "<b>Копирую...</b>\n"
            "Обработано: {done} / {total}\n"
            "Пропущено: {skipped}\n"
            "Альбомы: {albums}\n\n"
            "<b>⚠️ Ошибка отправки сообщения {msg_id}:</b>\n<code>{err}</code>"
        ),
        "steal_no_link": "<b>Ошибка:</b> Укажи ссылку на пост\nПример: <code>https://t.me/c/3872609933/8</code>",
        "steal_bad_link": "<b>Ошибка:</b> Не могу распарсить ссылку",
        "steal_not_found": "<b>Ошибка:</b> Пост не найден",
        "steal_error": "<b>Ошибка:</b> <code>{err}</code>",
        "sdl_no_link": "<b>Ошибка:</b> Укажи ссылку на историю\nПример: <code>https://t.me/username/s/53</code>",
        "sdl_bad_link": "<b>Ошибка:</b> Не могу распарсить ссылку",
        "sdl_not_found": "<b>Ошибка:</b> История не найдена или без медиа",
        "sdl_download_fail": "<b>Ошибка:</b> Не удалось скачать",
        "sdl_done": "<b>Готово!</b>",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "CREATE_CHANNEL",
                True,
                lambda: "If True creates channel, if false - group",
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

    @loader.command(
        ru_doc="Полностью скопировать канал",
        en_doc="Fully copy a channel"
    )
    async def double(self, message):
        """Fully copy a channel"""
        args = utils.get_args_raw(message).strip()
        if not args:
            await utils.answer(message, self.strings["double_no_id"])
            return

        status = await utils.answer(message, self.strings["double_start"])

        try:
            try:
                src = await self._client.get_entity(int(args))
            except ValueError:
                src = await self._client.get_entity(args)

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
                            status = await utils.answer(
                                status,
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
                            status = await utils.answer(
                                status,
                                self.strings["double_send_error"].format(
                                    done=done, total=total,
                                    skipped=skipped, albums=albums,
                                    msg_id=msg.id, err=str(e),
                                )
                            )
                            await asyncio.sleep(2)

                        j += 1

                    if done > 0 and done % PAUSE_EVERY == 0:
                        status = await utils.answer(
                            status,
                            self.strings["double_progress"].format(
                                done=done, total=total,
                                skipped=skipped, albums=albums,
                            )
                        )
                        await asyncio.sleep(PAUSE_DURATION)

                i += BATCH_SIZE

                status = await utils.answer(
                    status,
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

            await utils.answer(
                status,
                self.strings["double_done"].format(
                    done=done, skipped=skipped, albums=albums, link=link
                )
            )

        except Exception as e:
            logger.error(f"[Robber] double error: {e}", exc_info=True)
            await utils.answer(
                status,
                self.strings["double_error"].format(err=str(e))
            )

    @loader.command(
        ru_doc="Украсть пост в текущий чат",
        en_doc="Steal a post to current chat"
    )
    async def steal(self, message):
        """Steal a post to current chat"""
        args = utils.get_args_raw(message).strip()
        if not args:
            await utils.answer(message, self.strings["steal_no_link"])
            return

        if not POST_LINK_RE.search(args):
            await utils.answer(message, self.strings["steal_bad_link"])
            return

        target_chat = message.chat_id
        await message.delete()

        try:
            entity, msg_id = await self._resolve_post_link(args)

            anchor = await self._client.get_messages(entity, ids=[msg_id])
            anchor = anchor[0] if anchor else None

            if not anchor:
                await self._client.send_message(
                    target_chat, self.strings["steal_not_found"]
                )
                return

            if anchor.grouped_id:
                album = await self._fetch_album(entity, anchor)
                await self._send_album(target_chat, album, {})
            else:
                await self._send_msg(target_chat, anchor, {})

        except Exception as e:
            logger.error(f"[Robber] steal error: {e}", exc_info=True)
            await self._client.send_message(
                target_chat,
                self.strings["steal_error"].format(err=str(e))
            )

    @loader.command(
        ru_doc="Скачать историю как файл",
        en_doc="Download a story as file"
    )
    async def sdl(self, message):
        """Download a story as file"""
        args = utils.get_args_raw(message).strip()
        if not args:
            await utils.answer(message, self.strings["sdl_no_link"])
            return

        m = STORY_LINK_RE.search(args)
        if not m:
            await utils.answer(message, self.strings["sdl_bad_link"])
            return

        username = m.group(1)
        story_id = int(m.group(2))

        status = await utils.answer(message, "⏳")

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
                await utils.answer(status, self.strings["sdl_not_found"])
                return

            media = stories[0].media
            is_video = isinstance(media, types.MessageMediaDocument)
            ext = ".mp4" if is_video else ".jpg"
            fname = f"create_ur_dreams{ext}"
            path = self._tmp_path(fname)

            if os.path.exists(path):
                os.remove(path)

            downloaded = await self._client.download_media(media, file=path)

            if not downloaded or not os.path.exists(downloaded):
                await utils.answer(status, self.strings["sdl_download_fail"])
                return

            try:
                await self._client.send_file(
                    message.chat_id,
                    downloaded,
                    force_document=True,
                    reply_to=message.reply_to_msg_id or None,
                    attributes=[
                        types.DocumentAttributeFilename(file_name=fname)
                    ],
                )
                await utils.answer(status, self.strings["sdl_done"])
            finally:
                try:
                    os.remove(downloaded)
                except Exception:
                    pass

        except Exception as e:
            logger.error(f"[Robber] sdl error: {e}", exc_info=True)
            await utils.answer(
                status,
                self.strings["steal_error"].format(err=str(e))
            )