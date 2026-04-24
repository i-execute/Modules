__version__ = (2, 1, 3)
# meta developer: I_execute.t.me
# requires: aiohttp, Pillow

from telethon.tl.types import User, Channel, Message, InputPhotoFileLocation
from telethon.tl.functions.photos import GetUserPhotosRequest
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest
from telethon import functions
from .. import loader, utils
import os
import io
import asyncio
import tempfile

import aiohttp
from PIL import Image

async def _upload_to_x0(data: bytes, filename: str, content_type: str) -> str:
    try:
        form = aiohttp.FormData()
        form.add_field("file", data, filename=filename, content_type=content_type)
        async with aiohttp.ClientSession() as s:
            async with s.post(
                "https://x0.at",
                data=form,
                timeout=aiohttp.ClientTimeout(total=60),
            ) as r:
                text = (await r.text()).strip()
                if text.startswith("http"):
                    return text
    except Exception:
        pass
    return ""


def _normalize_to_jpeg(raw: bytes, max_size: int = 1200) -> bytes:
    try:
        img = Image.open(io.BytesIO(raw)).convert("RGB")
        w, h = img.size
        if w > max_size or h > max_size:
            ratio = min(max_size / w, max_size / h)
            img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=95)
        return buf.getvalue()
    except Exception:
        return raw


@loader.tds
class Info(loader.Module):
    """Get information about users and chats"""

    strings = {
        "name": "Info",
        "prem_user_full": (
            "<blockquote><b>┌ [</b><tg-emoji emoji-id=5188516803638236397>🔝</tg-emoji><b>] Name:</b> {name}\n"
            "├ <b>[</b><tg-emoji emoji-id=5188171393778359433>🤟</tg-emoji><b>] Username:</b> {username}\n"
            "├ <b>[</b><tg-emoji emoji-id=5188654053613150361>📀</tg-emoji><b>] User ID:</b> <code>{user_id}</code>\n"
            "<b>└ [</b><tg-emoji emoji-id=5188420042320020352>🪙</tg-emoji><b>] DC:</b> {dc}</blockquote>"
        ),
        "prem_user_no_dc": (
            "<blockquote><b>┌ [</b><tg-emoji emoji-id=5188516803638236397>🔝</tg-emoji><b>] Name:</b> {name}\n"
            "├ <b>[</b><tg-emoji emoji-id=5188171393778359433>🤟</tg-emoji><b>] Username:</b> {username}\n"
            "<b>└ [</b><tg-emoji emoji-id=5188654053613150361>📀</tg-emoji><b>] User ID:</b> <code>{user_id}</code></blockquote>"
        ),
        "prem_chat_full": (
            "<blockquote><b>┌ [</b><tg-emoji emoji-id=5188516803638236397>🔝</tg-emoji><b>] Name:</b> {name}\n"
            "├ <b>[</b><tg-emoji emoji-id=5188171393778359433>🤟</tg-emoji><b>] Username:</b> {username}\n"
            "├ <b>[</b><tg-emoji emoji-id=5188654053613150361>📀</tg-emoji><b>] Chat ID:</b> <code>{chat_id}</code>\n"
            "├ <b>[</b><tg-emoji emoji-id=5190758450149233016>🌡</tg-emoji><b>] Type:</b> {type}\n"
            "<b>└ [</b><tg-emoji emoji-id=5188420042320020352>🪙</tg-emoji><b>] DC:</b> {dc}</blockquote>"
        ),
        "prem_chat_no_dc": (
            "<blockquote><b>┌ [</b><tg-emoji emoji-id=5188516803638236397>🔝</tg-emoji><b>] Name:</b> {name}\n"
            "├ <b>[</b><tg-emoji emoji-id=5188171393778359433>🤟</tg-emoji><b>] Username:</b> {username}\n"
            "├ <b>[</b><tg-emoji emoji-id=5188654053613150361>📀</tg-emoji><b>] Chat ID:</b> <code>{chat_id}</code>\n"
            "<b>└ [</b><tg-emoji emoji-id=5190758450149233016>🌡</tg-emoji><b>] Type:</b> {type}</blockquote>"
        ),
        "noprem_user_full": (
            "<blockquote><b>┌[ Name:</b> {name} <b>]</b>\n"
            "├<b>[ Username:</b> {username} <b>]</b>\n"
            "├<b>[ User ID:</b> <code>{user_id}</code> <b>]</b>\n"
            "<b>└[ DC:</b> {dc} <b>]</b></blockquote>"
        ),
        "noprem_user_no_dc": (
            "<blockquote><b>┌[ Name:</b> {name} <b>]</b>\n"
            "├<b>[ Username:</b> {username} <b>]</b>\n"
            "<b>└[ User ID:</b> <code>{user_id}</code> <b>]</b></blockquote>"
        ),
        "noprem_chat_full": (
            "<blockquote><b>┌[ Name:</b> {name} <b>]</b>\n"
            "├<b>[ Username:</b> {username} <b>]</b>\n"
            "├<b>[ Chat ID:</b> <code>{chat_id}</code> <b>]</b>\n"
            "├<b>[ Type:</b> {type} <b>]</b>\n"
            "<b>└[ DC:</b> {dc} <b>]</b></blockquote>"
        ),
        "noprem_chat_no_dc": (
            "<blockquote><b>┌[ Name:</b> {name} <b>]</b>\n"
            "├<b>[ Username:</b> {username} <b>]</b>\n"
            "├<b>[ Chat ID:</b> <code>{chat_id}</code> <b>]</b>\n"
            "<b>└[ Type:</b> {type} <b>]</b></blockquote>"
        ),
        "error_reply": "<tg-emoji emoji-id=5188512006159766094>😵</tg-emoji><b> Error: </b>No reply or invalid username",
        "no_photo_msg": "<tg-emoji emoji-id=5188512006159766094>😵</tg-emoji><b> Error: </b>User hid avatar or blocked you",
        "no_chat_photo": "<tg-emoji emoji-id=5188512006159766094>😵</tg-emoji><b> Error: </b>Chat has no avatar",
        "not_a_chat": "<tg-emoji emoji-id=5188512006159766094>😵</tg-emoji><b> Error: </b>This command only works in groups and channels",
        "error_reply_noprem": "<b>Error:</b> No reply or invalid username",
        "no_photo_msg_noprem": "<b>Error:</b> User hid avatar or blocked you",
        "no_chat_photo_noprem": "<b>Error:</b> Chat has no avatar",
        "not_a_chat_noprem": "<b>Error:</b> This command only works in groups and channels",
        "type_channel": "Channel",
        "type_supergroup": "Supergroup",
        "type_group": "Group",
    }

    strings_ru = {
        "prem_user_full": (
            "<blockquote><b>┌ [</b><tg-emoji emoji-id=5188516803638236397>🔝</tg-emoji><b>] Name:</b> {name}\n"
            "├ <b>[</b><tg-emoji emoji-id=5188171393778359433>🤟</tg-emoji><b>] Username:</b> {username}\n"
            "├ <b>[</b><tg-emoji emoji-id=5188654053613150361>📀</tg-emoji><b>] User ID:</b> <code>{user_id}</code>\n"
            "<b>└ [</b><tg-emoji emoji-id=5188420042320020352>🪙</tg-emoji><b>] DC:</b> {dc}</blockquote>"
        ),
        "prem_user_no_dc": (
            "<blockquote><b>┌ [</b><tg-emoji emoji-id=5188516803638236397>🔝</tg-emoji><b>] Name:</b> {name}\n"
            "├ <b>[</b><tg-emoji emoji-id=5188171393778359433>🤟</tg-emoji><b>] Username:</b> {username}\n"
            "<b>└ [</b><tg-emoji emoji-id=5188654053613150361>📀</tg-emoji><b>] User ID:</b> <code>{user_id}</code></blockquote>"
        ),
        "prem_chat_full": (
            "<blockquote><b>┌ [</b><tg-emoji emoji-id=5188516803638236397>🔝</tg-emoji><b>] Name:</b> {name}\n"
            "├ <b>[</b><tg-emoji emoji-id=5188171393778359433>🤟</tg-emoji><b>] Username:</b> {username}\n"
            "├ <b>[</b><tg-emoji emoji-id=5188654053613150361>📀</tg-emoji><b>] Chat ID:</b> <code>{chat_id}</code>\n"
            "├ <b>[</b><tg-emoji emoji-id=5190758450149233016>🌡</tg-emoji><b>] Type:</b> {type}\n"
            "<b>└ [</b><tg-emoji emoji-id=5188420042320020352>🪙</tg-emoji><b>] DC:</b> {dc}</blockquote>"
        ),
        "prem_chat_no_dc": (
            "<blockquote><b>┌ [</b><tg-emoji emoji-id=5188516803638236397>🔝</tg-emoji><b>] Name:</b> {name}\n"
            "├ <b>[</b><tg-emoji emoji-id=5188171393778359433>🤟</tg-emoji><b>] Username:</b> {username}\n"
            "├ <b>[</b><tg-emoji emoji-id=5188654053613150361>📀</tg-emoji><b>] Chat ID:</b> <code>{chat_id}</code>\n"
            "<b>└ [</b><tg-emoji emoji-id=5190758450149233016>🌡</tg-emoji><b>] Type:</b> {type}</blockquote>"
        ),
        "noprem_user_full": (
            "<blockquote><b>┌[ Name:</b> {name} <b>]</b>\n"
            "├<b>[ Username:</b> {username} <b>]</b>\n"
            "├<b>[ User ID:</b> <code>{user_id}</code> <b>]</b>\n"
            "<b>└[ DC:</b> {dc} <b>]</b></blockquote>"
        ),
        "noprem_user_no_dc": (
            "<blockquote><b>┌[ Name:</b> {name} <b>]</b>\n"
            "├<b>[ Username:</b> {username} <b>]</b>\n"
            "<b>└[ User ID:</b> <code>{user_id}</code> <b>]</b></blockquote>"
        ),
        "noprem_chat_full": (
            "<blockquote><b>┌[ Name:</b> {name} <b>]</b>\n"
            "├<b>[ Username:</b> {username} <b>]</b>\n"
            "├<b>[ Chat ID:</b> <code>{chat_id}</code> <b>]</b>\n"
            "├<b>[ Type:</b> {type} <b>]</b>\n"
            "<b>└[ DC:</b> {dc} <b>]</b></blockquote>"
        ),
        "noprem_chat_no_dc": (
            "<blockquote><b>┌[ Name:</b> {name} <b>]</b>\n"
            "├<b>[ Username:</b> {username} <b>]</b>\n"
            "├<b>[ Chat ID:</b> <code>{chat_id}</code> <b>]</b>\n"
            "<b>└[ Type:</b> {type} <b>]</b></blockquote>"
        ),
        "error_reply": "<tg-emoji emoji-id=5188512006159766094>😵</tg-emoji><b> Error: </b>Нет реплая или некорректный юзернейм",
        "no_photo_msg": "<tg-emoji emoji-id=5188512006159766094>😵</tg-emoji><b> Error: </b>Пользователь скрыл аватарку или заблокировал тебя",
        "no_chat_photo": "<tg-emoji emoji-id=5188512006159766094>😵</tg-emoji><b> Error: </b>У чата нет аватарки",
        "not_a_chat": "<tg-emoji emoji-id=5188512006159766094>😵</tg-emoji><b> Error: </b>Эта команда работает только в группах и каналах",
        "error_reply_noprem": "<b>Error:</b> Нет реплая или некорректный юзернейм",
        "no_photo_msg_noprem": "<b>Error:</b> Пользователь скрыл аватарку или заблокировал тебя",
        "no_chat_photo_noprem": "<b>Error:</b> У чата нет аватарки",
        "not_a_chat_noprem": "<b>Error:</b> Эта команда работает только в группах и каналах",
        "type_channel": "Канал",
        "type_supergroup": "Супергруппа",
        "type_group": "Группа",
    }

    def __init__(self):
        self._premium_status = None

    async def _check_premium(self, client):
        if self._premium_status is None:
            me = await client.get_me()
            self._premium_status = getattr(me, "premium", False)
        return self._premium_status

    def _get_error_string(self, key, is_premium):
        if is_premium:
            return self.strings(key)
        return self.strings(f"{key}_noprem")

    def _get_username(self, entity):
        if hasattr(entity, "username") and entity.username:
            return entity.username
        if hasattr(entity, "usernames") and entity.usernames:
            for u in entity.usernames:
                if getattr(u, "active", False):
                    return u.username
            return entity.usernames[0].username
        return None

    def _get_dc(self, entity):
        photo = getattr(entity, "photo", None)
        if photo:
            return getattr(photo, "dc_id", None)
        return None

    def _has_video_avatar(self, entity):
        photo = getattr(entity, "photo", None)
        if photo:
            return getattr(photo, "has_video", False)
        return False

    def _get_topic_id(self, message: Message):
        reply_to = getattr(message, "reply_to", None)
        if reply_to:
            return getattr(reply_to, "reply_to_top_id", None) or getattr(
                reply_to, "reply_to_msg_id", None
            )
        return None

    def _pick_best_video_size(self, video_sizes):
        best = None
        for vs in video_sizes:
            if hasattr(vs, "type") and vs.type == "u":
                return vs
            if best is None:
                best = vs
        return best if best else video_sizes[-1]

    async def _download_video_from_photo(self, client, photo_obj):
        video_sizes = getattr(photo_obj, "video_sizes", None)
        if not video_sizes:
            return None
        best = self._pick_best_video_size(video_sizes)
        location = InputPhotoFileLocation(
            id=photo_obj.id,
            access_hash=photo_obj.access_hash,
            file_reference=photo_obj.file_reference,
            thumb_size=best.type,
        )
        path = tempfile.mktemp(suffix=".mp4")
        try:
            await client.download_file(location, path)
            if os.path.exists(path) and os.path.getsize(path) > 0:
                return path
            if os.path.exists(path):
                os.remove(path)
            return None
        except Exception:
            if os.path.exists(path):
                try:
                    os.remove(path)
                except Exception:
                    pass
            return None

    async def _get_user_profile_video(self, client, user):
        try:
            result = await client(
                GetUserPhotosRequest(user_id=user, offset=0, max_id=0, limit=1)
            )
            if not result.photos:
                return None
            return await self._download_video_from_photo(client, result.photos[0])
        except Exception:
            return None

    async def _get_chat_profile_video(self, client, chat):
        try:
            if isinstance(chat, Channel):
                full = await client(GetFullChannelRequest(channel=chat))
            else:
                full = await client(GetFullChatRequest(chat_id=chat.id))
            chat_photo = getattr(full.full_chat, "chat_photo", None)
            if not chat_photo:
                return None
            return await self._download_video_from_photo(client, chat_photo)
        except Exception:
            return None

    async def _get_avatar(self, client, entity):
        try:
            is_video = self._has_video_avatar(entity)
            if is_video:
                if isinstance(entity, User):
                    path = await self._get_user_profile_video(client, entity)
                else:
                    path = await self._get_chat_profile_video(client, entity)
                if path:
                    return path, True
                return None, False
            else:
                result = await client.download_profile_photo(entity, download_big=True)
                if result:
                    return result, False
                return None, False
        except Exception:
            return None, False

    async def _get_target_user(self, message, username=None):
        if username:
            try:
                return await message.client.get_entity(username)
            except Exception:
                return None
        elif message.is_reply:
            reply = await message.get_reply_message()
            if reply and reply.sender_id:
                try:
                    return await message.client.get_entity(reply.sender_id)
                except Exception:
                    return None
        return None

    def _build_user_info_text(self, user, is_premium):
        first = user.first_name or ""
        last = user.last_name or ""
        name = f"{first} {last}".strip()
        display_name = utils.escape_html(name)
        username = self._get_username(user)
        if username and username.strip():
            username_text = f"<u>@{utils.escape_html(username)}</u>"
        else:
            username_text = f'<a href="tg://user?id={user.id}">{display_name}</a>'
        dc = self._get_dc(user)
        prefix = "prem" if is_premium else "noprem"
        suffix = "_full" if dc else "_no_dc"
        key = f"{prefix}_user{suffix}"
        return self.strings(key).format(
            name=display_name,
            username=username_text,
            user_id=user.id,
            dc=dc,
        )

    def _build_chat_info_text(self, chat, is_premium):
        name = utils.escape_html(chat.title or "")
        username = self._get_username(chat)
        if username and username.strip():
            username_text = f"<u>@{utils.escape_html(username)}</u>"
        else:
            username_text = "—"
        if isinstance(chat, Channel):
            if chat.megagroup:
                chat_type = self.strings("type_supergroup")
            else:
                chat_type = self.strings("type_channel")
        else:
            chat_type = self.strings("type_group")
        dc = self._get_dc(chat)
        prefix = "prem" if is_premium else "noprem"
        suffix = "_full" if dc else "_no_dc"
        key = f"{prefix}_chat{suffix}"
        return self.strings(key).format(
            name=name,
            username=username_text,
            chat_id=chat.id,
            type=chat_type,
            dc=dc,
        )

    async def _cleanup_file(self, path):
        if isinstance(path, str) and os.path.exists(path):
            try:
                await asyncio.to_thread(os.remove, path)
            except Exception:
                pass

    async def _send_error(self, message: Message, text):
        topic_id = self._get_topic_id(message)
        await asyncio.gather(
            message.delete(),
            message.client.send_message(
                message.chat_id,
                text,
                reply_to=topic_id,
                parse_mode="HTML",
            ),
        )

    async def _send_result(self, message: Message, text, reply_to_msg_id=None):
        topic_id = self._get_topic_id(message)
        reply_to = reply_to_msg_id or topic_id
        await asyncio.gather(
            message.delete(),
            message.client.send_message(
                message.chat_id,
                text,
                reply_to=reply_to,
                parse_mode="HTML",
            ),
        )

    async def _send_photo_preview(self, message: Message, text, avatar_path, reply_to_msg_id=None):
        topic_id = self._get_topic_id(message)
        reply_to = reply_to_msg_id or topic_id
        img_url = ""
        try:
            with open(avatar_path, "rb") as f:
                raw = f.read()
            jpeg_data = _normalize_to_jpeg(raw)
            img_url = await _upload_to_x0(jpeg_data, "avatar.jpg", "image/jpeg")
        except Exception:
            pass
        # Удаляем сообщение моментально, параллельно с остальным
        delete_task = asyncio.ensure_future(message.delete())
        if img_url:
            try:
                await message.client(
                    functions.messages.GetWebPageRequest(url=img_url, hash=0)
                )
            except Exception:
                pass
            await asyncio.sleep(1)
            await delete_task
            sent = await message.client.send_message(
                message.chat_id,
                text,
                reply_to=reply_to,
                parse_mode="HTML",
            )
            try:
                from telethon.tl.types import InputMediaWebPage
                await sent.edit(
                    text,
                    file=InputMediaWebPage(img_url, optional=True),
                    parse_mode="HTML",
                    link_preview=True,
                    invert_media=True,
                )
                return
            except Exception:
                pass
        else:
            await delete_task
        await message.client.send_message(
            message.chat_id,
            text,
            file=avatar_path,
            reply_to=reply_to,
            parse_mode="HTML",
        )

    async def _send_video_preview(self, message: Message, text, avatar_path, reply_to_msg_id=None):
        topic_id = self._get_topic_id(message)
        reply_to = reply_to_msg_id or topic_id
        video_url = ""
        try:
            with open(avatar_path, "rb") as f:
                raw = f.read()
            video_url = await _upload_to_x0(raw, "avatar.mp4", "video/mp4")
        except Exception:
            pass
        # Удаляем сообщение моментально, параллельно с остальным
        delete_task = asyncio.ensure_future(message.delete())
        if video_url:
            try:
                await message.client(
                    functions.messages.GetWebPageRequest(url=video_url, hash=0)
                )
            except Exception:
                pass
            await asyncio.sleep(1)
            await delete_task
            sent = await message.client.send_message(
                message.chat_id,
                text,
                reply_to=reply_to,
                parse_mode="HTML",
            )
            try:
                from telethon.tl.types import InputMediaWebPage
                await sent.edit(
                    text,
                    file=InputMediaWebPage(video_url, optional=True),
                    parse_mode="HTML",
                    link_preview=True,
                    invert_media=True,
                )
                return
            except Exception:
                pass
        else:
            await delete_task
        await message.client.send_file(
            message.chat_id,
            avatar_path,
            caption=text,
            reply_to=reply_to,
            parse_mode="HTML",
            supports_streaming=True,
            attributes=[],
        )

    @loader.command(
        ru_doc="Инфо о пользователе (+ для аватарки)",
        en_doc="User info (+ for avatar)",
    )
    async def who(self, message: Message):
        """Get user info, reply or @username, add + for avatar"""
        args = utils.get_args_raw(message) or ""
        with_photo = "+" in args
        clean_args = args.replace("+", "").strip()
        is_premium = await self._check_premium(message.client)
        username = None
        if clean_args and not clean_args.lstrip("-").isdigit():
            username = clean_args
        user = await self._get_target_user(message, username)
        if not user:
            await self._send_error(
                message, self._get_error_string("error_reply", is_premium)
            )
            return
        text = self._build_user_info_text(user, is_premium)
        reply_to_msg_id = None
        if message.is_reply:
            reply = await message.get_reply_message()
            if reply:
                reply_to_msg_id = reply.id
        if with_photo:
            avatar, is_video = await self._get_avatar(message.client, user)
            if not avatar:
                await self._send_error(
                    message, self._get_error_string("no_photo_msg", is_premium)
                )
                return
            try:
                if is_video:
                    await self._send_video_preview(
                        message, text, avatar, reply_to_msg_id=reply_to_msg_id
                    )
                else:
                    await self._send_photo_preview(
                        message, text, avatar, reply_to_msg_id=reply_to_msg_id
                    )
            finally:
                await self._cleanup_file(avatar)
        else:
            await self._send_result(message, text, reply_to_msg_id=reply_to_msg_id)

    @loader.command(
        ru_doc="Инфо о группе/канале (+ для аватарки)",
        en_doc="Group/channel info (+ for avatar)",
    )
    async def where(self, message: Message):
        """Get group or channel info, add + for avatar"""
        args = utils.get_args_raw(message) or ""
        with_photo = "+" in args
        is_premium = await self._check_premium(message.client)
        chat = await message.get_chat()
        if isinstance(chat, User):
            await self._send_error(
                message, self._get_error_string("not_a_chat", is_premium)
            )
            return
        text = self._build_chat_info_text(chat, is_premium)
        if with_photo:
            avatar, is_video = await self._get_avatar(message.client, chat)
            if not avatar:
                await self._send_error(
                    message, self._get_error_string("no_chat_photo", is_premium)
                )
                return
            try:
                if is_video:
                    await self._send_video_preview(message, text, avatar)
                else:
                    await self._send_photo_preview(message, text, avatar)
            finally:
                await self._cleanup_file(avatar)
        else:
            await self._send_result(message, text)
