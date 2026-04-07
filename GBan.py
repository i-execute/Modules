__version__ = (2, 0, 0)
# meta developer: FireJester.t.me

import asyncio
import logging
import random
from telethon.tl.types import (
    Message,
    User,
    Channel,
    Chat,
    ChannelForbidden,
    ChatForbidden,
)
from .. import loader, utils

logger = logging.getLogger(__name__)

RESTRICT_BANNED = {
    "view_messages": False,
    "send_messages": False,
    "send_media": False,
    "send_stickers": False,
    "send_gifs": False,
    "send_games": False,
    "send_inline": False,
    "send_polls": False,
    "change_info": False,
    "invite_users": False,
}

RESTRICT_MUTED = {
    "view_messages": True,
    "send_messages": False,
    "send_media": False,
    "send_stickers": False,
    "send_gifs": False,
    "send_games": False,
    "send_inline": False,
    "send_polls": False,
    "change_info": False,
    "invite_users": False,
}


def _build_display(user: User) -> str:
    first = user.first_name or ""
    last = user.last_name or ""
    name = f"{first} {last}".strip()
    display = utils.escape_html(name)
    username = _extract_username(user)
    if username and username.strip():
        return f'<a href="https://t.me/{utils.escape_html(username)}">{display}</a>'
    return f'<a href="tg://user?id={user.id}">{display}</a>'


def _extract_username(user: User) -> str:
    username = getattr(user, "username", None)
    if username:
        return username
    usernames_list = getattr(user, "usernames", None)
    if usernames_list:
        for u in usernames_list:
            if getattr(u, "active", False):
                return u.username
    return None


async def _resolve_target(client, message: Message):
    reply = await message.get_reply_message()
    args = utils.get_args_raw(message)

    if reply and not args:
        sender = reply.sender_id
        if not sender:
            return None, "no_user"
        try:
            entity = await client.get_entity(sender)
            return entity, None
        except Exception:
            return None, "no_access"

    if args:
        raw = args.strip().split()[0]
        try:
            if raw.lstrip("-").isdigit():
                uid = int(raw)
                try:
                    entity = await client.get_entity(uid)
                    return entity, None
                except Exception:
                    return None, "no_access"
            else:
                entity = await client.get_entity(raw)
                return entity, None
        except Exception:
            return None, "no_user"

    if reply:
        sender = reply.sender_id
        if not sender:
            return None, "no_user"
        try:
            entity = await client.get_entity(sender)
            return entity, None
        except Exception:
            return None, "no_access"

    return None, "no_args"


async def _bulk_delete(client, chat_id, msg_ids: list) -> int:
    deleted = 0
    if not msg_ids:
        return deleted

    chunk_size = random.randint(90, 110)
    first_chunk = msg_ids[:chunk_size]
    rest = msg_ids[chunk_size:]

    try:
        await client.delete_messages(chat_id, first_chunk)
        deleted += len(first_chunk)
    except Exception:
        pass

    chunk = []
    chunk_size = random.randint(90, 110)

    for mid in rest:
        chunk.append(mid)
        if len(chunk) >= chunk_size:
            await asyncio.sleep(random.uniform(0.5, 1.5))
            try:
                await client.delete_messages(chat_id, chunk)
                deleted += len(chunk)
            except Exception:
                pass
            chunk.clear()
            chunk_size = random.randint(90, 110)

    if chunk:
        await asyncio.sleep(random.uniform(0.5, 1.5))
        try:
            await client.delete_messages(chat_id, chunk)
            deleted += len(chunk)
        except Exception:
            pass

    return deleted


@loader.tds
class GBan(loader.Module):
    """Global ban, mute and delete across all chats where you are admin"""

    strings = {
        "name": "GBan",
        "no_args": "<b>Error:</b> Provide a user (reply, @username or ID)",
        "no_user": "<b>Error:</b> User not found",
        "no_access": "<b>Error:</b> No access hash for this user ID. Try using @username instead",
        "self_action": "<b>Error:</b> You seriously?",
        "processing": "<b>Processing...</b>",
        "gbanned": "<b>{user} globally banned in {count} chat(s)</b>",
        "gunbanned": "<b>{user} globally unbanned in {count} chat(s)</b>",
        "gmuted": "<b>{user} globally muted in {count} chat(s)</b>",
        "gunmuted": "<b>{user} globally unmuted in {count} chat(s)</b>",
        "gdeleted": "<b>{user} globally banned and messages deleted in {count} chat(s)</b>",
        "gtest": (
            "<b>Stats</b>\n"
            "<blockquote>"
            "Ban rights: {ban_groups} group(s), {ban_channels} channel(s)\n"
            "Mute rights: {mute_groups} group(s)"
            "</blockquote>"
        ),
    }

    strings_ru = {
        "no_args": "<b>Ошибка:</b> Укажи пользователя (реплай, @username или ID)",
        "no_user": "<b>Ошибка:</b> Пользователь не найден",
        "no_access": "<b>Ошибка:</b> Нет access hash. Попробуй @username",
        "self_action": "<b>Ошибка:</b> Ты серьёзно?",
        "processing": "<b>Обработка...</b>",
        "gbanned": "<b>{user} глобально забанен в {count} чат(ах)</b>",
        "gunbanned": "<b>{user} глобально разбанен в {count} чат(ах)</b>",
        "gmuted": "<b>{user} глобально замучен в {count} чат(ах)</b>",
        "gunmuted": "<b>{user} глобально размучен в {count} чат(ах)</b>",
        "gdeleted": "<b>{user} забанен и сообщения удалены в {count} чат(ах)</b>",
        "gtest": (
            "<b>Статистика</b>\n"
            "<blockquote>"
            "Бан: {ban_groups} групп, {ban_channels} каналов\n"
            "Мут: {mute_groups} групп"
            "</blockquote>"
        ),
    }

    def __init__(self):
        self._ban_cache = {}
        self._mute_cache = {}
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "skip_ids",
                [],
                "Chat IDs to skip in global actions",
                validator=loader.validators.Series(loader.validators.Integer()),
            )
        )

    async def _collect_ban_chats(self):
        result = []
        skip = list(self.config["skip_ids"] or [])
        async for dialog in self._client.iter_dialogs():
            entity = dialog.entity
            if isinstance(entity, (ChannelForbidden, ChatForbidden)):
                continue
            if isinstance(entity, Chat):
                ar = entity.admin_rights
                if getattr(entity, "creator", False) or (ar and getattr(ar, "ban_users", False)):
                    result.append(("group", entity.id))
            elif isinstance(entity, Channel):
                if entity.id in skip:
                    continue
                if not entity.admin_rights and not getattr(entity, "creator", False):
                    continue
                ar = entity.admin_rights
                if getattr(entity, "creator", False) or (ar and getattr(ar, "ban_users", False)):
                    if getattr(entity, "megagroup", False):
                        result.append(("group", entity.id))
                    elif getattr(entity, "broadcast", False):
                        result.append(("channel", entity.id))
        return result

    async def _collect_mute_chats(self):
        result = []
        skip = list(self.config["skip_ids"] or [])
        async for dialog in self._client.iter_dialogs():
            entity = dialog.entity
            if isinstance(entity, (ChannelForbidden, ChatForbidden)):
                continue
            if isinstance(entity, Chat):
                ar = entity.admin_rights
                if getattr(entity, "creator", False) or (ar and getattr(ar, "ban_users", False)):
                    result.append(entity.id)
            elif isinstance(entity, Channel):
                if entity.id in skip:
                    continue
                if not getattr(entity, "megagroup", False):
                    continue
                if not entity.admin_rights and not getattr(entity, "creator", False):
                    continue
                ar = entity.admin_rights
                if getattr(entity, "creator", False) or (ar and getattr(ar, "ban_users", False)):
                    result.append(entity.id)
        return result

    async def _get_ban_chats(self):
        import time as _time
        if not self._ban_cache or self._ban_cache.get("exp", 0) < _time.time():
            chats = await self._collect_ban_chats()
            self._ban_cache = {"exp": _time.time() + 600, "chats": chats}
        return self._ban_cache["chats"]

    async def _get_mute_chats(self):
        import time as _time
        if not self._mute_cache or self._mute_cache.get("exp", 0) < _time.time():
            chats = await self._collect_mute_chats()
            self._mute_cache = {"exp": _time.time() + 600, "chats": chats}
        return self._mute_cache["chats"]

    @loader.command(
        ru_doc="реплай / @username / ID — глобальный бан",
        en_doc="reply / @username / ID — global ban",
    )
    async def gban(self, message: Message):
        """reply / @username / ID — global ban"""
        me = await self._client.get_me()
        target, err = await _resolve_target(self._client, message)

        if err:
            return await utils.answer(message, self.strings[err if err in self.strings else "no_args"])

        if not isinstance(target, User):
            return await utils.answer(message, self.strings["no_user"])

        if target.id == me.id:
            return await utils.answer(message, self.strings["self_action"])

        await utils.answer(message, self.strings["processing"])

        chats = await self._get_ban_chats()
        display = _build_display(target)
        count = 0

        for chat_type, chat_id in chats:
            try:
                await self._client.edit_permissions(
                    chat_id,
                    target,
                    until_date=0,
                    **RESTRICT_BANNED,
                )
                count += 1
            except Exception:
                pass

        await utils.answer(
            message,
            self.strings["gbanned"].format(user=display, count=count),
        )

    @loader.command(
        ru_doc="реплай / @username / ID — глобальный разбан",
        en_doc="reply / @username / ID — global unban",
    )
    async def gunban(self, message: Message):
        """reply / @username / ID — global unban"""
        me = await self._client.get_me()
        target, err = await _resolve_target(self._client, message)

        if err:
            return await utils.answer(message, self.strings[err if err in self.strings else "no_args"])

        if not isinstance(target, User):
            return await utils.answer(message, self.strings["no_user"])

        if target.id == me.id:
            return await utils.answer(message, self.strings["self_action"])

        await utils.answer(message, self.strings["processing"])

        chats = await self._get_ban_chats()
        display = _build_display(target)
        count = 0

        for chat_type, chat_id in chats:
            try:
                await self._client.edit_permissions(
                    chat_id,
                    target,
                    until_date=0,
                    **{k: True for k in RESTRICT_BANNED},
                )
                count += 1
            except Exception:
                pass

        await utils.answer(
            message,
            self.strings["gunbanned"].format(user=display, count=count),
        )

    @loader.command(
        ru_doc="реплай / @username / ID — глобальный мут",
        en_doc="reply / @username / ID — global mute",
    )
    async def gmute(self, message: Message):
        """reply / @username / ID — global mute"""
        me = await self._client.get_me()
        target, err = await _resolve_target(self._client, message)

        if err:
            return await utils.answer(message, self.strings[err if err in self.strings else "no_args"])

        if not isinstance(target, User):
            return await utils.answer(message, self.strings["no_user"])

        if target.id == me.id:
            return await utils.answer(message, self.strings["self_action"])

        chats = await self._get_mute_chats()
        display = _build_display(target)
        count = 0

        for chat_id in chats:
            try:
                await self._client.edit_permissions(
                    chat_id,
                    target,
                    until_date=0,
                    **RESTRICT_MUTED,
                )
                count += 1
            except Exception:
                pass

        self._mute_cache.setdefault("watched", set()).add(target.id)

        await utils.answer(
            message,
            self.strings["gmuted"].format(user=display, count=count),
        )

    @loader.command(
        ru_doc="реплай / @username / ID — глобальный размут",
        en_doc="reply / @username / ID — global unmute",
    )
    async def gunmute(self, message: Message):
        """reply / @username / ID — global unmute"""
        me = await self._client.get_me()
        target, err = await _resolve_target(self._client, message)

        if err:
            return await utils.answer(message, self.strings[err if err in self.strings else "no_args"])

        if not isinstance(target, User):
            return await utils.answer(message, self.strings["no_user"])

        if target.id == me.id:
            return await utils.answer(message, self.strings["self_action"])

        chats = await self._get_mute_chats()
        display = _build_display(target)
        count = 0

        for chat_id in chats:
            try:
                await self._client.edit_permissions(
                    chat_id,
                    target,
                    until_date=0,
                    **{k: True for k in RESTRICT_MUTED},
                )
                count += 1
            except Exception:
                pass

        watched = self._mute_cache.get("watched", set())
        watched.discard(target.id)

        await utils.answer(
            message,
            self.strings["gunmuted"].format(user=display, count=count),
        )

    @loader.command(
        ru_doc="реплай / @username / ID — удалить сообщения и забанить",
        en_doc="reply / @username / ID — delete messages and ban",
    )
    async def gdelete(self, message: Message):
        """reply / @username / ID — delete messages and ban"""
        me = await self._client.get_me()
        target, err = await _resolve_target(self._client, message)

        if err:
            return await utils.answer(message, self.strings[err if err in self.strings else "no_args"])

        if not isinstance(target, User):
            return await utils.answer(message, self.strings["no_user"])

        if target.id == me.id:
            return await utils.answer(message, self.strings["self_action"])

        await utils.answer(message, self.strings["processing"])

        chats = await self._get_ban_chats()
        display = _build_display(target)
        count = 0

        for chat_type, chat_id in chats:
            try:
                msg_ids = []
                async for msg in self._client.iter_messages(chat_id, from_user=target.id):
                    msg_ids.append(msg.id)
                if msg_ids:
                    await _bulk_delete(self._client, chat_id, msg_ids)
            except Exception:
                pass

            try:
                await self._client.edit_permissions(
                    chat_id,
                    target,
                    until_date=0,
                    **RESTRICT_BANNED,
                )
                count += 1
            except Exception:
                pass

        await utils.answer(
            message,
            self.strings["gdeleted"].format(user=display, count=count),
        )

    @loader.command(
        ru_doc="статистика чатов где ты админ",
        en_doc="stats of chats where you are admin",
    )
    async def gtest(self, message: Message):
        """stats of chats where you are admin"""
        ban_chats = await self._collect_ban_chats()
        mute_chats = await self._collect_mute_chats()

        ban_groups = sum(1 for t, _ in ban_chats if t == "group")
        ban_channels = sum(1 for t, _ in ban_chats if t == "channel")
        mute_groups = len(mute_chats)

        await utils.answer(
            message,
            self.strings["gtest"].format(
                ban_groups=ban_groups,
                ban_channels=ban_channels,
                mute_groups=mute_groups,
            ),
        )

    async def watcher(self, message: Message):
        if not isinstance(message, Message):
            return
        if message.out:
            return
        if not message.is_group:
            return

        watched = self._mute_cache.get("watched", set())
        if not watched:
            return

        if message.sender_id not in watched:
            return

        mute_chats = self._mute_cache.get("chats", [])
        if message.chat_id not in mute_chats:
            return

        try:
            await message.delete()
        except Exception:
            pass