__version__ = (3, 1, 1)
# meta developer: I_execute.t.me

import logging
import time
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
    "pin_messages": False,
    "send_photos": False,
    "send_videos": False,
    "send_roundvideos": False,
    "send_audios": False,
    "send_voices": False,
    "send_docs": False,
    "send_plain": False,
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
    "pin_messages": False,
    "send_photos": False,
    "send_videos": False,
    "send_roundvideos": False,
    "send_audios": False,
    "send_voices": False,
    "send_docs": False,
    "send_plain": False,
}

E_OK    = '<tg-emoji emoji-id=5429319011286423791>😎</tg-emoji>'
E_DEAD1 = '<tg-emoji emoji-id=5429465319347362227>😵</tg-emoji>'
E_ANGRY = '<tg-emoji emoji-id=5429638101586711317>😠</tg-emoji>'
E_DEAD2 = '<tg-emoji emoji-id=5429112861446147403>😵</tg-emoji>'
E_DEAD3 = '<tg-emoji emoji-id=5429369507216924126>😵</tg-emoji>'


def _parse_duration(raw: str):
    if not raw:
        return None
    raw = raw.strip().lower()
    if len(raw) < 2:
        return None
    suffix = raw[-1]
    num_str = raw[:-1]
    if not num_str.isdigit():
        return None
    num = int(num_str)
    if suffix == "m":
        return num * 60
    if suffix == "h":
        return num * 3600
    if suffix == "d":
        return num * 86400
    return None


def _format_duration(seconds: int) -> str:
    if not seconds or seconds <= 0:
        return "∞"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h"
    return f"{seconds // 86400}d"


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


def _has_ban_right(entity) -> bool:
    if isinstance(entity, Chat) and getattr(entity, "creator", False):
        return True
    ar = getattr(entity, "admin_rights", None)
    if not ar:
        return False
    return getattr(ar, "ban_users", False)

async def _resolve_target(client, message: Message):
    reply = await message.get_reply_message()
    args_raw = utils.get_args_raw(message)
    args_list = args_raw.strip().split() if args_raw and args_raw.strip() else []

    if reply:
        sender = reply.sender_id
        if not sender:
            return None, "no_user", []
        try:
            entity = await client.get_entity(sender)
            return entity, None, args_list
        except Exception:
            return None, "no_access", []
    if args_list:
        raw = args_list[0]
        extra = args_list[1:]
        try:
            if raw.lstrip("-").isdigit():
                entity = await client.get_entity(int(raw))
            else:
                entity = await client.get_entity(raw)
            return entity, None, extra
        except Exception:
            return None, "no_user", []

    return None, "no_args", []


async def _bulk_delete(client, chat_id, msg_ids: list) -> int:
    deleted = 0
    for i in range(0, len(msg_ids), 100):
        chunk = msg_ids[i:i + 100]
        try:
            await client.delete_messages(chat_id, chunk)
            deleted += len(chunk)
        except Exception:
            pass
    return deleted


@loader.tds
class AdminTool(loader.Module):
    """Ban, mute and delete across all chats where you are admin"""

    strings = {
        "name": "AdminTool",
        "no_args":            f"{E_DEAD1} <b>Error:</b> Provide a user (reply, @username or ID)",
        "no_user":            f"{E_DEAD1} <b>Error:</b> User not found",
        "no_access":          f"{E_DEAD1} <b>Error:</b> No access hash. Try @username instead",
        "self_action":        f"{E_DEAD1} <b>Error:</b> You seriously?",
        "not_group":          f"{E_DEAD1} <b>Error:</b> This command works only in groups",
        "no_args_noprem":     "<b>Error:</b> Provide a user (reply, @username or ID)",
        "no_user_noprem":     "<b>Error:</b> User not found",
        "no_access_noprem":   "<b>Error:</b> No access hash. Try @username instead",
        "self_action_noprem": "<b>Error:</b> You seriously?",
        "not_group_noprem":   "<b>Error:</b> This command works only in groups",
        "processing":         f"{E_DEAD2} <b>Processing...</b>",
        "gbanned":            f"{E_OK} <b>{{user}} globally banned in {{count}} chat(s)</b>",
        "gunbanned":          f"{E_DEAD3} <b>{{user}} globally unbanned in {{count}} chat(s)</b>",
        "gmuted":             f"{E_ANGRY} <b>{{user}} globally muted [{{duration}}] in {{count}} chat(s)</b>",
        "gunmuted":           f"{E_DEAD3} <b>{{user}} globally unmuted in {{count}} chat(s)</b>",
        "gdeleted":           f"{E_ANGRY} <b>{{user}} banned and messages deleted in {{count}} chat(s)</b>",
        "banned":             f"{E_OK} <b>{{user}} banned in this chat</b>",
        "muted":              f"{E_ANGRY} <b>{{user}} muted [{{duration}}] in this chat</b>",
        "gbanned_noprem":     "<b>{user} globally banned in {count} chat(s)</b>",
        "gunbanned_noprem":   "<b>{user} globally unbanned in {count} chat(s)</b>",
        "gmuted_noprem":      "<b>{user} globally muted [{duration}] in {count} chat(s)</b>",
        "gunmuted_noprem":    "<b>{user} globally unmuted in {count} chat(s)</b>",
        "gdeleted_noprem":    "<b>{user} banned and messages deleted in {count} chat(s)</b>",
        "banned_noprem":      "<b>{user} banned in this chat</b>",
        "muted_noprem":       "<b>{user} muted [{duration}] in this chat</b>",
        "gtest": (
            f"{E_OK} <b>Stats</b>\n"
            "<blockquote>"
            "Total chats: <b>{total}</b>\n"
            "Created by me: <b>{created}</b>\n"
            "Can mute: <b>{can_mute}</b>\n"
            "Can ban: <b>{can_ban}</b>"
            "</blockquote>"
        ),
        "gtest_noprem": (
            "<b>Stats</b>\n"
            "<blockquote>"
            "Total chats: <b>{total}</b>\n"
            "Created by me: <b>{created}</b>\n"
            "Can mute: <b>{can_mute}</b>\n"
            "Can ban: <b>{can_ban}</b>"
            "</blockquote>"
        ),
    }

    strings_ru = {
        "no_args":            f"{E_DEAD1} <b>Ошибка:</b> Укажи пользователя (реплай, @username или ID)",
        "no_user":            f"{E_DEAD1} <b>Ошибка:</b> Пользователь не найден",
        "no_access":          f"{E_DEAD1} <b>Ошибка:</b> Нет access hash. Попробуй @username",
        "self_action":        f"{E_DEAD1} <b>Ошибка:</b> Ты серьёзно?",
        "not_group":          f"{E_DEAD1} <b>Ошибка:</b> Команда работает только в группах",
        "no_args_noprem":     "<b>Ошибка:</b> Укажи пользователя (реплай, @username или ID)",
        "no_user_noprem":     "<b>Ошибка:</b> Пользователь не найден",
        "no_access_noprem":   "<b>Ошибка:</b> Нет access hash. Попробуй @username",
        "self_action_noprem": "<b>Ошибка:</b> Ты серьёзно?",
        "not_group_noprem":   "<b>Ошибка:</b> Команда работает только в группах",
        "processing":         f"{E_DEAD2} <b>Обработка...</b>",
        "gbanned":            f"{E_OK} <b>{{user}} глобально забанен в {{count}} чат(ах)</b>",
        "gunbanned":          f"{E_DEAD3} <b>{{user}} глобально разбанен в {{count}} чат(ах)</b>",
        "gmuted":             f"{E_ANGRY} <b>{{user}} глобально замучен [{{duration}}] в {{count}} чат(ах)</b>",
        "gunmuted":           f"{E_DEAD3} <b>{{user}} глобально размучен в {{count}} чат(ах)</b>",
        "gdeleted":           f"{E_ANGRY} <b>{{user}} забанен и сообщения удалены в {{count}} чат(ах)</b>",
        "banned":             f"{E_OK} <b>{{user}} забанен в этом чате</b>",
        "muted":              f"{E_ANGRY} <b>{{user}} замучен [{{duration}}] в этом чате</b>",
        "gbanned_noprem":     "<b>{user} глобально забанен в {count} чат(ах)</b>",
        "gunbanned_noprem":   "<b>{user} глобально разбанен в {count} чат(ах)</b>",
        "gmuted_noprem":      "<b>{user} глобально замучен [{duration}] в {count} чат(ах)</b>",
        "gunmuted_noprem":    "<b>{user} глобально размучен в {count} чат(ах)</b>",
        "gdeleted_noprem":    "<b>{user} забанен и сообщения удалены в {count} чат(ах)</b>",
        "banned_noprem":      "<b>{user} забанен в этом чате</b>",
        "muted_noprem":       "<b>{user} замучен [{duration}] в этом чате</b>",
        "gtest": (
            f"{E_OK} <b>Статистика</b>\n"
            "<blockquote>"
            "Всего чатов: <b>{total}</b>\n"
            "Создано мной: <b>{created}</b>\n"
            "Можно мутить: <b>{can_mute}</b>\n"
            "Можно банить: <b>{can_ban}</b>"
            "</blockquote>"
        ),
        "gtest_noprem": (
            "<b>Статистика</b>\n"
            "<blockquote>"
            "Всего чатов: <b>{total}</b>\n"
            "Создано мной: <b>{created}</b>\n"
            "Можно мутить: <b>{can_mute}</b>\n"
            "Можно банить: <b>{can_ban}</b>"
            "</blockquote>"
        ),
    }

    def __init__(self):
        self._ban_cache = {}
        self._mute_cache = {}
        self._watched: set = set()
        self._premium_status = None
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "skip_ids",
                [],
                "Chat IDs to skip in global actions",
                validator=loader.validators.Series(loader.validators.Integer()),
            )
        )

    async def _check_premium(self) -> bool:
        if self._premium_status is None:
            me = await self._client.get_me()
            self._premium_status = getattr(me, "premium", False)
        return self._premium_status

    def _s(self, key: str, is_prem: bool) -> str:
        if is_prem:
            return self.strings[key]
        noprem_key = f"{key}_noprem"
        return self.strings.get(noprem_key, self.strings[key])

    async def _collect_ban_chats(self):
        result = []
        skip = list(self.config["skip_ids"] or [])
        async for dialog in self._client.iter_dialogs():
            entity = dialog.entity
            if isinstance(entity, (ChannelForbidden, ChatForbidden)):
                continue
            if isinstance(entity, Chat):
                if entity.id in skip:
                    continue
                if _has_ban_right(entity):
                    result.append(("group", dialog.input_entity))
            elif isinstance(entity, Channel):
                if entity.id in skip:
                    continue
                if _has_ban_right(entity):
                    if getattr(entity, "megagroup", False):
                        result.append(("group", dialog.input_entity))
                    elif getattr(entity, "broadcast", False):
                        result.append(("channel", dialog.input_entity))
        return result
        return result

    async def _collect_mute_chats(self):
        result = []
        skip = list(self.config["skip_ids"] or [])
        async for dialog in self._client.iter_dialogs():
            entity = dialog.entity
            if isinstance(entity, (ChannelForbidden, ChatForbidden)):
                continue
            if isinstance(entity, Chat):
                if entity.id in skip:
                    continue
                if _has_ban_right(entity):
                    result.append(dialog.input_entity)
            elif isinstance(entity, Channel):
                if entity.id in skip:
                    continue
                if not getattr(entity, "megagroup", False):
                    continue
                if _has_ban_right(entity):
                    result.append(dialog.input_entity)
        return result

    async def _get_ban_chats(self):
        if not self._ban_cache or self._ban_cache.get("exp", 0) < time.time():
            chats = await self._collect_ban_chats()
            self._ban_cache = {"exp": time.time() + 600, "chats": chats}
        return self._ban_cache["chats"]

    async def _get_mute_chats(self):
        if not self._mute_cache or self._mute_cache.get("exp", 0) < time.time():
            chats = await self._collect_mute_chats()
            self._mute_cache = {
                "exp": time.time() + 600,
                "chats": chats,
                "ids": {getattr(p, "channel_id", None) or getattr(p, "chat_id", None) for p in chats},
            }
        return self._mute_cache["chats"]

    async def _collect_full_stats(self):
        total = 0
        created = 0
        can_mute = 0
        can_ban = 0
        skip = list(self.config["skip_ids"] or [])

        async for dialog in self._client.iter_dialogs():
            entity = dialog.entity
            if isinstance(entity, (ChannelForbidden, ChatForbidden)):
                continue

            if isinstance(entity, Chat):
                total += 1
                if getattr(entity, "creator", False):
                    created += 1
                if _has_ban_right(entity):
                    can_mute += 1
                    can_ban += 1

            elif isinstance(entity, Channel):
                if entity.id in skip:
                    continue
                total += 1
                if getattr(entity, "creator", False):
                    created += 1
                if _has_ban_right(entity):
                    can_ban += 1
                    if getattr(entity, "megagroup", False):
                        can_mute += 1

        return total, created, can_mute, can_ban

    @loader.command(
        ru_doc="реплай / @username / ID — бан в этом чате",
        en_doc="reply / @username / ID — ban in this chat",
    )
    async def ban(self, message: Message):
        """reply / @username / ID — ban in this chat"""
        me = await self._client.get_me()
        is_prem = await self._check_premium()
        target, err, _ = await _resolve_target(self._client, message)

        if err:
            return await utils.answer(message, self._s(err, is_prem))
        if not isinstance(target, User):
            return await utils.answer(message, self._s("no_user", is_prem))
        if target.id == me.id:
            return await utils.answer(message, self._s("self_action", is_prem))

        chat = await message.get_chat()
        if isinstance(chat, User):
            return await utils.answer(message, self._s("not_group", is_prem))

        display = _build_display(target)
        try:
            await self._client.edit_permissions(
                message.chat_id, target, until_date=0, **RESTRICT_BANNED,
            )
        except Exception:
            pass

        await utils.answer(message, self._s("banned", is_prem).format(user=display))

    @loader.command(
        ru_doc="реплай / @username / ID [1m/1h/1d] — мут в этом чате",
        en_doc="reply / @username / ID [1m/1h/1d] — mute in this chat",
    )
    async def mute(self, message: Message):
        """reply / @username / ID [1m/1h/1d] — mute in this chat"""
        me = await self._client.get_me()
        is_prem = await self._check_premium()
        target, err, extra = await _resolve_target(self._client, message)

        if err:
            return await utils.answer(message, self._s(err, is_prem))
        if not isinstance(target, User):
            return await utils.answer(message, self._s("no_user", is_prem))
        if target.id == me.id:
            return await utils.answer(message, self._s("self_action", is_prem))

        chat = await message.get_chat()
        if isinstance(chat, User):
            return await utils.answer(message, self._s("not_group", is_prem))

        seconds = _parse_duration(extra[0]) if extra else None
        until_date = int(time.time()) + seconds if seconds else 0
        duration_str = _format_duration(seconds)
        display = _build_display(target)

        try:
            await self._client.edit_permissions(
                message.chat_id, target, until_date=until_date, **RESTRICT_MUTED,
            )
        except Exception:
            pass

        self._watched.add(target.id)
        await utils.answer(message, self._s("muted", is_prem).format(user=display, duration=duration_str))

    @loader.command(
        ru_doc="реплай / @username / ID — глобальный бан",
        en_doc="reply / @username / ID — global ban",
    )
    async def gban(self, message: Message):
        """reply / @username / ID — global ban"""
        me = await self._client.get_me()
        is_prem = await self._check_premium()
        target, err, _ = await _resolve_target(self._client, message)

        if err:
            return await utils.answer(message, self._s(err, is_prem))
        if not isinstance(target, User):
            return await utils.answer(message, self._s("no_user", is_prem))
        if target.id == me.id:
            return await utils.answer(message, self._s("self_action", is_prem))

        await utils.answer(message, self.strings["processing"])

        chats = await self._get_ban_chats()
        display = _build_display(target)
        count = 0

        for _, chat_id in chats:
            try:
                await self._client.edit_permissions(
                    chat_id, target, until_date=0, **RESTRICT_BANNED,
                )
                count += 1
            except Exception:
                pass

        await utils.answer(message, self._s("gbanned", is_prem).format(user=display, count=count))

    @loader.command(
        ru_doc="реплай / @username / ID — глобальный разбан",
        en_doc="reply / @username / ID — global unban",
    )
    async def gunban(self, message: Message):
        """reply / @username / ID — global unban"""
        me = await self._client.get_me()
        is_prem = await self._check_premium()
        target, err, _ = await _resolve_target(self._client, message)

        if err:
            return await utils.answer(message, self._s(err, is_prem))
        if not isinstance(target, User):
            return await utils.answer(message, self._s("no_user", is_prem))
        if target.id == me.id:
            return await utils.answer(message, self._s("self_action", is_prem))

        await utils.answer(message, self.strings["processing"])

        chats = await self._get_ban_chats()
        display = _build_display(target)
        count = 0

        for _, chat_id in chats:
            try:
                await self._client.edit_permissions(
                    chat_id, target, until_date=0, **{k: True for k in RESTRICT_BANNED},
                )
                count += 1
            except Exception:
                pass

        await utils.answer(message, self._s("gunbanned", is_prem).format(user=display, count=count))

    @loader.command(
        ru_doc="реплай / @username / ID [1m/1h/1d] — глобальный мут",
        en_doc="reply / @username / ID [1m/1h/1d] — global mute",
    )
    async def gmute(self, message: Message):
        """reply / @username / ID [1m/1h/1d] — global mute"""
        me = await self._client.get_me()
        is_prem = await self._check_premium()
        target, err, extra = await _resolve_target(self._client, message)

        if err:
            return await utils.answer(message, self._s(err, is_prem))
        if not isinstance(target, User):
            return await utils.answer(message, self._s("no_user", is_prem))
        if target.id == me.id:
            return await utils.answer(message, self._s("self_action", is_prem))

        seconds = _parse_duration(extra[0]) if extra else None
        until_date = int(time.time()) + seconds if seconds else 0
        duration_str = _format_duration(seconds)

        chats = await self._get_mute_chats()
        display = _build_display(target)
        count = 0

        for chat_id in chats:
            try:
                await self._client.edit_permissions(
                    chat_id, target, until_date=until_date, **RESTRICT_MUTED,
                )
                count += 1
            except Exception:
                pass

        self._watched.add(target.id)
        await utils.answer(message, self._s("gmuted", is_prem).format(user=display, count=count, duration=duration_str))

    @loader.command(
        ru_doc="реплай / @username / ID — глобальный размут",
        en_doc="reply / @username / ID — global unmute",
    )
    async def gunmute(self, message: Message):
        """reply / @username / ID — global unmute"""
        me = await self._client.get_me()
        is_prem = await self._check_premium()
        target, err, _ = await _resolve_target(self._client, message)

        if err:
            return await utils.answer(message, self._s(err, is_prem))
        if not isinstance(target, User):
            return await utils.answer(message, self._s("no_user", is_prem))
        if target.id == me.id:
            return await utils.answer(message, self._s("self_action", is_prem))

        chats = await self._get_mute_chats()
        display = _build_display(target)
        count = 0

        for chat_id in chats:
            try:
                await self._client.edit_permissions(
                    chat_id, target, until_date=0, **{k: True for k in RESTRICT_MUTED},
                )
                count += 1
            except Exception:
                pass

        self._watched.discard(target.id)
        await utils.answer(message, self._s("gunmuted", is_prem).format(user=display, count=count))

    @loader.command(
        ru_doc="реплай / @username / ID — удалить ВСЕ сообщения и забанить глобально",
        en_doc="reply / @username / ID — delete ALL messages and global ban",
    )
    async def gdelete(self, message: Message):
        """reply / @username / ID — delete ALL messages and global ban"""
        me = await self._client.get_me()
        is_prem = await self._check_premium()
        target, err, _ = await _resolve_target(self._client, message)

        if err:
            return await utils.answer(message, self._s(err, is_prem))
        if not isinstance(target, User):
            return await utils.answer(message, self._s("no_user", is_prem))
        if target.id == me.id:
            return await utils.answer(message, self._s("self_action", is_prem))

        await utils.answer(message, self.strings["processing"])

        chats = await self._get_ban_chats()
        display = _build_display(target)
        count = 0

        for _, chat_id in chats:
            try:
                msg_ids = [
                    msg.id
                    async for msg in self._client.iter_messages(chat_id, from_user=target.id)
                ]
                if msg_ids:
                    await _bulk_delete(self._client, chat_id, msg_ids)
            except Exception:
                pass

            try:
                await self._client.edit_permissions(
                    chat_id, target, until_date=0, **RESTRICT_BANNED,
                )
                count += 1
            except Exception:
                pass

        await utils.answer(message, self._s("gdeleted", is_prem).format(user=display, count=count))

    @loader.command(
        ru_doc="полная статистика всех чатов на аккаунте",
        en_doc="full stats of all chats on account",
    )
    async def gtest(self, message: Message):
        """full stats of all chats on account"""
        is_prem = await self._check_premium()
        await utils.answer(message, self.strings["processing"])

        total, created, can_mute, can_ban = await self._collect_full_stats()

        await utils.answer(
            message,
            self._s("gtest", is_prem).format(
                total=total,
                created=created,
                can_mute=can_mute,
                can_ban=can_ban,
            ),
        )

    async def watcher(self, message: Message):
        if not isinstance(message, Message):
            return
        if message.out:
            return
        if not message.is_group:
            return
        if not self._watched:
            return
        if message.sender_id not in self._watched:
            return

        mute_chat_ids = self._mute_cache.get("ids", set())
        if message.chat_id not in mute_chat_ids:
            return

        try:
            await message.delete()
        except Exception:
            pass