__version__ = (3, 2, 0)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/AdminTool/MetaBanner.jpeg

import logging
import time
from telethon.tl.types import (
    Message,
    User,
    Channel,
    Chat,
    ChannelForbidden,
    ChatForbidden,
    ChatBannedRights,
    InputPeerChannel,
    InputPeerChat,
)
from telethon.tl.functions.channels import EditBannedRequest
from .. import loader, utils

logger = logging.getLogger(__name__)

RIGHTS_BANNED = ChatBannedRights(
    until_date=None,
    view_messages=True,
    send_messages=True,
    send_media=True,
    send_stickers=True,
    send_gifs=True,
    send_games=True,
    send_inline=True,
    send_polls=True,
    change_info=True,
    invite_users=True,
    pin_messages=True,
    send_photos=True,
    send_videos=True,
    send_roundvideos=True,
    send_audios=True,
    send_voices=True,
    send_docs=True,
    send_plain=True,
)

RIGHTS_MUTED = ChatBannedRights(
    until_date=None,
    view_messages=False,
    send_messages=True,
    send_media=True,
    send_stickers=True,
    send_gifs=True,
    send_games=True,
    send_inline=True,
    send_polls=True,
    change_info=True,
    invite_users=True,
    pin_messages=True,
    send_photos=True,
    send_videos=True,
    send_roundvideos=True,
    send_audios=True,
    send_voices=True,
    send_docs=True,
    send_plain=True,
)

RIGHTS_UNBANNED = ChatBannedRights(
    until_date=None,
    view_messages=False,
    send_messages=False,
    send_media=False,
    send_stickers=False,
    send_gifs=False,
    send_games=False,
    send_inline=False,
    send_polls=False,
    change_info=False,
    invite_users=False,
    pin_messages=False,
    send_photos=False,
    send_videos=False,
    send_roundvideos=False,
    send_audios=False,
    send_voices=False,
    send_docs=False,
    send_plain=False,
)

E_OK    = '<tg-emoji emoji-id=5255888339248125403>😎</tg-emoji>'
E_ANGRY = '<tg-emoji emoji-id=5258084738278658226>😠</tg-emoji>'
E_DEAD1 = '<tg-emoji emoji-id=5258511597898340942>😵</tg-emoji>'
E_DEAD2 = '<tg-emoji emoji-id=5258497347196852362>😵</tg-emoji>'
E_DEAD3 = '<tg-emoji emoji-id=5256108885818778479>😵</tg-emoji>'


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

    if isinstance(entity, Chat):
        if getattr(entity, "creator", False):
            return True
        ar = getattr(entity, "admin_rights", None)
        return bool(ar and getattr(ar, "ban_users", False))
    if isinstance(entity, Channel):
        if getattr(entity, "creator", False):
            return True
        ar = getattr(entity, "admin_rights", None)
        return bool(ar and getattr(ar, "ban_users", False))
    return False


def _has_delete_right(entity) -> bool:
    if isinstance(entity, Chat):
        if getattr(entity, "creator", False):
            return True
        ar = getattr(entity, "admin_rights", None)
        return bool(ar and getattr(ar, "delete_messages", False))
    if isinstance(entity, Channel):
        if getattr(entity, "creator", False):
            return True
        ar = getattr(entity, "admin_rights", None)
        return bool(ar and getattr(ar, "delete_messages", False))
    return False


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


async def _edit_banned(client, chat_input, user_input, rights: ChatBannedRights, until_seconds: int = 0):"
    import datetime
    if until_seconds:
        rights = ChatBannedRights(
            until_date=datetime.datetime.utcfromtimestamp(int(time.time()) + until_seconds),
            view_messages=rights.view_messages,
            send_messages=rights.send_messages,
            send_media=rights.send_media,
            send_stickers=rights.send_stickers,
            send_gifs=rights.send_gifs,
            send_games=rights.send_games,
            send_inline=rights.send_inline,
            send_polls=rights.send_polls,
            change_info=rights.change_info,
            invite_users=rights.invite_users,
            pin_messages=rights.pin_messages,
            send_photos=rights.send_photos,
            send_videos=rights.send_videos,
            send_roundvideos=rights.send_roundvideos,
            send_audios=rights.send_audios,
            send_voices=rights.send_voices,
            send_docs=rights.send_docs,
            send_plain=rights.send_plain,
        )
    await client(EditBannedRequest(
        channel=chat_input,
        participant=user_input,
        banned_rights=rights,
    ))


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
            "Can ban: <b>{can_ban}</b>\n"
            "Can delete: <b>{can_delete}</b>"
            "</blockquote>"
        ),
        "gtest_noprem": (
            "<b>Stats</b>\n"
            "<blockquote>"
            "Total chats: <b>{total}</b>\n"
            "Created by me: <b>{created}</b>\n"
            "Can mute: <b>{can_mute}</b>\n"
            "Can ban: <b>{can_ban}</b>\n"
            "Can delete: <b>{can_delete}</b>"
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
            "Можно банить: <b>{can_ban}</b>\n"
            "Можно удалять: <b>{can_delete}</b>"
            "</blockquote>"
        ),
        "gtest_noprem": (
            "<b>Статистика</b>\n"
            "<blockquote>"
            "Всего чатов: <b>{total}</b>\n"
            "Создано мной: <b>{created}</b>\n"
            "Можно мутить: <b>{can_mute}</b>\n"
            "Можно банить: <b>{can_ban}</b>\n"
            "Можно удалять: <b>{can_delete}</b>"
            "</blockquote>"
        ),
    }

    def __init__(self):
        self._ban_cache = {}
        self._mute_cache = {}
        self._delete_cache = {}
        self._watched: dict = {}
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

    async def _collect_chats(self):
        """Returns (ban_chats, mute_chats, delete_chats) as lists of (input_entity, entity)"""
        ban_chats = []
        mute_chats = []
        delete_chats = []
        skip = set(self.config["skip_ids"] or [])

        async for dialog in self._client.iter_dialogs():
            entity = dialog.entity
            if isinstance(entity, (ChannelForbidden, ChatForbidden)):
                continue

            if isinstance(entity, Chat):
                if entity.id in skip:
                    continue
                inp = dialog.input_entity
                can_ban = _has_ban_right(entity)
                can_del = _has_delete_right(entity)
                if can_ban:
                    ban_chats.append(inp)
                    mute_chats.append(inp)
                if can_del:
                    delete_chats.append(inp)

            elif isinstance(entity, Channel):
                if entity.id in skip:
                    continue
                inp = dialog.input_entity
                can_ban = _has_ban_right(entity)
                can_del = _has_delete_right(entity)
                is_mega = getattr(entity, "megagroup", False)
                is_broad = getattr(entity, "broadcast", False)
                if can_ban:
                    ban_chats.append(inp)
                    if is_mega:
                        mute_chats.append(inp)
                if can_del:
                    delete_chats.append(inp)

        return ban_chats, mute_chats, delete_chats

    async def _refresh_cache(self):
        ban, mute, delete = await self._collect_chats()
        exp = time.time() + 600

        def _peer_id(inp):
            return getattr(inp, "channel_id", None) or getattr(inp, "chat_id", None)

        self._ban_cache    = {"exp": exp, "chats": ban}
        self._mute_cache   = {"exp": exp, "chats": mute,   "ids": {_peer_id(p) for p in mute}}
        self._delete_cache = {"exp": exp, "chats": delete, "ids": {_peer_id(p) for p in delete}}

    async def _get_ban_chats(self):
        if not self._ban_cache or self._ban_cache.get("exp", 0) < time.time():
            await self._refresh_cache()
        return self._ban_cache["chats"]

    async def _get_mute_chats(self):
        if not self._mute_cache or self._mute_cache.get("exp", 0) < time.time():
            await self._refresh_cache()
        return self._mute_cache["chats"]

    async def _get_delete_chats(self):
        if not self._delete_cache or self._delete_cache.get("exp", 0) < time.time():
            await self._refresh_cache()
        return self._delete_cache["chats"]

    async def _collect_full_stats(self):
        total = 0
        created = 0
        can_mute = 0
        can_ban = 0
        can_delete = 0
        skip = set(self.config["skip_ids"] or [])

        async for dialog in self._client.iter_dialogs():
            entity = dialog.entity
            if isinstance(entity, (ChannelForbidden, ChatForbidden)):
                continue
            if isinstance(entity, Chat):
                if entity.id in skip:
                    continue
                total += 1
                if getattr(entity, "creator", False):
                    created += 1
                if _has_ban_right(entity):
                    can_mute += 1
                    can_ban += 1
                if _has_delete_right(entity):
                    can_delete += 1
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
                if _has_delete_right(entity):
                    can_delete += 1

        return total, created, can_mute, can_ban, can_delete

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
            await self._client(EditBannedRequest(
                channel=message.chat_id,
                participant=target.id,
                banned_rights=RIGHTS_BANNED,
            ))
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
        duration_str = _format_duration(seconds)
        display = _build_display(target)

        import datetime
        until = datetime.datetime.utcfromtimestamp(int(time.time()) + seconds) if seconds else None
        rights = ChatBannedRights(
            until_date=until,
            view_messages=False,
            send_messages=True,
            send_media=True,
            send_stickers=True,
            send_gifs=True,
            send_games=True,
            send_inline=True,
            send_polls=True,
            change_info=True,
            invite_users=True,
            pin_messages=True,
            send_photos=True,
            send_videos=True,
            send_roundvideos=True,
            send_audios=True,
            send_voices=True,
            send_docs=True,
            send_plain=True,
        )

        try:
            await self._client(EditBannedRequest(
                channel=message.chat_id,
                participant=target.id,
                banned_rights=rights,
            ))
        except Exception:
            pass

        chat_id = message.chat_id
        if target.id not in self._watched:
            self._watched[target.id] = set()
        self._watched[target.id].add(chat_id)

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

        for chat_inp in chats:
            try:
                await self._client(EditBannedRequest(
                    channel=chat_inp,
                    participant=target.id,
                    banned_rights=RIGHTS_BANNED,
                ))
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

        for chat_inp in chats:
            try:
                await self._client(EditBannedRequest(
                    channel=chat_inp,
                    participant=target.id,
                    banned_rights=RIGHTS_UNBANNED,
                ))
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
        duration_str = _format_duration(seconds)

        import datetime
        until = datetime.datetime.utcfromtimestamp(int(time.time()) + seconds) if seconds else None
        rights = ChatBannedRights(
            until_date=until,
            view_messages=False,
            send_messages=True,
            send_media=True,
            send_stickers=True,
            send_gifs=True,
            send_games=True,
            send_inline=True,
            send_polls=True,
            change_info=True,
            invite_users=True,
            pin_messages=True,
            send_photos=True,
            send_videos=True,
            send_roundvideos=True,
            send_audios=True,
            send_voices=True,
            send_docs=True,
            send_plain=True,
        )

        chats = await self._get_mute_chats()
        display = _build_display(target)
        count = 0

        for chat_inp in chats:
            try:
                await self._client(EditBannedRequest(
                    channel=chat_inp,
                    participant=target.id,
                    banned_rights=rights,
                ))
                count += 1
                chat_id = getattr(chat_inp, "channel_id", None) or getattr(chat_inp, "chat_id", None)
                if chat_id:
                    if target.id not in self._watched:
                        self._watched[target.id] = set()
                    self._watched[target.id].add(chat_id)
            except Exception:
                pass

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

        for chat_inp in chats:
            try:
                await self._client(EditBannedRequest(
                    channel=chat_inp,
                    participant=target.id,
                    banned_rights=RIGHTS_UNBANNED,
                ))
                count += 1
            except Exception:
                pass

        self._watched.pop(target.id, None)
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

        ban_chats = await self._get_ban_chats()
        delete_chats = await self._get_delete_chats()
        display = _build_display(target)
        count = 0

        for chat_inp in delete_chats:
            try:
                msg_ids = [
                    msg.id
                    async for msg in self._client.iter_messages(chat_inp, from_user=target.id)
                ]
                if msg_ids:
                    await _bulk_delete(self._client, chat_inp, msg_ids)
            except Exception:
                pass

        for chat_inp in ban_chats:
            try:
                await self._client(EditBannedRequest(
                    channel=chat_inp,
                    participant=target.id,
                    banned_rights=RIGHTS_BANNED,
                ))
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

        total, created, can_mute, can_ban, can_delete = await self._collect_full_stats()

        await utils.answer(
            message,
            self._s("gtest", is_prem).format(
                total=total,
                created=created,
                can_mute=can_mute,
                can_ban=can_ban,
                can_delete=can_delete,
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

        sender_id = message.sender_id
        if sender_id not in self._watched:
            return

        chat_id = message.chat_id

        if chat_id in self._watched[sender_id]:
            try:
                await message.delete()
            except Exception:
                pass
            return

        delete_ids = self._delete_cache.get("ids", set())
        if chat_id in delete_ids:
            try:
                await message.delete()
            except Exception:
                pass

