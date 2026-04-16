__version__ = (2, 0, 0)
# meta developer: FireJester.t.me forked from @dev_angel_7553

import io
import json
import asyncio
import logging
import string
from concurrent.futures import ThreadPoolExecutor

import requests
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import (
    ChannelParticipantsSearch,
    ChannelParticipantsRecent,
    ChannelParticipantsAdmins,
)

from .. import loader, utils

logger = logging.getLogger(__name__)

BASE_URL = "https://calls.okcdn.ru"
API_KEY = "CHKIPMKGDIHBABABA"
SESSION = '{"device_id":"telega_detector","version":2,"client_version":"android_8","client_type":"SDK_ANDROID"}'
WORKERS = 30
HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.3 Mobile/15E148 Safari/604.1"
}


@loader.tds
class TelegaDetectorMod(loader.Module):
    """Telega user detector forked from @dev_angel_7553"""

    strings = {
        "name": "TelegaDetector",
        "help": (
            "<b>TelegaDetector - find Telega users</b>\n\n"
            "<code>{prefix}telega now</code> - scan current chat or dialog\n"
            "<code>{prefix}telega all</code> - scan all private dialogs\n"
            "<code>{prefix}telega user [id]</code> - check specific user by id\n"
            "<code>{prefix}telega group [-100id]</code> - scan group by id\n"
        ),
        "checking": "<b>Analyzing members...</b>",
        "searching": "<b>Searching: {count} members found</b>",
        "not_found": "<b>No Telega users detected</b>",
        "found_private": (
            "<b>⚠️ Telega detected</b>\n\n"
            "<b>Name:</b> {name}\n"
            "<b>ID:</b> <code>{uid}</code>\n"
            "<b>Username:</b> {username}"
        ),
        "not_found_private": "<b>Telega not detected for this user</b>",
        "error": "<b>Error:</b> {error}",
        "no_users": "<b>No users found</b>",
        "session_error": "<b>Failed to get session</b>",
        "no_id": "<b>Error:</b> Provide a valid ID",
    }

    strings_ru = {
        "help": (
            "<b>TelegaDetector - поиск пользователей Telega</b>\n\n"
            "<code>{prefix}telega now</code> - сканировать текущий чат или диалог\n"
            "<code>{prefix}telega all</code> - сканировать все личные диалоги\n"
            "<code>{prefix}telega user [id]</code> - проверить конкретного пользователя по id\n"
            "<code>{prefix}telega group [-100id]</code> - сканировать группу по id\n"
        ),
        "checking": "<b>Анализирую участников...</b>",
        "searching": "<b>Поиск: найдено {count} участников</b>",
        "not_found": "<b>Пользователи Telega не обнаружены</b>",
        "found_private": (
            "<b>⚠️ Telega обнаружен</b>\n\n"
            "<b>Имя:</b> {name}\n"
            "<b>ID:</b> <code>{uid}</code>\n"
            "<b>Username:</b> {username}"
        ),
        "not_found_private": "<b>Telega не обнаружен у этого пользователя</b>",
        "error": "<b>Ошибка:</b> {error}",
        "no_users": "<b>Пользователи не найдены</b>",
        "session_error": "<b>Не удалось получить сессию</b>",
        "no_id": "<b>Ошибка:</b> Укажите корректный ID",
    }

    async def client_ready(self, client, db):
        self.db = db
        self._client = client
        self._executor = ThreadPoolExecutor(max_workers=WORKERS)

    def _get_username(self, entity):
        if hasattr(entity, "username") and entity.username:
            return entity.username
        if hasattr(entity, "usernames") and entity.usernames:
            for u in entity.usernames:
                if getattr(u, "active", False):
                    return u.username
            return entity.usernames[0].username
        return None

    def _get_session_key(self):
        try:
            resp = requests.post(
                f"{BASE_URL}/api/auth/anonymLogin",
                json={
                    "application_key": API_KEY,
                    "session_data": SESSION,
                },
                headers=HEADERS,
                timeout=10,
            )
            return resp.json().get("session_key", "")
        except Exception:
            return ""

    def _check_single(self, user_id, session_key):
        try:
            external_ids = json.dumps([{"id": str(user_id), "ok_anonym": False}])
            resp = requests.post(
                f"{BASE_URL}/api/vchat/getOkIdsByExternalIds",
                json={
                    "application_key": API_KEY,
                    "session_key": session_key,
                    "externalIds": external_ids,
                },
                headers=HEADERS,
                timeout=15,
            )
            ids = resp.json().get("ids", [])
            for item in ids:
                if str(item.get("external_user_id", {}).get("id", "")) == str(user_id):
                    return True
            return False
        except Exception:
            return False

    async def _check_single_async(self, user_id, session_key):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor, self._check_single, user_id, session_key
        )

    async def _collect_members(self, entity, status_msg):
        users = {}

        try:
            async for member in self._client.iter_participants(entity):
                if not member.bot and not member.is_self:
                    users[member.id] = member
        except Exception:
            pass

        await utils.answer(
            status_msg,
            self.strings["searching"].format(count=len(users)),
        )

        for filter_type in [ChannelParticipantsRecent(), ChannelParticipantsAdmins()]:
            try:
                offset = 0
                limit = 200
                while True:
                    result = await self._client(
                        GetParticipantsRequest(entity, filter_type, offset, limit, hash=0)
                    )
                    if not result.users:
                        break
                    for u in result.users:
                        if not u.bot and not u.is_self:
                            users[u.id] = u
                    if len(result.users) < limit:
                        break
                    offset += len(result.users)
                    await asyncio.sleep(0.5)
            except Exception:
                pass

        letters = list(string.ascii_lowercase) + list("абвгдеёжзийклмнопрстуфхцчшщъыьэюя")
        for letter in letters:
            try:
                offset = 0
                limit = 200
                while True:
                    result = await self._client(
                        GetParticipantsRequest(
                            entity,
                            ChannelParticipantsSearch(letter),
                            offset,
                            limit,
                            hash=0,
                        )
                    )
                    if not result.users:
                        break
                    for u in result.users:
                        if not u.bot and not u.is_self:
                            users[u.id] = u
                    if len(result.users) < limit:
                        break
                    offset += len(result.users)
                    await asyncio.sleep(0.3)
            except Exception:
                pass

        await utils.answer(
            status_msg,
            self.strings["searching"].format(count=len(users)),
        )

        if len(users) < 300:
            try:
                offset_id = 0
                limit = 100
                total_fetched = 0
                max_messages = 5000

                while total_fetched < max_messages:
                    history = await self._client(
                        GetHistoryRequest(
                            peer=entity,
                            offset_id=offset_id,
                            offset_date=None,
                            add_offset=0,
                            limit=limit,
                            max_id=0,
                            min_id=0,
                            hash=0,
                        )
                    )
                    if not history.messages:
                        break
                    for msg in history.messages:
                        sender_id = getattr(msg, "from_id", None)
                        if sender_id and hasattr(sender_id, "user_id"):
                            uid = sender_id.user_id
                            if uid not in users:
                                try:
                                    u = await self._client.get_entity(uid)
                                    if not getattr(u, "bot", False) and not getattr(u, "is_self", False):
                                        users[uid] = u
                                except Exception:
                                    pass
                    offset_id = history.messages[-1].id
                    total_fetched += len(history.messages)
                    await asyncio.sleep(0.5)
            except Exception:
                pass

            await utils.answer(
                status_msg,
                self.strings["searching"].format(count=len(users)),
            )

        return users

    async def _scan_users(self, users_dict, session_key):
        user_ids = list(users_dict.keys())
        total = len(user_ids)
        telega_found = []
        chunks = [user_ids[i:i + WORKERS] for i in range(0, total, WORKERS)]

        for chunk in chunks:
            tasks = [self._check_single_async(uid, session_key) for uid in chunk]
            results = await asyncio.gather(*tasks)
            for uid, is_telega in zip(chunk, results):
                if is_telega:
                    telega_found.append(users_dict[uid])
            await asyncio.sleep(0.05)

        return telega_found

    def _format_user(self, user):
        name = f"{user.first_name or ''} {user.last_name or ''}".strip() or "No Name"
        username = self._get_username(user)
        username_str = f"@{username}" if username else "None"
        return (
            f"Name: {name}\n"
            f"ID: {user.id}\n"
            f"Username: {username_str}\n"
            f"{'----------'}\n"
        )

    async def _send_file(self, message, telega_users, label="TELEGA USERS"):
        content = f"{label} - total: {len(telega_users)}\n{'=' * 35}\n\n"
        for user in telega_users:
            content += self._format_user(user)

        buf = io.BytesIO(content.encode("utf-8"))
        buf.name = "telega_users.txt"

        await self._client.send_file(
            message.chat_id,
            buf,
            caption=f"⚠️ Telega users found: {len(telega_users)}",
        )

    @loader.command(
        ru_doc="Поиск пользователей Telega",
        en_doc="Telega user detection",
    )
    async def telega(self, message):
        """Telega user detection"""
        args = utils.get_args_raw(message).strip()
        parts = args.split()

        if not parts:
            prefix = self.get_prefix()
            await utils.answer(
                message,
                self.strings["help"].format(prefix=prefix),
            )
            return

        cmd = parts[0].lower()

        if cmd == "now":
            await self._cmd_now(message)
        elif cmd == "all":
            await self._cmd_all(message)
        elif cmd == "user":
            if len(parts) < 2 or not parts[1].lstrip("-").isdigit():
                await utils.answer(message, self.strings["no_id"])
                return
            await self._cmd_user(message, int(parts[1]))
        elif cmd == "group":
            if len(parts) < 2 or not parts[1].lstrip("-").isdigit():
                await utils.answer(message, self.strings["no_id"])
                return
            await self._cmd_group(message, int(parts[1]))
        else:
            prefix = self.get_prefix()
            await utils.answer(
                message,
                self.strings["help"].format(prefix=prefix),
            )

    async def _cmd_now(self, message):
        status_msg = await utils.answer(message, self.strings["checking"])

        try:
            if message.is_private:
                user = await self._client.get_entity(message.chat_id)
                if getattr(user, "bot", False):
                    await utils.answer(status_msg, self.strings["not_found"])
                    return

                loop = asyncio.get_event_loop()
                session_key = await loop.run_in_executor(self._executor, self._get_session_key)
                if not session_key:
                    await utils.answer(status_msg, self.strings["session_error"])
                    return

                is_telega = await self._check_single_async(user.id, session_key)

                if not is_telega:
                    await utils.answer(status_msg, self.strings["not_found_private"])
                    return

                name = f"{user.first_name or ''} {user.last_name or ''}".strip() or "No Name"
                username = self._get_username(user)
                username_str = f"@{username}" if username else "None"

                await utils.answer(
                    status_msg,
                    self.strings["found_private"].format(
                        name=name,
                        uid=user.id,
                        username=username_str,
                    ),
                )
            else:
                entity = await self._client.get_entity(message.chat_id)
                users_dict = await self._collect_members(entity, status_msg)

                if not users_dict:
                    await utils.answer(status_msg, self.strings["no_users"])
                    return

                loop = asyncio.get_event_loop()
                session_key = await loop.run_in_executor(self._executor, self._get_session_key)
                if not session_key:
                    await utils.answer(status_msg, self.strings["session_error"])
                    return

                telega_users = await self._scan_users(users_dict, session_key)
                await status_msg.delete()

                if not telega_users:
                    await self._client.send_message(message.chat_id, self.strings["not_found"])
                    return

                await self._send_file(message, telega_users, "TELEGA IN CHAT")

        except Exception as e:
            logger.error(f"[TelegaDetector] now error: {e}")
            await utils.answer(status_msg, self.strings["error"].format(error=str(e)))

    async def _cmd_all(self, message):
        status_msg = await utils.answer(message, self.strings["checking"])

        try:
            users = {}
            async for dialog in self._client.iter_dialogs():
                if dialog.is_user:
                    entity = dialog.entity
                    if not getattr(entity, "bot", False):
                        users[entity.id] = entity

            if not users:
                await utils.answer(status_msg, self.strings["no_users"])
                return

            await utils.answer(
                status_msg,
                self.strings["searching"].format(count=len(users)),
            )

            loop = asyncio.get_event_loop()
            session_key = await loop.run_in_executor(self._executor, self._get_session_key)
            if not session_key:
                await utils.answer(status_msg, self.strings["session_error"])
                return

            telega_users = await self._scan_users(users, session_key)
            await status_msg.delete()

            if not telega_users:
                await self._client.send_message(message.chat_id, self.strings["not_found"])
                return

            await self._send_file(message, telega_users, "TELEGA ALL DIALOGS")

        except Exception as e:
            logger.error(f"[TelegaDetector] all error: {e}")
            await utils.answer(status_msg, self.strings["error"].format(error=str(e)))

    async def _cmd_user(self, message, user_id):
        status_msg = await utils.answer(message, self.strings["checking"])

        try:
            user = await self._client.get_entity(user_id)

            if getattr(user, "bot", False):
                await utils.answer(status_msg, self.strings["not_found_private"])
                return

            loop = asyncio.get_event_loop()
            session_key = await loop.run_in_executor(self._executor, self._get_session_key)
            if not session_key:
                await utils.answer(status_msg, self.strings["session_error"])
                return

            is_telega = await self._check_single_async(user.id, session_key)

            if not is_telega:
                await utils.answer(status_msg, self.strings["not_found_private"])
                return

            name = f"{user.first_name or ''} {user.last_name or ''}".strip() or "No Name"
            username = self._get_username(user)
            username_str = f"@{username}" if username else "None"

            await utils.answer(
                status_msg,
                self.strings["found_private"].format(
                    name=name,
                    uid=user.id,
                    username=username_str,
                ),
            )

        except Exception as e:
            logger.error(f"[TelegaDetector] user error: {e}")
            await utils.answer(status_msg, self.strings["error"].format(error=str(e)))

    async def _cmd_group(self, message, chat_id):
        status_msg = await utils.answer(message, self.strings["checking"])

        try:
            entity = await self._client.get_entity(chat_id)
            users_dict = await self._collect_members(entity, status_msg)

            if not users_dict:
                await utils.answer(status_msg, self.strings["no_users"])
                return

            loop = asyncio.get_event_loop()
            session_key = await loop.run_in_executor(self._executor, self._get_session_key)
            if not session_key:
                await utils.answer(status_msg, self.strings["session_error"])
                return

            telega_users = await self._scan_users(users_dict, session_key)
            await status_msg.delete()

            if not telega_users:
                await self._client.send_message(message.chat_id, self.strings["not_found"])
                return

            await self._send_file(message, telega_users, f"TELEGA IN GROUP {chat_id}")

        except Exception as e:
            logger.error(f"[TelegaDetector] group error: {e}")
            await utils.answer(status_msg, self.strings["error"].format(error=str(e)))