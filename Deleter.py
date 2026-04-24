__version__ = (2, 1, 1)
# meta developer: I_execute.t.me

import asyncio
import logging
import random
from datetime import timedelta, timezone, datetime

from telethon.tl.functions.channels import LeaveChannelRequest
from telethon.tl.types import Message, User

from .. import loader, utils

logger = logging.getLogger(__name__)

SAYONARA_URL = "https://raw.githubusercontent.com/FireJester/Modules/main/Assets/Deleter/Sayonara.mp4"


@loader.tds
class Deleter(loader.Module):
    """Swift deleting messages"""

    strings = {
        "name": "Deleter",
        "help": (
            "<b>Deleter - swift message deletion</b>\n\n"
            "<b>Own messages:</b>\n"
            "<code>{prefix}del me</code> - delete all your messages in current chat\n"
            "<code>{prefix}del me -f</code> - delete all your messages and leave the chat\n"
            "<code>{prefix}del [n]</code> - delete last n your messages\n"
            "<code>{prefix}del [n]</code> (reply) - delete n your messages before replied message\n"
            "<code>{prefix}del now</code> - delete your messages from last 5 minutes\n"
            "<code>{prefix}del today</code> - delete your messages since 00:00 today\n\n"
            "<b>Other users:</b>\n"
            "<code>{prefix}del @username</code> - delete all messages from specified user\n"
            "<code>{prefix}del before</code> (reply) - delete all messages from everyone before replied message\n"
            "<code>{prefix}del after</code> (reply) - delete all messages from everyone after replied message\n"
        ),
        "no_count": "<b>Error:</b> Provide a valid number of messages",
        "no_reply": "<b>Error:</b> Reply to a message",
        "no_user": "<b>Error:</b> User not found",
        "no_perms": "<b>Error:</b> Not enough permissions to delete some messages",
        "error": "<b>Error:</b>\n<blockquote>{error}</blockquote>",
        "done_me": "<b>All your messages in chat</b>\n<blockquote>{chat}</blockquote>\n<b>have been deleted.</b>",
        "done_leave": (
            "<b>Left chat:</b>\n<blockquote>{chat}</blockquote>\n"
            "<b>Deleted messages:</b> <code>{count}</code>\n"
            "<b>First message date:</b> <code>{first_date}</code>\n"
            "<b>Last message date:</b> <code>{last_date}</code>"
        ),
    }

    strings_ru = {
        "help": (
            "<b>Deleter - быстрое удаление сообщений</b>\n\n"
            "<b>Свои сообщения:</b>\n"
            "<code>{prefix}del me</code> - удалить все свои сообщения в текущем чате\n"
            "<code>{prefix}del me -f</code> - удалить все свои сообщения и покинуть чат\n"
            "<code>{prefix}del [n]</code> - удалить последние n своих сообщений\n"
            "<code>{prefix}del [n]</code> (реплай) - удалить n своих сообщений до указанного сообщения\n"
            "<code>{prefix}del now</code> - удалить свои сообщения за последние 5 минут\n"
            "<code>{prefix}del today</code> - удалить свои сообщения с 00:00 сегодня\n\n"
            "<b>Другие пользователи:</b>\n"
            "<code>{prefix}del @username</code> - удалить все сообщения указанного пользователя\n"
            "<code>{prefix}del before</code> (реплай) - удалить все сообщения от всех до указанного сообщения\n"
            "<code>{prefix}del after</code> (реплай) - удалить все сообщения от всех после указанного сообщения\n"
        ),
        "no_count": "<b>Ошибка:</b> Укажите корректное количество сообщений",
        "no_reply": "<b>Ошибка:</b> Ответьте на сообщение",
        "no_user": "<b>Ошибка:</b> Пользователь не найден",
        "no_perms": "<b>Ошибка:</b> Недостаточно прав для удаления некоторых сообщений",
        "error": "<b>Ошибка:</b>\n<blockquote>{error}</blockquote>",
        "done_me": "<b>Все ваши сообщения в чате</b>\n<blockquote>{chat}</blockquote>\n<b>удалены.</b>",
        "done_leave": (
            "<b>Вышел из чата:</b>\n<blockquote>{chat}</blockquote>\n"
            "<b>Удалено сообщений:</b> <code>{count}</code>\n"
            "<b>Дата первого сообщения:</b> <code>{first_date}</code>\n"
            "<b>Дата последнего сообщения:</b> <code>{last_date}</code>"
        ),
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "TIMEZONE_OFFSET",
                3,
                doc="UTC timezone offset. Used for .del today command",
                validator=loader.validators.Integer(),
            ),
        )
        self._deleter_topic = None
        self._asset_channel = None

    async def client_ready(self):
        self._asset_channel = self._db.get("heroku.forums", "channel_id", None)

        if not self._asset_channel:
            logger.warning("[Deleter] heroku.forums channel_id not found in DB, notifications will be disabled.")
            return

        try:
            self._deleter_topic = await utils.asset_forum_topic(
                self._client,
                self._db,
                self._asset_channel,
                "Deleter",
                description="Logs of message deletion by Deleter module.",
                icon_emoji_id=5188466187448650036,
            )
        except Exception as e:
            logger.error(f"[Deleter] Failed to create/get forum topic: {e}")

    async def _send_log(self, text: str):
        if not self._deleter_topic or not self._asset_channel:
            try:
                me = await self._client.get_me()
                await self._client.send_message(me.id, text, parse_mode="html")
            except Exception as e:
                logger.error(f"[Deleter] Failed to send fallback log: {e}")
            return

        try:
            await self.inline.bot.send_message(
                int(f"-100{self._asset_channel}"),
                text,
                parse_mode="HTML",
                message_thread_id=self._deleter_topic.id,
            )
        except Exception as e:
            logger.error(f"[Deleter] Failed to send log to topic: {e}")

    async def _bulk_delete(self, client, chat_id, msg_ids: list) -> tuple:
        deleted = 0
        failed = 0

        if not msg_ids:
            return deleted, failed

        chunk_size = random.randint(90, 110)
        first_chunk = msg_ids[:chunk_size]
        rest = msg_ids[chunk_size:]

        try:
            await client.delete_messages(chat_id, first_chunk)
            deleted += len(first_chunk)
        except Exception:
            failed += len(first_chunk)

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
                    failed += len(chunk)
                chunk.clear()
                chunk_size = random.randint(90, 110)

        if chunk:
            await asyncio.sleep(random.uniform(0.5, 1.5))
            try:
                await client.delete_messages(chat_id, chunk)
                deleted += len(chunk)
            except Exception:
                failed += len(chunk)

        return deleted, failed

    @loader.command(
        ru_doc="Быстрое удаление сообщений",
        en_doc="Swift message deletion",
    )
    async def delcmd(self, message: Message):
        """Swift message deletion"""
        args = utils.get_args_raw(message)
        args_list = args.split() if args else []

        if not args_list:
            prefix = self.get_prefix()
            await utils.answer(
                message,
                self.strings["help"].format(prefix=prefix),
            )
            return

        cmd = args_list[0].lower()

        if cmd == "me":
            leave = "-f" in args_list
            await self._delete_me(message, leave=leave)
        elif cmd == "before":
            await self._delete_before(message)
        elif cmd == "after":
            await self._delete_after(message)
        elif cmd == "now":
            await self._delete_now(message)
        elif cmd == "today":
            await self._delete_today(message)
        elif cmd.startswith("@"):
            await self._delete_user(message, cmd)
        elif cmd.isdigit():
            await self._delete_own_n(message, int(cmd))
        else:
            try:
                entity = await message.client.get_entity(cmd)
                if isinstance(entity, User):
                    await self._delete_user_by_entity(message, entity)
                    return
            except Exception:
                pass
            prefix = self.get_prefix()
            await utils.answer(
                message,
                self.strings["help"].format(prefix=prefix),
            )

    async def _get_chat_name(self, message: Message) -> str:
        try:
            entity = await message.client.get_entity(message.chat_id)
            if hasattr(entity, "title") and entity.title:
                return entity.title
            if hasattr(entity, "first_name"):
                name = entity.first_name or ""
                if hasattr(entity, "last_name") and entity.last_name:
                    name = name + " " + entity.last_name
                return name.strip() or str(message.chat_id)
        except Exception:
            pass
        return str(message.chat_id)

    async def _delete_me(self, message: Message, leave: bool = False):
        chat_id = message.chat_id
        client = message.client
        cmd_msg_id = message.id

        chat_name = await self._get_chat_name(message)

        # Удаляем командное сообщение
        await message.delete()

        try:
            if leave:
                await self._delete_me_and_leave(client, chat_id, chat_name, cmd_msg_id)
            else:
                await self._delete_me_simple(client, chat_id, chat_name)
        except Exception as e:
            logger.error(f"[Deleter] del me error: {e}")
            await self._send_log(self.strings["error"].format(error=str(e)))

    async def _delete_me_simple(self, client, chat_id, chat_name: str):
        try:
            ids = []
            async for msg in client.iter_messages(chat_id, from_user="me"):
                ids.append(msg.id)

            if not ids:
                await self._send_log(
                    self.strings["done_me"].format(chat=chat_name)
                )
                return

            await self._bulk_delete(client, chat_id, ids)
            await self._send_log(
                self.strings["done_me"].format(chat=chat_name)
            )

        except Exception as e:
            logger.error(f"[Deleter] _delete_me_simple error: {e}")
            await self._send_log(self.strings["error"].format(error=str(e)))

    async def _delete_me_and_leave(self, client, chat_id, chat_name: str, cmd_msg_id: int):
        try:
            # 1. Сначала собираем ВСЕ свои сообщения до отправки видео
            # чтобы точно не пропустить ничего
            ids_to_delete = []
            first_date = None
            last_date = None

            async for msg in client.iter_messages(chat_id, from_user="me"):
                # Пропускаем командное сообщение - оно уже удалено,
                # но на всякий случай исключаем
                if msg.id == cmd_msg_id:
                    continue
                ids_to_delete.append(msg.id)
                if last_date is None:
                    last_date = msg.date
                first_date = msg.date

            count = len(ids_to_delete)

            # 2. Отправляем Sayonara видео
            try:
                sayonara_msg = await client.send_file(
                    chat_id,
                    SAYONARA_URL,
                )
                sayonara_msg_id = sayonara_msg.id
            except Exception as e:
                logger.error(f"[Deleter] Failed to send Sayonara: {e}")
                sayonara_msg_id = None

            # Небольшая пауза
            await asyncio.sleep(1)

            # 3. Удаляем все собранные сообщения
            # (видео мы НЕ добавляли в список - оно было собрано до отправки)
            if ids_to_delete:
                await self._bulk_delete(client, chat_id, ids_to_delete)

            # 4. Выходим из чата
            await self._leave_chat(client, chat_id)

            # 5. Лог
            def fmt_date(d):
                if d is None:
                    return "—"
                return d.strftime("%Y-%m-%d %H:%M:%S UTC")

            log_text = self.strings["done_leave"].format(
                chat=chat_name,
                count=count,
                first_date=fmt_date(first_date),
                last_date=fmt_date(last_date),
            )
            await self._send_log(log_text)

        except Exception as e:
            logger.error(f"[Deleter] _delete_me_and_leave error: {e}")
            await self._send_log(self.strings["error"].format(error=str(e)))

    async def _leave_chat(self, client, chat_id):
        try:
            await client(LeaveChannelRequest(chat_id))
        except Exception:
            try:
                await client.kick_participant(chat_id, "me")
            except Exception as e:
                logger.error(f"[Deleter] leave error: {e}")

    async def _delete_own_n(self, message: Message, count: int):
        if count <= 0:
            return await utils.answer(message, self.strings["no_count"])

        chat_id = message.chat_id
        cmd_msg_id = message.id

        try:
            ids = [cmd_msg_id]
            kwargs = {"entity": chat_id, "from_user": "me"}
            if message.is_reply:
                kwargs["max_id"] = message.reply_to_msg_id

            async for msg in message.client.iter_messages(**kwargs):
                if msg.id != cmd_msg_id:
                    ids.append(msg.id)
                    if len(ids) - 1 >= count:
                        break

            deleted, failed = await self._bulk_delete(message.client, chat_id, ids)

            if failed:
                await message.client.send_message(chat_id, self.strings["no_perms"])
        except Exception as e:
            logger.error(f"[Deleter] del n error: {e}")
            await message.client.send_message(
                chat_id, self.strings["error"].format(error=str(e))
            )

    async def _delete_before(self, message: Message):
        if not message.is_reply:
            return await utils.answer(message, self.strings["no_reply"])

        reply_id = message.reply_to_msg_id
        chat_id = message.chat_id
        cmd_msg_id = message.id

        try:
            ids = [cmd_msg_id]
            async for msg in message.client.iter_messages(chat_id, max_id=reply_id):
                ids.append(msg.id)

            deleted, failed = await self._bulk_delete(message.client, chat_id, ids)

            if failed:
                await message.client.send_message(chat_id, self.strings["no_perms"])
        except Exception as e:
            logger.error(f"[Deleter] del before error: {e}")
            await message.client.send_message(
                chat_id, self.strings["error"].format(error=str(e))
            )

    async def _delete_after(self, message: Message):
        if not message.is_reply:
            return await utils.answer(message, self.strings["no_reply"])

        reply_id = message.reply_to_msg_id
        chat_id = message.chat_id
        cmd_msg_id = message.id

        try:
            ids = [cmd_msg_id]
            async for msg in message.client.iter_messages(chat_id, min_id=reply_id):
                if msg.id != cmd_msg_id:
                    ids.append(msg.id)

            deleted, failed = await self._bulk_delete(message.client, chat_id, ids)

            if failed:
                await message.client.send_message(chat_id, self.strings["no_perms"])
        except Exception as e:
            logger.error(f"[Deleter] del after error: {e}")
            await message.client.send_message(
                chat_id, self.strings["error"].format(error=str(e))
            )

    async def _delete_now(self, message: Message):
        chat_id = message.chat_id
        cmd_msg_id = message.id
        cmd_time = message.date
        cutoff = cmd_time - timedelta(minutes=5)

        try:
            ids = [cmd_msg_id]
            async for msg in message.client.iter_messages(chat_id, from_user="me"):
                if msg.id == cmd_msg_id:
                    continue
                if msg.date < cutoff:
                    break
                ids.append(msg.id)

            deleted, failed = await self._bulk_delete(message.client, chat_id, ids)

            if failed:
                await message.client.send_message(chat_id, self.strings["no_perms"])
        except Exception as e:
            logger.error(f"[Deleter] del now error: {e}")
            await message.client.send_message(
                chat_id, self.strings["error"].format(error=str(e))
            )

    async def _delete_today(self, message: Message):
        chat_id = message.chat_id
        cmd_msg_id = message.id

        offset = self.config["TIMEZONE_OFFSET"]
        tz = timezone(timedelta(hours=offset))
        now = datetime.now(tz)
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)

        try:
            ids = [cmd_msg_id]
            async for msg in message.client.iter_messages(chat_id, from_user="me"):
                if msg.id == cmd_msg_id:
                    continue
                msg_local = msg.date.astimezone(tz)
                if msg_local < midnight:
                    break
                ids.append(msg.id)

            deleted, failed = await self._bulk_delete(message.client, chat_id, ids)

            if failed:
                await message.client.send_message(chat_id, self.strings["no_perms"])
        except Exception as e:
            logger.error(f"[Deleter] del today error: {e}")
            await message.client.send_message(
                chat_id, self.strings["error"].format(error=str(e))
            )

    async def _delete_user(self, message: Message, username: str):
        try:
            entity = await message.client.get_entity(username)
            if not isinstance(entity, User):
                return await utils.answer(message, self.strings["no_user"])
        except Exception:
            return await utils.answer(message, self.strings["no_user"])

        await self._delete_user_by_entity(message, entity)

    async def _delete_user_by_entity(self, message: Message, entity):
        chat_id = message.chat_id
        cmd_msg_id = message.id

        try:
            ids = [cmd_msg_id]
            async for msg in message.client.iter_messages(chat_id, from_user=entity.id):
                ids.append(msg.id)

            deleted, failed = await self._bulk_delete(message.client, chat_id, ids)

            if failed:
                await message.client.send_message(chat_id, self.strings["no_perms"])
        except Exception as e:
            logger.error(f"[Deleter] del user error: {e}")
            await message.client.send_message(
                chat_id, self.strings["error"].format(error=str(e))
            )