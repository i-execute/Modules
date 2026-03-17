__version__ = (2, 0, 2)
# meta developer: FireJester.t.me

import asyncio
import logging
import random
from datetime import timedelta

from telethon.tl.types import Message, User

from .. import loader, utils

logger = logging.getLogger(__name__)


@loader.tds
class Deleter(loader.Module):
    """Swift deleting messages"""

    strings = {
        "name": "Deleter",
    }

    strings_en = {
        "help": (
            "<b>Deleter - swift message deletion</b>\n\n"
            "<b>Own messages:</b>\n"
            "<code>{prefix}del me</code> - delete all your messages in current chat\n"
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
        "error": "<b>Error:</b> {error}",
    }

    strings_ru = {
        "help": (
            "<b>Deleter - быстрое удаление сообщений</b>\n\n"
            "<b>Свои сообщения:</b>\n"
            "<code>{prefix}del me</code> - удалить все свои сообщения в текущем чате\n"
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
        "error": "<b>Ошибка:</b> {error}",
    }

    async def _bulk_delete(self, client, chat_id, msg_ids: list) -> tuple:
        deleted = 0
        failed = 0
        chunk = []
        chunk_size = random.randint(90, 110)

        for mid in msg_ids:
            chunk.append(mid)
            if len(chunk) >= chunk_size:
                try:
                    await client.delete_messages(chat_id, chunk)
                    deleted += len(chunk)
                except Exception:
                    failed += len(chunk)
                chunk.clear()
                chunk_size = random.randint(90, 110)
                await asyncio.sleep(random.uniform(0.5, 1.5))

        if chunk:
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
            await self._delete_me(message)
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

    async def _delete_me(self, message: Message):
        chat_id = message.chat_id
        await message.delete()

        try:
            ids = []
            async for msg in message.client.iter_messages(chat_id, from_user="me"):
                ids.append(msg.id)

            deleted, failed = await self._bulk_delete(message.client, chat_id, ids)

            if failed:
                await message.client.send_message(chat_id, self.strings["no_perms"])
        except Exception as e:
            logger.error(f"[Deleter] del me error: {e}")
            await message.client.send_message(
                chat_id, self.strings["error"].format(error=str(e))
            )

    async def _delete_own_n(self, message: Message, count: int):
        if count <= 0:
            return await utils.answer(message, self.strings["no_count"])

        chat_id = message.chat_id
        reply_id = message.reply_to_msg_id if message.is_reply else None
        await message.delete()

        try:
            ids = []
            kwargs = {"entity": chat_id, "from_user": "me"}
            if reply_id:
                kwargs["max_id"] = reply_id

            async for msg in message.client.iter_messages(**kwargs):
                ids.append(msg.id)
                if len(ids) >= count:
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
        await message.delete()

        try:
            ids = []
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
        await message.delete()

        try:
            ids = []
            async for msg in message.client.iter_messages(chat_id, min_id=reply_id):
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
        cmd_time = message.date
        cutoff = cmd_time - timedelta(minutes=5)
        await message.delete()

        try:
            ids = []
            async for msg in message.client.iter_messages(chat_id, from_user="me"):
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
        cmd_time = message.date
        midnight = cmd_time.replace(hour=0, minute=0, second=0, microsecond=0)
        await message.delete()

        try:
            ids = []
            async for msg in message.client.iter_messages(chat_id, from_user="me"):
                if msg.date < midnight:
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
        await message.delete()

        try:
            ids = []
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