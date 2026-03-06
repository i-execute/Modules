__version__ = (1, 0, 0)
# meta developer: FireJester.t.me

import asyncio
import logging

from telethon.tl.types import Message, User

from .. import loader, utils

logger = logging.getLogger(__name__)


@loader.tds
class Deleter(loader.Module):
    """Swift deleting messages"""
    strings = {
        "name": "Deleter",
        "line": "--------------------",
        "help": (
            "<b>Deleter</b>\n\n"
            "<code>.delete [n]</code> - delete last n your messages\n"
            "<code>.delete all [n]</code> - delete last n messages from everyone\n"
            "<code>.delete from</code> - reply, delete all messages before replied message\n"
            "<code>.delete @username</code> - delete all messages from user\n"
        ),
        "deleted": "<b>Deleted {count} messages</b>",
        "no_count": "<b>Error:</b> Provide number of messages",
        "no_reply": "<b>Error:</b> Reply to a message",
        "no_user": "<b>Error:</b> User not found",
        "error": "<b>Error:</b> {error}",
    }

    @loader.command(
        ru_doc="- instruction for module",
        en_doc="- instruction for module",
    )
    async def delete(self, message: Message):
        args = utils.get_args_raw(message)
        args_list = args.split() if args else []

        if not args_list:
            await utils.answer(message, self.strings["help"])
            return

        cmd = args_list[0].lower()

        if cmd == "all":
            await self._delete_all(message, args_list)
        elif cmd == "from":
            await self._delete_from(message)
        elif cmd.startswith("@"):
            await self._delete_user(message, cmd)
        elif cmd.isdigit():
            await self._delete_own(message, int(cmd))
        else:
            try:
                entity = await message.client.get_entity(cmd)
                if isinstance(entity, User):
                    await self._delete_user_by_entity(message, entity)
                    return
            except Exception:
                pass
            await utils.answer(message, self.strings["help"])

    async def _delete_own(self, message: Message, count: int):
        if count <= 0:
            return await utils.answer(message, self.strings["no_count"])

        await message.delete()

        deleted = 0
        chunk = []

        async for msg in message.client.iter_messages(
            message.chat_id,
            from_user="me",
        ):
            if deleted >= count:
                break

            chunk.append(msg.id)
            deleted += 1

            if len(chunk) >= 99:
                await message.client.delete_messages(message.chat_id, chunk)
                chunk.clear()
                await asyncio.sleep(1)

        if chunk:
            await message.client.delete_messages(message.chat_id, chunk)

    async def _delete_all(self, message: Message, args_list: list):
        if len(args_list) < 2 or not args_list[1].isdigit():
            return await utils.answer(message, self.strings["no_count"])

        count = int(args_list[1])
        if count <= 0:
            return await utils.answer(message, self.strings["no_count"])

        await message.delete()

        try:
            deleted = 0
            chunk = []

            async for msg in message.client.iter_messages(message.chat_id):
                if deleted >= count:
                    break

                chunk.append(msg.id)
                deleted += 1

                if len(chunk) >= 99:
                    await message.client.delete_messages(message.chat_id, chunk)
                    chunk.clear()
                    await asyncio.sleep(1)

            if chunk:
                await message.client.delete_messages(message.chat_id, chunk)

        except Exception as e:
            logger.error(f"[Deleter] delete all error: {e}")
            await message.client.send_message(
                message.chat_id,
                self.strings["error"].format(error=str(e)),
            )

    async def _delete_from(self, message: Message):
        if not message.is_reply:
            return await utils.answer(message, self.strings["no_reply"])

        reply_id = message.reply_to_msg_id
        await message.delete()

        try:
            chunk = []

            async for msg in message.client.iter_messages(
                message.chat_id,
                max_id=reply_id,
            ):
                chunk.append(msg.id)

                if len(chunk) >= 99:
                    await message.client.delete_messages(message.chat_id, chunk)
                    chunk.clear()
                    await asyncio.sleep(1)

            if chunk:
                await message.client.delete_messages(message.chat_id, chunk)

        except Exception as e:
            logger.error(f"[Deleter] delete from error: {e}")
            await message.client.send_message(
                message.chat_id,
                self.strings["error"].format(error=str(e)),
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
        await message.delete()

        try:
            chunk = []

            async for msg in message.client.iter_messages(
                message.chat_id,
                from_user=entity.id,
            ):
                chunk.append(msg.id)

                if len(chunk) >= 99:
                    await message.client.delete_messages(message.chat_id, chunk)
                    chunk.clear()
                    await asyncio.sleep(1)

            if chunk:
                await message.client.delete_messages(message.chat_id, chunk)

        except Exception as e:
            logger.error(f"[Deleter] delete user error: {e}")
            await message.client.send_message(
                message.chat_id,
                self.strings["error"].format(error=str(e)),
            )