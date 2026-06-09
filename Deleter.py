__version__ = (2, 2, 0)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/Deleter/MetaBanner.jpeg

import asyncio
import logging
import random
from datetime import timedelta, timezone, datetime

from telethon.tl.functions.channels import LeaveChannelRequest
from telethon.tl.types import Message, User

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

SAYONARA_URL = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/Deleter/Sayonara.mp4"


@loader.tds
class Deleter(loader.Module):
    """Swift deleting messages"""

    strings = {
        "name": "Deleter",
        "help": (
            "<b>Deleter - swift message deletion</b>\n"
            "<b>Own messages:</b>\n"
            "<blockquote>"
            "<code>{prefix}del me</code> - delete all your messages in current chat\n"
            "<code>{prefix}del me -f</code> - delete all your messages and leave the chat\n"
            "<code>{prefix}del [n]</code> - delete last n your messages\n"
            "<code>{prefix}del [n]</code> (reply) - delete n your messages before replied message\n"
            "<code>{prefix}del now</code> - delete your messages from last 5 minutes\n"
            "<code>{prefix}del today</code> - delete your messages since 00:00 today"
            "</blockquote>\n"
            "<b>Other users:</b>\n"
            "<blockquote>"
            "<code>{prefix}del @username</code> - delete all messages from specified user\n"
            "<code>{prefix}del before</code> (reply) - delete all messages from everyone before replied message\n"
            "<code>{prefix}del after</code> (reply) - delete all messages from everyone after replied message"
            "</blockquote>"
        ),
        "main_menu": (
            "<b>Deleter Control Panel</b>\n"
            "<blockquote>Select action:</blockquote>"
        ),
        "btn_usage": "Usage",
        "btn_purger": "Purger",
        "btn_del_today": "Delete All Today",
        "btn_back": "Back",
        "btn_confirm": "Confirm",
        "btn_cancel": "Cancel",
        "btn_stop": "Stop",
        "purger_menu": (
            "<b>Purger Mode</b>\n"
            "<blockquote>Delete all new messages in this chat automatically.</blockquote>"
        ),
        "btn_enable_purger": "Enable Auto-Delete",
        "btn_stop_all": "Stop All Active",
        "purger_confirm": (
            "<b>Are you sure?</b>\n"
            "<blockquote>This will delete all new messages in this chat automatically.</blockquote>"
        ),
        "purger_activated": (
            "<b>Purger Mode Activated</b>\n"
            "<blockquote>All new messages in this chat will be deleted.</blockquote>"
        ),
        "purger_stopped": (
            "<b>Purger Stopped</b>\n"
            "<blockquote>Auto-delete disabled for this chat.</blockquote>"
        ),
        "all_purgers_stopped": "<b>All Active Purgers Stopped</b>",
        "del_today_confirm": (
            "<b>Are you sure?</b>\n"
            "<blockquote>This will delete ALL messages from everyone in this chat since 00:00 today.</blockquote>"
        ),
        "del_today_progress": (
            "<b>Deleting messages...</b>\n"
            "<blockquote>Deleted: {count}</blockquote>"
        ),
        "del_today_done": (
            "<b>Done</b>\n"
            "<blockquote>Deleted {count} messages from today.</blockquote>"
        ),
        "no_count": "<b>Error:</b> Provide a valid number of messages",
        "no_reply": "<b>Error:</b> Reply to a message",
        "no_user": "<b>Error:</b> User not found",
        "no_perms": "<b>Error:</b> Not enough permissions to delete some messages",
        "error": "<b>Error:</b>\n<blockquote>{error}</blockquote>",
        "done_me": (
            "<b>All your {count} messages in chat</b>\n"
            "<blockquote>{chat}</blockquote>\n"
            "<b>have been deleted.</b>"
        ),
        "done_leave": (
            "<b>Left chat:</b>\n"
            "<blockquote>{chat}</blockquote>\n"
            "<b>Deleted messages:</b> <code>{count}</code>\n"
            "<b>First message date:</b> <code>{first_date}</code>\n"
            "<b>Last message date:</b> <code>{last_date}</code>"
        ),
    }

    strings_ru = {
        "help": (
            "<b>Deleter - быстрое удаление сообщений</b>\n"
            "<b>Свои сообщения:</b>\n"
            "<blockquote>"
            "<code>{prefix}del me</code> - удалить все свои сообщения в текущем чате\n"
            "<code>{prefix}del me -f</code> - удалить все свои сообщения и покинуть чат\n"
            "<code>{prefix}del [n]</code> - удалить последние n своих сообщений\n"
            "<code>{prefix}del [n]</code> (реплай) - удалить n своих сообщений до указанного сообщения\n"
            "<code>{prefix}del now</code> - удалить свои сообщения за последние 5 минут\n"
            "<code>{prefix}del today</code> - удалить свои сообщения с 00:00 сегодня"
            "</blockquote>\n"
            "<b>Другие пользователи:</b>\n"
            "<blockquote>"
            "<code>{prefix}del @username</code> - удалить все сообщения указанного пользователя\n"
            "<code>{prefix}del before</code> (реплай) - удалить все сообщения от всех до указанного сообщения\n"
            "<code>{prefix}del after</code> (реплай) - удалить все сообщения от всех после указанного сообщения"
            "</blockquote>"
        ),
        "main_menu": (
            "<b>Панель управления Deleter</b>\n"
            "<blockquote>Выберите действие:</blockquote>"
        ),
        "btn_usage": "Инструкция",
        "btn_purger": "Purger",
        "btn_del_today": "Удалить все за сегодня",
        "btn_back": "Назад",
        "btn_confirm": "Подтвердить",
        "btn_cancel": "Отмена",
        "btn_stop": "Остановить",
        "purger_menu": (
            "<b>Режим Purger</b>\n"
            "<blockquote>Автоматическое удаление всех новых сообщений в этом чате.</blockquote>"
        ),
        "btn_enable_purger": "Включить авто-удаление",
        "btn_stop_all": "Остановить все активные",
        "purger_confirm": (
            "<b>Вы уверены?</b>\n"
            "<blockquote>Все новые сообщения в этом чате будут автоматически удаляться.</blockquote>"
        ),
        "purger_activated": (
            "<b>Режим Purger активирован</b>\n"
            "<blockquote>Все новые сообщения в этом чате будут удалены.</blockquote>"
        ),
        "purger_stopped": (
            "<b>Purger остановлен</b>\n"
            "<blockquote>Авто-удаление отключено для этого чата.</blockquote>"
        ),
        "all_purgers_stopped": "<b>Все активные Purger'ы остановлены</b>",
        "del_today_confirm": (
            "<b>Вы уверены?</b>\n"
            "<blockquote>Будут удалены ВСЕ сообщения от всех в этом чате с 00:00 сегодня.</blockquote>"
        ),
        "del_today_progress": (
            "<b>Удаление сообщений...</b>\n"
            "<blockquote>Удалено: {count}</blockquote>"
        ),
        "del_today_done": (
            "<b>Готово</b>\n"
            "<blockquote>Удалено {count} сообщений за сегодня.</blockquote>"
        ),
        "no_count": "<b>Ошибка:</b> Укажите корректное количество сообщений",
        "no_reply": "<b>Ошибка:</b> Ответьте на сообщение",
        "no_user": "<b>Ошибка:</b> Пользователь не найден",
        "no_perms": "<b>Ошибка:</b> Недостаточно прав для удаления некоторых сообщений",
        "error": "<b>Ошибка:</b>\n<blockquote>{error}</blockquote>",
        "done_me": (
            "<b>Все ваши {count} сообщения в чате</b>\n"
            "<blockquote>{chat}</blockquote>\n"
            "<b>удалены.</b>"
        ),
        "done_leave": (
            "<b>Вышел из чата:</b>\n"
            "<blockquote>{chat}</blockquote>\n"
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
        self._purger_tasks = {}
        self._purger_chats = set()

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

    async def _cb_main_menu(self, call: InlineCall):
        await call.edit(
            self.strings["main_menu"],
            reply_markup=[
                [
                    {"text": self.strings["btn_usage"], "callback": self._cb_usage, "style": "primary"},
                ],
                [
                    {"text": self.strings["btn_purger"], "callback": self._cb_purger_menu, "style": "primary"},
                ],
                [
                    {"text": self.strings["btn_del_today"], "callback": self._cb_del_today_confirm, "style": "danger"},
                ],
            ],
        )

    async def _cb_usage(self, call: InlineCall):
        prefix = self.get_prefix()
        await call.edit(
            self.strings["help"].format(prefix=prefix),
            reply_markup=[
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}],
            ],
        )

    async def _cb_purger_menu(self, call: InlineCall):
        await call.edit(
            self.strings["purger_menu"],
            reply_markup=[
                [
                    {"text": self.strings["btn_enable_purger"], "callback": self._cb_purger_confirm, "style": "success"},
                ],
                [
                    {"text": self.strings["btn_stop_all"], "callback": self._cb_stop_all_purgers, "style": "danger"},
                ],
                [
                    {"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"},
                ],
            ],
        )

    async def _cb_purger_confirm(self, call: InlineCall):
        await call.edit(
            self.strings["purger_confirm"],
            reply_markup=[
                [
                    {"text": self.strings["btn_confirm"], "callback": self._cb_enable_purger, "style": "success"},
                    {"text": self.strings["btn_cancel"], "callback": self._cb_purger_menu, "style": "danger"},
                ],
            ],
        )

    async def _cb_enable_purger(self, call: InlineCall):
        chat_id = call.form["chat"]
        
        if chat_id in self._purger_chats:
            await call.answer("Purger already active in this chat", show_alert=True)
            return

        self._purger_chats.add(chat_id)
        
        task = asyncio.create_task(self._purger_worker(chat_id))
        self._purger_tasks[chat_id] = task

        await call.edit(
            self.strings["purger_activated"],
            reply_markup=[
                [
                    {"text": self.strings["btn_stop"], "callback": self._cb_stop_purger, "args": (chat_id,), "style": "danger"},
                ],
            ],
        )

    async def _cb_stop_purger(self, call: InlineCall, chat_id: int):
        if chat_id in self._purger_tasks:
            self._purger_tasks[chat_id].cancel()
            del self._purger_tasks[chat_id]
        
        if chat_id in self._purger_chats:
            self._purger_chats.remove(chat_id)

        await call.edit(
            self.strings["purger_stopped"],
            reply_markup=[
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}],
            ],
        )

    async def _cb_stop_all_purgers(self, call: InlineCall):
        for task in self._purger_tasks.values():
            task.cancel()
        
        self._purger_tasks.clear()
        self._purger_chats.clear()

        await call.edit(
            self.strings["all_purgers_stopped"],
            reply_markup=[
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}],
            ],
        )

    async def _purger_worker(self, chat_id: int):
        try:
            last_id = None
            
            async for msg in self._client.iter_messages(chat_id, limit=1):
                last_id = msg.id
                break

            while chat_id in self._purger_chats:
                await asyncio.sleep(2)
                
                try:
                    new_msgs = []
                    async for msg in self._client.iter_messages(chat_id, min_id=last_id or 0):
                        new_msgs.append(msg.id)
                        if msg.id > (last_id or 0):
                            last_id = msg.id

                    if new_msgs:
                        try:
                            await self._client.delete_messages(chat_id, new_msgs)
                        except Exception as e:
                            logger.error(f"[Deleter] Purger delete error: {e}")

                except Exception as e:
                    logger.error(f"[Deleter] Purger iter error: {e}")
                    await asyncio.sleep(5)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"[Deleter] Purger worker error: {e}")

    async def _cb_del_today_confirm(self, call: InlineCall):
        await call.edit(
            self.strings["del_today_confirm"],
            reply_markup=[
                [
                    {"text": self.strings["btn_confirm"], "callback": self._cb_del_today_execute, "style": "danger"},
                    {"text": self.strings["btn_cancel"], "callback": self._cb_main_menu, "style": "primary"},
                ],
            ],
        )

    async def _cb_del_today_execute(self, call: InlineCall):
        chat_id = call.form["chat"]
        
        offset = self.config["TIMEZONE_OFFSET"]
        tz = timezone(timedelta(hours=offset))
        now = datetime.now(tz)
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)

        await call.edit(self.strings["del_today_progress"].format(count=0))

        try:
            ids = []
            count = 0

            async for msg in self._client.iter_messages(chat_id):
                msg_local = msg.date.astimezone(tz)
                if msg_local < midnight:
                    break
                
                ids.append(msg.id)
                count += 1

                if count % 100 == 0:
                    await call.edit(self.strings["del_today_progress"].format(count=count))

            if ids:
                deleted, failed = await self._bulk_delete(self._client, chat_id, ids)

            await call.edit(
                self.strings["del_today_done"].format(count=count),
                reply_markup=[
                    [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}],
                ],
            )

        except Exception as e:
            logger.error(f"[Deleter] del today execute error: {e}")
            await call.edit(
                self.strings["error"].format(error=str(e)),
                reply_markup=[
                    [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "primary"}],
                ],
            )

    @loader.command(
        ru_doc="Быстрое удаление сообщений",
        en_doc="Swift message deletion",
    )
    async def delcmd(self, message: Message):
        """Swift message deletion"""
        args = utils.get_args_raw(message)
        args_list = args.split() if args else []

        if not args_list:
            await self.inline.form(
                text=self.strings["main_menu"],
                message=message,
                reply_markup=[
                    [
                        {"text": self.strings["btn_usage"], "callback": self._cb_usage, "style": "primary"},
                    ],
                    [
                        {"text": self.strings["btn_purger"], "callback": self._cb_purger_menu, "style": "primary"},
                    ],
                    [
                        {"text": self.strings["btn_del_today"], "callback": self._cb_del_today_confirm, "style": "danger"},
                    ],
                ],
                silent=True,
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
            
            await self.inline.form(
                text=self.strings["main_menu"],
                message=message,
                reply_markup=[
                    [
                        {"text": self.strings["btn_usage"], "callback": self._cb_usage, "style": "primary"},
                    ],
                    [
                        {"text": self.strings["btn_purger"], "callback": self._cb_purger_menu, "style": "primary"},
                    ],
                    [
                        {"text": self.strings["btn_del_today"], "callback": self._cb_del_today_confirm, "style": "danger"},
                    ],
                ],
                silent=True,
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

            total_count = len(ids)

            if not ids:
                await self._send_log(
                    self.strings["done_me"].format(chat=chat_name, count=0)
                )
                return

            deleted, failed = await self._bulk_delete(client, chat_id, ids)
            
            await self._send_log(
                self.strings["done_me"].format(chat=chat_name, count=total_count)
            )

        except Exception as e:
            logger.error(f"[Deleter] _delete_me_simple error: {e}")
            await self._send_log(self.strings["error"].format(error=str(e)))

    async def _delete_me_and_leave(self, client, chat_id, chat_name: str, cmd_msg_id: int):
        try:
            ids_to_delete = []
            first_date = None
            last_date = None

            async for msg in client.iter_messages(chat_id, from_user="me"):
                if msg.id == cmd_msg_id:
                    continue
                ids_to_delete.append(msg.id)
                if last_date is None:
                    last_date = msg.date
                first_date = msg.date

            count = len(ids_to_delete)

            try:
                sayonara_msg = await client.send_file(
                    chat_id,
                    SAYONARA_URL,
                )
                sayonara_msg_id = sayonara_msg.id
            except Exception as e:
                logger.error(f"[Deleter] Failed to send Sayonara: {e}")
                sayonara_msg_id = None

            if ids_to_delete:
                deleted, failed = await self._bulk_delete(client, chat_id, ids_to_delete)

            await self._leave_chat(client, chat_id)

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