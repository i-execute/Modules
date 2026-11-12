# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (1, 5, 0)
# meta developer: Execute_forge.t.me

import asyncio
import html
import logging
import re

from telethon.tl.functions.messages import EditForumTopicRequest, EditMessageRequest
from telethon.tl.types import InputMediaWebPage, Message

from .. import loader, utils

logger = logging.getLogger("TW")

TOPIC_ICON_ID = 5386654279341747455
RELOADING_MEDIA_URL = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/TW/Reloading.jpeg"
GREETING_MEDIA_URL = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/TW/Greetings.jpeg"
TAG_RE = re.compile(r"<[^>]+>")


@loader.tds
class TW(loader.Module):
    strings = {
        "name": "TW",
        "_cls_doc": "Tag watcher and private-message auto-read with inline controls",
        "_cmd_doc_tw": "Open Tag Watcher controls",
        "_cfg_doc_notify_tags": "Send mention notifications",
        "_cfg_doc_pm_autoread": "Auto-read private messages",
        "_cfg_doc_ignore_chats": "Chat IDs to ignore",
        "_cfg_doc_blacklist_chats": "Chat IDs not to notify",
        "_cfg_doc_ignore_users": "User IDs to ignore",
        "_cfg_doc_blacklist_users": "User IDs not to notify",
        "_cfg_doc_custom_notif_text": "Custom HTML. Fields: {title}, {chat_id}, {name}, {user_id}, {msg_content}, {reply_content}, {link}",
        "panel": (
            "<b>Tag Watcher</b> <code>v{version}</code>\n"
            "<blockquote>"
            "<b>Mentions:</b> {mentions}\n"
            "<b>Auto-read DMs:</b> {pm}</blockquote>"
        ),
        "on_label": "ON",
        "off_label": "OFF",
        "toggle_notify_tags": "Tag watcher",
        "toggle_pm_autoread": "Auto-read DMs",
        "trans_on": "<b>Turning ON: {what}</b>\n<blockquote>Please wait...</blockquote>",
        "trans_off": "<b>Turning OFF: {what}</b>\n<blockquote>Please wait...</blockquote>",
        "btn_close": "Close",
        "greeting": (
            "<blockquote><b>TW is ready</b></blockquote>\n"
            "<blockquote>"
            "<b>Version:</b> {version}\n"
            "<b>Mentions:</b> {mentions}\n"
            "<b>Auto-read DMs:</b> {pm}\n"
            "<b>Settings:</b> <code>{prefix}tw</code></blockquote>"
        ),
        "reloaded": (
            "<blockquote><b>TW reloaded</b></blockquote>\n"
            "<blockquote>"
            "<b>Version:</b> {version}\n"
            "<b>Mentions:</b> {mentions}\n"
            "<b>Auto-read DMs:</b> {pm}</blockquote>"
        ),
        "log_header": "LOG",
        "log_toggle": "<b>Switch</b>: {name} -> {state}",
        "mentioned": (
            "<blockquote><b>MENTION</b>\n"
            "<b>From:</b> <a href='tg://user?id={user_id}'>{name}</a> [<code>{user_id}</code>]\n"
            "<b>In chat:</b> {title} [<code>{chat_id}</code>]{chat_uname}\n"
            "{reply_content}"
            "<b>Text:</b> {msg_content}\n"
            "<a href='{link}'>Open message</a></blockquote>"
        ),
        "reply_content": "<b>Reply on:</b> {reply_content}\n",
        "no_message_content": "Empty message text",
        "topic_description": "Tag Watcher lifecycle messages, event log and notifications",
    }

    strings_ru = {
        "name": "TW",
        "_cls_doc": "Уведомления об упоминаниях и авто-прочтение личных сообщений",
        "_cmd_doc_tw": "Открыть панель управления Tag Watcher",
        "_cfg_doc_notify_tags": "Отправлять уведомления об упоминаниях",
        "_cfg_doc_pm_autoread": "Автоматически читать личные сообщения",
        "_cfg_doc_ignore_chats": "ID игнорируемых чатов",
        "_cfg_doc_blacklist_chats": "ID чатов без уведомлений",
        "_cfg_doc_ignore_users": "ID игнорируемых пользователей",
        "_cfg_doc_blacklist_users": "ID пользователей без уведомлений",
        "_cfg_doc_custom_notif_text": "Пользовательский HTML. Поля: {title}, {chat_id}, {name}, {user_id}, {msg_content}, {reply_content}, {link}",
        "panel": (
            "<b>Tag Watcher</b> <code>v{version}</code>\n"
            "<blockquote>"
            "<b>Упоминания:</b> {mentions}\n"
            "<b>Авто-прочтение ЛС:</b> {pm}</blockquote>"
        ),
        "on_label": "ВКЛ",
        "off_label": "ВЫКЛ",
        "toggle_notify_tags": "Тэгвотчер",
        "toggle_pm_autoread": "Авторид ЛС",
        "trans_on": "<b>Включаю: {what}</b>\n<blockquote>Подождите...</blockquote>",
        "trans_off": "<b>Выключаю: {what}</b>\n<blockquote>Подождите...</blockquote>",
        "btn_close": "Закрыть",
        "greeting": (
            "<blockquote><b>TW готов к работе</b></blockquote>\n"
            "<blockquote>"
            "<b>Версия:</b> {version}\n"
            "<b>Упоминания:</b> {mentions}\n"
            "<b>Авто-прочтение ЛС:</b> {pm}\n"
            "<b>Настройки:</b> <code>{prefix}tw</code></blockquote>"
        ),
        "reloaded": (
            "<blockquote><b>TW перезагружен</b></blockquote>\n"
            "<blockquote>"
            "<b>Версия:</b> {version}\n"
            "<b>Упоминания:</b> {mentions}\n"
            "<b>Авто-прочтение ЛС:</b> {pm}</blockquote>"
        ),
        "log_header": "ЛОГ",
        "log_toggle": "<b>Переключатель</b>: {name} -> {state}",
        "mentioned": (
            "<blockquote><b>ВАС УПОМЯНУЛИ</b>\n"
            "<b>От кого:</b> <a href='tg://user?id={user_id}'>{name}</a> [<code>{user_id}</code>]\n"
            "<b>В чате:</b> {title} [<code>{chat_id}</code>]{chat_uname}\n"
            "{reply_content}"
            "<b>Текст:</b> {msg_content}\n"
            "<a href='{link}'>Открыть сообщение</a></blockquote>"
        ),
        "reply_content": "<b>Ответ на:</b> {reply_content}\n",
        "no_message_content": "Пустой текст сообщения",
        "topic_description": "Сообщения о запуске, журнал событий и уведомления Tag Watcher",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue("notify_tags", True, lambda: self.strings["_cfg_doc_notify_tags"], validator=loader.validators.Boolean()),
            loader.ConfigValue("pm_autoread", False, lambda: self.strings["_cfg_doc_pm_autoread"], validator=loader.validators.Boolean()),
            loader.ConfigValue("ignore_chats", [], lambda: self.strings["_cfg_doc_ignore_chats"], validator=loader.validators.Series(validator=loader.validators.TelegramID())),
            loader.ConfigValue("blacklist_chats", [], lambda: self.strings["_cfg_doc_blacklist_chats"], validator=loader.validators.Series(validator=loader.validators.TelegramID())),
            loader.ConfigValue("ignore_users", [], lambda: self.strings["_cfg_doc_ignore_users"], validator=loader.validators.Series(validator=loader.validators.TelegramID())),
            loader.ConfigValue("blacklist_users", [], lambda: self.strings["_cfg_doc_blacklist_users"], validator=loader.validators.Series(validator=loader.validators.TelegramID())),
            loader.ConfigValue("custom_notif_text", None, lambda: self.strings["_cfg_doc_custom_notif_text"], validator=loader.validators.Union(loader.validators.String(), loader.validators.NoneType())),
        )
        self._asset_channel = None
        self._notif_topic = None
        self._owner_id = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        owner = await client.get_me()
        self._owner_id = getattr(owner, "id", None)
        self._asset_channel = db.get("feroku.forums", "channel_id", None) or db.get("heroku.forums", "channel_id", None)
        if not self._asset_channel:
            logger.warning("TW: Feroku content channel is not configured; lifecycle topic is unavailable")
            return
        try:
            self._notif_topic = await utils.asset_forum_topic(
                client,
                db,
                self._asset_channel,
                "TW",
                description=self.strings["topic_description"],
                icon_emoji_id=TOPIC_ICON_ID,
            )
            await self._apply_topic_icon()
        except Exception:
            logger.exception("TW: failed to create or find the notification topic")
            return
        chat_id = self._asset_chat_id()
        greeted_key = f"tw_greeted_{self._asset_channel}_{self._notif_topic.id}"
        values = {
            "version": ".".join(map(str, __version__)),
            "prefix": utils.escape_html(self.get_prefix()),
            "mentions": self._state("notify_tags"),
            "pm": self._state("pm_autoread"),
        }
        if self.get(greeted_key, False):
            await self._send_with_preview(chat_id, self.strings["reloaded"].format(**values))
        else:
            self.set(greeted_key, True)
            await self._send_with_preview(chat_id, self.strings["greeting"].format(**values), is_greeting=True)

    def _asset_entity_id(self):
        value = str(abs(int(self._asset_channel)))
        if value.startswith("100") and len(value) > 12:
            return int(value[3:])
        return int(value)

    def _asset_chat_id(self):
        return int(f"-100{self._asset_entity_id()}")

    async def _apply_topic_icon(self):
        if not self._notif_topic or getattr(self._notif_topic, "icon_emoji_id", None) == TOPIC_ICON_ID:
            return
        try:
            await self._client(EditForumTopicRequest(peer=self._asset_channel, topic_id=self._notif_topic.id, icon_emoji_id=TOPIC_ICON_ID))
        except Exception:
            logger.debug("TW: could not update the topic icon", exc_info=True)

    async def _refresh_topic(self):
        self._notif_topic = await utils.asset_forum_topic(
            self._client,
            self._db,
            self._asset_channel,
            "TW",
            description=self.strings["topic_description"],
            icon_emoji_id=TOPIC_ICON_ID,
        )
        await self._apply_topic_icon()

    async def _send_with_preview(self, chat_id, text, is_greeting=False):
        if not self._notif_topic or not getattr(self.inline, "bot", None):
            return
        bot = self.inline.bot.client
        try:
            parsed_text, entities = await bot._parse_message_text(text, "html")
            try:
                message = await bot.send_message(
                    chat_id,
                    parsed_text,
                    formatting_entities=entities,
                    reply_to=self._notif_topic.id,
                    link_preview=False,
                )
            except Exception:
                await self._refresh_topic()
                message = await bot.send_message(
                    chat_id,
                    parsed_text,
                    formatting_entities=entities,
                    reply_to=self._notif_topic.id,
                    link_preview=False,
                )
            try:
                peer = await bot.get_input_entity(chat_id)
                current = await bot.get_messages(chat_id, ids=message.id)
                await bot(EditMessageRequest(
                    peer=peer,
                    id=message.id,
                    message=parsed_text,
                    media=InputMediaWebPage(
                        url=GREETING_MEDIA_URL if is_greeting else RELOADING_MEDIA_URL,
                        optional=True,
                        force_large_media=True,
                    ),
                    invert_media=True,
                    reply_markup=current.reply_markup if current else None,
                    entities=entities,
                    no_webpage=False,
                ))
            except Exception:
                logger.exception("TW: failed to attach the lifecycle image")
        except Exception:
            logger.exception("TW: failed to send a lifecycle message")

    def _state(self, key):
        return self.strings["on_label"] if self.config[key] else self.strings["off_label"]

    def _panel_text(self):
        return self.strings["panel"].format(
            mentions=self._state("notify_tags"),
            pm=self._state("pm_autoread"),
            version=".".join(map(str, __version__)),
        )

    def _toggle_button(self, key, label_key):
        state = bool(self.config[key])
        return {
            "text": f"{self.strings[label_key]}: {self.strings['on_label'] if state else self.strings['off_label']}",
            "callback": self._cb_toggle,
            "args": (key,),
            "style": "success" if state else "danger",
        }

    def _main_markup(self):
        return [
            [self._toggle_button("notify_tags", "toggle_notify_tags")],
            [self._toggle_button("pm_autoread", "toggle_pm_autoread")],
            [{"text": self.strings["btn_close"], "action": "close", "style": "primary"}],
        ]

    def _log_safe(self, raw):
        text = html.escape(str(raw), quote=True)
        text = text.lstrip("@")
        text = re.sub(r"https?://\S+", lambda m: m.group(0).split("//", 1)[1], text)
        return text

    async def _log_event(self, event_text):
        logger.info(f"TW: {TAG_RE.sub('', event_text)}")
        if not self._asset_channel or not self._notif_topic or not getattr(self.inline, "bot", None):
            return
        try:
            text = f"<blockquote><b>{self.strings['log_header']}</b>\n{event_text}</blockquote>"
            await self.inline.bot.send_message(
                self._asset_chat_id(),
                text,
                parse_mode="HTML",
                disable_web_page_preview=True,
                disable_notification=True,
                message_thread_id=self._notif_topic.id,
            )
        except Exception:
            logger.debug("TW: failed to write the event log", exc_info=True)

    def _authorized_input(self, call):
        user = getattr(call, "from_user", None)
        user_id = getattr(user, "id", None) or getattr(call, "sender_id", None)
        return user_id == self._owner_id

    async def _cb_toggle(self, call, key):
        if not self._authorized_input(call):
            return
        allowed = {
            "notify_tags": "toggle_notify_tags",
            "pm_autoread": "toggle_pm_autoread",
        }
        if key not in allowed:
            return
        turning_on = not bool(self.config[key])
        what = self.strings[allowed[key]]
        await call.edit((self.strings["trans_on"] if turning_on else self.strings["trans_off"]).format(what=what))
        await asyncio.sleep(0.8)
        self.config[key] = turning_on
        await call.edit(self._panel_text(), reply_markup=self._main_markup())
        await self._log_event(
            self.strings["log_toggle"].format(
                name=self._log_safe(what),
                state=self.strings["on_label"] if turning_on else self.strings["off_label"],
            )
        )

    @loader.command(ru_doc="Открыть панель управления Tag Watcher", en_doc="Open Tag Watcher controls")
    async def twcmd(self, message: Message):
        """Open Tag Watcher controls"""
        await self.inline.form(text=self._panel_text(), message=message, reply_markup=self._main_markup(), silent=True)

    @loader.watcher("only_pm")
    async def pm_reader(self, message: Message):
        if not self.config["pm_autoread"] or message.out:
            return
        chat = await message.get_chat()
        if (
            not chat
            or getattr(chat, "bot", False)
            or chat.id in self.config["ignore_users"]
            or chat.id in self.config["ignore_chats"]
        ):
            return
        try:
            await self._client.send_read_acknowledge(chat.id, message, clear_mentions=True)
        except Exception:
            logger.exception("TW: failed to auto-read a private message")

    @loader.watcher("mention", "no_pm")
    async def inform(self, message: Message):
        if not self.config["notify_tags"]:
            return
        chat_id = utils.get_chat_id(message)
        chat = await message.get_chat()
        sender = await utils.get_user(message)
        sender_id = getattr(sender, "id", None)
        if chat_id in self.config["ignore_chats"] or sender_id in self.config["ignore_users"]:
            return
        try:
            await self._client.send_read_acknowledge(message.chat_id, message, clear_mentions=True)
        except Exception:
            logger.debug("TW: failed to acknowledge a mention", exc_info=True)
        if (
            not sender
            or chat_id in self.config["blacklist_chats"]
            or sender_id in self.config["blacklist_users"]
            or (self._notif_topic and chat_id == self._asset_entity_id() and getattr(message, "reply_to_msg_id", None) == self._notif_topic.id)
        ):
            return
        if not self._asset_channel or not self._notif_topic or not getattr(self.inline, "bot", None):
            return
        try:
            text = await self._render_notification(message, sender)
            await self.inline.bot.send_message(
                self._asset_chat_id(),
                text,
                parse_mode="HTML",
                disable_web_page_preview=True,
                message_thread_id=self._notif_topic.id,
            )
        except Exception:
            logger.exception("TW: failed to send a mention notification")

    async def _render_notification(self, message, sender):
        chat = await message.get_chat()
        title = getattr(chat, "title", None) or getattr(chat, "first_name", None) or "Unknown chat"
        name = " ".join(filter(None, (getattr(sender, "first_name", None), getattr(sender, "last_name", None)))) or getattr(sender, "title", None) or "Unknown"
        content = message.message or self.strings["no_message_content"]
        reply_content = ""
        if message.is_reply:
            reply = await message.get_reply_message()
            if reply:
                reply_content = reply.message or self.strings["no_message_content"]
        text = self.config["custom_notif_text"] or self.strings["mentioned"]
        sender_uname = getattr(sender, "username", None)
        chat_uname = getattr(chat, "username", None)
        values = {
            "title": html.escape(str(title), quote=True),
            "chat_id": html.escape(str(utils.get_chat_id(message)), quote=True),
            "chat_uname": f" [<code>{html.escape(str(chat_uname), quote=True)}</code>]" if chat_uname else "",
            "name": html.escape(str(name), quote=True),
            "user_id": int(sender.id),
            "msg_content": html.escape(str(content), quote=True),
            "reply_content": self.strings["reply_content"].format(reply_content=html.escape(str(reply_content), quote=True)) if reply_content else "",
            "link": html.escape(self._message_link(message, chat), quote=True),
        }
        try:
            return text.format(**values)
        except (KeyError, ValueError, IndexError):
            logger.warning("TW: invalid custom notification template; using the default")
            return self.strings["mentioned"].format(**values)

    def _message_link(self, message, chat):
        username = getattr(chat, "username", None)
        message_id = getattr(message, "id", None)
        reply_to = getattr(message, "reply_to", None)
        topic_id = None
        if reply_to and getattr(reply_to, "forum_topic", False):
            topic_id = getattr(reply_to, "reply_to_top_id", None) or getattr(reply_to, "reply_to_msg_id", None)
        if username:
            if topic_id:
                return f"https://t.me/{username}/{topic_id}/{message_id}"
            return f"https://t.me/{username}/{message_id}"
        raw_id = str(abs(int(utils.get_chat_id(message))))
        if raw_id.startswith("100") and len(raw_id) > 12:
            raw_id = raw_id[3:]
        if topic_id:
            return f"https://t.me/c/{raw_id}/{topic_id}/{message_id}"
        return f"https://t.me/c/{raw_id}/{message_id}"
