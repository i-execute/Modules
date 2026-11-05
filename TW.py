import asyncio
import html
import logging

from telethon.errors import FloodWaitError
from telethon.tl.functions.messages import EditForumTopicRequest, EditMessageRequest, MarkDialogUnreadRequest
from telethon.tl.types import InputMediaWebPage, Message

from .. import loader, utils

__version__ = (1, 0, 0)

logger = logging.getLogger("TW")

TOPIC_ICON_ID = 5386654279341747455
RELOADING_MEDIA_URL = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/TW/Reloading.jpeg"
GREETING_MEDIA_URL = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/TW/Greetings.jpeg"


@loader.tds
class TW(loader.Module):
    strings = {
        "name": "TW",
        "_cls_doc": "Tag and private-message watcher with inline controls.",
        "_cmd_doc_tw": "Open Tag Watcher controls.",
        "_cfg_doc_enabled": "Enable Tag Watcher.",
        "_cfg_doc_notify_tags": "Send mention notifications.",
        "_cfg_doc_ignore_bots": "Ignore bot mentions.",
        "_cfg_doc_ignore_chats": "Chat IDs to ignore.",
        "_cfg_doc_blacklist_chats": "Chat IDs not to notify.",
        "_cfg_doc_ignore_users": "User IDs to ignore.",
        "_cfg_doc_blacklist_users": "User IDs not to notify.",
        "_cfg_doc_pm_autoread": "Auto-read private messages.",
        "_cfg_doc_pm_mark_unread": "Mark auto-read dialogs unread again.",
        "_cfg_doc_custom_notif_text": "Custom HTML. Fields: {title}, {chat_id}, {name}, {user_id}, {msg_content}, {reply_content}, {link}.",
        "panel": "<b>Tag Watcher</b>\n<blockquote>Status: {status}\nMode: {mode}\nMentions: {mentions}\nPrivate-message auto-read: {pm}\nMark unread again: {mark_unread}\nIgnore bots: {bots}\nTargets: {targets}</blockquote>",
        "state_on": "ON",
        "state_off": "OFF",
        "toggle_enabled": "Watcher",
        "toggle_notify_tags": "Mention notifications",
        "toggle_pm_autoread": "Private-message auto-read",
        "toggle_pm_mark_unread": "Mark dialog unread again",
        "toggle_ignore_bots": "Ignore bots",
        "mode_all_except": "Enabled everywhere except selected chats",
        "mode_only_selected": "Disabled everywhere except selected chats",
        "mode_all_except_button": "Enable everywhere except",
        "mode_only_selected_button": "Disable everywhere except",
        "input_target": "Enter a chat ID or username",
        "add_target": "Add chat",
        "remove_target": "Remove",
        "targets_empty": "None",
        "invalid_target": "<b>Invalid chat</b>\n<blockquote>Enter a numeric chat ID or a username.</blockquote>",
        "close": "Close",
        "greeting": "<blockquote><b>TW is ready</b></blockquote>\n<blockquote>Version: <b>{version}</b>\nStatus: <b>{status}</b>\nMode: <b>{mode}</b>\nMentions: <b>{mentions}</b>\nPrivate-message auto-read: <b>{pm}</b>\nMark unread again: <b>{mark_unread}</b>\nIgnore bots: <b>{bots}</b>\nTargets: <b>{target_count}</b>\nUse <code>{prefix}tw</code> to change settings.</blockquote>",
        "reloaded": "<blockquote><b>TW reloaded</b></blockquote>\n<blockquote>Version: <b>{version}</b>\nStatus: <b>{status}</b>\nMode: <b>{mode}</b>\nMentions: <b>{mentions}</b>\nPrivate-message auto-read: <b>{pm}</b>\nMark unread again: <b>{mark_unread}</b>\nIgnore bots: <b>{bots}</b>\nTargets: <b>{target_count}</b></blockquote>",
        "mentioned": "<b>You were mentioned in <code>{title}</code> [ <code>{chat_id}</code> ] by <a href='tg://user?id={user_id}'>{name}</a> [ <code>{user_id}</code> ]:</b>\n{reply_content}<b>Message content:</b> {msg_content}\n\n<a href='{link}'>Go to message</a>",
        "reply_content": "<b>Reply content:</b> {reply_content}\n",
        "no_message_content": "Empty message text",
        "topic_description": "Tag Watcher lifecycle messages and notifications.",
    }

    strings_ru = {
        "name": "TW",
        "_cls_doc": "Отслеживание упоминаний и личных сообщений.",
        "_cmd_doc_tw": "Open Tag Watcher controls.",
        "_cfg_doc_enabled": "Включить Tag Watcher.",
        "_cfg_doc_notify_tags": "Отправлять уведомления об упоминаниях.",
        "_cfg_doc_ignore_bots": "Игнорировать ботов.",
        "_cfg_doc_ignore_chats": "ID игнорируемых чатов.",
        "_cfg_doc_blacklist_chats": "ID чатов без уведомлений.",
        "_cfg_doc_ignore_users": "ID игнорируемых пользователей.",
        "_cfg_doc_blacklist_users": "ID пользователей без уведомлений.",
        "_cfg_doc_pm_autoread": "Автоматически читать личные сообщения.",
        "_cfg_doc_pm_mark_unread": "Повторно отмечать диалоги непрочитанными.",
        "_cfg_doc_custom_notif_text": "Пользовательский HTML. Поля: {title}, {chat_id}, {name}, {user_id}, {msg_content}, {reply_content}, {link}.",
        "panel": "<b>Tag Watcher</b>\n<blockquote>Статус: {status}\nРежим: {mode}\nУпоминания: {mentions}\nАвто-прочтение ЛС: {pm}\nПовторная отметка: {mark_unread}\nИгнорировать ботов: {bots}\nЧаты: {targets}</blockquote>",
        "state_on": "ВКЛ",
        "state_off": "ВЫКЛ",
        "toggle_enabled": "Модуль",
        "toggle_notify_tags": "Уведомления об упоминаниях",
        "toggle_pm_autoread": "Авто-прочтение ЛС",
        "toggle_pm_mark_unread": "Повторно отметить непрочитанным",
        "toggle_ignore_bots": "Игнорировать ботов",
        "mode_all_except": "Включен везде, кроме выбранных чатов",
        "mode_only_selected": "Выключен везде, кроме выбранных чатов",
        "mode_all_except_button": "Включить везде, кроме",
        "mode_only_selected_button": "Выключить везде, кроме",
        "input_target": "Введите ID чата или username",
        "add_target": "Добавить чат",
        "remove_target": "Удалить",
        "targets_empty": "Нет",
        "invalid_target": "<b>Неверный чат</b>\n<blockquote>Введите числовой ID или username.</blockquote>",
        "close": "Закрыть",
        "greeting": "<blockquote><b>TW готов к работе</b></blockquote>\n<blockquote>Версия: <b>{version}</b>\nСтатус: <b>{status}</b>\nРежим: <b>{mode}</b>\nУпоминания: <b>{mentions}</b>\nАвто-прочтение ЛС: <b>{pm}</b>\nПовторная отметка: <b>{mark_unread}</b>\nИгнорировать ботов: <b>{bots}</b>\nЧатов: <b>{target_count}</b>\nНастройки: <code>{prefix}tw</code></blockquote>",
        "reloaded": "<blockquote><b>TW перезагружен</b></blockquote>\n<blockquote>Версия: <b>{version}</b>\nСтатус: <b>{status}</b>\nРежим: <b>{mode}</b>\nУпоминания: <b>{mentions}</b>\nАвто-прочтение ЛС: <b>{pm}</b>\nПовторная отметка: <b>{mark_unread}</b>\nИгнорировать ботов: <b>{bots}</b>\nЧатов: <b>{target_count}</b></blockquote>",
        "mentioned": "<b>Вас упомянули в <code>{title}</code> [ <code>{chat_id}</code> ]: <a href='tg://user?id={user_id}'>{name}</a> [ <code>{user_id}</code> ]:</b>\n{reply_content}<b>Текст сообщения:</b> {msg_content}\n\n<a href='{link}'>Перейти к сообщению</a>",
        "reply_content": "<b>Ответ на сообщение:</b> {reply_content}\n",
        "no_message_content": "Пустой текст сообщения",
        "topic_description": "Сообщения о запуске и уведомления Tag Watcher.",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue("enabled", True, lambda: self.strings["_cfg_doc_enabled"], validator=loader.validators.Boolean()),
            loader.ConfigValue("notify_tags", True, lambda: self.strings["_cfg_doc_notify_tags"], validator=loader.validators.Boolean()),
            loader.ConfigValue("ignore_bots", True, lambda: self.strings["_cfg_doc_ignore_bots"], validator=loader.validators.Boolean()),
            loader.ConfigValue("ignore_chats", [], lambda: self.strings["_cfg_doc_ignore_chats"], validator=loader.validators.Series(validator=loader.validators.TelegramID())),
            loader.ConfigValue("blacklist_chats", [], lambda: self.strings["_cfg_doc_blacklist_chats"], validator=loader.validators.Series(validator=loader.validators.TelegramID())),
            loader.ConfigValue("ignore_users", [], lambda: self.strings["_cfg_doc_ignore_users"], validator=loader.validators.Series(validator=loader.validators.TelegramID())),
            loader.ConfigValue("blacklist_users", [], lambda: self.strings["_cfg_doc_blacklist_users"], validator=loader.validators.Series(validator=loader.validators.TelegramID())),
            loader.ConfigValue("pm_autoread", False, lambda: self.strings["_cfg_doc_pm_autoread"], validator=loader.validators.Boolean()),
            loader.ConfigValue("pm_mark_unread", False, lambda: self.strings["_cfg_doc_pm_mark_unread"], validator=loader.validators.Boolean()),
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
            "status": self.strings["state_on"] if self.config["enabled"] else self.strings["state_off"],
            "mode": self._mode_label(),
            "mentions": self.strings["state_on"] if self.config["notify_tags"] else self.strings["state_off"],
            "pm": self.strings["state_on"] if self.config["pm_autoread"] else self.strings["state_off"],
            "mark_unread": self.strings["state_on"] if self.config["pm_mark_unread"] else self.strings["state_off"],
            "bots": self.strings["state_on"] if self.config["ignore_bots"] else self.strings["state_off"],
            "target_count": len(self._scope_targets()),
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

    def _scope_mode(self):
        mode = self.get("scope_mode", "all_except")
        return mode if mode in {"all_except", "only_selected"} else "all_except"

    def _scope_targets(self):
        targets = self.get("scope_targets", [])
        if not isinstance(targets, list):
            return []
        return list(dict.fromkeys(str(item) for item in targets if isinstance(item, (str, int))))

    def _mode_label(self):
        return self.strings["mode_all_except" if self._scope_mode() == "all_except" else "mode_only_selected"]

    def _summary_text(self):
        status = self.strings["state_on"] if self.config["enabled"] else self.strings["state_off"]
        mentions = self.strings["state_on"] if self.config["notify_tags"] else self.strings["state_off"]
        pm = self.strings["state_on"] if self.config["pm_autoread"] else self.strings["state_off"]
        targets = self._scope_targets()
        target_text = ", ".join(html.escape(item, quote=True) for item in targets[:6])
        if len(targets) > 6:
            target_text += f" +{len(targets) - 6}"
        return self.strings["panel"].format(
            status=status,
            mode=self._mode_label(),
            mentions=mentions,
            pm=pm,
            mark_unread=self.strings["state_on"] if self.config["pm_mark_unread"] else self.strings["state_off"],
            bots=self.strings["state_on"] if self.config["ignore_bots"] else self.strings["state_off"],
            targets=target_text or self.strings["targets_empty"],
        )

    def _menu(self):
        options = (
            ("enabled", "toggle_enabled"),
            ("notify_tags", "toggle_notify_tags"),
            ("pm_autoread", "toggle_pm_autoread"),
            ("pm_mark_unread", "toggle_pm_mark_unread"),
            ("ignore_bots", "toggle_ignore_bots"),
        )
        markup = []
        for key, label_key in options:
            state = bool(self.config[key])
            label = self.strings["state_on"] if state else self.strings["state_off"]
            markup.append([{"text": f"[{label}] {self.strings[label_key]}", "callback": self._cb_toggle, "args": (key,), "style": "success" if state else "primary"}])
        mode = self._scope_mode()
        markup.append([
            {"text": self.strings["mode_all_except_button"], "callback": self._cb_set_mode, "args": ("all_except",), "style": "success" if mode == "all_except" else "primary"},
            {"text": self.strings["mode_only_selected_button"], "callback": self._cb_set_mode, "args": ("only_selected",), "style": "success" if mode == "only_selected" else "primary"},
        ])
        markup.append([{"text": self.strings["add_target"], "input": self.strings["input_target"], "handler": self._cb_add_target, "style": "primary"}])
        for target in self._scope_targets()[:8]:
            markup.append([{"text": f"{self.strings['remove_target']} {target}", "callback": self._cb_remove_target, "args": (target,), "style": "danger"}])
        markup.append([{"text": self.strings["close"], "action": "close", "style": "primary"}])
        return self._summary_text(), markup

    def _authorized_input(self, call):
        user = getattr(call, "from_user", None)
        user_id = getattr(user, "id", None) or getattr(call, "sender_id", None)
        return user_id == self._owner_id

    def _normalize_target(self, value):
        value = str(value or "").strip()
        if not value or any(char.isspace() for char in value):
            return None
        if value.startswith("@"):
            value = value[1:]
        if not value:
            return None
        if value.lstrip("-").isdigit():
            number = abs(int(value))
            text = str(number)
            if text.startswith("100") and len(text) > 12:
                text = text[3:]
            return text if number else None
        if value.isascii() and value[0].isalpha() and 5 <= len(value) <= 32 and all(char.isalnum() or char == "_" for char in value):
            return "@" + value.lower()
        return None

    def _target_matches(self, chat_id, chat=None):
        targets = set(self._scope_targets())
        if not targets:
            return False
        if str(abs(int(chat_id))) in targets:
            return True
        username = getattr(chat, "username", None)
        return bool(username and ("@" + username.lower()) in targets)

    def _active_for_chat(self, chat_id, chat=None):
        if not self.config["enabled"]:
            return False
        selected = self._target_matches(chat_id, chat)
        return not selected if self._scope_mode() == "all_except" else selected

    async def _edit_menu(self, call):
        text, markup = self._menu()
        await call.edit(text, reply_markup=markup)

    async def _cb_toggle(self, call, key):
        if not self._authorized_input(call):
            return
        allowed = {"enabled", "notify_tags", "pm_autoread", "pm_mark_unread", "ignore_bots"}
        if key not in allowed:
            return
        self.config[key] = not bool(self.config[key])
        await self._edit_menu(call)

    async def _cb_set_mode(self, call, mode):
        if not self._authorized_input(call) or mode not in {"all_except", "only_selected"}:
            return
        self.set("scope_mode", mode)
        await self._edit_menu(call)

    async def _cb_add_target(self, call, query):
        if not self._authorized_input(call):
            return
        target = self._normalize_target(query)
        if not target:
            await call.edit(self.strings["invalid_target"], reply_markup=[[{"text": self.strings["close"], "action": "close"}]])
            return
        targets = self._scope_targets()
        if target not in targets:
            targets.append(target)
            self.set("scope_targets", targets)
        await self._edit_menu(call)

    async def _cb_remove_target(self, call, target):
        if not self._authorized_input(call):
            return
        self.set("scope_targets", [item for item in self._scope_targets() if item != target])
        await self._edit_menu(call)

    @loader.command()
    async def twcmd(self, message: Message):
        text, markup = self._menu()
        await self.inline.form(text=text, message=message, reply_markup=markup, silent=True)

    @loader.watcher("only_pm")
    async def pm_reader(self, message: Message):
        if not self.config["pm_autoread"] or message.out:
            return
        chat = await message.get_chat()
        if (
            not chat
            or not self._active_for_chat(chat.id, chat)
            or getattr(chat, "bot", False)
            or chat.id in self.config["ignore_users"]
            or chat.id in self.config["ignore_chats"]
        ):
            return
        try:
            await self._client.send_read_acknowledge(chat.id, message, clear_mentions=True)
            if self.config["pm_mark_unread"]:
                peer = await self._client.get_input_entity(chat.id)
                await self._client(MarkDialogUnreadRequest(peer, True))
        except Exception:
            logger.exception("TW: failed to auto-read a private message")

    @loader.watcher("mention", "no_pm")
    async def inform(self, message: Message):
        if not self.config["notify_tags"]:
            return
        chat_id = utils.get_chat_id(message)
        chat = await message.get_chat()
        if not self._active_for_chat(chat_id, chat):
            return
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
            or (self.config["ignore_bots"] and getattr(sender, "bot", False))
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
        values = {
            "title": html.escape(str(title), quote=True),
            "chat_id": html.escape(str(utils.get_chat_id(message)), quote=True),
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
