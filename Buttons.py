__version__ = (1, 1, 0)
# meta developer: FireJester.t.me 

import contextlib
import re
from .. import loader, utils
from ..inline.types import InlineCall


_URL_PATTERN = re.compile(r'https?://[^\s<>"\']+', re.IGNORECASE)


@loader.tds
class Buttons(loader.Module):
    """Inline buttons constructor"""

    strings = {
        "name": "Buttons",
        "main_message": "Choose an action:",
        
        "media_set": "<b>Media set</b>\nType: <code>{}</code>\nURL: <code>{}</code>",
        "media_cleared": "<b>Media removed</b>",
        "media_invalid": "<b>Provide a media URL</b>",
        
        "rows_set": "<b>Number of rows:</b> {}",
        "rows_invalid": "<b>Specify a number from 1 to 10</b>",
        
        "btn_count_set": "<b>Row {}: {} buttons</b>",
        "btn_count_invalid": "<b>Specify row (1-10) and number of buttons (1-10)</b>",
        "row_not_exist": "<b>Row {} doesn't exist.</b> First create rows with <code>{prefix}buttons rows [1-10]</code>",
        
        "btn_text_set": "<b>Button [{},{}] is now:</b> <code>{}</code>",
        "btn_text_invalid": "<b>Specify row, button number and text</b>",
        "btn_not_exist": "<b>Button [{},{}] doesn't exist</b>",
        
        "response_set": "<b>Text for [{},{}] set</b>",
        "response_invalid": "<b>Specify row, button number and text</b>",
        
        "default_btn_name": "none",
        "default_response": "Text for button [{},{}] is not configured",
        
        "help_text": (
            "<b>Buttons - Inline buttons setup</b>\n\n"
            "<b>Sending:</b>\n"
            "<code>{prefix}send</code> - Send message with buttons\n\n"
            "<b>Media:</b>\n"
            "<code>{prefix}buttons media [url]</code> - Set photo/video/gif\n"
            "<code>{prefix}buttons media clear</code> - Remove media\n\n"
            "<b>Buttons:</b>\n"
            "<code>{prefix}buttons rows [1-10]</code> - Number of rows\n"
            "<code>{prefix}buttons count [row] [1-10]</code> - Buttons per row\n"
            "<code>{prefix}buttons name [row] [button] [text]</code> - Button name\n\n"
            "<b>Texts:</b>\n"
            "<code>{prefix}buttons response [row] [button] [text]</code> - Text on click\n"
            "<code>{prefix}buttons msg [text]</code> - Main message\n\n"
            "<b>Info:</b>\n"
            "<code>{prefix}buttons status</code> - Current settings\n\n"
            "<i>HTML markup is supported in texts</i>"
        ),
        
        "status_text": (
            "<b>Current settings</b>\n\n"
            "<b>Media:</b> {media}\n"
            "<b>Type:</b> {media_type}\n"
            "<b>Message:</b>\n<code>{message}</code>\n\n"
            "<b>Rows:</b> {rows}\n\n"
            "{buttons_info}"
        ),
        
        "status_no_media": "Not set",
        "status_no_buttons": "Buttons not configured",
        
        "message_set": "<b>Main message set</b>",
        "message_invalid": "<b>Provide message text</b>",
        
        "send_error": "<b>Send error:</b> <code>{}</code>",
        "send_no_media": "<b>Media unavailable, sent without it</b>",
    }

    strings_ru = {
        "main_message": "Выберите действие:",
        
        "media_set": "<b>Медиа установлено</b>\nТип: <code>{}</code>\nURL: <code>{}</code>",
        "media_cleared": "<b>Медиа удалено</b>",
        "media_invalid": "<b>Укажи ссылку на медиа</b>",
        
        "rows_set": "<b>Количество рядов:</b> {}",
        "rows_invalid": "<b>Укажи число от 1 до 10</b>",
        
        "btn_count_set": "<b>Ряд {}: {} кнопок</b>",
        "btn_count_invalid": "<b>Укажи ряд (1-10) и количество кнопок (1-10)</b>",
        "row_not_exist": "<b>Ряд {} не существует.</b> Сначала создай ряды командой <code>{prefix}buttons rows [1-10]</code>",
        
        "btn_text_set": "<b>Кнопка [{},{}] теперь:</b> <code>{}</code>",
        "btn_text_invalid": "<b>Укажи ряд, номер кнопки и текст</b>",
        "btn_not_exist": "<b>Кнопка [{},{}] не существует</b>",
        
        "response_set": "<b>Текст для [{},{}] установлен</b>",
        "response_invalid": "<b>Укажи ряд, номер кнопки и текст</b>",
        
        "default_btn_name": "none",
        "default_response": "Текст для кнопки [{},{}] не настроен",
        
        "help_text": (
            "<b>Buttons - Настройка инлайн кнопок</b>\n\n"
            "<b>Отправка:</b>\n"
            "<code>{prefix}send</code> - Отправить сообщение с кнопками\n\n"
            "<b>Медиа:</b>\n"
            "<code>{prefix}buttons media [url]</code> - Установить фото/видео/гифку\n"
            "<code>{prefix}buttons media clear</code> - Удалить медиа\n\n"
            "<b>Кнопки:</b>\n"
            "<code>{prefix}buttons rows [1-10]</code> - Количество рядов\n"
            "<code>{prefix}buttons count [ряд] [1-10]</code> - Кнопок в ряду\n"
            "<code>{prefix}buttons name [ряд] [кнопка] [текст]</code> - Имя кнопки\n\n"
            "<b>Тексты:</b>\n"
            "<code>{prefix}buttons response [ряд] [кнопка] [текст]</code> - Текст при нажатии\n"
            "<code>{prefix}buttons msg [текст]</code> - Главное сообщение\n\n"
            "<b>Инфо:</b>\n"
            "<code>{prefix}buttons status</code> - Текущие настройки\n\n"
            "<i>Поддерживается HTML разметка в текстах</i>"
        ),
        
        "status_text": (
            "<b>Текущие настройки</b>\n\n"
            "<b>Медиа:</b> {media}\n"
            "<b>Тип:</b> {media_type}\n"
            "<b>Сообщение:</b>\n<code>{message}</code>\n\n"
            "<b>Рядов:</b> {rows}\n\n"
            "{buttons_info}"
        ),
        
        "status_no_media": "Не установлено",
        "status_no_buttons": "Кнопки не настроены",
        
        "message_set": "<b>Главное сообщение установлено</b>",
        "message_invalid": "<b>Укажи текст сообщения</b>",
        
        "send_error": "<b>Ошибка отправки:</b> <code>{}</code>",
        "send_no_media": "<b>Медиа недоступно, отправлено без него</b>",
    }

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._load_config()
        self._build_markup()

    def _load_config(self):
        self._media = self._db.get("InlineButtons", "media", None)
        self._media_type = self._db.get("InlineButtons", "media_type", None)
        self._rows = self._db.get("InlineButtons", "rows", 1)
        self._buttons = self._db.get("InlineButtons", "buttons", self._default_buttons())
        self._message = self._db.get("InlineButtons", "message", self.strings["main_message"])

    def _save_config(self):
        self._db.set("InlineButtons", "media", self._media)
        self._db.set("InlineButtons", "media_type", self._media_type)
        self._db.set("InlineButtons", "rows", self._rows)
        self._db.set("InlineButtons", "buttons", self._buttons)
        self._db.set("InlineButtons", "message", self._message)
        self._build_markup()

    def _default_buttons(self) -> dict:
        return {
            "1": {
                "count": 1,
                "items": {
                    "1": {"name": "none", "text": ""}
                }
            }
        }

    def _detect_media_type(self, url: str) -> str:
        url_lower = url.lower()
        
        video_indicators = ['.mp4', '.webm', '.mov', '.avi', '.mkv']
        for indicator in video_indicators:
            if indicator in url_lower:
                return "video"
        
        gif_indicators = ['.gif', 'giphy.com', 'tenor.com', 'gfycat.com']
        for indicator in gif_indicators:
            if indicator in url_lower:
                return "gif"
        
        return "photo"

    def _extract_url(self, text: str) -> str:
        if not text:
            return None
        match = _URL_PATTERN.search(text)
        return match.group(0) if match else None

    def _ensure_row(self, row: int):
        row_str = str(row)
        if row_str not in self._buttons:
            self._buttons[row_str] = {
                "count": 1,
                "items": {
                    "1": {"name": "none", "text": ""}
                }
            }

    def _ensure_button(self, row: int, col: int):
        row_str = str(row)
        col_str = str(col)
        
        self._ensure_row(row)
        
        if col_str not in self._buttons[row_str]["items"]:
            self._buttons[row_str]["items"][col_str] = {"name": "none", "text": ""}

    def _build_markup(self):
        markup = []
        
        for row_num in range(1, self._rows + 1):
            row_str = str(row_num)
            
            if row_str not in self._buttons:
                self._ensure_row(row_num)
            
            row_data = self._buttons[row_str]
            row_buttons = []
            
            btn_count = row_data.get("count", 1)
            
            for col_num in range(1, btn_count + 1):
                col_str = str(col_num)
                
                if col_str not in row_data["items"]:
                    self._ensure_button(row_num, col_num)
                
                btn_data = row_data["items"].get(col_str, {"name": "none", "text": ""})
                btn_name = btn_data.get("name", "none")
                
                row_buttons.append({
                    "text": btn_name,
                    "callback": self._inline__button_click,
                    "args": (row_num, col_num),
                })
            
            if row_buttons:
                markup.append(row_buttons)
        
        if not markup:
            markup = [[{
                "text": "none",
                "callback": self._inline__button_click,
                "args": (1, 1),
            }]]
        
        self._markup = markup

    async def _inline__button_click(self, call: InlineCall, row: int, col: int):
        row_str = str(row)
        col_str = str(col)
        
        response = self.strings["default_response"].format(row, col)
        
        if row_str in self._buttons:
            row_data = self._buttons[row_str]
            if col_str in row_data.get("items", {}):
                btn_text = row_data["items"][col_str].get("text", "")
                if btn_text:
                    response = btn_text
        
        with contextlib.suppress(Exception):
            await call.edit(
                text=response,
                reply_markup=self._markup,
            )

    async def _get_text_from_message(self, message, skip_words: int = 0) -> str:
        if message.is_reply:
            reply = await message.get_reply_message()
            if reply and reply.raw_text:
                return reply.raw_text
        
        raw_text = message.raw_text
        if not raw_text:
            return None
        
        parts = raw_text.split(maxsplit=skip_words)
        if len(parts) > skip_words:
            return parts[skip_words]
        return None

    @loader.command(
        ru_doc="Отправить сообщение с инлайн кнопками",
        en_doc="Send message with inline buttons",
    )
    async def send(self, message):
        """Send message with inline buttons"""
        
        base_kwargs = {
            "message": message,
            "text": self._message,
            "reply_markup": self._markup,
            "disable_security": True,
        }
        
        if not self._media:
            try:
                await self.inline.form(**base_kwargs)
                return
            except Exception as e:
                await utils.answer(message, self.strings["send_error"].format(str(e)[:100]))
                return
        
        sent = False
        
        if self._media_type == "video":
            try:
                await self.inline.form(**base_kwargs, video=self._media)
                sent = True
            except Exception:
                pass
        
        if not sent and self._media_type == "gif":
            try:
                await self.inline.form(**base_kwargs, gif=self._media)
                sent = True
            except Exception:
                pass
        
        if not sent:
            try:
                await self.inline.form(**base_kwargs, photo=self._media)
                sent = True
            except Exception:
                pass
        
        if not sent:
            try:
                await self.inline.form(**base_kwargs, gif=self._media)
                sent = True
            except Exception:
                pass
        
        if not sent:
            try:
                await self.inline.form(**base_kwargs, video=self._media)
                sent = True
            except Exception:
                pass
        
        if not sent:
            try:
                await self.inline.form(**base_kwargs)
                sent = True
                await message.respond(self.strings["send_no_media"])
            except Exception as e:
                await utils.answer(message, self.strings["send_error"].format(str(e)[:100]))

    @loader.command(
        ru_doc="Настройка инлайн кнопок",
        en_doc="Inline buttons configuration",
    )
    async def buttons(self, message):
        """Inline buttons configuration"""
        args = utils.get_args_raw(message).strip()
        prefix = self.get_prefix()
        
        if not args:
            await utils.answer(
                message,
                self.strings["help_text"].format(prefix=prefix),
            )
            return
        
        parts = args.split()
        cmd = parts[0].lower()
        
        if cmd == "status":
            await self._show_status(message)
        
        elif cmd == "media":
            await self._handle_media(message, parts)
        
        elif cmd == "msg":
            await self._handle_message(message, args)
        
        elif cmd == "rows":
            await self._handle_rows(message, parts)
        
        elif cmd == "count":
            await self._handle_count(message, parts)
        
        elif cmd == "name":
            await self._handle_name(message, parts)
        
        elif cmd == "response":
            await self._handle_response(message, parts)
        
        else:
            await utils.answer(
                message,
                self.strings["help_text"].format(prefix=prefix),
            )

    async def _handle_media(self, message, parts):
        url = None
        
        if message.is_reply:
            reply = await message.get_reply_message()
            if reply and reply.raw_text:
                url = self._extract_url(reply.raw_text)
        
        if not url and len(parts) >= 2:
            if parts[1].lower() == "clear":
                self._media = None
                self._media_type = None
                self._save_config()
                await utils.answer(message, self.strings["media_cleared"])
                return
            
            url = self._extract_url(" ".join(parts[1:]))
        
        if not url or not url.startswith(("http://", "https://")):
            await utils.answer(message, self.strings["media_invalid"])
            return
        
        media_type = self._detect_media_type(url)
        
        self._media = url
        self._media_type = media_type
        self._save_config()
        
        short_url = url[:50] + "..." if len(url) > 50 else url
        await utils.answer(message, self.strings["media_set"].format(media_type, short_url))

    async def _handle_message(self, message, args):
        text = await self._get_text_from_message(message, 2)
        
        if not text:
            idx = args.find("msg") + len("msg")
            text = args[idx:].strip() if idx < len(args) else None
        
        if not text:
            await utils.answer(message, self.strings["message_invalid"])
            return
            
        self._message = text
        self._save_config()
        await utils.answer(message, self.strings["message_set"])

    async def _handle_rows(self, message, parts):
        if len(parts) < 2:
            await utils.answer(message, self.strings["rows_invalid"])
            return
        
        try:
            rows = int(parts[1])
            if rows < 1 or rows > 10:
                raise ValueError
        except ValueError:
            await utils.answer(message, self.strings["rows_invalid"])
            return
        
        self._rows = rows
        
        for r in range(1, rows + 1):
            self._ensure_row(r)
        
        self._save_config()
        await utils.answer(message, self.strings["rows_set"].format(rows))

    async def _handle_count(self, message, parts):
        if len(parts) < 3:
            await utils.answer(message, self.strings["btn_count_invalid"])
            return
        
        try:
            row = int(parts[1])
            count = int(parts[2])
            
            if row < 1 or row > 10 or count < 1 or count > 10:
                raise ValueError
        except ValueError:
            await utils.answer(message, self.strings["btn_count_invalid"])
            return
        
        if row > self._rows:
            await utils.answer(
                message,
                self.strings["row_not_exist"].format(row, prefix=self.get_prefix()),
            )
            return
        
        row_str = str(row)
        self._ensure_row(row)
        self._buttons[row_str]["count"] = count
        
        for c in range(1, count + 1):
            self._ensure_button(row, c)
        
        self._save_config()
        await utils.answer(message, self.strings["btn_count_set"].format(row, count))

    async def _handle_name(self, message, parts):
        if len(parts) < 4:
            await utils.answer(message, self.strings["btn_text_invalid"])
            return
        
        try:
            row = int(parts[1])
            col = int(parts[2])
        except ValueError:
            await utils.answer(message, self.strings["btn_text_invalid"])
            return
        
        row_str = str(row)
        col_str = str(col)
        
        if row > self._rows:
            await utils.answer(
                message,
                self.strings["row_not_exist"].format(row, prefix=self.get_prefix()),
            )
            return
        
        if row_str not in self._buttons:
            await utils.answer(message, self.strings["btn_not_exist"].format(row, col))
            return
        
        btn_count = self._buttons[row_str].get("count", 1)
        if col > btn_count or col < 1:
            await utils.answer(message, self.strings["btn_not_exist"].format(row, col))
            return
        
        text = " ".join(parts[3:])
        
        if not text:
            await utils.answer(message, self.strings["btn_text_invalid"])
            return
        
        self._ensure_button(row, col)
        self._buttons[row_str]["items"][col_str]["name"] = text
        self._save_config()
        
        await utils.answer(message, self.strings["btn_text_set"].format(row, col, text))

    async def _handle_response(self, message, parts):
        if len(parts) < 3:
            await utils.answer(message, self.strings["response_invalid"])
            return
        
        try:
            row = int(parts[1])
            col = int(parts[2])
        except ValueError:
            await utils.answer(message, self.strings["response_invalid"])
            return
        
        row_str = str(row)
        col_str = str(col)
        
        if row > self._rows:
            await utils.answer(
                message,
                self.strings["row_not_exist"].format(row, prefix=self.get_prefix()),
            )
            return
        
        if row_str not in self._buttons:
            await utils.answer(message, self.strings["btn_not_exist"].format(row, col))
            return
        
        btn_count = self._buttons[row_str].get("count", 1)
        if col > btn_count or col < 1:
            await utils.answer(message, self.strings["btn_not_exist"].format(row, col))
            return
        
        text = await self._get_text_from_message(message, 4)
        if not text:
            text = " ".join(parts[3:]) if len(parts) > 3 else None
        
        if not text:
            await utils.answer(message, self.strings["response_invalid"])
            return
        
        self._ensure_button(row, col)
        self._buttons[row_str]["items"][col_str]["text"] = text
        self._save_config()
        
        await utils.answer(message, self.strings["response_set"].format(row, col))

    async def _show_status(self, message):
        if self._media:
            media_preview = self._media[:40] + "..." if len(self._media) > 40 else self._media
            media_status = f"<code>{media_preview}</code>"
        else:
            media_status = self.strings["status_no_media"]
        
        media_type_status = self._media_type.upper() if self._media_type else "-"
        
        buttons_info = []
        for row_num in range(1, self._rows + 1):
            row_str = str(row_num)
            if row_str in self._buttons:
                row_data = self._buttons[row_str]
                btn_count = row_data.get("count", 1)
                
                btn_names = []
                for col_num in range(1, btn_count + 1):
                    col_str = str(col_num)
                    if col_str in row_data.get("items", {}):
                        name = row_data["items"][col_str].get("name", "none")
                        has_text = "+" if row_data["items"][col_str].get("text") else "-"
                        btn_names.append(f"[{name}]{has_text}")
                    else:
                        btn_names.append("[none]-")
                
                buttons_info.append(f"<b>Ряд {row_num}:</b> {' '.join(btn_names)}")
        
        buttons_text = "\n".join(buttons_info) if buttons_info else self.strings["status_no_buttons"]
        
        msg_preview = self._message[:80] + "..." if len(self._message) > 80 else self._message
        
        await utils.answer(message, self.strings["status_text"].format(
            media=media_status,
            media_type=media_type_status,
            message=msg_preview,
            rows=self._rows,
            buttons_info=buttons_text,
        ))