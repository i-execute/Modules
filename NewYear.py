__version__ = (2, 1, 1)
# meta developer: I_execute.t.me 
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/NewYear/MetaBanner.jpeg

from .. import loader, utils
from datetime import datetime, timezone, timedelta
from telethon.types import InputMediaWebPage
from telethon.errors import WebpageMediaEmptyError

@loader.tds
class NewYear(loader.Module):
    """New Year countdown timer"""

    strings = {
        "name": "NewYear",
        "prem_help_text": (
            "<tg-emoji emoji-id=\"5251548158831400680\">☃️</tg-emoji><b> New Year module commands:</b>\n"
            "<blockquote expandable>"
            "<b>{prefix}new year</b> - show time until New Year\n"
            "<b>{prefix}new set [timezone]</b> - set timezone (from -12 to +12)\n"
            "<b>{prefix}new add [direct link/reply to media]</b> - add media file to send with {prefix}new year\n"
            "<b>{prefix}new remove</b> - remove media file\n\n"
            "<b>Examples:</b>\n"
            "<code>{prefix}new set 3</code> - set UTC+3 (Moscow)\n"
            "<code>{prefix}new set -5</code> - set UTC-5 (New York)\n"
            "<code>{prefix}new add https://example.com/New_Year.gif</code> - add a gif"
            "</blockquote>"
        ),
        "noprem_help_text": (
            "<b>New Year module commands:</b>\n"
            "<blockquote expandable>"
            "<b>{prefix}new year</b> - show time until New Year\n"
            "<b>{prefix}new set [timezone]</b> - set timezone (from -12 to +12)\n"
            "<b>{prefix}new add [direct link/reply to media]</b> - add media file to send with {prefix}new year\n"
            "<b>{prefix}new remove</b> - remove media file\n\n"
            "<b>Examples:</b>\n"
            "<code>{prefix}new set 3</code> - set UTC+3 (Moscow)\n"
            "<code>{prefix}new set -5</code> - set UTC-5 (New York)\n"
            "<code>{prefix}new add https://example.com/New_Year.gif</code> - add a gif"
            "</blockquote>"
        ),
        "prem_new_year_template": (
            "<blockquote>"
            "<tg-emoji emoji-id=\"5251721344797678028\">🎩</tg-emoji><b> Time until New Year {year}:</b>"
            "</blockquote>\n"
            "<blockquote>"
            "<tg-emoji emoji-id=\"5251534981871736729\">🎆</tg-emoji><b> Days: </b><code>{days}</code>\n"
            "<tg-emoji emoji-id=\"5251210071890762657\">🕯</tg-emoji><b> Hours: </b><code>{hours}</code>\n"
            "<tg-emoji emoji-id=\"5251413099289811996\">🫐</tg-emoji><b> Minutes: </b><code>{minutes}</code>\n"
            "<tg-emoji emoji-id=\"5253679386028187387\">🎄</tg-emoji><b> Seconds: </b><code>{seconds}</code>"
            "</blockquote>\n"
            "<blockquote>"
            "<tg-emoji emoji-id=\"5251354202403282317\">🍪</tg-emoji><b> Timezone: </b>UTC{timezone_str}\n"
            "<tg-emoji emoji-id=\"5251362057898466040\">☕️</tg-emoji><b> Current time: </b>{current_time}"
            "</blockquote>"
        ),
        "noprem_new_year_template": (
            "<blockquote>"
            "<b>Time until New Year {year}:</b>"
            "</blockquote>\n"
            "<blockquote>"
            "<b>Days: </b><code>{days}</code>\n"
            "<b>Hours: </b><code>{hours}</code>\n"
            "<b>Minutes: </b><code>{minutes}</code>\n"
            "<b>Seconds: </b><code>{seconds}</code>"
            "</blockquote>\n"
            "<blockquote>"
            "<b>Timezone: </b>UTC{timezone_str}\n"
            "<b>Current time: </b>{current_time}"
            "</blockquote>"
        ),
        "prem_timezone_set": "<tg-emoji emoji-id=\"5251215414830078979\">👌</tg-emoji><b> Timezone set: </b>UTC{timezone_str}",
        "noprem_timezone_set": "<b>Timezone set: </b>UTC{timezone_str}",
        "prem_media_added": (
            "<tg-emoji emoji-id=\"5251215414830078979\">👌</tg-emoji><b> Media file added!</b>\n"
            "<blockquote>It will now be sent with the {prefix}new year command</blockquote>"
        ),
        "noprem_media_added": (
            "<b>Media file added!</b>\n"
            "<blockquote>It will now be sent with the {prefix}new year command</blockquote>"
        ),
        "prem_invalid_timezone": (
            "<tg-emoji emoji-id=\"5251663040616635665\">😵</tg-emoji><b> Invalid timezone!</b>\n"
            "<blockquote>Use a number from -12 to +12</blockquote>"
        ),
        "noprem_invalid_timezone": (
            "<b>Invalid timezone!</b>\n"
            "<blockquote>Use a number from -12 to +12</blockquote>"
        ),
        "prem_invalid_media": (
            "<tg-emoji emoji-id=\"5251663040616635665\">😵</tg-emoji><b> Provide a media link or reply to a media file!</b>\n"
            "<blockquote>Example: {prefix}new add https://example.com/image.gif</blockquote>"
        ),
        "noprem_invalid_media": (
            "<b>Provide a media link or reply to a media file!</b>\n"
            "<blockquote>Example: {prefix}new add https://example.com/image.gif</blockquote>"
        ),
        "prem_media_deleted_error": (
            "<tg-emoji emoji-id=\"5251663040616635665\">😵</tg-emoji><b> Media file is no longer available!</b>\n"
            "<blockquote>Add a new media file with {prefix}new add</blockquote>\n"
            "<blockquote>This message only appears on error, for the module to work correctly, your media value in cfg has just been reset</blockquote>"
        ),
        "noprem_media_deleted_error": (
            "<b>Media file is no longer available!</b>\n"
            "<blockquote>Add a new media file with {prefix}new add</blockquote>\n"
            "<blockquote>This message only appears on error, for the module to work correctly, your media value in cfg has just been reset</blockquote>"
        ),
        "prem_no_media_in_reply": (
            "<tg-emoji emoji-id=\"5251663040616635665\">😵</tg-emoji><b> No media in reply!</b>\n"
            "<blockquote>Reply to a photo, video or GIF</blockquote>"
        ),
        "noprem_no_media_in_reply": (
            "<b>No media in reply!</b>\n"
            "<blockquote>Reply to a photo, video or GIF</blockquote>"
        ),
        "prem_media_load_error": (
            "<tg-emoji emoji-id=\"5251663040616635665\">😵</tg-emoji><b> Failed to load media!</b>\n"
            "<blockquote>Check the link or try another file</blockquote>"
        ),
        "noprem_media_load_error": (
            "<b>Failed to load media!</b>\n"
            "<blockquote>Check the link or try another file</blockquote>"
        ),
        "prem_media_preview_error": (
            "<tg-emoji emoji-id=\"5251663040616635665\">😵</tg-emoji><b> Failed to load media preview from this link!</b>\n"
            "<blockquote>The link may not support previews. Try another link or reply to a media file directly</blockquote>"
        ),
        "noprem_media_preview_error": (
            "<b>Failed to load media preview from this link!</b>\n"
            "<blockquote>The link may not support previews. Try another link or reply to a media file directly</blockquote>"
        ),
        "prem_media_removed": "<tg-emoji emoji-id=\"5251720043422585661\">❌</tg-emoji><b> Media file removed!</b>",
        "noprem_media_removed": "<b>Media file removed!</b>",
        "prem_saved_caption": "<tg-emoji emoji-id=\"5251350293983041557\">🪟</tg-emoji><b> Do not delete - media for the New Year module</b>",
        "noprem_saved_caption": "<b>Do not delete - media for the New Year module</b>",
    }

    strings_ru = {
        "prem_help_text": (
            "<tg-emoji emoji-id=\"5251548158831400680\">☃️</tg-emoji><b> Команды модуля New Year:</b>\n"
            "<blockquote expandable>"
            "<b>{prefix}new year</b> - показать время до Нового года\n"
            "<b>{prefix}new set [часовой пояс]</b> - установить часовой пояс (от -12 до +12)\n"
            "<b>{prefix}new add [прямая ссылка/реплай на медиа]</b> - добавить медиафайл для отправки с {prefix}new year\n"
            "<b>{prefix}new remove</b> - удалить медиафайл\n\n"
            "<b>Примеры:</b>\n"
            "<code>{prefix}new set 3</code> - установить UTC+3 (Москва)\n"
            "<code>{prefix}new set -5</code> - установить UTC-5 (Нью-Йорк)\n"
            "<code>{prefix}new add https://example.com/New_Year.gif</code> - добавить гифку"
            "</blockquote>"
        ),
        "noprem_help_text": (
            "<b>Команды модуля New Year:</b>\n"
            "<blockquote expandable>"
            "<b>{prefix}new year</b> - показать время до Нового года\n"
            "<b>{prefix}new set [часовой пояс]</b> - установить часовой пояс (от -12 до +12)\n"
            "<b>{prefix}new add [прямая ссылка/реплай на медиа]</b> - добавить медиафайл для отправки с {prefix}new year\n"
            "<b>{prefix}new remove</b> - удалить медиафайл\n\n"
            "<b>Примеры:</b>\n"
            "<code>{prefix}new set 3</code> - установить UTC+3 (Москва)\n"
            "<code>{prefix}new set -5</code> - установить UTC-5 (Нью-Йорк)\n"
            "<code>{prefix}new add https://example.com/New_Year.gif</code> - добавить гифку"
            "</blockquote>"
        ),
        "prem_new_year_template": (
            "<blockquote>"
            "<tg-emoji emoji-id=\"5251721344797678028\">🎩</tg-emoji><b> До Нового {year} года осталось:</b>"
            "</blockquote>\n"
            "<blockquote>"
            "<tg-emoji emoji-id=\"5251534981871736729\">🎆</tg-emoji><b> Дней: </b><code>{days}</code>\n"
            "<tg-emoji emoji-id=\"5251210071890762657\">🕯</tg-emoji><b> Часов: </b><code>{hours}</code>\n"
            "<tg-emoji emoji-id=\"5251413099289811996\">🫐</tg-emoji><b> Минут: </b><code>{minutes}</code>\n"
            "<tg-emoji emoji-id=\"5253679386028187387\">🎄</tg-emoji><b> Секунд: </b><code>{seconds}</code>"
            "</blockquote>\n"
            "<blockquote>"
            "<tg-emoji emoji-id=\"5251354202403282317\">🍪</tg-emoji><b> Часовой пояс: </b>UTC{timezone_str}\n"
            "<tg-emoji emoji-id=\"5251362057898466040\">☕️</tg-emoji><b> Текущее время: </b>{current_time}"
            "</blockquote>"
        ),
        "noprem_new_year_template": (
            "<blockquote>"
            "<b>До Нового {year} года осталось:</b>"
            "</blockquote>\n"
            "<blockquote>"
            "<b>Дней: </b><code>{days}</code>\n"
            "<b>Часов: </b><code>{hours}</code>\n"
            "<b>Минут: </b><code>{minutes}</code>\n"
            "<b>Секунд: </b><code>{seconds}</code>"
            "</blockquote>\n"
            "<blockquote>"
            "<b>Часовой пояс: </b>UTC{timezone_str}\n"
            "<b>Текущее время: </b>{current_time}"
            "</blockquote>"
        ),
        "prem_timezone_set": "<tg-emoji emoji-id=\"5251215414830078979\">👌</tg-emoji><b> Часовой пояс установлен: </b>UTC{timezone_str}",
        "noprem_timezone_set": "<b>Часовой пояс установлен: </b>UTC{timezone_str}",
        "prem_media_added": (
            "<tg-emoji emoji-id=\"5251215414830078979\">👌</tg-emoji><b> Медиафайл добавлен!</b>\n"
            "<blockquote>Теперь он будет отправляться с командой {prefix}new year</blockquote>"
        ),
        "noprem_media_added": (
            "<b>Медиафайл добавлен!</b>\n"
            "<blockquote>Теперь он будет отправляться с командой {prefix}new year</blockquote>"
        ),
        "prem_invalid_timezone": (
            "<tg-emoji emoji-id=\"5251663040616635665\">😵</tg-emoji><b> Неверный часовой пояс!</b>\n"
            "<blockquote>Используйте число от -12 до +12</blockquote>"
        ),
        "noprem_invalid_timezone": (
            "<b>Неверный часовой пояс!</b>\n"
            "<blockquote>Используйте число от -12 до +12</blockquote>"
        ),
        "prem_invalid_media": (
            "<tg-emoji emoji-id=\"5251663040616635665\">😵</tg-emoji><b> Укажите ссылку на медиафайл или ответьте этой командой на него!</b>\n"
            "<blockquote>Пример: {prefix}new add https://example.com/image.gif</blockquote>"
        ),
        "noprem_invalid_media": (
            "<b>Укажите ссылку на медиафайл или ответьте этой командой на него!</b>\n"
            "<blockquote>Пример: {prefix}new add https://example.com/image.gif</blockquote>"
        ),
        "prem_media_deleted_error": (
            "<tg-emoji emoji-id=\"5251663040616635665\">😵</tg-emoji><b> Медиафайл больше не доступен!</b>\n"
            "<blockquote>Добавьте новый медиафайл командой {prefix}new add</blockquote>\n"
            "<blockquote>Это сообщение появляется только при ошибке, для корректной работы модуля, значение вашего медиафайла в cfg только что было сброшено</blockquote>"
        ),
        "noprem_media_deleted_error": (
            "<b>Медиафайл больше не доступен!</b>\n"
            "<blockquote>Добавьте новый медиафайл командой {prefix}new add</blockquote>\n"
            "<blockquote>Это сообщение появляется только при ошибке, для корректной работы модуля, значение вашего медиафайла в cfg только что было сброшено</blockquote>"
        ),
        "prem_no_media_in_reply": (
            "<tg-emoji emoji-id=\"5251663040616635665\">😵</tg-emoji><b> В реплае нет медиа!</b>\n"
            "<blockquote>Ответьте на фото, видео или GIF</blockquote>"
        ),
        "noprem_no_media_in_reply": (
            "<b>В реплае нет медиа!</b>\n"
            "<blockquote>Ответьте на фото, видео или GIF</blockquote>"
        ),
        "prem_media_load_error": (
            "<tg-emoji emoji-id=\"5251663040616635665\">😵</tg-emoji><b> Не удалось загрузить медиа!</b>\n"
            "<blockquote>Проверьте ссылку или попробуйте другой файл</blockquote>"
        ),
        "noprem_media_load_error": (
            "<b>Не удалось загрузить медиа!</b>\n"
            "<blockquote>Проверьте ссылку или попробуйте другой файл</blockquote>"
        ),
        "prem_media_preview_error": (
            "<tg-emoji emoji-id=\"5251663040616635665\">😵</tg-emoji><b> Не удалось загрузить превью медиа по этой ссылке!</b>\n"
            "<blockquote>Возможно, ссылка не поддерживает превью. Попробуйте другую ссылку или ответьте на медиафайл напрямую</blockquote>"
        ),
        "noprem_media_preview_error": (
            "<b>Не удалось загрузить превью медиа по этой ссылке!</b>\n"
            "<blockquote>Возможно, ссылка не поддерживает превью. Попробуйте другую ссылку или ответьте на медиафайл напрямую</blockquote>"
        ),
        "prem_media_removed": "<tg-emoji emoji-id=\"5251720043422585661\">❌</tg-emoji><b> Медиафайл удален!</b>",
        "noprem_media_removed": "<b>Медиафайл удален!</b>",
        "prem_saved_caption": "<tg-emoji emoji-id=\"5251350293983041557\">🪟</tg-emoji><b> Не удалять - медиа для модуля New Year</b>",
        "noprem_saved_caption": "<b>Не удалять - медиа для модуля New Year</b>",
    }

    def __init__(self):
        self._premium_status = None
        self.config = loader.ModuleConfig(
            "TIMEZONE_OFFSET", 3, "смещение часового пояса от UTC",
            "MEDIA_URL", "", "ссылка на файл",
            "SAVED_MSG_ID", 0, "ID сохраненного сообщения в избранном"
        )

    async def _check_premium(self):
        if self._premium_status is None:
            me = await self.client.get_me()
            self._premium_status = getattr(me, "premium", False)
        return self._premium_status

    def _get_string(self, key, is_premium):
        prefix = "prem" if is_premium else "noprem"
        return self.strings(f"{prefix}_{key}")

    def get_timezone_str(self, offset):
        if offset >= 0:
            return f"+{offset}"
        return str(offset)

    def get_time_until_new_year(self):
        offset = self.config.get("TIMEZONE_OFFSET", 3)
        tz = timezone(timedelta(hours=offset))
        now = datetime.now(tz)
        current_year = now.year
        new_year = datetime(current_year + 1, 1, 1, 0, 0, 0, tzinfo=tz)
        next_year = current_year + 1
        time_diff = new_year - now
        return {
            "days": time_diff.days,
            "hours": time_diff.seconds // 3600,
            "minutes": (time_diff.seconds % 3600) // 60,
            "seconds": time_diff.seconds % 60,
            "current_time": now.strftime("%d.%m.%Y %H:%M:%S"),
            "year": next_year,
            "timezone_str": self.get_timezone_str(offset)
        }

    async def get_saved_media(self):
        try:
            msg_id = self.config.get("SAVED_MSG_ID", 0)
            if msg_id:
                saved_msg = await self.client.get_messages("me", ids=msg_id)
                if saved_msg and saved_msg.media:
                    return saved_msg.media
        except:
            pass
        return None

    @loader.command(
        ru_doc="Управление модулем New Year",
        en_doc="New Year module management",
    )
    async def new(self, message):
        """New Year module management"""
        args = utils.get_args_raw(message)
        prefix = self.get_prefix()
        is_premium = await self._check_premium()

        if not args:
            await utils.answer(
                message,
                self._get_string("help_text", is_premium).format(prefix=prefix),
            )
            return

        args_list = args.split()
        cmd = args_list[0].lower()

        if cmd == "year":
            time_data = self.get_time_until_new_year()
            msg = self._get_string("new_year_template", is_premium).format(
                year=time_data["year"],
                days=time_data["days"],
                hours=time_data["hours"],
                minutes=time_data["minutes"],
                seconds=time_data["seconds"],
                timezone_str=time_data["timezone_str"],
                current_time=time_data["current_time"]
            )
            media_url = self.config.get("MEDIA_URL", "")
            if media_url:
                if media_url.startswith("saved:"):
                    saved_media = await self.get_saved_media()
                    if saved_media:
                        try:
                            await utils.answer(
                                message, msg,
                                file=saved_media,
                                invert_media=True,
                            )
                        except:
                            await utils.answer(
                                message,
                                self._get_string("media_deleted_error", is_premium).format(prefix=prefix),
                            )
                    else:
                        await utils.answer(
                            message,
                            self._get_string("media_deleted_error", is_premium).format(prefix=prefix),
                        )
                        self.config["MEDIA_URL"] = ""
                        self.config["SAVED_MSG_ID"] = 0
                else:
                    try:
                        media = InputMediaWebPage(url=media_url, optional=True)
                        await utils.answer(
                            message, msg,
                            file=media,
                            invert_media=True,
                        )
                    except WebpageMediaEmptyError:
                        await utils.answer(
                            message,
                            self._get_string("media_preview_error", is_premium),
                        )
                    except:
                        await utils.answer(message, msg)
            else:
                await utils.answer(message, msg)

        elif cmd == "set":
            if len(args_list) < 2:
                await utils.answer(
                    message,
                    self._get_string("invalid_timezone", is_premium),
                )
                return
            try:
                timezone_str = args_list[1].replace('+', '')
                timezone_offset = int(timezone_str)
                if not -12 <= timezone_offset <= 12:
                    await utils.answer(
                        message,
                        self._get_string("invalid_timezone", is_premium),
                    )
                    return
                self.config["TIMEZONE_OFFSET"] = timezone_offset
                msg = self._get_string("timezone_set", is_premium).format(
                    timezone_str=self.get_timezone_str(timezone_offset)
                )
                await utils.answer(message, msg)
            except (ValueError, IndexError):
                await utils.answer(
                    message,
                    self._get_string("invalid_timezone", is_premium),
                )

        elif cmd == "add":
            reply = await message.get_reply_message()
            if reply:
                if not reply.media:
                    await utils.answer(
                        message,
                        self._get_string("no_media_in_reply", is_premium),
                    )
                    return
                is_valid_media = False
                if reply.photo or reply.video or reply.gif:
                    is_valid_media = True
                elif reply.document:
                    if reply.document.mime_type and any(x in reply.document.mime_type.lower()
                                                        for x in ['image', 'video', 'gif']):
                        is_valid_media = True
                if not is_valid_media:
                    await utils.answer(
                        message,
                        self._get_string("no_media_in_reply", is_premium),
                    )
                    return
                try:
                    old_msg_id = self.config.get("SAVED_MSG_ID", 0)
                    if old_msg_id:
                        try:
                            old_msg = await self.client.get_messages("me", ids=old_msg_id)
                            if old_msg:
                                await old_msg.delete()
                        except:
                            pass
                    saved_msg = await self.client.send_file(
                        "me",
                        reply.media,
                        caption=self._get_string("saved_caption", is_premium),
                        silent=True
                    )
                    self.config["SAVED_MSG_ID"] = saved_msg.id
                    self.config["MEDIA_URL"] = f"saved:{saved_msg.id}"
                    await utils.answer(
                        message,
                        self._get_string("media_added", is_premium).format(prefix=prefix),
                        file=reply.media,
                        invert_media=True,
                    )
                    return
                except Exception as e:
                    await utils.answer(message, f"<b>Unknown error:</b> <code>{str(e)}</code>")
                    return
            if len(args_list) < 2:
                await utils.answer(
                    message,
                    self._get_string("invalid_media", is_premium).format(prefix=prefix),
                )
                return
            media_url = args[4:].strip()
            old_msg_id = self.config.get("SAVED_MSG_ID", 0)
            if old_msg_id:
                try:
                    old_msg = await self.client.get_messages("me", ids=old_msg_id)
                    if old_msg:
                        await old_msg.delete()
                except:
                    pass
            self.config["SAVED_MSG_ID"] = 0
            try:
                media_preview = InputMediaWebPage(url=media_url, optional=True)
                await utils.answer(
                    message,
                    self._get_string("media_added", is_premium).format(prefix=prefix),
                    file=media_preview,
                    invert_media=True,
                )
                self.config["MEDIA_URL"] = media_url
            except WebpageMediaEmptyError:
                await utils.answer(
                    message,
                    self._get_string("media_preview_error", is_premium),
                )
            except Exception:
                await utils.answer(
                    message,
                    self._get_string("media_load_error", is_premium),
                )

        elif cmd == "remove":
            msg_id = self.config.get("SAVED_MSG_ID", 0)
            if msg_id:
                try:
                    saved_msg = await self.client.get_messages("me", ids=msg_id)
                    if saved_msg:
                        await saved_msg.delete()
                except:
                    pass
            self.config["MEDIA_URL"] = ""
            self.config["SAVED_MSG_ID"] = 0
            await utils.answer(
                message,
                self._get_string("media_removed", is_premium),
            )

        else:
            await utils.answer(
                message,
                self._get_string("help_text", is_premium).format(prefix=prefix),
            )