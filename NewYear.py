__version__ = (2, 0, 0)
# meta developer: FireJester.t.me 

from .. import loader, utils
from datetime import datetime, timezone, timedelta
from telethon.types import InputMediaWebPage
from telethon.errors import WebpageMediaEmptyError

@loader.tds
class NewYear(loader.Module):
    """New Year countdown timer"""

    strings = {
        "name": "NewYear",
    }

    strings_en = {
        "help_text": (
            "<emoji document_id=5319189482811985933>☃️</emoji><b> New Year module commands:</b>\n"
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
        "new_year_template": (
            "<emoji document_id=5318911370794671211>🎩</emoji><b> Time until New Year {year}:</b>\n\n"
            "<emoji document_id=5319037874761405673>🎆</emoji><b> Days: </b><code>{days}</code>\n"
            "<emoji document_id=5319072286039382810>🕯</emoji><b> Hours: </b><code>{hours}</code>\n"
            "<emoji document_id=5316650362571101462>🫐</emoji><b> Minutes: </b><code>{minutes}</code>\n"
            "<emoji document_id=5316670952644316954>🎄</emoji><b> Seconds: </b><code>{seconds}</code>\n\n"
            "<blockquote>"
            "<emoji document_id=5316829604441265941>🍪</emoji><b> Timezone: </b>UTC{timezone_str}\n"
            "<emoji document_id=5316792757916833948>☕️</emoji><b> Current time: </b>{current_time}"
            "</blockquote>"
        ),
        "timezone_set": "<emoji document_id=5318910065124615176>👌</emoji><b> Timezone set: </b>UTC{timezone_str}",
        "media_added": (
            "<emoji document_id=5318910065124615176>👌</emoji><b> Media file added!</b>\n"
            "<blockquote>It will now be sent with the {prefix}new year command</blockquote>"
        ),
        "invalid_timezone": (
            "<emoji document_id=5316833049005035923>😵</emoji><b> Invalid timezone!</b>\n"
            "<blockquote>Use a number from -12 to +12</blockquote>"
        ),
        "invalid_media": (
            "<emoji document_id=5316833049005035923>😵</emoji><b> Provide a media link or reply to a media file!</b>\n"
            "<blockquote>Example: {prefix}new add https://example.com/image.gif</blockquote>"
        ),
        "media_deleted_error": (
            "<emoji document_id=5316833049005035923>😵</emoji><b> Media file is no longer available!</b>\n"
            "<blockquote>Add a new media file with {prefix}new add</blockquote>\n"
            "<blockquote>This message only appears on error, for the module to work correctly, your media value in cfg has just been reset</blockquote>"
        ),
        "no_media_in_reply": (
            "<emoji document_id=5316833049005035923>😵</emoji><b> No media in reply!</b>\n"
            "<blockquote>Reply to a photo, video or GIF</blockquote>"
        ),
        "media_load_error": (
            "<emoji document_id=5316833049005035923>😵</emoji><b> Failed to load media!</b>\n"
            "<blockquote>Check the link or try another file</blockquote>"
        ),
        "media_preview_error": (
            "<emoji document_id=5316833049005035923>😵</emoji><b> Failed to load media preview from this link!</b>\n"
            "<blockquote>The link may not support previews. Try another link or reply to a media file directly</blockquote>"
        ),
        "media_removed": "<emoji document_id=5319222343606773249>❌</emoji><b> Media file removed!</b>",
        "saved_caption": "<emoji document_id=5316890674581248786>🪟</emoji><b> Do not delete - media for the New Year module</b>",
    }

    strings_ru = {
        "help_text": (
            "<emoji document_id=5319189482811985933>☃️</emoji><b> Команды модуля New Year:</b>\n"
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
        "new_year_template": (
            "<emoji document_id=5318911370794671211>🎩</emoji><b> До Нового {year} года осталось:</b>\n\n"
            "<emoji document_id=5319037874761405673>🎆</emoji><b> Дней: </b><code>{days}</code>\n"
            "<emoji document_id=5319072286039382810>🕯</emoji><b> Часов: </b><code>{hours}</code>\n"
            "<emoji document_id=5316650362571101462>🫐</emoji><b> Минут: </b><code>{minutes}</code>\n"
            "<emoji document_id=5316670952644316954>🎄</emoji><b> Секунд: </b><code>{seconds}</code>\n\n"
            "<blockquote>"
            "<emoji document_id=5316829604441265941>🍪</emoji><b> Часовой пояс: </b>UTC{timezone_str}\n"
            "<emoji document_id=5316792757916833948>☕️</emoji><b> Текущее время: </b>{current_time}"
            "</blockquote>"
        ),
        "timezone_set": "<emoji document_id=5318910065124615176>👌</emoji><b> Часовой пояс установлен: </b>UTC{timezone_str}",
        "media_added": (
            "<emoji document_id=5318910065124615176>👌</emoji><b> Медиафайл добавлен!</b>\n"
            "<blockquote>Теперь он будет отправляться с командой {prefix}new year</blockquote>"
        ),
        "invalid_timezone": (
            "<emoji document_id=5316833049005035923>😵</emoji><b> Неверный часовой пояс!</b>\n"
            "<blockquote>Используйте число от -12 до +12</blockquote>"
        ),
        "invalid_media": (
            "<emoji document_id=5316833049005035923>😵</emoji><b> Укажите ссылку на медиафайл или ответьте этой командой на него!</b>\n"
            "<blockquote>Пример: {prefix}new add https://example.com/image.gif</blockquote>"
        ),
        "media_deleted_error": (
            "<emoji document_id=5316833049005035923>😵</emoji><b> Медиафайл больше не доступен!</b>\n"
            "<blockquote>Добавьте новый медиафайл командой {prefix}new add</blockquote>\n"
            "<blockquote>Это сообщение появляется только при ошибке, для корректной работы модуля, значение вашего медиафайла в cfg только что было сброшено</blockquote>"
        ),
        "no_media_in_reply": (
            "<emoji document_id=5316833049005035923>😵</emoji><b> В реплае нет медиа!</b>\n"
            "<blockquote>Ответьте на фото, видео или GIF</blockquote>"
        ),
        "media_load_error": (
            "<emoji document_id=5316833049005035923>😵</emoji><b> Не удалось загрузить медиа!</b>\n"
            "<blockquote>Проверьте ссылку или попробуйте другой файл</blockquote>"
        ),
        "media_preview_error": (
            "<emoji document_id=5316833049005035923>😵</emoji><b> Не удалось загрузить превью медиа по этой ссылке!</b>\n"
            "<blockquote>Возможно, ссылка не поддерживает превью. Попробуйте другую ссылку или ответьте на медиафайл напрямую</blockquote>"
        ),
        "media_removed": "<emoji document_id=5319222343606773249>❌</emoji><b> Медиафайл удален!</b>",
        "saved_caption": "<emoji document_id=5316890674581248786>🪟</emoji><b> Не удалять - медиа для модуля New Year</b>",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            "TIMEZONE_OFFSET", 3, "смещение часового пояса от UTC",
            "MEDIA_URL", "", "ссылка на файл",
            "SAVED_MSG_ID", 0, "ID сохраненного сообщения в избранном"
        )

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

        if not args:
            await utils.answer(
                message,
                self.strings["help_text"].format(prefix=prefix),
            )
            return

        args_list = args.split()
        cmd = args_list[0].lower()

        if cmd == "year":
            time_data = self.get_time_until_new_year()
            msg = self.strings["new_year_template"].format(
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
                            await utils.answer(message, msg, file=saved_media)
                        except:
                            await utils.answer(
                                message,
                                self.strings["media_deleted_error"].format(prefix=prefix),
                            )
                    else:
                        await utils.answer(
                            message,
                            self.strings["media_deleted_error"].format(prefix=prefix),
                        )
                        self.config["MEDIA_URL"] = ""
                        self.config["SAVED_MSG_ID"] = 0
                else:
                    try:
                        media = InputMediaWebPage(url=media_url, optional=True)
                        await utils.answer(message, msg, file=media)
                    except WebpageMediaEmptyError:
                        await utils.answer(
                            message,
                            self.strings["media_preview_error"],
                        )
                    except:
                        await utils.answer(message, msg)
            else:
                await utils.answer(message, msg)

        elif cmd == "set":
            if len(args_list) < 2:
                await utils.answer(message, self.strings["invalid_timezone"])
                return
            try:
                timezone_str = args_list[1].replace('+', '')
                timezone_offset = int(timezone_str)
                if not -12 <= timezone_offset <= 12:
                    await utils.answer(message, self.strings["invalid_timezone"])
                    return
                self.config["TIMEZONE_OFFSET"] = timezone_offset
                msg = self.strings["timezone_set"].format(
                    timezone_str=self.get_timezone_str(timezone_offset)
                )
                await utils.answer(message, msg)
            except (ValueError, IndexError):
                await utils.answer(message, self.strings["invalid_timezone"])

        elif cmd == "add":
            reply = await message.get_reply_message()
            if reply:
                if not reply.media:
                    await utils.answer(message, self.strings["no_media_in_reply"])
                    return
                is_valid_media = False
                if reply.photo or reply.video or reply.gif:
                    is_valid_media = True
                elif reply.document:
                    if reply.document.mime_type and any(x in reply.document.mime_type.lower()
                                                        for x in ['image', 'video', 'gif']):
                        is_valid_media = True
                if not is_valid_media:
                    await utils.answer(message, self.strings["no_media_in_reply"])
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
                        caption=self.strings["saved_caption"],
                        silent=True
                    )
                    self.config["SAVED_MSG_ID"] = saved_msg.id
                    self.config["MEDIA_URL"] = f"saved:{saved_msg.id}"
                    await utils.answer(
                        message,
                        self.strings["media_added"].format(prefix=prefix),
                        file=reply.media,
                    )
                    return
                except Exception as e:
                    await utils.answer(message, f"<b>Unknown error:</b> <code>{str(e)}</code>")
                    return
            if len(args_list) < 2:
                await utils.answer(
                    message,
                    self.strings["invalid_media"].format(prefix=prefix),
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
                    self.strings["media_added"].format(prefix=prefix),
                    file=media_preview,
                )
                self.config["MEDIA_URL"] = media_url
            except WebpageMediaEmptyError:
                await utils.answer(message, self.strings["media_preview_error"])
            except Exception:
                await utils.answer(message, self.strings["media_load_error"])

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
            await utils.answer(message, self.strings["media_removed"])

        else:
            await utils.answer(
                message,
                self.strings["help_text"].format(prefix=prefix),
            )