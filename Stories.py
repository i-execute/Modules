__version__ = (1, 2, 0)
# meta developer: I_execute.t.me 

import io
import asyncio

from telethon import functions, types
from PIL import Image

from .. import loader, utils
from ..inline.types import InlineCall


@loader.tds
class Stories(loader.Module):
    """Post photo in stories like grid, make albums, post new photos in early created albums and more other"""

    strings = {
        "name": "Stories",
        
        "main_menu": (
            "<b>Stories Manager</b>\n"
            "<blockquote>Select operation</blockquote>"
        ),
        
        "btn_post": "Post",
        "btn_archive_menu": "Archive",
        "btn_delete_menu": "Delete",
        "btn_back": "Back",
        "btn_close": "Close",
        
        "post_menu": (
            "<b>Post Stories</b>\n"
            "<blockquote>Select where to post</blockquote>"
        ),
        
        "btn_post_profile": "Post to Profile",
        "btn_post_archive": "Post to Archive",
        "btn_post_album": "Post to Album",
        
        "archive_menu": (
            "<b>Archive Stories</b>\n"
            "<blockquote>Select what to archive</blockquote>"
        ),
        
        "btn_archive_all": "Archive All Active",
        "btn_archive_album": "Archive Album",
        "btn_unarchive_all": "Unarchive All",
        
        "delete_menu": (
            "<b>Delete Stories</b>\n"
            "<blockquote>Select what to delete</blockquote>"
        ),
        
        "btn_delete_all": "Delete All",
        "btn_delete_active": "Delete Active",
        "btn_delete_archive": "Delete Archived",
        "btn_delete_album": "Delete Album",
        
        "album_menu": (
            "<b>Select Album</b>\n"
            "<blockquote>Available albums</blockquote>"
        ),
        
        "no_albums": (
            "<b>No Albums Found</b>\n"
            "<blockquote>Create an album first</blockquote>"
        ),
        
        "no_reply": (
            "<b>No Photo in Reply</b>\n"
            "<blockquote>Reply to a photo to use this feature</blockquote>"
        ),
        
        "uploading": (
            "<b>Uploading Stories</b>\n"
            "<blockquote>Please wait...</blockquote>"
        ),
        
        "deleting": (
            "<b>Deleting Stories</b>\n"
            "<blockquote>Please wait...</blockquote>"
        ),
        
        "archiving": (
            "<b>Archiving Stories</b>\n"
            "<blockquote>Please wait...</blockquote>"
        ),
        
        "unarchiving": (
            "<b>Unarchiving Stories</b>\n"
            "<blockquote>Please wait...</blockquote>"
        ),
        
        "posted": (
            "<b>Posted Successfully</b>\n"
            "<blockquote>{} stories uploaded</blockquote>"
        ),
        
        "posted_archive": (
            "<b>Posted to Archive</b>\n"
            "<blockquote>{} stories uploaded</blockquote>"
        ),
        
        "posted_album_new": (
            "<b>Album Created</b>\n"
            "<blockquote>Album '{}' created with {} stories</blockquote>"
        ),
        
        "posted_album": (
            "<b>Posted to Album</b>\n"
            "<blockquote>Album '{}' updated with {} stories</blockquote>"
        ),
        
        "deleted": (
            "<b>Deleted Successfully</b>\n"
            "<blockquote>{} stories deleted</blockquote>"
        ),
        
        "deleted_active": (
            "<b>Active Deleted</b>\n"
            "<blockquote>{} active stories deleted</blockquote>"
        ),
        
        "deleted_archive": (
            "<b>Archive Deleted</b>\n"
            "<blockquote>{} archived stories deleted</blockquote>"
        ),
        
        "deleted_album": (
            "<b>Album Deleted</b>\n"
            "<blockquote>Album '{}' with {} stories deleted</blockquote>"
        ),
        
        "archived": (
            "<b>Archived Successfully</b>\n"
            "<blockquote>{} stories archived</blockquote>"
        ),
        
        "archived_album": (
            "<b>Album Archived</b>\n"
            "<blockquote>Album '{}' with {} stories archived</blockquote>"
        ),
        
        "unarchived": (
            "<b>Unarchived Successfully</b>\n"
            "<blockquote>{} stories unarchived</blockquote>"
        ),
        
        "no_active": (
            "<b>No Active Stories</b>\n"
            "<blockquote>Nothing to archive</blockquote>"
        ),
        
        "no_archived": (
            "<b>No Archived Stories</b>\n"
            "<blockquote>Nothing to unarchive</blockquote>"
        ),
        
        "no_stories": (
            "<b>Upload Failed</b>\n"
            "<blockquote>Failed to upload any stories</blockquote>"
        ),
        
        "album_not_found": (
            "<b>Album Not Found</b>\n"
            "<blockquote>Album '{}' doesn't exist</blockquote>"
        ),
        
        "error": (
            "<b>Error Occurred</b>\n"
            "<blockquote>{}</blockquote>"
        ),
        
        "input_new_album_name": "Enter new album name:",
        
        "wrong_ratio": (
            "<b>Wrong Aspect Ratio</b>\n"
            "<blockquote>Your ratio: {:.2f}</blockquote>\n"
            "<blockquote>Supported ratios:\n"
            "1.25 (5:4) - 2 rows, 6 stories\n"
            "0.80 (4:5) - 3 rows, 9 stories\n"
            "0.60 (3:5) - 4 rows, 12 stories\n"
            "0.56 (9:16) - 5 rows, 15 stories\n"
            "Tolerance: 5%</blockquote>"
        ),
    }

    strings_ru = {
        "main_menu": (
            "<b>Менеджер историй</b>\n"
            "<blockquote>Выберите операцию</blockquote>"
        ),
        
        "btn_post": "Публикация",
        "btn_archive_menu": "Архив",
        "btn_delete_menu": "Удаление",
        "btn_back": "Назад",
        "btn_close": "Закрыть",
        
        "post_menu": (
            "<b>Публикация историй</b>\n"
            "<blockquote>Выберите куда опубликовать</blockquote>"
        ),
        
        "btn_post_profile": "В профиль",
        "btn_post_archive": "В архив",
        "btn_post_album": "В альбом",
        
        "archive_menu": (
            "<b>Архивация историй</b>\n"
            "<blockquote>Выберите что архивировать</blockquote>"
        ),
        
        "btn_archive_all": "Все активные",
        "btn_archive_album": "Альбом",
        "btn_unarchive_all": "Разархивировать все",
        
        "delete_menu": (
            "<b>Удаление историй</b>\n"
            "<blockquote>Выберите что удалить</blockquote>"
        ),
        
        "btn_delete_all": "Все",
        "btn_delete_active": "Активные",
        "btn_delete_archive": "Архивные",
        "btn_delete_album": "Альбом",
        
        "album_menu": (
            "<b>Выбор альбома</b>\n"
            "<blockquote>Доступные альбомы</blockquote>"
        ),
        
        "no_albums": (
            "<b>Альбомы не найдены</b>\n"
            "<blockquote>Сначала создайте альбом</blockquote>"
        ),
        
        "no_reply": (
            "<b>Нет фото в ответе</b>\n"
            "<blockquote>Ответьте на фото чтобы использовать эту функцию</blockquote>"
        ),
        
        "uploading": (
            "<b>Загрузка историй</b>\n"
            "<blockquote>Пожалуйста, подождите...</blockquote>"
        ),
        
        "deleting": (
            "<b>Удаление историй</b>\n"
            "<blockquote>Пожалуйста, подождите...</blockquote>"
        ),
        
        "archiving": (
            "<b>Архивация историй</b>\n"
            "<blockquote>Пожалуйста, подождите...</blockquote>"
        ),
        
        "unarchiving": (
            "<b>Разархивация историй</b>\n"
            "<blockquote>Пожалуйста, подождите...</blockquote>"
        ),
        
        "posted": (
            "<b>Успешно опубликовано</b>\n"
            "<blockquote>Загружено {} историй</blockquote>"
        ),
        
        "posted_archive": (
            "<b>Опубликовано в архив</b>\n"
            "<blockquote>Загружено {} историй</blockquote>"
        ),
        
        "posted_album_new": (
            "<b>Альбом создан</b>\n"
            "<blockquote>Альбом '{}' создан с {} историями</blockquote>"
        ),
        
        "posted_album": (
            "<b>Опубликовано в альбом</b>\n"
            "<blockquote>Альбом '{}' обновлен {} историями</blockquote>"
        ),
        
        "deleted": (
            "<b>Успешно удалено</b>\n"
            "<blockquote>Удалено {} историй</blockquote>"
        ),
        
        "deleted_active": (
            "<b>Активные удалены</b>\n"
            "<blockquote>Удалено {} активных историй</blockquote>"
        ),
        
        "deleted_archive": (
            "<b>Архив удален</b>\n"
            "<blockquote>Удалено {} архивных историй</blockquote>"
        ),
        
        "deleted_album": (
            "<b>Альбом удален</b>\n"
            "<blockquote>Альбом '{}' с {} историями удален</blockquote>"
        ),
        
        "archived": (
            "<b>Успешно архивировано</b>\n"
            "<blockquote>Архивировано {} историй</blockquote>"
        ),
        
        "archived_album": (
            "<b>Альбом архивирован</b>\n"
            "<blockquote>Альбом '{}' с {} историями архивирован</blockquote>"
        ),
        
        "unarchived": (
            "<b>Успешно разархивировано</b>\n"
            "<blockquote>Разархивировано {} историй</blockquote>"
        ),
        
        "no_active": (
            "<b>Нет активных историй</b>\n"
            "<blockquote>Нечего архивировать</blockquote>"
        ),
        
        "no_archived": (
            "<b>Нет архивных историй</b>\n"
            "<blockquote>Нечего разархивировать</blockquote>"
        ),
        
        "no_stories": (
            "<b>Загрузка не удалась</b>\n"
            "<blockquote>Не удалось загрузить ни одной истории</blockquote>"
        ),
        
        "album_not_found": (
            "<b>Альбом не найден</b>\n"
            "<blockquote>Альбом '{}' не существует</blockquote>"
        ),
        
        "error": (
            "<b>Произошла ошибка</b>\n"
            "<blockquote>{}</blockquote>"
        ),
        
        "input_new_album_name": "Введите название нового альбома:",
        
        "wrong_ratio": (
            "<b>Неправильное соотношение сторон</b>\n"
            "<blockquote>Ваше соотношение: {:.2f}</blockquote>\n"
            "<blockquote>Поддерживаемые соотношения:\n"
            "1.25 (5:4) - 2 ряда, 6 историй\n"
            "0.80 (4:5) - 3 ряда, 9 историй\n"
            "0.60 (3:5) - 4 ряда, 12 историй\n"
            "0.56 (9:16) - 5 рядов, 15 историй\n"
            "Допуск: 5%</blockquote>"
        ),
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "period",
                24,
                "Visibility period in hours (6, 12, 24, or 48)",
                validator=loader.validators.Choice([6, 12, 24, 48]),
            ),
            loader.ConfigValue(
                "cooldown",
                1,
                "Cooldown between actions in seconds",
                validator=loader.validators.Integer(minimum=0),
            ),
        )

    VALID_PERIODS = {6: 21600, 12: 43200, 24: 86400, 48: 172800}
    ASPECT_RATIOS = [(5/4, 2), (4/5, 3), (3/5, 4), (9/16, 5)]
    TOLERANCE = 0.05

    def _check_aspect_ratio(self, width, height):
        current_ratio = width / height
        for target_ratio, rows in self.ASPECT_RATIOS:
            min_ratio = target_ratio * (1 - self.TOLERANCE)
            max_ratio = target_ratio * (1 + self.TOLERANCE)
            if min_ratio <= current_ratio <= max_ratio:
                return (target_ratio, rows)
        return None

    def _extract_story_id(self, res):
        if hasattr(res, 'updates'):
            for update in res.updates:
                if hasattr(update, 'story') and hasattr(update.story, 'id'):
                    return update.story.id
        return None

    async def _get_all_stories(self, func, **kwargs):
        stories = []
        offset_id = 0
        while True:
            result = await self.client(func(
                peer=types.InputPeerSelf(),
                offset_id=offset_id,
                limit=100,
                **kwargs
            ))
            if not result.stories:
                break
            stories.extend(result.stories)
            offset_id = result.stories[-1].id
            if len(result.stories) < 100:
                break
        return stories

    async def _get_album_stories(self, album_id):
        stories = []
        offset = 0
        while True:
            result = await self.client(functions.stories.GetAlbumStoriesRequest(
                peer=types.InputPeerSelf(),
                album_id=album_id,
                offset=offset,
                limit=100
            ))
            if not result.stories:
                break
            stories.extend(result.stories)
            offset += len(result.stories)
            if len(result.stories) < 100:
                break
        return stories

    async def _get_albums(self):
        result = await self.client(functions.stories.GetAlbumsRequest(
            peer=types.InputPeerSelf(),
            hash=0
        ))
        if hasattr(result, 'albums'):
            return result.albums
        return []

    async def _find_album(self, name):
        albums = await self._get_albums()
        for album in albums:
            if album.title.lower() == name.lower():
                return album
        return None

    async def _delete_stories(self, story_ids):
        c = 0
        for sid in story_ids:
            try:
                await self.client(functions.stories.DeleteStoriesRequest(
                    peer=types.InputPeerSelf(),
                    id=[sid]
                ))
                c += 1
                await asyncio.sleep(self.config["cooldown"])
            except:
                pass
        return c

    async def _upload_story(self, part):
        out = io.BytesIO()
        part.save(out, "JPEG", quality=95)
        out.seek(0)
        uploaded = await self.client.upload_file(out, file_name="story.jpg")
        period = self.VALID_PERIODS.get(self.config["period"], 86400)
        res = await self.client(functions.stories.SendStoryRequest(
            peer=types.InputPeerSelf(),
            media=types.InputMediaUploadedPhoto(file=uploaded),
            privacy_rules=[types.InputPrivacyValueAllowAll()],
            period=period,
        ))
        return self._extract_story_id(res)

    async def _process_image(self, reply):
        try:
            image_bytes = await reply.download_media(file=bytes)
            img = Image.open(io.BytesIO(image_bytes))
            if img.mode != 'RGB':
                img = img.convert('RGB')
            return img
        except Exception:
            return None

    async def _post_stories(self, img, action, album_name=None):
        w, h = img.size
        ratio_result = self._check_aspect_ratio(w, h)
        if ratio_result is None:
            return None, w / h

        _, rows = ratio_result

        parts_list = []
        pw, ph = w // 3, h // rows
        for r in range(rows):
            for c in range(3):
                x, y = c * pw, r * ph
                part = img.crop((x, y, x + pw, y + ph))
                parts_list.append(part)
        parts_list.reverse()

        story_ids = []
        for i, part in enumerate(parts_list):
            try:
                story_id = await self._upload_story(part)
                if story_id:
                    story_ids.append(story_id)
                if self.config["cooldown"] > 0 and i < len(parts_list) - 1:
                    await asyncio.sleep(self.config["cooldown"])
            except:
                continue

        if not story_ids:
            return None, None

        if action == "archive":
            for sid in story_ids:
                try:
                    await self.client(functions.stories.TogglePinnedRequest(
                        peer=types.InputPeerSelf(),
                        id=[sid],
                        pinned=False
                    ))
                except:
                    pass

        elif action == "new_album":
            await self.client(functions.stories.TogglePinnedRequest(
                peer=types.InputPeerSelf(),
                id=story_ids,
                pinned=True
            ))
            await self.client(functions.stories.CreateAlbumRequest(
                peer=types.InputPeerSelf(),
                title=album_name,
                stories=story_ids
            ))

        elif action == "album":
            album = await self._find_album(album_name)
            await self.client(functions.stories.TogglePinnedRequest(
                peer=types.InputPeerSelf(),
                id=story_ids,
                pinned=True
            ))
            await self.client(functions.stories.UpdateAlbumRequest(
                peer=types.InputPeerSelf(),
                album_id=album.album_id,
                add_stories=story_ids
            ))

        else:
            await self.client(functions.stories.TogglePinnedRequest(
                peer=types.InputPeerSelf(),
                id=story_ids,
                pinned=True
            ))

        return len(story_ids), None

    def _get_main_markup(self):
        return [
            [
                {"text": self.strings["btn_post"], "callback": self._cb_post_menu, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_archive_menu"], "callback": self._cb_archive_menu, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_delete_menu"], "callback": self._cb_delete_menu, "style": "primary"},
            ],
            [
                {"text": self.strings["btn_close"], "callback": self._cb_close, "style": "danger"},
            ],
        ]

    async def _cb_main_menu(self, call: InlineCall):
        await call.edit(
            self.strings["main_menu"],
            reply_markup=self._get_main_markup()
        )

    async def _cb_post_menu(self, call: InlineCall):
        await call.edit(
            self.strings["post_menu"],
            reply_markup=[
                [{"text": self.strings["btn_post_profile"], "callback": self._cb_post_profile, "style": "primary"}],
                [{"text": self.strings["btn_post_archive"], "callback": self._cb_post_archive, "style": "primary"}],
                [{"text": self.strings["btn_post_album"], "callback": self._cb_post_album_menu, "style": "primary"}],
                [{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}],
            ]
        )

    async def _cb_post_profile(self, call: InlineCall):
        reply = call.form.get("reply_message")
        if not reply or not reply.photo:
            await call.edit(
                self.strings["no_reply"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}]]
            )
            return

        await call.edit(self.strings["uploading"])

        try:
            img = await self._process_image(reply)
            if not img:
                await call.edit(
                    self.strings["error"].format("Failed to process image"),
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}]]
                )
                return

            count, ratio = await self._post_stories(img, "post")
            
            if count is None and ratio is not None:
                await call.edit(
                    self.strings["wrong_ratio"].format(ratio),
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}]]
                )
                return

            if count is None:
                await call.edit(
                    self.strings["no_stories"],
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}]]
                )
                return

            await call.edit(
                self.strings["posted"].format(count),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
            )

        except Exception as e:
            await call.edit(
                self.strings["error"].format(str(e)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}]]
            )

    async def _cb_post_archive(self, call: InlineCall):
        reply = call.form.get("reply_message")
        if not reply or not reply.photo:
            await call.edit(
                self.strings["no_reply"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}]]
            )
            return

        await call.edit(self.strings["uploading"])

        try:
            img = await self._process_image(reply)
            if not img:
                await call.edit(
                    self.strings["error"].format("Failed to process image"),
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}]]
                )
                return

            count, ratio = await self._post_stories(img, "archive")
            
            if count is None and ratio is not None:
                await call.edit(
                    self.strings["wrong_ratio"].format(ratio),
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}]]
                )
                return

            if count is None:
                await call.edit(
                    self.strings["no_stories"],
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}]]
                )
                return

            await call.edit(
                self.strings["posted_archive"].format(count),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
            )

        except Exception as e:
            await call.edit(
                self.strings["error"].format(str(e)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}]]
            )

    async def _cb_post_album_menu(self, call: InlineCall):
        reply = call.form.get("reply_message")
        if not reply or not reply.photo:
            await call.edit(
                self.strings["no_reply"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}]]
            )
            return

        albums = await self._get_albums()
        
        markup = []
        for album in albums:
            markup.append([
                {"text": album.title, "callback": self._cb_post_album_select, "args": (album.title,), "style": "primary"}
            ])
        
        markup.append([
            {"text": "Create New Album", "input": self.strings["input_new_album_name"], "handler": self._cb_post_new_album, "style": "success"}
        ])
        markup.append([
            {"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}
        ])

        if not albums:
            text = self.strings["no_albums"]
        else:
            text = self.strings["album_menu"]

        await call.edit(text, reply_markup=markup)

    async def _cb_post_album_select(self, call: InlineCall, album_name: str):
        reply = call.form.get("reply_message")
        if not reply or not reply.photo:
            await call.edit(
                self.strings["no_reply"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}]]
            )
            return

        await call.edit(self.strings["uploading"])

        try:
            img = await self._process_image(reply)
            if not img:
                await call.edit(
                    self.strings["error"].format("Failed to process image"),
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}]]
                )
                return

            count, ratio = await self._post_stories(img, "album", album_name)
            
            if count is None and ratio is not None:
                await call.edit(
                    self.strings["wrong_ratio"].format(ratio),
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}]]
                )
                return

            if count is None:
                await call.edit(
                    self.strings["no_stories"],
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}]]
                )
                return

            await call.edit(
                self.strings["posted_album"].format(album_name, count),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
            )

        except Exception as e:
            await call.edit(
                self.strings["error"].format(str(e)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}]]
            )

    async def _cb_post_new_album(self, call: InlineCall, album_name: str):
        reply = call.form.get("reply_message")
        if not reply or not reply.photo:
            await call.edit(
                self.strings["no_reply"],
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}]]
            )
            return

        album_name = album_name.strip()
        if not album_name:
            await call.answer("Album name required", show_alert=True)
            return

        await call.edit(self.strings["uploading"])

        try:
            img = await self._process_image(reply)
            if not img:
                await call.edit(
                    self.strings["error"].format("Failed to process image"),
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}]]
                )
                return

            count, ratio = await self._post_stories(img, "new_album", album_name)
            
            if count is None and ratio is not None:
                await call.edit(
                    self.strings["wrong_ratio"].format(ratio),
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}]]
                )
                return

            if count is None:
                await call.edit(
                    self.strings["no_stories"],
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}]]
                )
                return

            await call.edit(
                self.strings["posted_album_new"].format(album_name, count),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
            )

        except Exception as e:
            await call.edit(
                self.strings["error"].format(str(e)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_post_menu, "style": "danger"}]]
            )

    async def _cb_archive_menu(self, call: InlineCall):
        albums = await self._get_albums()
        
        markup = [
            [{"text": self.strings["btn_archive_all"], "callback": self._cb_archive_all, "style": "primary"}],
        ]
        
        if albums:
            for album in albums:
                markup.append([
                    {"text": f"Archive: {album.title}", "callback": self._cb_archive_album, "args": (album.title, album.album_id), "style": "primary"}
                ])
        
        markup.append([{"text": self.strings["btn_unarchive_all"], "callback": self._cb_unarchive_all, "style": "success"}])
        markup.append([{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}])

        await call.edit(self.strings["archive_menu"], reply_markup=markup)

    async def _cb_archive_all(self, call: InlineCall):
        await call.edit(self.strings["archiving"])

        try:
            stories = await self._get_all_stories(functions.stories.GetPinnedStoriesRequest)
            if not stories:
                await call.edit(
                    self.strings["no_active"],
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_archive_menu, "style": "danger"}]]
                )
                return

            ids = [s.id for s in stories]
            c = 0
            for sid in ids:
                try:
                    await self.client(functions.stories.TogglePinnedRequest(
                        peer=types.InputPeerSelf(),
                        id=[sid],
                        pinned=False
                    ))
                    c += 1
                    await asyncio.sleep(self.config["cooldown"])
                except:
                    pass

            await call.edit(
                self.strings["archived"].format(c),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
            )
        except Exception as e:
            await call.edit(
                self.strings["error"].format(str(e)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_archive_menu, "style": "danger"}]]
            )

    async def _cb_archive_album(self, call: InlineCall, album_name: str, album_id: int):
        await call.edit(self.strings["archiving"])

        try:
            stories = await self._get_album_stories(album_id)
            if not stories:
                await call.edit(
                    self.strings["no_active"],
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_archive_menu, "style": "danger"}]]
                )
                return

            ids = [s.id for s in stories]
            c = 0
            for sid in ids:
                try:
                    await self.client(functions.stories.TogglePinnedRequest(
                        peer=types.InputPeerSelf(),
                        id=[sid],
                        pinned=False
                    ))
                    c += 1
                    await asyncio.sleep(self.config["cooldown"])
                except:
                    pass

            await call.edit(
                self.strings["archived_album"].format(album_name, c),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
            )
        except Exception as e:
            await call.edit(
                self.strings["error"].format(str(e)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_archive_menu, "style": "danger"}]]
            )

    async def _cb_unarchive_all(self, call: InlineCall):
        await call.edit(self.strings["unarchiving"])

        try:
            archive = await self._get_all_stories(functions.stories.GetStoriesArchiveRequest)
            active = await self._get_all_stories(functions.stories.GetPinnedStoriesRequest)
            active_ids = set(s.id for s in active)
            ids = [s.id for s in archive if s.id not in active_ids]
            
            if not ids:
                await call.edit(
                    self.strings["no_archived"],
                    reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_archive_menu, "style": "danger"}]]
                )
                return

            c = 0
            for sid in ids:
                try:
                    await self.client(functions.stories.TogglePinnedRequest(
                        peer=types.InputPeerSelf(),
                        id=[sid],
                        pinned=True
                    ))
                    c += 1
                    await asyncio.sleep(self.config["cooldown"])
                except:
                    pass

            await call.edit(
                self.strings["unarchived"].format(c),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
            )
        except Exception as e:
            await call.edit(
                self.strings["error"].format(str(e)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_archive_menu, "style": "danger"}]]
            )

    async def _cb_delete_menu(self, call: InlineCall):
        albums = await self._get_albums()
        
        markup = [
            [{"text": self.strings["btn_delete_all"], "callback": self._cb_delete_all, "style": "danger"}],
            [{"text": self.strings["btn_delete_active"], "callback": self._cb_delete_active, "style": "danger"}],
            [{"text": self.strings["btn_delete_archive"], "callback": self._cb_delete_archive, "style": "danger"}],
        ]
        
        if albums:
            for album in albums:
                markup.append([
                    {"text": f"Delete: {album.title}", "callback": self._cb_delete_album, "args": (album.title, album.album_id), "style": "danger"}
                ])
        
        markup.append([{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}])

        await call.edit(self.strings["delete_menu"], reply_markup=markup)

    async def _cb_delete_all(self, call: InlineCall):
        await call.edit(self.strings["deleting"])

        try:
            active = await self._get_all_stories(functions.stories.GetPinnedStoriesRequest)
            archive = await self._get_all_stories(functions.stories.GetStoriesArchiveRequest)
            all_ids = list(set([s.id for s in active] + [s.id for s in archive]))
            c = await self._delete_stories(all_ids)
            
            await call.edit(
                self.strings["deleted"].format(c),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
            )
        except Exception as e:
            await call.edit(
                self.strings["error"].format(str(e)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_delete_menu, "style": "danger"}]]
            )

    async def _cb_delete_active(self, call: InlineCall):
        await call.edit(self.strings["deleting"])

        try:
            stories = await self._get_all_stories(functions.stories.GetPinnedStoriesRequest)
            ids = [s.id for s in stories]
            c = await self._delete_stories(ids)
            
            await call.edit(
                self.strings["deleted_active"].format(c),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
            )
        except Exception as e:
            await call.edit(
                self.strings["error"].format(str(e)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_delete_menu, "style": "danger"}]]
            )

    async def _cb_delete_archive(self, call: InlineCall):
        await call.edit(self.strings["deleting"])

        try:
            stories = await self._get_all_stories(functions.stories.GetStoriesArchiveRequest)
            active = await self._get_all_stories(functions.stories.GetPinnedStoriesRequest)
            active_ids = set(s.id for s in active)
            ids = [s.id for s in stories if s.id not in active_ids]
            c = await self._delete_stories(ids)
            
            await call.edit(
                self.strings["deleted_archive"].format(c),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
            )
        except Exception as e:
            await call.edit(
                self.strings["error"].format(str(e)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_delete_menu, "style": "danger"}]]
            )

    async def _cb_delete_album(self, call: InlineCall, album_name: str, album_id: int):
        await call.edit(self.strings["deleting"])

        try:
            stories = await self._get_album_stories(album_id)
            ids = [s.id for s in stories]
            c = await self._delete_stories(ids)
            await self.client(functions.stories.DeleteAlbumRequest(
                peer=types.InputPeerSelf(),
                album_id=album_id
            ))
            
            await call.edit(
                self.strings["deleted_album"].format(album_name, c),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_main_menu, "style": "danger"}]]
            )
        except Exception as e:
            await call.edit(
                self.strings["error"].format(str(e)),
                reply_markup=[[{"text": self.strings["btn_back"], "callback": self._cb_delete_menu, "style": "danger"}]]
            )

    async def _cb_close(self, call: InlineCall):
        await call.delete()

    @loader.command()
    async def stories(self, message):
        """Stories manager"""
        reply = await message.get_reply_message()
        
        if reply and reply.photo:
            await self.inline.form(
                text=self.strings["main_menu"],
                message=message,
                reply_markup=self._get_main_markup(),
                reply_message=reply,
                silent=True,
            )
        else:
            await self.inline.form(
                text=self.strings["main_menu"],
                message=message,
                reply_markup=self._get_main_markup(),
                silent=True,
            )