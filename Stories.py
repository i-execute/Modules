__version__ = (1, 1, 1)
# meta developer: FireJester.t.me 

import io
import asyncio

from telethon import functions, types
from PIL import Image

from .. import loader, utils


@loader.tds
class Stories(loader.Module):
    """Manage your Telegram stories"""

    strings = {
        "name": "Stories",
        "no_reply": "<b>Reply to photo!</b>",
        "uploading": "<b>Uploading...</b>",
        "deleting": "<b>Deleting...</b>",
        "archiving": "<b>Archiving...</b>",
        "unarchiving": "<b>Unarchiving...</b>",
        "posted": "<b>Posted! {} stories uploaded.</b>",
        "posted_archive": "<b>Posted to archive! {} stories.</b>",
        "posted_album_new": "<b>Posted & created album '{}'! {} stories.</b>",
        "posted_album": "<b>Posted to album '{}'! {} stories.</b>",
        "deleted": "<b>Deleted: {}</b>",
        "deleted_active": "<b>Deleted active: {}</b>",
        "deleted_archive": "<b>Deleted archived: {}</b>",
        "deleted_album": "<b>Deleted album '{}': {} stories</b>",
        "archived": "<b>Archived: {}</b>",
        "unarchived": "<b>Unarchived: {}</b>",
        "no_active": "<b>No active stories</b>",
        "no_archived": "<b>No archived stories</b>",
        "no_stories": "<b>Failed to upload any stories!</b>",
        "album_not_found": "<b>Album '{}' not found!</b>",
        "album_name_required": "<b>Album name required!</b>",
        "error": "<b>Error:</b> {}",
        "usage": (
            "<b>Stories Manager</b>\n"
            "<blockquote expandable><b>Posting (reply to photo):</b>\n"
            "<code>{prefix}st post</code> post grid to profile\n"
            "<code>{prefix}st post archive</code> post grid to archive\n"
            "<code>{prefix}st post album Name</code> post to existing album\n"
            "<code>{prefix}st post new album Name</code> post and create new album\n\n"
            "<b>Deleting:</b>\n"
            "<code>{prefix}st delete all</code> delete all stories\n"
            "<code>{prefix}st delete active</code> delete only active (in profile)\n"
            "<code>{prefix}st delete archive</code> delete only archived\n"
            "<code>{prefix}st delete album Name</code> delete album with stories\n\n"
            "<b>Archiving:</b>\n"
            "<code>{prefix}st archive</code> move all active to archive\n"
            "<code>{prefix}st unarchive</code> move all archived to profile\n\n"
            "<b>Supported aspect ratios:</b>\n"
            "5:4 (1.25) 2 rows, 6 stories\n"
            "4:5 (0.80) 3 rows, 9 stories\n"
            "3:5 (0.60) 4 rows, 12 stories\n"
            "9:16 (0.56) 5 rows, 15 stories</blockquote>"
        ),
        "unknown_cmd": "<b>Unknown command</b>",
        "unknown_target": "<b>Unknown target</b>",
        "specify_target": "<b>Specify: all/active/archive/album [name]</b>",
        "wrong_ratio": (
            "<b>Wrong image aspect ratio!</b>\n\n"
            "<b>Your ratio:</b> <code>{:.2f}</code>\n"
            "<b>Supported ratios:</b>\n"
            "<code>1.25</code> (5:4) 2 rows, 6 stories\n"
            "<code>0.80</code> (4:5) 3 rows, 9 stories\n"
            "<code>0.60</code> (3:5) 4 rows, 12 stories\n"
            "<code>0.56</code> (9:16) 5 rows, 15 stories\n\n"
            "<b>Tolerance:</b> 5%"
        ),
    }

    strings_ru = {
        "no_reply": "<b>Ответь на фото!</b>",
        "uploading": "<b>Загружаю...</b>",
        "deleting": "<b>Удаляю...</b>",
        "archiving": "<b>Архивирую...</b>",
        "unarchiving": "<b>Разархивирую...</b>",
        "posted": "<b>Готово! Загружено {} историй.</b>",
        "posted_archive": "<b>Загружено в архив! {} историй.</b>",
        "posted_album_new": "<b>Загружено и создан альбом '{}'! {} историй.</b>",
        "posted_album": "<b>Загружено в альбом '{}'! {} историй.</b>",
        "deleted": "<b>Удалено: {}</b>",
        "deleted_active": "<b>Удалено активных: {}</b>",
        "deleted_archive": "<b>Удалено из архива: {}</b>",
        "deleted_album": "<b>Удалён альбом '{}': {} историй</b>",
        "archived": "<b>Архивировано: {}</b>",
        "unarchived": "<b>Разархивировано: {}</b>",
        "no_active": "<b>Нет активных историй</b>",
        "no_archived": "<b>Нет архивных историй</b>",
        "no_stories": "<b>Не удалось загрузить ни одной истории!</b>",
        "album_not_found": "<b>Альбом '{}' не найден!</b>",
        "album_name_required": "<b>Укажите название альбома!</b>",
        "error": "<b>Ошибка:</b> {}",
        "usage": (
            "<b>Менеджер историй</b>\n"
            "<blockquote expandable><b>Публикация (ответ на фото):</b>\n"
            "<code>{prefix}st post</code> опубликовать сетку в профиль\n"
            "<code>{prefix}st post archive</code> опубликовать сетку в архив\n"
            "<code>{prefix}st post album Название</code> опубликовать в существующий альбом\n"
            "<code>{prefix}st post new album Название</code> опубликовать и создать новый альбом\n\n"
            "<b>Удаление:</b>\n"
            "<code>{prefix}st delete all</code> удалить все истории\n"
            "<code>{prefix}st delete active</code> удалить только активные (в профиле)\n"
            "<code>{prefix}st delete archive</code> удалить только архивные\n"
            "<code>{prefix}st delete album Название</code> удалить альбом с историями\n\n"
            "<b>Архивация:</b>\n"
            "<code>{prefix}st archive</code> переместить все активные в архив\n"
            "<code>{prefix}st unarchive</code> переместить все из архива в профиль\n\n"
            "<b>Поддерживаемые соотношения сторон:</b>\n"
            "5:4 (1.25) 2 ряда, 6 историй\n"
            "4:5 (0.80) 3 ряда, 9 историй\n"
            "3:5 (0.60) 4 ряда, 12 историй\n"
            "9:16 (0.56) 5 рядов, 15 историй</blockquote>"
        ),
        "unknown_cmd": "<b>Неизвестная команда</b>",
        "unknown_target": "<b>Неизвестная цель</b>",
        "specify_target": "<b>Укажи: all/active/archive/album [название]</b>",
        "wrong_ratio": (
            "<b>Неправильное соотношение сторон!</b>\n\n"
            "<b>Ваше соотношение:</b> <code>{:.2f}</code>\n"
            "<b>Поддерживаемые соотношения:</b>\n"
            "<code>1.25</code> (5:4) 2 ряда, 6 историй\n"
            "<code>0.80</code> (4:5) 3 ряда, 9 историй\n"
            "<code>0.60</code> (3:5) 4 ряда, 12 историй\n"
            "<code>0.56</code> (9:16) 5 рядов, 15 историй\n\n"
            "<b>Допуск:</b> 5%"
        ),
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "period",
                24,
                lambda: "Visibility period in hours (6, 12, 24, or 48)",
                validator=loader.validators.Choice([6, 12, 24, 48]),
            ),
            loader.ConfigValue(
                "cooldown",
                1,
                lambda: "Cooldown between actions in seconds",
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

    def _parse_post_args(self, args):
        args_lower = args.lower()
        
        if "post new album" in args_lower:
            idx = args_lower.find("post new album")
            album_name = args[idx + len("post new album"):].strip()
            return ("new_album", album_name)
        
        if "post album" in args_lower:
            idx = args_lower.find("post album")
            album_name = args[idx + len("post album"):].strip()
            return ("album", album_name)
        
        if "post archive" in args_lower:
            return ("archive", None)
        
        if "post" in args_lower:
            return ("post", None)
        
        return (None, None)

    @loader.command(
        ru_doc="Управление историями Telegram",
        en_doc="Manage your Telegram stories",
    )
    async def st(self, message):
        """Manage your Telegram stories"""
        args = utils.get_args_raw(message).strip()
        prefix = self.get_prefix()
        if not args:
            return await utils.answer(message, self.strings("usage").format(prefix=prefix))

        parts = args.lower().split()
        cmd = parts[0]

        if cmd == "post":
            await self._handle_post(message, args)
        elif cmd == "delete":
            await self._handle_delete(message, parts[1:])
        elif cmd == "archive":
            await self._handle_archive(message)
        elif cmd == "unarchive":
            await self._handle_unarchive(message)
        else:
            await utils.answer(message, self.strings("unknown_cmd"))

    async def _handle_post(self, message, args):
        action, album_name = self._parse_post_args(args)
        
        if action == "album":
            if not album_name:
                return await utils.answer(message, self.strings("album_name_required"))
            album = await self._find_album(album_name)
            if not album:
                return await utils.answer(message, self.strings("album_not_found").format(album_name))
        
        if action == "new_album" and not album_name:
            return await utils.answer(message, self.strings("album_name_required"))
        
        reply = await message.get_reply_message()
        if not reply or not reply.photo:
            return await utils.answer(message, self.strings("no_reply"))

        try:
            image_bytes = await reply.download_media(file=bytes)
            img = Image.open(io.BytesIO(image_bytes))
            if img.mode != 'RGB':
                img = img.convert('RGB')
        except Exception as e:
            return await utils.answer(message, self.strings("error").format(e))

        w, h = img.size
        ratio_result = self._check_aspect_ratio(w, h)
        if ratio_result is None:
            return await utils.answer(message, self.strings("wrong_ratio").format(w / h))

        _, rows = ratio_result
        await utils.answer(message, self.strings("uploading"))

        try:
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
                return await utils.answer(message, self.strings("no_stories"))

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
                await utils.answer(message, self.strings("posted_archive").format(len(story_ids)))

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
                await utils.answer(message, self.strings("posted_album_new").format(album_name, len(story_ids)))

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
                await utils.answer(message, self.strings("posted_album").format(album_name, len(story_ids)))

            else:
                await self.client(functions.stories.TogglePinnedRequest(
                    peer=types.InputPeerSelf(),
                    id=story_ids,
                    pinned=True
                ))
                await utils.answer(message, self.strings("posted").format(len(story_ids)))

        except Exception as e:
            await utils.answer(message, self.strings("error").format(e))

    async def _handle_delete(self, message, args):
        if not args:
            return await utils.answer(message, self.strings("specify_target"))

        target = args[0]
        await utils.answer(message, self.strings("deleting"))

        try:
            if target == "all":
                active = await self._get_all_stories(functions.stories.GetPinnedStoriesRequest)
                archive = await self._get_all_stories(functions.stories.GetStoriesArchiveRequest)
                all_ids = list(set([s.id for s in active] + [s.id for s in archive]))
                c = await self._delete_stories(all_ids)
                await utils.answer(message, self.strings("deleted").format(c))

            elif target == "active":
                stories = await self._get_all_stories(functions.stories.GetPinnedStoriesRequest)
                ids = [s.id for s in stories]
                c = await self._delete_stories(ids)
                await utils.answer(message, self.strings("deleted_active").format(c))

            elif target == "archive":
                stories = await self._get_all_stories(functions.stories.GetStoriesArchiveRequest)
                active = await self._get_all_stories(functions.stories.GetPinnedStoriesRequest)
                active_ids = set(s.id for s in active)
                ids = [s.id for s in stories if s.id not in active_ids]
                c = await self._delete_stories(ids)
                await utils.answer(message, self.strings("deleted_archive").format(c))

            elif target == "album" and len(args) >= 2:
                album_name = " ".join(args[1:])
                album = await self._find_album(album_name)
                if not album:
                    return await utils.answer(message, self.strings("album_not_found").format(album_name))
                stories = await self._get_album_stories(album.album_id)
                ids = [s.id for s in stories]
                c = await self._delete_stories(ids)
                await self.client(functions.stories.DeleteAlbumRequest(
                    peer=types.InputPeerSelf(),
                    album_id=album.album_id
                ))
                await utils.answer(message, self.strings("deleted_album").format(album_name, c))
            else:
                await utils.answer(message, self.strings("unknown_target"))

        except Exception as e:
            await utils.answer(message, self.strings("error").format(e))

    async def _handle_archive(self, message):
        await utils.answer(message, self.strings("archiving"))
        try:
            stories = await self._get_all_stories(functions.stories.GetPinnedStoriesRequest)
            if not stories:
                return await utils.answer(message, self.strings("no_active"))
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
            await utils.answer(message, self.strings("archived").format(c))
        except Exception as e:
            await utils.answer(message, self.strings("error").format(e))

    async def _handle_unarchive(self, message):
        await utils.answer(message, self.strings("unarchiving"))
        try:
            archive = await self._get_all_stories(functions.stories.GetStoriesArchiveRequest)
            active = await self._get_all_stories(functions.stories.GetPinnedStoriesRequest)
            active_ids = set(s.id for s in active)
            ids = [s.id for s in archive if s.id not in active_ids]
            if not ids:
                return await utils.answer(message, self.strings("no_archived"))
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
            await utils.answer(message, self.strings("unarchived").format(c))
        except Exception as e:
            await utils.answer(message, self.strings("error").format(e))