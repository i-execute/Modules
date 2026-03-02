__version__ = (2, 2, 0)
# meta developer: FireJester.t.me

import logging
import asyncio
from telethon.tl.functions.channels import CreateChannelRequest
from telethon import errors
from telethon.tl.types import MessageMediaWebPage, DocumentAttributeVideo, DocumentAttributeAudio
from .. import loader, utils

logger = logging.getLogger(__name__)

STORAGE_AVATAR = "https://github.com/FireJester/Media/raw/main/Group_avatar_in_note_module.jpeg"

@loader.tds
class Note(loader.Module):
    """Module for saving any media as notes"""

    strings = {
        "name": "Note",
    }

    strings_en = {
        "help": (
            "<b>Note module commands</b>\n\n"
            "<code>.note create [name]</code> - save replied media\n"
            "<code>.note add [name]</code> - add media to existing note\n"
            "<code>.note remove [name]</code> - delete note\n"
            "<code>.note list</code> - list all notes\n"
            "<code>.note [name]</code> - post note"
        ),
        "created": "<b>Note</b> <code>{name}</code> <b>saved!</b>",
        "created_prem": "<emoji document_id=5265214770537075100>&#x1F920;</emoji><emoji document_id=5265253528321954044>&#x1F920;</emoji> <b>Note</b> <code>{name}</code> <b>saved!</b>",
        "added": "<b>Media added to note</b> <code>{name}</code><b>!</b>",
        "added_prem": "<emoji document_id=5265214770537075100>&#x1F920;</emoji><emoji document_id=5265253528321954044>&#x1F920;</emoji> <b>Media added to note</b> <code>{name}</code><b>!</b>",
        "no_media": "<b>Error: reply to media!</b>",
        "no_media_prem": "<emoji document_id=5265046712761748665>&#x270F;&#xFE0F;</emoji> <b>Error: reply to media!</b>",
        "no_reply": "<b>Error: reply to a message!</b>",
        "no_reply_prem": "<emoji document_id=5265046712761748665>&#x270F;&#xFE0F;</emoji> <b>Error: reply to a message!</b>",
        "name_required": "<b>Error: specify a name!</b>",
        "name_required_prem": "<emoji document_id=5265046712761748665>&#x270F;&#xFE0F;</emoji> <b>Error: specify a name!</b>",
        "exists": "<b>Error: note <code>{name}</code> already exists!</b>",
        "exists_prem": "<emoji document_id=5265046712761748665>&#x270F;&#xFE0F;</emoji> <b>Error: note <code>{name}</code> already exists!</b>",
        "not_found": "<b>Error: note <code>{name}</code> not found!</b>",
        "not_found_prem": "<emoji document_id=5265046712761748665>&#x270F;&#xFE0F;</emoji> <b>Error: note <code>{name}</code> not found!</b>",
        "removed": "<b>Note <code>{name}</code> removed!</b>",
        "removed_prem": "<emoji document_id=5265050947599508098>&#x2B50;</emoji> <b>Note <code>{name}</code> removed!</b>",
        "list_header": "<b>Your notes list:</b>\n<blockquote expandable>{notes}</blockquote>",
        "list_header_prem": "<emoji document_id=5264781623085276513>&#x2764;&#xFE0F;</emoji> <b>Your notes list:</b>\n<blockquote expandable>{notes}</blockquote>",
        "no_notes": "<b>No notes saved yet.</b>",
        "no_notes_prem": "<emoji document_id=5265046712761748665>&#x270F;&#xFE0F;</emoji> <b>No notes saved yet.</b>",
        "storage_error": "<b>Error interacting with storage. Please try again.</b>",
        "storage_error_prem": "<emoji document_id=5265046712761748665>&#x270F;&#xFE0F;</emoji> <b>Storage error. Please try again.</b>",
    }

    strings_ru = {
        "help": (
            "<b>Команды модуля Note</b>\n\n"
            "<code>.note create [название]</code> - сохранить медиа из реплая\n"
            "<code>.note add [название]</code> - добавить медиа к существующей заметке\n"
            "<code>.note remove [название]</code> - удалить заметку\n"
            "<code>.note list</code> - список всех заметок\n"
            "<code>.note [название]</code> - отправить заметку"
        ),
        "created": "<b>Заметка</b> <code>{name}</code> <b>сохранена!</b>",
        "created_prem": "<emoji document_id=5265214770537075100>&#x1F920;</emoji><emoji document_id=5265253528321954044>&#x1F920;</emoji> <b>Заметка</b> <code>{name}</code> <b>сохранена!</b>",
        "added": "<b>Медиа добавлено в заметку</b> <code>{name}</code><b>!</b>",
        "added_prem": "<emoji document_id=5265214770537075100>&#x1F920;</emoji><emoji document_id=5265253528321954044>&#x1F920;</emoji> <b>Медиа добавлено в заметку</b> <code>{name}</code><b>!</b>",
        "no_media": "<b>Ошибка: ответьте на медиа!</b>",
        "no_media_prem": "<emoji document_id=5265046712761748665>&#x270F;&#xFE0F;</emoji> <b>Ошибка: ответьте на медиа!</b>",
        "no_reply": "<b>Ошибка: ответьте на сообщение!</b>",
        "no_reply_prem": "<emoji document_id=5265046712761748665>&#x270F;&#xFE0F;</emoji> <b>Ошибка: ответьте на сообщение!</b>",
        "name_required": "<b>Ошибка: укажите название!</b>",
        "name_required_prem": "<emoji document_id=5265046712761748665>&#x270F;&#xFE0F;</emoji> <b>Ошибка: укажите название!</b>",
        "exists": "<b>Ошибка: заметка <code>{name}</code> уже существует!</b>",
        "exists_prem": "<emoji document_id=5265046712761748665>&#x270F;&#xFE0F;</emoji> <b>Ошибка: заметка <code>{name}</code> уже существует!</b>",
        "not_found": "<b>Ошибка: заметка <code>{name}</code> не найдена!</b>",
        "not_found_prem": "<emoji document_id=5265046712761748665>&#x270F;&#xFE0F;</emoji> <b>Ошибка: заметка <code>{name}</code> не найдена!</b>",
        "removed": "<b>Заметка <code>{name}</code> удалена!</b>",
        "removed_prem": "<emoji document_id=5265050947599508098>&#x2B50;</emoji> <b>Заметка <code>{name}</code> удалена!</b>",
        "list_header": "<b>Список ваших заметок:</b>\n<blockquote expandable>{notes}</blockquote>",
        "list_header_prem": "<emoji document_id=5264781623085276513>&#x2764;&#xFE0F;</emoji> <b>Список ваших заметок:</b>\n<blockquote expandable>{notes}</blockquote>",
        "no_notes": "<b>У вас пока нет сохраненных заметок.</b>",
        "no_notes_prem": "<emoji document_id=5265046712761748665>&#x270F;&#xFE0F;</emoji> <b>У вас пока нет сохраненных заметок.</b>",
        "storage_error": "<b>Ошибка взаимодействия с хранилищем. Попробуйте снова.</b>",
        "storage_error_prem": "<emoji document_id=5265046712761748665>&#x270F;&#xFE0F;</emoji> <b>Ошибка хранилища. Попробуйте снова.</b>",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue("STORAGE_CHAT_ID", 0, "storage group ID"),
            loader.ConfigValue("NOTES", {}, "List of ur notes"),
        )
        self._storage_chat_entity = None
        self._premium = None

    async def _get_premium_status(self):
        if self._premium is None:
            me = await self._client.get_me()
            self._premium = getattr(me, "premium", False)
        return self._premium

    def _get_str(self, key):
        if self._premium:
            prem_res = self.strings(f"{key}_prem")
            if not prem_res.startswith("Unknown string"):
                return prem_res
        return self.strings(key)

    async def _ensure_storage(self):
        chat_id = self.config["STORAGE_CHAT_ID"]
        if chat_id:
            try:
                entity = await self._client.get_entity(int(f"-100{chat_id}"))
                test_msg = await self._client.send_message(entity, "note_ping", silent=True)
                await self._client.delete_messages(entity, test_msg.id)
                self._storage_chat_entity = entity
                return entity
            except Exception:
                self.config["NOTES"] = {}
        
        try:
            chat_entity, _ = await utils.asset_channel(
                self._client, "Note Storage", "Notes storage. @FireJester with \u26a1",
                silent=True, avatar=STORAGE_AVATAR,
            )
        except Exception:
            try:
                r = await self._client(CreateChannelRequest(title="Note Storage", about="Notes", megagroup=True))
                chat_entity = r.chats[0]
            except Exception as e:
                logger.error(f"[Note] Create storage error: {e}")
                return None
        
        self.config["STORAGE_CHAT_ID"] = chat_entity.id
        self._storage_chat_entity = chat_entity
        return chat_entity

    async def client_ready(self, client, _):
        self._client = client
        await self._get_premium_status()
        await self._ensure_storage()
        self._migrate_notes()

    def _migrate_notes(self):
        notes = self.config["NOTES"]
        changed = False
        for name in notes:
            val = notes[name]
            if not isinstance(val, list):
                notes[name] = [[val]]
                changed = True
            elif val and not isinstance(val[0], list):
                notes[name] = [val]
                changed = True
        if changed:
            self.config["NOTES"] = notes

    async def _send_with_flood_wait(self, coro, *a, **k):
        try:
            return await coro(*a, **k)
        except errors.FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
            return await coro(*a, **k)
        except Exception:
            raise

    async def _get_album_messages(self, chat_id, message_id, grouped_id):
        album_messages = []
        search_range = 30
        async for msg in self._client.iter_messages(chat_id, max_id=message_id + 15, limit=search_range):
            if msg.grouped_id == grouped_id:
                album_messages.append(msg)
        return sorted(album_messages, key=lambda m: m.id)

    def _is_albumable(self, media):
        if not media:
            return False
        if hasattr(media, 'document'):
            for attr in media.document.attributes:
                if isinstance(attr, (DocumentAttributeVideo, DocumentAttributeAudio)):
                    if getattr(attr, 'round_message', False) or getattr(attr, 'voice', False):
                        return False
        return True

    async def _store_media_to_storage(self, storage, reply):
        media_list = []
        if reply.grouped_id:
            album = await self._get_album_messages(reply.chat_id, reply.id, reply.grouped_id)
            media_list = [m.media for m in album if m.media]
        else:
            media_list = [reply.media]

        stored_ids = []
        if any(not self._is_albumable(m) for m in media_list) or len(media_list) == 1:
            for m in media_list:
                s = await self._send_with_flood_wait(self._client.send_file, storage.id, m)
                stored_ids.append(s.id)
        else:
            stored = await self._send_with_flood_wait(self._client.send_file, storage.id, media_list)
            stored_ids = [s.id for s in stored] if isinstance(stored, list) else [stored.id]

        return stored_ids

    def _get_topic_id(self, message):
        """Вытаскиваем ID топика из сообщения."""
        reply_to = getattr(message, 'reply_to', None)
        if reply_to:
            top_id = getattr(reply_to, 'reply_to_top_id', None)
            if top_id:
                return top_id
            msg_id = getattr(reply_to, 'reply_to_msg_id', None)
            if msg_id and getattr(reply_to, 'forum_topic', False):
                return msg_id
        return None

    @loader.command(ru_doc="Управление заметками", en_doc="Manage notes")
    async def note(self, message):
        args = utils.get_args_raw(message).strip()
        await self._get_premium_status()

        storage = await self._ensure_storage()
        if not storage:
            await utils.answer(message, self._get_str("storage_error"))
            return

        parts = args.split(maxsplit=1)
        if not parts:
            await utils.answer(message, self._get_str("help"))
            return
        
        cmd = parts[0].lower()

        try:
            if cmd == "create":
                name = parts[1].strip() if len(parts) > 1 else None
                if not name:
                    await utils.answer(message, self._get_str("name_required"))
                    return
                
                notes = self.config["NOTES"]
                if name in notes:
                    await utils.answer(message, self._get_str("exists").format(name=name))
                    return

                reply = await message.get_reply_message()
                if not reply:
                    await utils.answer(message, self._get_str("no_reply"))
                    return
                if not reply.media or isinstance(reply.media, MessageMediaWebPage):
                    await utils.answer(message, self._get_str("no_media"))
                    return

                stored_ids = await self._store_media_to_storage(storage, reply)

                notes[name] = [stored_ids]
                self.config["NOTES"] = notes
                await utils.answer(message, self._get_str("created").format(name=name))

            elif cmd == "add":
                name = parts[1].strip() if len(parts) > 1 else None
                if not name:
                    await utils.answer(message, self._get_str("name_required"))
                    return

                notes = self.config["NOTES"]
                if name not in notes:
                    await utils.answer(message, self._get_str("not_found").format(name=name))
                    return

                reply = await message.get_reply_message()
                if not reply:
                    await utils.answer(message, self._get_str("no_reply"))
                    return
                if not reply.media or isinstance(reply.media, MessageMediaWebPage):
                    await utils.answer(message, self._get_str("no_media"))
                    return

                stored_ids = await self._store_media_to_storage(storage, reply)

                notes[name].append(stored_ids)
                self.config["NOTES"] = notes
                await utils.answer(message, self._get_str("added").format(name=name))

            elif cmd == "remove":
                name = parts[1].strip() if len(parts) > 1 else None
                if not name:
                    await utils.answer(message, self._get_str("name_required"))
                    return
                
                notes = self.config["NOTES"]
                if name not in notes:
                    await utils.answer(message, self._get_str("not_found").format(name=name))
                    return
                
                all_ids = []
                for group in notes[name]:
                    if isinstance(group, list):
                        all_ids.extend(group)
                    else:
                        all_ids.append(group)
                
                try:
                    await self._send_with_flood_wait(self._client.delete_messages, storage.id, all_ids)
                except Exception:
                    pass
                
                del notes[name]
                self.config["NOTES"] = notes
                await utils.answer(message, self._get_str("removed").format(name=name))

            elif cmd == "list":
                notes = self.config["NOTES"]
                if not notes:
                    await utils.answer(message, self._get_str("no_notes"))
                else:
                    n_list = "\n".join([f"\u2022 <code>{n}</code>" for n in sorted(notes.keys())])
                    await utils.answer(message, self._get_str("list_header").format(notes=n_list))

            else:
                name = args
                notes = self.config["NOTES"]
                if name not in notes:
                    await utils.answer(message, self._get_str("not_found").format(name=name))
                    return
                
                groups = notes[name]
                reply = await message.get_reply_message()
                topic_id = self._get_topic_id(message)

                is_first_group = True
                for group in groups:
                    msg_ids = group if isinstance(group, list) else [group]

                    fetched = await self._send_with_flood_wait(self._client.get_messages, storage.id, ids=msg_ids)
                    if not fetched:
                        continue

                    if isinstance(fetched, list):
                        media_to_send = [m.media for m in fetched if m and m.media]
                    else:
                        media_to_send = [fetched.media] if fetched and fetched.media else []

                    if not media_to_send:
                        continue

                    if is_first_group:
                        if reply:
                            reply_to = reply.id
                        elif topic_id:
                            reply_to = topic_id
                        else:
                            reply_to = None
                    else:
                        reply_to = topic_id if topic_id else None

                    if any(not self._is_albumable(m) for m in media_to_send) or len(media_to_send) == 1:
                        for idx, m in enumerate(media_to_send):
                            r = reply_to if idx == 0 else (topic_id if topic_id else None)
                            await self._client.send_file(message.chat_id, m, reply_to=r)
                    else:
                        await self._client.send_file(message.chat_id, media_to_send, reply_to=reply_to)

                    is_first_group = False

                await message.delete()

        except Exception as e:
            logger.exception(f"[Note] Error: {e}")
            try:
                await utils.answer(message, self._get_str("storage_error"))
            except Exception:
                pass