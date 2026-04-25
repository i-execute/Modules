__version__ = (3, 0, 1)
# meta developer: I_execute.t.me

import logging
import asyncio
from telethon import errors
from telethon.tl.types import MessageMediaWebPage, DocumentAttributeVideo, DocumentAttributeAudio
from .. import loader, utils

logger = logging.getLogger(__name__)


@loader.tds
class Note(loader.Module):
    """Module for saving any media as notes"""

    strings = {
        "name": "Note",
        "help": (
            "<b>Note module commands</b>\n\n"
            "<code>{prefix}note create [name]</code> - save replied media\n"
            "<code>{prefix}note add [name]</code> - add media to existing note\n"
            "<code>{prefix}note remove [name]</code> - delete note\n"
            "<code>{prefix}note rmall</code> - delete all notes\n"
            "<code>{prefix}note list</code> - list all notes\n"
            "<code>{prefix}note [name]</code> - post note"
        ),
        "created": "<b>Note</b> <code>{name}</code> <b>saved!</b>",
        "created_prem": "<tg-emoji emoji-id=5431518146211125713>😎</tg-emoji> <b>Note</b> <code>{name}</code> <b>saved!</b>",
        "added": "<b>Media added to note</b> <code>{name}</code><b>!</b>",
        "added_prem": "<tg-emoji emoji-id=5431518146211125713>😎</tg-emoji> <b>Media added to note</b> <code>{name}</code><b>!</b>",
        "no_media": "<b>Error: reply to media!</b>",
        "no_media_prem": "<tg-emoji emoji-id=5429196733567504495>😵</tg-emoji> <b>Error: reply to media!</b>",
        "no_reply": "<b>Error: reply to a message!</b>",
        "no_reply_prem": "<tg-emoji emoji-id=5429196733567504495>😵</tg-emoji> <b>Error: reply to a message!</b>",
        "name_required": "<b>Error: specify a name!</b>",
        "name_required_prem": "<tg-emoji emoji-id=5429196733567504495>😵</tg-emoji> <b>Error: specify a name!</b>",
        "exists": "<b>Error: note <code>{name}</code> already exists!</b>",
        "exists_prem": "<tg-emoji emoji-id=5429196733567504495>😵</tg-emoji> <b>Error: note <code>{name}</code> already exists!</b>",
        "not_found": "<b>Error: note <code>{name}</code> not found!</b>",
        "not_found_prem": "<tg-emoji emoji-id=5429196733567504495>😵</tg-emoji> <b>Error: note <code>{name}</code> not found!</b>",
        "removed": "<b>Note <code>{name}</code> removed!</b>",
        "removed_prem": "<tg-emoji emoji-id=5429351975160421386>😸</tg-emoji> <b>Note <code>{name}</code> removed!</b>",
        "rmall_done": "<b>All notes removed!</b>",
        "rmall_done_prem": "<tg-emoji emoji-id=5429351975160421386>😸</tg-emoji> <b>All notes removed!</b>",
        "rmall_empty": "<b>No notes to remove.</b>",
        "rmall_empty_prem": "<tg-emoji emoji-id=5429196733567504495>😵</tg-emoji> <b>No notes to remove.</b>",
        "list_header": "<b>Your notes list:</b>\n<blockquote expandable>{notes}</blockquote>",
        "list_header_prem": "<tg-emoji emoji-id=5429203764428971590>😠</tg-emoji> <b>Your notes list:</b>\n<blockquote expandable>{notes}</blockquote>",
        "no_notes": "<b>No notes saved yet.</b>",
        "no_notes_prem": "<tg-emoji emoji-id=5429196733567504495>😵</tg-emoji> <b>No notes saved yet.</b>",
        "storage_error": "<b>Error interacting with storage. Please try again.</b>",
        "storage_error_prem": "<tg-emoji emoji-id=5429196733567504495>😵</tg-emoji> <b>Storage error. Please try again.</b>",
    }

    strings_ru = {
        "help": (
            "<b>Команды модуля Note</b>\n\n"
            "<code>{prefix}note create [название]</code> - сохранить медиа из реплая\n"
            "<code>{prefix}note add [название]</code> - добавить медиа к существующей заметке\n"
            "<code>{prefix}note remove [название]</code> - удалить заметку\n"
            "<code>{prefix}note rmall</code> - удалить все заметки\n"
            "<code>{prefix}note list</code> - список всех заметок\n"
            "<code>{prefix}note [название]</code> - отправить заметку"
        ),
        "created": "<b>Заметка</b> <code>{name}</code> <b>сохранена!</b>",
        "created_prem": "<tg-emoji emoji-id=5431518146211125713>😎</tg-emoji> <b>Заметка</b> <code>{name}</code> <b>сохранена!</b>",
        "added": "<b>Медиа добавлено в заметку</b> <code>{name}</code><b>!</b>",
        "added_prem": "<tg-emoji emoji-id=5431518146211125713>😎</tg-emoji> <b>Медиа добавлено в заметку</b> <code>{name}</code><b>!</b>",
        "no_media": "<b>Ошибка: ответьте на медиа!</b>",
        "no_media_prem": "<tg-emoji emoji-id=5429196733567504495>😵</tg-emoji> <b>Ошибка: ответьте на медиа!</b>",
        "no_reply": "<b>Ошибка: ответьте на сообщение!</b>",
        "no_reply_prem": "<tg-emoji emoji-id=5429196733567504495>😵</tg-emoji> <b>Ошибка: ответьте на сообщение!</b>",
        "name_required": "<b>Ошибка: укажите название!</b>",
        "name_required_prem": "<tg-emoji emoji-id=5429196733567504495>😵</tg-emoji> <b>Ошибка: укажите название!</b>",
        "exists": "<b>Ошибка: заметка <code>{name}</code> уже существует!</b>",
        "exists_prem": "<tg-emoji emoji-id=5429196733567504495>😵</tg-emoji> <b>Ошибка: заметка <code>{name}</code> уже существует!</b>",
        "not_found": "<b>Ошибка: заметка <code>{name}</code> не найдена!</b>",
        "not_found_prem": "<tg-emoji emoji-id=5429196733567504495>😵</tg-emoji> <b>Ошибка: заметка <code>{name}</code> не найдена!</b>",
        "removed": "<b>Заметка <code>{name}</code> удалена!</b>",
        "removed_prem": "<tg-emoji emoji-id=5429351975160421386>😸</tg-emoji> <b>Заметка <code>{name}</code> удалена!</b>",
        "rmall_done": "<b>Все заметки удалены!</b>",
        "rmall_done_prem": "<tg-emoji emoji-id=5429351975160421386>😸</tg-emoji> <b>Все заметки удалены!</b>",
        "rmall_empty": "<b>Нет заметок для удаления.</b>",
        "rmall_empty_prem": "<tg-emoji emoji-id=5429196733567504495>😵</tg-emoji> <b>Нет заметок для удаления.</b>",
        "list_header": "<b>Список ваших заметок:</b>\n<blockquote expandable>{notes}</blockquote>",
        "list_header_prem": "<tg-emoji emoji-id=5429203764428971590>😠</tg-emoji> <b>Список ваших заметок:</b>\n<blockquote expandable>{notes}</blockquote>",
        "no_notes": "<b>У вас пока нет сохраненных заметок.</b>",
        "no_notes_prem": "<tg-emoji emoji-id=5429196733567504495>😵</tg-emoji> <b>У вас пока нет сохраненных заметок.</b>",
        "storage_error": "<b>Ошибка взаимодействия с хранилищем. Попробуйте снова.</b>",
        "storage_error_prem": "<tg-emoji emoji-id=5429196733567504495>😵</tg-emoji> <b>Ошибка хранилища. Попробуйте снова.</b>",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue("NOTES", {}, "List of ur notes"),
        )
        self._storage_topic = None
        self._asset_channel = None
        self._premium = None

    async def _get_premium_status(self):
        if self._premium is None:
            me = await self._client.get_me()
            self._premium = getattr(me, "premium", False)
        return self._premium

    def _get_str(self, key):
        if self._premium:
            prem_key = f"{key}_prem"
            try:
                val = self.strings(prem_key)
                if val and not val.startswith("Unknown string"):
                    return val
            except Exception:
                pass
        return self.strings(key)

    async def client_ready(self):
        await self._get_premium_status()

        self._asset_channel = self._db.get("heroku.forums", "channel_id", None)

        if not self._asset_channel:
            logger.warning("[Note] heroku.forums channel_id not found in DB.")
            return

        try:
            self._storage_topic = await utils.asset_forum_topic(
                self._client,
                self._db,
                self._asset_channel,
                "Note Storage",
                description="Media storage for Note module.",
                icon_emoji_id=5188466187448650036,
            )
        except Exception as e:
            logger.error(f"[Note] Failed to create/get storage topic: {e}")

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

    async def _store_media_to_storage(self, reply):
        storage_chat_id = int(f"-100{self._asset_channel}")
        topic_id = self._storage_topic.id

        media_list = []
        if reply.grouped_id:
            album = await self._get_album_messages(reply.chat_id, reply.id, reply.grouped_id)
            media_list = [m.media for m in album if m.media]
        else:
            media_list = [reply.media]

        stored_ids = []
        if any(not self._is_albumable(m) for m in media_list) or len(media_list) == 1:
            for m in media_list:
                s = await self._send_with_flood_wait(
                    self._client.send_file,
                    storage_chat_id,
                    m,
                    reply_to=topic_id,
                )
                stored_ids.append(s.id)
        else:
            stored = await self._send_with_flood_wait(
                self._client.send_file,
                storage_chat_id,
                media_list,
                reply_to=topic_id,
            )
            stored_ids = [s.id for s in stored] if isinstance(stored, list) else [stored.id]

        return stored_ids

    async def _is_forum_chat(self, message):
        if message.is_private:
            return False
        try:
            chat = await message.get_chat()
            return getattr(chat, 'forum', False)
        except Exception:
            return False

    def _get_topic_id(self, message):
        reply_to = getattr(message, 'reply_to', None)
        if reply_to:
            top_id = getattr(reply_to, 'reply_to_top_id', None)
            if top_id:
                return top_id
            msg_id = getattr(reply_to, 'reply_to_msg_id', None)
            if msg_id and getattr(reply_to, 'forum_topic', False):
                return msg_id
        return None

    async def _get_messages_from_storage(self, msg_ids):
        storage_chat_id = int(f"-100{self._asset_channel}")
        return await self._send_with_flood_wait(
            self._client.get_messages,
            storage_chat_id,
            ids=msg_ids,
        )

    def _collect_ids_from_note(self, note_groups):
        all_ids = []
        for group in note_groups:
            if isinstance(group, list):
                for item in group:
                    if isinstance(item, int):
                        all_ids.append(item)
                    elif isinstance(item, list):
                        all_ids.extend(i for i in item if isinstance(i, int))
            elif isinstance(group, int):
                all_ids.append(group)
        return all_ids

    async def _delete_messages_from_storage(self, msg_ids):
        if not msg_ids:
            logger.debug("[Note] _delete_messages_from_storage: empty list, skipping...")
            return

        storage_chat_id = int(f"-100{self._asset_channel}")
        unique_ids = list(set(msg_ids))
        logger.debug(f"[Note] Removing media... {unique_ids} из {storage_chat_id}")

        await self._send_with_flood_wait(
            self._client.delete_messages,
            storage_chat_id,
            unique_ids,
            revoke=True,
        )

    @loader.command(
        ru_doc="Управление заметками",
        en_doc="Manage notes",
    )
    async def note(self, message):
        """Manage notes"""
        args = utils.get_args_raw(message).strip()
        await self._get_premium_status()
        prefix = self.get_prefix()

        if not self._storage_topic or not self._asset_channel:
            await utils.answer(message, self._get_str("storage_error"))
            return

        parts = args.split(maxsplit=1)
        if not parts:
            await utils.answer(
                message,
                self._get_str("help").format(prefix=prefix),
            )
            return

        cmd = parts[0].lower()
        is_forum = False
        topic_id = None

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

                stored_ids = await self._store_media_to_storage(reply)
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

                stored_ids = await self._store_media_to_storage(reply)
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
                ids_to_delete = self._collect_ids_from_note(notes[name])
                logger.debug(f"[Note] remove '{name}': Removing ID {ids_to_delete}")

                try:
                    await self._delete_messages_from_storage(ids_to_delete)
                except Exception as e:
                    logger.warning(f"[Note] Deleting error '{name}': {e}")

                del notes[name]
                self.config["NOTES"] = notes
                await utils.answer(message, self._get_str("removed").format(name=name))

            elif cmd == "rmall":
                notes = self.config["NOTES"]
                if not notes:
                    await utils.answer(message, self._get_str("rmall_empty"))
                    return
                all_ids = []
                for note_name, note_groups in notes.items():
                    ids = self._collect_ids_from_note(note_groups)
                    logger.debug(f"[Note] rmall: заметка '{note_name}' → ID {ids}")
                    all_ids.extend(ids)

                try:
                    await self._delete_messages_from_storage(all_ids)
                except Exception as e:
                    logger.warning(f"[Note] Total error: {e}")

                self.config["NOTES"] = {}
                await utils.answer(message, self._get_str("rmall_done"))

            elif cmd == "list":
                notes = self.config["NOTES"]
                if not notes:
                    await utils.answer(message, self._get_str("no_notes"))
                else:
                    n_list = "\n".join([f"• <code>{n}</code>" for n in sorted(notes.keys())])
                    await utils.answer(message, self._get_str("list_header").format(notes=n_list))

            else:
                name = args
                notes = self.config["NOTES"]
                if name not in notes:
                    await utils.answer(message, self._get_str("not_found").format(name=name))
                    return

                groups = notes[name]
                reply = await message.get_reply_message()
                is_forum = await self._is_forum_chat(message)
                topic_id = self._get_topic_id(message) if is_forum else None
                chat_id = message.chat_id
                reply_id = reply.id if reply else None

                await message.delete()

                is_first_group = True
                for group in groups:
                    msg_ids = group if isinstance(group, list) else [group]

                    fetched = await self._get_messages_from_storage(msg_ids)
                    if not fetched:
                        continue

                    if isinstance(fetched, list):
                        media_to_send = [m.media for m in fetched if m and m.media]
                    else:
                        media_to_send = [fetched.media] if fetched and fetched.media else []

                    if not media_to_send:
                        continue

                    if is_forum:
                        if is_first_group and reply_id:
                            reply_to = reply_id
                        elif topic_id:
                            reply_to = topic_id
                        else:
                            reply_to = None
                    else:
                        reply_to = reply_id if is_first_group else None

                    if any(not self._is_albumable(m) for m in media_to_send) or len(media_to_send) == 1:
                        for idx, m in enumerate(media_to_send):
                            r = reply_to if idx == 0 else (topic_id if is_forum and topic_id else None)
                            await self._client.send_file(chat_id, m, reply_to=r)
                    else:
                        await self._client.send_file(chat_id, media_to_send, reply_to=reply_to)

                    is_first_group = False

        except Exception as e:
            logger.exception(f"[Note] Error: {e}")
            try:
                await self._client.send_message(
                    message.chat_id,
                    self._get_str("storage_error"),
                    reply_to=topic_id if (is_forum and topic_id) else None,
                )
            except Exception:
                pass
