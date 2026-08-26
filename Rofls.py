__version__ = (1, 1, 0)
# meta developer: I_execute.t.me
# meta banner: https://raw.githubusercontent.com/i-execute/Modules/main/Storage/Rofls/MetaBanner.jpeg

import io
import os
import sys
import asyncio
import aiohttp
import tempfile
import subprocess
import logging

from PIL import Image, ImageDraw, ImageFont
from telethon.tl.functions.messages import UploadMediaRequest, GetStickerSetRequest
from telethon.tl.functions.stickers import (
    CreateStickerSetRequest,
    AddStickerToSetRequest,
    RemoveStickerFromSetRequest,
)
from telethon.tl.types import (
    InputPeerSelf,
    InputStickerSetItem,
    InputStickerSetShortName,
)
from telethon.errors import StickersetInvalidError
from telethon.utils import get_input_document

from .. import loader, utils

logger = logging.getLogger(__name__)

BASE_IMAGE_URL = "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/Rofls/Down.jpeg"
AVATAR_BOX = (607, 148, 1080, 581)

TG_COLORS = [
    (255, 80, 80),
    (255, 150, 0),
    (230, 185, 0),
    (50, 190, 100),
    (0, 150, 240),
    (120, 80, 230),
    (235, 90, 165),
]

FONT_PATHS = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
]


def _ensure_all_deps():
    for mod, pip_name in {"PIL": "Pillow", "aiohttp": "aiohttp"}.items():
        try:
            __import__(mod)
        except ImportError:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", pip_name, "-q"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
    try:
        __import__("petpetgif")
    except ImportError:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "setuptools<81", "-q"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.check_call(
            [
                sys.executable, "-m", "pip", "install",
                "git+https://github.com/camprevail/pet-pet-gif.git", "-q",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


_ensure_all_deps()

from petpetgif import petpet as ppg


def _jpeg_shakalize(img, quality):
    tmp = io.BytesIO()
    img.save(tmp, format="JPEG", quality=quality)
    tmp.seek(0)
    return Image.open(tmp).convert("RGB")


def _make_name_avatar(name, uid):
    left, top, right, bottom = AVATAR_BOX
    w = right - left
    h = bottom - top
    color = TG_COLORS[uid % 7]
    img = Image.new("RGB", (w, h), color)
    draw = ImageDraw.Draw(img)
    font = None
    for path in FONT_PATHS:
        try:
            font = ImageFont.truetype(path, 10)
            break
        except Exception:
            continue
    for font_size in range(120, 8, -2):
        f = font.font_variant(size=font_size) if font else ImageFont.load_default()
        bbox = draw.textbbox((0, 0), name, font=f)
        if (bbox[2] - bbox[0]) <= w - 24 and (bbox[3] - bbox[1]) <= h - 24:
            break
    bbox = draw.textbbox((0, 0), name, font=f)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    x = (w - tw) // 2 - bbox[0]
    y = (h - th) // 2 - bbox[1]
    draw.text((x, y), name, fill="white", font=f)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=95)
    buf.seek(0)
    return buf.read()


def _resolve_avatar(data):
    try:
        Image.open(io.BytesIO(data)).verify()
        return io.BytesIO(data)
    except Exception:
        fi = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
        fi.write(data)
        fi.close()
        fo = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
        fo.close()
        q = subprocess.run(
            [
                "ffmpeg", "-y", "-i", fi.name,
                "-frames:v", "1", "-vf", "scale=512:512", fo.name,
            ],
            capture_output=True,
        )
        os.unlink(fi.name)
        if q.returncode:
            return None
        src = io.BytesIO(open(fo.name, "rb").read())
        os.unlink(fo.name)
        return src


def _make_petpet_webm(src_buf):
    g = io.BytesIO()
    ppg.make(src_buf, g)
    g.seek(0)
    f = tempfile.NamedTemporaryFile(suffix=".gif", delete=False)
    f.write(g.read())
    f.close()
    w = f.name[:-4] + ".webm"
    q = subprocess.run(
        [
            "ffmpeg", "-y", "-i", f.name,
            "-vf", "scale=512:512:flags=lanczos",
            "-r", "30", "-t", "2.99", "-an",
            "-c:v", "libvpx-vp9", "-pix_fmt", "yuva420p",
            "-b:v", "350K", w,
        ],
        capture_output=True,
    )
    os.unlink(f.name)
    if q.returncode:
        return None
    return w


def _make_down_mp4(base_bytes, avatar_bytes):
    base = Image.open(io.BytesIO(base_bytes)).convert("RGB")
    avatar_img = Image.open(io.BytesIO(avatar_bytes)).convert("RGB")
    left, top, right, bottom = AVATAR_BOX
    box_w = right - left
    box_h = bottom - top
    avatar_img = avatar_img.resize((box_w, box_h), Image.LANCZOS)
    avatar_img = _jpeg_shakalize(avatar_img, quality=8)
    base.paste(avatar_img, (left, top))
    base = _jpeg_shakalize(base, quality=8)
    frame = base.quantize(colors=128, method=Image.Quantize.FASTOCTREE)
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as ff:
        frame.save(ff, format="PNG")
        frame_path = ff.name
    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as vf:
        video_path = vf.name
    try:
        w, h = frame.size
        w_even = w if w % 2 == 0 else w - 1
        h_even = h if h % 2 == 0 else h - 1
        subprocess.run(
            [
                "ffmpeg", "-y",
                "-loop", "1", "-i", frame_path,
                "-c:v", "libx264", "-t", "3",
                "-pix_fmt", "yuv420p",
                "-vf", f"fps=30,scale={w_even}:{h_even}",
                "-an", video_path,
            ],
            check=True, capture_output=True,
        )
        with open(video_path, "rb") as f:
            mp4_data = f.read()
    finally:
        os.unlink(frame_path)
        os.unlink(video_path)
    buf = io.BytesIO(mp4_data)
    buf.name = "downed.mp4"
    buf.seek(0)
    return buf


@loader.tds
class RoflsMod(loader.Module):
    """PetPet stickers and Down meme"""

    strings = {
        "name": "Rofls",
        "no_reply": "Reply to a user",
        "no_uid": "Cannot get user ID",
        "making": "Making petpet...",
        "no_avatar": "No avatar",
        "avatar_err": "Cannot process avatar",
        "ffmpeg_err": "FFmpeg error",
        "error": "Error: <code>{}</code>",
        "pack_none": "<b>PetPet Pack</b>\nPack does not exist yet.",
        "pack_info": "<b>PetPet Pack</b>\nStickers: <code>{count}</code>\nTracked: <code>{tracked}</code>\n<a href=\"{link}\">Open pack</a>",
        "pack_err": "<b>PetPet Pack</b>\nNo pack info available.",
        "btn_delete_pack": "Delete Pack",
        "btn_delete_last": "Delete Last Sticker",
        "btn_close": "Close",
        "pack_cleared": "<b>Pack cleared:</b> <code>{}</code>",
        "no_stickers": "No stickers to remove",
        "last_removed": "Last sticker removed",
    }

    strings_ru = {
        "no_reply": "Ответьте на сообщение пользователя",
        "no_uid": "Не удалось получить ID пользователя",
        "making": "Создаю petpet...",
        "no_avatar": "Нет аватара",
        "avatar_err": "Не удалось обработать аватар",
        "ffmpeg_err": "Ошибка FFmpeg",
        "error": "Ошибка: <code>{}</code>",
        "pack_none": "<b>PetPet Pack</b>\nПак ещё не создан.",
        "pack_info": "<b>PetPet Pack</b>\nСтикеров: <code>{count}</code>\nОтслеживается: <code>{tracked}</code>\n<a href=\"{link}\">Открыть пак</a>",
        "pack_err": "<b>PetPet Pack</b>\nНет данных о паке.",
        "btn_delete_pack": "Удалить пак",
        "btn_delete_last": "Удалить последний",
        "btn_close": "Закрыть",
        "pack_cleared": "<b>Пак очищен:</b> <code>{}</code>",
        "no_stickers": "Нет стикеров для удаления",
        "last_removed": "Последний стикер удалён",
    }

    async def _get_sn(self, client):
        me = await client.get_me()
        return f"petpetpackby_{me.id}"

    @loader.command(
        ru_doc="Ответьте на пользователя для petpet стикера, или без реплая для управления паком",
        en_doc="Reply to user for petpet sticker, or send without reply for management",
    )
    async def petcmd(self, message):
        """Reply to user for petpet sticker, or send without reply for management"""
        reply = await message.get_reply_message()

        if not reply:
            sn = await self._get_sn(message.client)
            sticker_ids = self.get("sticker_ids", [])
            try:
                ss = InputStickerSetShortName(sn)
                sticker_set = await message.client(GetStickerSetRequest(ss, 0))
                count = len(sticker_set.documents)
                link = f"https://t.me/addstickers/{sn}"
                text = self.strings["pack_info"].format(
                    count=count,
                    tracked=len(sticker_ids),
                    link=link,
                )
            except StickersetInvalidError:
                text = self.strings["pack_none"]
            except Exception:
                text = self.strings["pack_err"]

            await self.inline.form(
                text=text,
                message=message,
                reply_markup=[
                    [{"text": self.strings["btn_delete_pack"], "callback": self._cb_delete_pack}],
                    [{"text": self.strings["btn_delete_last"], "callback": self._cb_delete_last}],
                    [{"text": self.strings["btn_close"], "callback": self._cb_close}],
                ],
            )
            return

        sender = await reply.get_sender()
        uid = getattr(sender, "id", None)
        if not uid:
            await utils.answer(message, self.strings["no_uid"])
            return

        await utils.answer(message, self.strings["making"])

        try:
            a = io.BytesIO()
            r = await message.client.download_profile_photo(uid, file=a)
            if not r:
                await utils.answer(message, self.strings["no_avatar"])
                return

            a.seek(0)
            data = a.read()
            src = _resolve_avatar(data)
            if not src:
                await utils.answer(message, self.strings["avatar_err"])
                return

            loop = asyncio.get_event_loop()
            w = await loop.run_in_executor(None, _make_petpet_webm, src)
            if not w:
                await utils.answer(message, self.strings["ffmpeg_err"])
                return

            me = await message.client.get_me()
            sn = f"petpetpackby_{me.id}"

            u = await message.client.upload_file(w, file_name="sticker.webm")
            d = get_input_document(
                await message.client(UploadMediaRequest(InputPeerSelf(), u))
            )
            sticker = InputStickerSetItem(document=d, emoji="\U0001F43E")
            ss = InputStickerSetShortName(sn)

            new_doc_id = None

            try:
                old = await message.client(GetStickerSetRequest(ss, 0))
                old_ids = [x.id for x in old.documents]
                res = await message.client(AddStickerToSetRequest(ss, sticker))
                new_docs = [x for x in res.documents if x.id not in old_ids]
                if new_docs:
                    new_doc_id = new_docs[0]
            except StickersetInvalidError:
                res = await message.client(
                    CreateStickerSetRequest(
                        user_id=me,
                        title="PetPet by @Hotaru_modules",
                        short_name=sn,
                        stickers=[sticker],
                    )
                )
                if res.documents:
                    new_doc_id = res.documents[0]

            os.unlink(w)

            sticker_ids = self.get("sticker_ids", [])
            if new_doc_id:
                sticker_ids.append(new_doc_id.id)
                self.set("sticker_ids", sticker_ids)

            await message.delete()

            if new_doc_id:
                await message.client.send_file(
                    message.chat_id,
                    new_doc_id,
                    reply_to=reply.id,
                )
            else:
                await message.client.send_message(
                    message.chat_id,
                    f"Done: https://t.me/addstickers/{sn}",
                    reply_to=reply.id,
                )

        except Exception as e:
            await utils.answer(message, self.strings["error"].format(e))

    @loader.command(
        ru_doc="Ответьте на пользователя для down мема",
        en_doc="Reply to user for down meme",
    )
    async def dncmd(self, message):
        """Reply to user for down meme"""
        reply = await message.get_reply_message()
        if not reply:
            await utils.answer(message, self.strings["no_reply"])
            return

        sender = await reply.get_sender()

        try:
            await message.delete()

            base_bytes = await self._fetch_url(BASE_IMAGE_URL)
            avatar_bytes = await self._get_avatar(message.client, sender)

            if sender:
                first = getattr(sender, "first_name", "") or ""
                last = getattr(sender, "last_name", "") or ""
                name = (first + " " + last).strip() or getattr(sender, "username", None) or "??"
                uid = getattr(sender, "id", 0) or 0
            else:
                name, uid = "??", 0

            if not avatar_bytes:
                avatar_bytes = _make_name_avatar(name, uid)

            loop = asyncio.get_event_loop()
            mp4_buf = await loop.run_in_executor(
                None, _make_down_mp4, base_bytes, avatar_bytes
            )

            await message.client.send_file(
                message.chat_id,
                mp4_buf,
                caption=None,
                reply_to=reply.id,
                force_document=False,
            )

        except Exception as e:
            await message.client.send_message(
                message.chat_id,
                self.strings["error"].format(e),
                parse_mode="html",
            )

    async def _cb_delete_pack(self, call):
        client = self._client
        sn = await self._get_sn(client)
        ss = InputStickerSetShortName(sn)

        try:
            sticker_set = await client(GetStickerSetRequest(ss, 0))
            for doc in sticker_set.documents:
                try:
                    inp = get_input_document(doc)
                    await client(RemoveStickerFromSetRequest(inp))
                except Exception:
                    pass
        except StickersetInvalidError:
            pass
        except Exception as e:
            await call.edit(self.strings["error"].format(e))
            return

        self.set("sticker_ids", [])
        await call.edit(self.strings["pack_cleared"].format(sn))

    async def _cb_delete_last(self, call):
        client = self._client
        sticker_ids = self.get("sticker_ids", [])
        if not sticker_ids:
            await call.edit(self.strings["no_stickers"])
            return

        sn = await self._get_sn(client)
        ss = InputStickerSetShortName(sn)

        try:
            sticker_set = await client(GetStickerSetRequest(ss, 0))
            last_id = sticker_ids[-1]
            target_doc = None
            for doc in sticker_set.documents:
                if doc.id == last_id:
                    target_doc = doc
                    break
            if not target_doc:
                target_doc = sticker_set.documents[-1]

            inp = get_input_document(target_doc)
            await client(RemoveStickerFromSetRequest(inp))
            sticker_ids.pop()
            self.set("sticker_ids", sticker_ids)
            await call.edit(self.strings["last_removed"])
        except Exception as e:
            await call.edit(self.strings["error"].format(e))

    async def _cb_close(self, call):
        await call.delete()

    async def _fetch_url(self, url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                resp.raise_for_status()
                return await resp.read()

    async def _get_avatar(self, client, entity):
        try:
            buf = io.BytesIO()
            result = await client.download_profile_photo(entity, file=buf)
            if result is None:
                return None
            buf.seek(0)
            data = buf.read()
            return data if data else None
        except Exception:
            return None