# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (1, 0, 0)
# meta developer: Execute_forge.t.me

import asyncio
import hashlib
import io
import logging
import random
import re
import subprocess
import sys
import time
import uuid
from collections import OrderedDict


def _ensure_deps():
    try:
        __import__("PIL")
    except ImportError:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "Pillow", "-q"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


_ensure_deps()

from PIL import Image, ImageOps

from telethon.errors import (
    FloodWaitError,
    PackShortNameInvalidError,
    PackShortNameOccupiedError,
    PackTitleInvalidError,
    ShortNameInvalidError,
    ShortNameOccupiedError,
)
from telethon.tl.functions.messages import GetStickerSetRequest, UploadMediaRequest
from telethon.tl.functions.stickers import (
    AddStickerToSetRequest,
    CheckShortNameRequest,
    CreateStickerSetRequest,
)
from telethon.tl.types import (
    DocumentAttributeCustomEmoji,
    DocumentAttributeFilename,
    DocumentAttributeImageSize,
    DocumentAttributeSticker,
    DocumentAttributeVideo,
    InputDocument,
    InputMediaUploadedDocument,
    InputPeerSelf,
    InputStickerSetEmpty,
    InputStickerSetItem,
    InputStickerSetShortName,
    InputUserSelf,
    Message,
    MessageEntityCustomEmoji,
    MessageMediaPhoto,
)

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

TILE = 100
MAX_CELLS = 100
MAX_COLS = 10
MIN_SIDE = 2
CREATE_BATCH = 50
NET_RETRIES = 5
SRC_MAX = 2000
SRC_MAX_BYTES = 50 * 1024 * 1024
TILE_MAX_BYTES = 60 * 1024
EDIT_INTERVAL = 1.5
BLANK = "blank"
BLANK_ALT = "\u2b1c"
SHORT_RE = re.compile(r"^(?:https?://)?(?:t\.me/addemoji/)?([A-Za-z][A-Za-z0-9_]{0,63})$")
GRID_RE = re.compile(r"^(\d{1,3})\s*[xX\u0445\u0425\u00d7*]\s*(\d{1,3})$")
HEIF_BRANDS = (b"heic", b"heix", b"hevc", b"hevx", b"mif1", b"msf1", b"heim", b"heis", b"avif")
IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif", ".tif", ".tiff", ".heic", ".heif", ".avif")
PALETTE = (
    ("\u2b1b", (0, 0, 0)),
    ("\u2b1c", (255, 255, 255)),
    ("\U0001f7e5", (221, 46, 68)),
    ("\U0001f7e7", (244, 144, 12)),
    ("\U0001f7e8", (253, 203, 88)),
    ("\U0001f7e9", (120, 177, 89)),
    ("\U0001f7e6", (85, 172, 238)),
    ("\U0001f7ea", (170, 142, 214)),
    ("\U0001f7eb", (193, 105, 79)),
)


class _Stopped(Exception):
    pass


def _esc(text):
    return utils.escape_html(str(text if text is not None else ""))


def _utf16(text):
    return len(text.encode("utf-16-le")) // 2


def _is_heif(raw):
    return raw[4:8] == b"ftyp" and raw[8:12] in HEIF_BRANDS


def _load_heif_support():
    try:
        import pillow_heif
    except ImportError:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "pillow-heif", "-q"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=300,
        )
        import pillow_heif
    pillow_heif.register_heif_opener()


def _resize(img, size):
    if img.size == size:
        return img.copy()
    return img.convert("RGBa").resize(size, Image.LANCZOS).convert("RGBA")


def _open_image(raw):
    if _is_heif(raw):
        _load_heif_support()
    img = Image.open(io.BytesIO(raw))
    fmt = img.format or "IMAGE"
    img = ImageOps.exif_transpose(img)
    alpha = img.mode in ("RGBA", "LA", "PA", "RGBa", "La") or (img.mode == "P" and "transparency" in img.info)
    img = img.convert("RGBA")
    if alpha:
        alpha = img.getchannel("A").getextrema()[0] < 255
    size = img.size
    w, h = size
    if max(w, h) > SRC_MAX:
        ratio = SRC_MAX / max(w, h)
        img = _resize(img, (max(1, round(w * ratio)), max(1, round(h * ratio))))
    return img, fmt, alpha, size


def _auto_grid(w, h, side):
    if w >= h:
        cols = side
        rows = max(1, min(round(side * h / w), MAX_CELLS // cols))
    else:
        rows = side
        cols = max(1, min(round(side * w / h), MAX_CELLS // rows))
    return cols, rows


def _make_canvas(img, cols, rows, mode):
    width, height = cols * TILE, rows * TILE
    w, h = img.size
    if mode == "crop":
        scale = max(width / w, height / h)
        nw, nh = max(width, round(w * scale)), max(height, round(h * scale))
        res = _resize(img, (nw, nh))
        left, top = (nw - width) // 2, (nh - height) // 2
        canvas = res.crop((left, top, left + width, top + height))
    else:
        scale = min(width / w, height / h)
        nw = max(1, min(width, round(w * scale)))
        nh = max(1, min(height, round(h * scale)))
        canvas = Image.new("RGBA", (width, height), (0, 0, 0, 0))
        canvas.paste(_resize(img, (nw, nh)), ((width - nw) // 2, (height - nh) // 2))
    return canvas.convert("RGBa").convert("RGBA"), scale


def _encode_tile(tile):
    buf = io.BytesIO()
    tile.save(buf, format="WEBP", lossless=True, method=6)
    data = buf.getvalue()
    quality = 95
    while len(data) > TILE_MAX_BYTES and quality >= 50:
        buf = io.BytesIO()
        tile.save(buf, format="WEBP", quality=quality, method=6)
        data = buf.getvalue()
        quality -= 15
    return data


def _tile_alt(tile):
    r, g, b, a = tile.convert("RGBa").resize((1, 1), Image.BOX).convert("RGBA").getpixel((0, 0))
    if a == 0:
        return BLANK_ALT
    best = min(PALETTE, key=lambda p: (p[1][0] - r) ** 2 + (p[1][1] - g) ** 2 + (p[1][2] - b) ** 2)
    return best[0]


def _blank_tile():
    buf = io.BytesIO()
    Image.new("RGBA", (TILE, TILE), (0, 0, 0, 0)).save(buf, format="WEBP", lossless=True)
    return buf.getvalue()


def _build_tiles(img, cols, rows, mode, threshold):
    canvas, scale = _make_canvas(img, cols, rows, mode)
    cells = []
    tiles = OrderedDict()
    for r in range(rows):
        for c in range(cols):
            tile = canvas.crop((c * TILE, r * TILE, (c + 1) * TILE, (r + 1) * TILE))
            if tile.getchannel("A").getextrema()[1] <= threshold:
                cells.append(None)
                continue
            key = hashlib.sha1(tile.tobytes()).hexdigest()[:20]
            if key not in tiles:
                tiles[key] = (_encode_tile(tile), _tile_alt(tile))
            cells.append(key)
    return {"cells": cells, "tiles": tiles, "scale": scale}


def _grid_text(cells, cols, doc_map, blank_alt=BLANK_ALT):
    parts = []
    entities = []
    offset = 0
    for i, key in enumerate(cells):
        if i and i % cols == 0:
            parts.append("\n")
            offset += 1
        ref = doc_map.get(key if key is not None else BLANK)
        if ref:
            doc_id, alt = ref
            length = _utf16(alt)
            entities.append(MessageEntityCustomEmoji(offset=offset, length=length, document_id=doc_id))
        else:
            alt = blank_alt
            length = _utf16(alt)
        parts.append(alt)
        offset += length
    return "".join(parts), entities


@loader.tds
class Pic2Emoji(loader.Module):
    """Turn a picture into a custom emoji pack cut into a grid of up to 10x10 tiles"""

    strings = {
        "name": "Pic2Emoji",
        "_cls_doc": "Turn a picture into a custom emoji pack cut into a grid of up to 10x10 tiles",
        "_cfg_grid": "Default number of cells on the long side of the picture (2-10)",
        "_cfg_mode": "Default fill mode: fit keeps the whole picture with transparent margins, crop fills the grid and cuts the edges",
        "_cfg_alpha": "A tile is empty and skipped when none of its pixels is more opaque than this alpha value (0-254)",
        "_cfg_blank": "Add one shared transparent emoji for empty cells so the grid message keeps its shape",
        "_cfg_send": "Send the assembled grid message after the pack is created",
        "no_image": (
            "<b>No picture</b>\n"
            "<blockquote>Reply to a photo, an image file or a static sticker with <code>{prefix}pte</code>\n"
            "Optional grid: <code>{prefix}pte 8</code> or <code>{prefix}pte 7x13</code></blockquote>"
        ),
        "too_big": "<b>File is too big</b>\n<blockquote>Maximum is {max} MB</blockquote>",
        "bad_image": "<b>Cannot read the picture</b>\n<blockquote>{error}</blockquote>",
        "bad_grid": (
            "<b>Wrong grid</b>\n"
            "<blockquote>Use a number of cells on the long side (2-10) or CxR, "
            "where C is 1-10 columns and C*R is 2-100</blockquote>"
        ),
        "loading": "<b>Reading the picture...</b>",
        "setup": (
            "<b>Pic2Emoji</b>\n"
            "<blockquote><b>Picture:</b> <code>{w}x{h}</code>, {fmt}, transparency: {alpha}\n"
            "<b>Grid:</b> <code>{cols}x{rows}</code> ({cells} cells), {mode}\n"
            "<b>Emoji in pack:</b> <code>{unique}</code>{blank}\n"
            "<b>Empty cells:</b> <code>{empty}</code> (skipped)\n"
            "<b>Repeats:</b> <code>{dups}</code> (uploaded once)</blockquote>\n"
            "<blockquote><b>Title:</b> {title}\n"
            "<b>Link:</b> <code>https://t.me/addemoji/{short}</code></blockquote>"
        ),
        "blank_info": " + 1 transparent",
        "upscale_note": "\n<blockquote>The picture is smaller than the grid ({canvas}px) and will be upscaled, fewer cells look sharper</blockquote>",
        "alpha_yes": "yes",
        "alpha_no": "no",
        "mode_fit": "fit",
        "mode_crop": "crop",
        "btn_less": "- Cells",
        "btn_more": "+ Cells",
        "btn_mode_fit": "Mode: Fit",
        "btn_mode_crop": "Mode: Crop",
        "btn_title": "Title",
        "btn_short": "Link",
        "btn_create": "Create pack",
        "btn_cancel": "Cancel",
        "btn_stop": "Stop",
        "btn_back": "Back to settings",
        "btn_close": "Close",
        "btn_send_here": "Send grid here",
        "btn_send_saved": "Send grid to Saved",
        "input_grid": "Grid as CxR (e.g. 10x10, 7x13) or cells on the long side (2-10)",
        "input_title": "Pack title, 1-64 characters",
        "input_short": "Short name or https://t.me/addemoji/NAME link (latin letters, digits, underscores)",
        "note_title": "Title must be 1-64 characters",
        "note_short_invalid": "This short name is invalid",
        "note_short_taken": "This short name is already taken",
        "note_grid": "Wrong grid: C is 1-10 columns and C*R is 2-100",
        "busy": "Already working",
        "expired": "Session expired, run .pte again",
        "stopping": "Stopping...",
        "st_prepare": "<b>Cutting the picture...</b>",
        "st_upload": "<b>Uploading tiles</b>\n<blockquote>{bar} {done}/{total}</blockquote>",
        "st_create": "<b>Creating the pack</b>\n<blockquote>{bar} {done}/{total}\nFirst {batch} emoji in one request</blockquote>",
        "st_add": "<b>Adding to the pack</b>\n<blockquote>{bar} {done}/{total}</blockquote>",
        "st_finish": "<b>Assembling the grid...</b>",
        "floodwait": "\n<blockquote>Got floodwait, waiting {wait} + {rand} seconds</blockquote>",
        "reconnect": "\n<blockquote>Connection lost, retry {n}/{max} in {wait} seconds</blockquote>",
        "done": (
            "<b>Emoji pack created</b>\n"
            "<blockquote><b>Title:</b> {title}\n"
            "<b>Grid:</b> <code>{cols}x{rows}</code> ({cells} cells)\n"
            "<b>Emoji in pack:</b> <code>{count}</code>\n"
            "<b>Empty cells skipped:</b> <code>{empty}</code>\n"
            "<b>Repeats merged:</b> <code>{dups}</code>\n"
            "<b>Failed:</b> <code>{failed}</code></blockquote>\n"
            "<blockquote><a href='https://t.me/addemoji/{short}'>{short}</a></blockquote>"
        ),
        "grid_sent": "\n<blockquote>Grid message sent. Custom emoji in messages need Telegram Premium, without it they work only in Saved Messages</blockquote>",
        "grid_failed": "\n<blockquote>Could not send the grid: {error}</blockquote>",
        "stopped": "<b>Stopped</b>\n<blockquote>Uploaded tiles are kept, you can continue from the settings</blockquote>",
        "stopped_created": (
            "<b>Stopped</b>\n"
            "<blockquote>The pack is created but incomplete: {count} emoji</blockquote>\n"
            "<blockquote><a href='https://t.me/addemoji/{short}'>{short}</a></blockquote>"
        ),
        "failed": "<b>Error</b>\n<blockquote>{error}</blockquote>",
        "err_short_taken": "The short name is already taken, set another link",
        "err_short_invalid": "The short name is invalid, set another link",
        "err_title": "The title is invalid, set another one",
        "err_nothing": "No tile could be uploaded",
        "err_empty_picture": "The picture is fully transparent, nothing to cut",
    }

    strings_ru = {
        "_cls_doc": "Превращает картинку в набор кастом-эмодзи, нарезанный сеткой до 10x10",
        "_cfg_grid": "Сколько клеток по длинной стороне картинки по умолчанию (2-10)",
        "_cfg_mode": "Режим по умолчанию: fit вписывает картинку целиком с прозрачными полями, crop заполняет сетку и обрезает края",
        "_cfg_alpha": "Тайл считается пустым и пропускается, если ни один пиксель не плотнее этого значения альфы (0-254)",
        "_cfg_blank": "Добавлять один общий прозрачный эмодзи для пустых клеток, чтобы сетка в сообщении не съезжала",
        "_cfg_send": "Отправлять собранную сетку сообщением после создания набора",
        "no_image": (
            "<b>Нет картинки</b>\n"
            "<blockquote>Ответьте командой <code>{prefix}pte</code> на фото, файл-картинку или статичный стикер\n"
            "Сетку можно задать сразу: <code>{prefix}pte 8</code> или <code>{prefix}pte 7x13</code></blockquote>"
        ),
        "too_big": "<b>Файл слишком большой</b>\n<blockquote>Максимум {max} МБ</blockquote>",
        "bad_image": "<b>Не удалось прочитать картинку</b>\n<blockquote>{error}</blockquote>",
        "bad_grid": (
            "<b>Неверная сетка</b>\n"
            "<blockquote>Укажите число клеток по длинной стороне (2-10) или CxR, "
            "где C от 1 до 10 столбцов, а C*R от 2 до 100</blockquote>"
        ),
        "loading": "<b>Читаю картинку...</b>",
        "setup": (
            "<b>Pic2Emoji</b>\n"
            "<blockquote><b>Картинка:</b> <code>{w}x{h}</code>, {fmt}, прозрачность: {alpha}\n"
            "<b>Сетка:</b> <code>{cols}x{rows}</code> ({cells} клеток), {mode}\n"
            "<b>Эмодзи в наборе:</b> <code>{unique}</code>{blank}\n"
            "<b>Пустых клеток:</b> <code>{empty}</code> (пропускаются)\n"
            "<b>Повторов:</b> <code>{dups}</code> (загружаются один раз)</blockquote>\n"
            "<blockquote><b>Название:</b> {title}\n"
            "<b>Ссылка:</b> <code>https://t.me/addemoji/{short}</code></blockquote>"
        ),
        "blank_info": " + 1 прозрачный",
        "upscale_note": "\n<blockquote>Картинка меньше сетки ({canvas}px) и будет растянута, с меньшим числом клеток будет четче</blockquote>",
        "alpha_yes": "есть",
        "alpha_no": "нет",
        "mode_fit": "вписать",
        "mode_crop": "обрезать",
        "btn_less": "- Клетки",
        "btn_more": "+ Клетки",
        "btn_mode_fit": "Режим: Вписать",
        "btn_mode_crop": "Режим: Обрезать",
        "btn_title": "Название",
        "btn_short": "Ссылка",
        "btn_create": "Создать набор",
        "btn_cancel": "Отмена",
        "btn_stop": "Остановить",
        "btn_back": "К настройкам",
        "btn_close": "Закрыть",
        "btn_send_here": "Сетку сюда",
        "btn_send_saved": "Сетку в Избранное",
        "input_grid": "Сетка в виде CxR (например 10x10, 7x13) или число клеток по длинной стороне (2-10)",
        "input_title": "Название набора, 1-64 символа",
        "input_short": "Короткое имя или ссылка https://t.me/addemoji/NAME (латиница, цифры, подчеркивания)",
        "note_title": "Название должно быть от 1 до 64 символов",
        "note_short_invalid": "Такое короткое имя недопустимо",
        "note_short_taken": "Это короткое имя уже занято",
        "note_grid": "Неверная сетка: C от 1 до 10 столбцов, C*R от 2 до 100",
        "busy": "Уже выполняется",
        "expired": "Сессия устарела, запустите .pte заново",
        "stopping": "Останавливаю...",
        "st_prepare": "<b>Нарезаю картинку...</b>",
        "st_upload": "<b>Загружаю тайлы</b>\n<blockquote>{bar} {done}/{total}</blockquote>",
        "st_create": "<b>Создаю набор</b>\n<blockquote>{bar} {done}/{total}\nПервые {batch} эмодзи одним запросом</blockquote>",
        "st_add": "<b>Добавляю в набор</b>\n<blockquote>{bar} {done}/{total}</blockquote>",
        "st_finish": "<b>Собираю сетку...</b>",
        "floodwait": "\n<blockquote>Словил флудвейт, жду {wait} + {rand} секунд</blockquote>",
        "reconnect": "\n<blockquote>Связь потеряна, повтор {n}/{max} через {wait} секунд</blockquote>",
        "done": (
            "<b>Набор эмодзи создан</b>\n"
            "<blockquote><b>Название:</b> {title}\n"
            "<b>Сетка:</b> <code>{cols}x{rows}</code> ({cells} клеток)\n"
            "<b>Эмодзи в наборе:</b> <code>{count}</code>\n"
            "<b>Пустых клеток пропущено:</b> <code>{empty}</code>\n"
            "<b>Повторов объединено:</b> <code>{dups}</code>\n"
            "<b>Ошибок:</b> <code>{failed}</code></blockquote>\n"
            "<blockquote><a href='https://t.me/addemoji/{short}'>{short}</a></blockquote>"
        ),
        "grid_sent": "\n<blockquote>Сетка отправлена сообщением. Кастом-эмодзи в сообщениях требуют Telegram Premium, без него работают только в Избранном</blockquote>",
        "grid_failed": "\n<blockquote>Не удалось отправить сетку: {error}</blockquote>",
        "stopped": "<b>Остановлено</b>\n<blockquote>Загруженные тайлы сохранены, можно продолжить из настроек</blockquote>",
        "stopped_created": (
            "<b>Остановлено</b>\n"
            "<blockquote>Набор создан, но не полностью: {count} эмодзи</blockquote>\n"
            "<blockquote><a href='https://t.me/addemoji/{short}'>{short}</a></blockquote>"
        ),
        "failed": "<b>Ошибка</b>\n<blockquote>{error}</blockquote>",
        "err_short_taken": "Короткое имя уже занято, задайте другую ссылку",
        "err_short_invalid": "Короткое имя недопустимо, задайте другую ссылку",
        "err_title": "Недопустимое название, задайте другое",
        "err_nothing": "Не удалось загрузить ни одного тайла",
        "err_empty_picture": "Картинка полностью прозрачная, резать нечего",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "DEFAULT_GRID",
                10,
                lambda: self.strings["_cfg_grid"],
                validator=loader.validators.Integer(minimum=MIN_SIDE, maximum=MAX_COLS),
            ),
            loader.ConfigValue(
                "FILL_MODE",
                "fit",
                lambda: self.strings["_cfg_mode"],
                validator=loader.validators.Choice(["fit", "crop"]),
            ),
            loader.ConfigValue(
                "ALPHA_THRESHOLD",
                8,
                lambda: self.strings["_cfg_alpha"],
                validator=loader.validators.Integer(minimum=0, maximum=254),
            ),
            loader.ConfigValue(
                "BLANK_TILE",
                True,
                lambda: self.strings["_cfg_blank"],
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "SEND_GRID",
                True,
                lambda: self.strings["_cfg_send"],
                validator=loader.validators.Boolean(),
            ),
        )
        self._sessions = OrderedDict()

    async def client_ready(self, client, db):
        self._client = client

    async def on_unload(self):
        for sess in self._sessions.values():
            sess["stop"].set()
        self._sessions.clear()

    def _s(self, key, **kwargs):
        try:
            return self.strings[key].format(prefix=self.get_prefix(), **kwargs)
        except (KeyError, IndexError):
            return self.strings[key]

    def _source_doc(self, msg):
        if not isinstance(msg, Message) or not msg.media:
            return None, None
        if isinstance(msg.media, MessageMediaPhoto) and msg.photo:
            return "photo", None
        doc = msg.document
        if not doc:
            return None, None
        mime = (doc.mime_type or "").lower()
        name = ""
        for attr in doc.attributes:
            if isinstance(attr, DocumentAttributeFilename):
                name = (attr.file_name or "").lower()
            if isinstance(attr, DocumentAttributeVideo):
                return None, None
            if isinstance(attr, (DocumentAttributeSticker, DocumentAttributeCustomEmoji)) and mime != "image/webp":
                return None, None
        if mime.startswith("image/") or name.endswith(IMAGE_EXTS):
            return "document", doc
        return None, None

    def _parse_grid(self, text, w, h):
        text = (text or "").strip()
        if not text:
            return None
        if text.isdigit():
            side = int(text)
            if MIN_SIDE <= side <= MAX_COLS:
                return side, _auto_grid(w, h, side)
            return False
        m = GRID_RE.match(text)
        if not m:
            return False
        cols, rows = int(m.group(1)), int(m.group(2))
        if not (1 <= cols <= MAX_COLS and rows >= 1 and 2 <= cols * rows <= MAX_CELLS):
            return False
        return None, (cols, rows)

    def _bar(self, done, total, width=12):
        fill = int(width * done / total) if total else width
        return "\u2588" * fill + "\u2591" * (width - fill)

    async def _analyze(self, sess):
        sess["data"] = await asyncio.to_thread(
            _build_tiles,
            sess["img"],
            sess["cols"],
            sess["rows"],
            sess["mode"],
            int(self.config["ALPHA_THRESHOLD"]),
        )

    def _stats(self, sess):
        cells = sess["data"]["cells"]
        empty = sum(1 for c in cells if c is None)
        unique = len(sess["data"]["tiles"])
        dups = len(cells) - empty - unique
        blank = bool(empty and self.config["BLANK_TILE"])
        return empty, unique, dups, blank

    def _render(self, sess):
        empty, unique, dups, blank = self._stats(sess)
        w, h = sess["size"]
        text = self.strings["setup"].format(
            w=w,
            h=h,
            fmt=_esc(sess["fmt"]),
            alpha=self.strings["alpha_yes" if sess["alpha"] else "alpha_no"],
            cols=sess["cols"],
            rows=sess["rows"],
            cells=sess["cols"] * sess["rows"],
            mode=self.strings["mode_" + sess["mode"]],
            unique=unique,
            blank=self.strings["blank_info"] if blank else "",
            empty=empty,
            dups=dups,
            title=_esc(sess["title"]),
            short=_esc(sess["short"]),
        )
        if sess["data"]["scale"] > 1.5:
            text += self.strings["upscale_note"].format(canvas=f"{sess['cols'] * TILE}x{sess['rows'] * TILE}")
        if sess.get("note"):
            text += f"\n<blockquote><b>{_esc(sess['note'])}</b></blockquote>"
            sess["note"] = None
        sid = sess["sid"]
        mode_key = "btn_mode_fit" if sess["mode"] == "fit" else "btn_mode_crop"
        markup = [
            [
                {"text": self.strings["btn_less"], "callback": self._cb_side, "args": (sid, -1), "style": "primary"},
                {"text": f"{sess['cols']}x{sess['rows']}", "input": self.strings["input_grid"], "handler": self._in_grid, "args": (sid,), "style": "primary"},
                {"text": self.strings["btn_more"], "callback": self._cb_side, "args": (sid, 1), "style": "primary"},
            ],
            [{"text": self.strings[mode_key], "callback": self._cb_mode, "args": (sid,), "style": "primary"}],
            [
                {"text": self.strings["btn_title"], "input": self.strings["input_title"], "handler": self._in_title, "args": (sid,), "style": "primary"},
                {"text": self.strings["btn_short"], "input": self.strings["input_short"], "handler": self._in_short, "args": (sid,), "style": "primary"},
            ],
            [
                {"text": self.strings["btn_create"], "callback": self._cb_create, "args": (sid,), "style": "success"},
                {"text": self.strings["btn_cancel"], "callback": self._cb_cancel, "args": (sid,), "style": "danger"},
            ],
        ]
        return text, markup

    def _stop_markup(self, sid):
        return [[{"text": self.strings["btn_stop"], "callback": self._cb_stop, "args": (sid,), "style": "danger"}]]

    def _close_markup(self, sid):
        return [[{"text": self.strings["btn_close"], "callback": self._cb_cancel, "args": (sid,), "style": "danger"}]]

    def _back_markup(self, sid):
        return [
            [
                {"text": self.strings["btn_back"], "callback": self._cb_back, "args": (sid,), "style": "primary"},
                {"text": self.strings["btn_close"], "callback": self._cb_cancel, "args": (sid,), "style": "danger"},
            ]
        ]

    def _done_markup(self, sid):
        return [
            [
                {"text": self.strings["btn_send_here"], "callback": self._cb_send, "args": (sid, False), "style": "primary"},
                {"text": self.strings["btn_send_saved"], "callback": self._cb_send, "args": (sid, True), "style": "primary"},
            ],
            [{"text": self.strings["btn_close"], "callback": self._cb_cancel, "args": (sid,), "style": "danger"}],
        ]

    async def _edit(self, sess, text, markup, call=None):
        target = call or sess["form"]
        try:
            await target.edit(text, reply_markup=markup)
        except Exception as e:
            logger.debug("[Pic2Emoji] edit failed: %s", e)

    async def _show(self, sess, call=None):
        text, markup = self._render(sess)
        await self._edit(sess, text, markup, call)

    async def _progress(self, sess, text, force=False):
        now = time.monotonic()
        if not force and now - sess["last_edit"] < EDIT_INTERVAL:
            return
        sess["last_edit"] = now
        await self._edit(sess, text, self._stop_markup(sess["sid"]))

    async def _get(self, call, sid, check_busy=True):
        sess = self._sessions.get(sid)
        if not sess:
            await call.answer(self.strings["expired"], show_alert=True)
            return None
        if check_busy and sess["busy"]:
            await call.answer(self.strings["busy"])
            return None
        self._sessions.move_to_end(sid)
        return sess

    async def _sleep(self, sess, seconds):
        try:
            await asyncio.wait_for(sess["stop"].wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass
        if sess["stop"].is_set():
            raise _Stopped()

    async def _retry(self, sess, func, text):
        net_fails = 0
        while True:
            if sess["stop"].is_set():
                raise _Stopped()
            try:
                return await func()
            except FloodWaitError as e:
                wait_secs = e.seconds
                rand_secs = random.randint(1, 10)
                logger.info("[Pic2Emoji] FloodWait %ss, sleeping %ss", wait_secs, wait_secs + rand_secs)
                await self._progress(sess, text + self.strings["floodwait"].format(wait=wait_secs, rand=rand_secs), force=True)
                await self._sleep(sess, wait_secs + rand_secs)
            except (ConnectionError, asyncio.TimeoutError) as e:
                net_fails += 1
                if net_fails > NET_RETRIES:
                    raise
                wait_secs = 5 * net_fails
                logger.info("[Pic2Emoji] network error %s, retry %s in %ss", e, net_fails, wait_secs)
                await self._progress(sess, text + self.strings["reconnect"].format(n=net_fails, max=NET_RETRIES, wait=wait_secs), force=True)
                await self._sleep(sess, wait_secs)
            sess["last_edit"] = 0

    async def _upload_tile(self, data, alt):
        uploaded = await self._client.upload_file(data, file_name="emoji.webp")
        media = InputMediaUploadedDocument(
            file=uploaded,
            mime_type="image/webp",
            attributes=[
                DocumentAttributeFilename(file_name="emoji.webp"),
                DocumentAttributeImageSize(w=TILE, h=TILE),
                DocumentAttributeCustomEmoji(alt=alt, stickerset=InputStickerSetEmpty(), free=False, text_color=False),
            ],
        )
        result = await self._client(UploadMediaRequest(peer=InputPeerSelf(), media=media))
        d = result.document
        return InputDocument(id=d.id, access_hash=d.access_hash, file_reference=d.file_reference)

    def _create_request(self, sess, items):
        return CreateStickerSetRequest(
            user_id=InputUserSelf(),
            title=sess["title"],
            short_name=sess["short"],
            stickers=[item for _, item in items],
            emojis=True,
        )

    async def _create(self, sess, items, total):
        batch = items[:CREATE_BATCH]
        text = self.strings["st_create"].format(bar=self._bar(0, total), done=0, total=total, batch=len(batch))
        await self._progress(sess, text, force=True)
        try:
            await self._retry(sess, lambda: self._client(self._create_request(sess, batch)), text)
            return batch
        except (_Stopped, PackShortNameOccupiedError, ShortNameOccupiedError, PackShortNameInvalidError, ShortNameInvalidError, PackTitleInvalidError):
            raise
        except Exception as e:
            if len(batch) == 1:
                raise
            logger.warning("[Pic2Emoji] batch create failed (%s), falling back to one by one", e)
        batch = items[:1]
        text = self.strings["st_create"].format(bar=self._bar(0, total), done=0, total=total, batch=1)
        await self._retry(sess, lambda: self._client(self._create_request(sess, batch)), text)
        return batch

    async def _run(self, sess):
        data = sess["data"]
        empty, unique, dups, blank = self._stats(sess)
        order = list(data["tiles"].items())
        if blank:
            order.append((BLANK, (_blank_tile(), BLANK_ALT)))
        total = len(order)
        alts = {key: alt for key, (_, alt) in order}
        uploaded = sess["uploaded"]
        failed = 0
        for i, (key, (raw, alt)) in enumerate(order, 1):
            if key in uploaded:
                continue
            text = self.strings["st_upload"].format(bar=self._bar(i - 1, total), done=i - 1, total=total)
            await self._progress(sess, text, force=i == 1)
            try:
                uploaded[key] = await self._retry(sess, lambda: self._upload_tile(raw, alt), text)
            except _Stopped:
                raise
            except Exception as e:
                failed += 1
                logger.error("[Pic2Emoji] upload tile %s/%s failed: %s", i, total, e)
        items = [(key, InputStickerSetItem(document=uploaded[key], emoji=alts[key])) for key, _ in order if key in uploaded]
        if not items:
            raise RuntimeError(self.strings["err_nothing"])
        try:
            added = [key for key, _ in await self._create(sess, items, len(items))]
        except (PackShortNameOccupiedError, ShortNameOccupiedError):
            raise RuntimeError(self.strings["err_short_taken"])
        except (PackShortNameInvalidError, ShortNameInvalidError):
            raise RuntimeError(self.strings["err_short_invalid"])
        except PackTitleInvalidError:
            raise RuntimeError(self.strings["err_title"])
        sess["created"] = True
        sess["added"] = added
        stickerset = InputStickerSetShortName(short_name=sess["short"])
        for key, item in items[len(added):]:
            done = len(added)
            text = self.strings["st_add"].format(bar=self._bar(done, len(items)), done=done, total=len(items))
            await self._progress(sess, text)
            try:
                await self._retry(sess, lambda: self._client(AddStickerToSetRequest(stickerset=stickerset, sticker=item)), text)
                added.append(key)
            except _Stopped:
                raise
            except Exception as e:
                failed += 1
                logger.error("[Pic2Emoji] add emoji %s failed: %s", key, e)
        await self._progress(sess, self.strings["st_finish"], force=True)
        result = await self._retry(
            sess,
            lambda: self._client(GetStickerSetRequest(stickerset=stickerset, hash=0)),
            self.strings["st_finish"],
        )
        doc_map = {}
        for key, doc in zip(added, result.documents):
            alt = alts[key]
            for attr in doc.attributes:
                if isinstance(attr, (DocumentAttributeCustomEmoji, DocumentAttributeSticker)) and attr.alt:
                    alt = attr.alt
            doc_map[key] = (doc.id, alt)
        sess["doc_map"] = doc_map
        sess["failed"] = failed
        return empty, dups

    async def _send_grid(self, sess, saved=False):
        text, entities = _grid_text(sess["data"]["cells"], sess["cols"], sess["doc_map"])
        if saved:
            await self._client.send_message("me", text, formatting_entities=entities, link_preview=False)
        else:
            await self._client.send_message(
                sess["chat_id"],
                text,
                formatting_entities=entities,
                reply_to=sess["reply_id"],
                link_preview=False,
            )

    def _done_text(self, sess, empty, dups):
        return self.strings["done"].format(
            title=_esc(sess["title"]),
            cols=sess["cols"],
            rows=sess["rows"],
            cells=sess["cols"] * sess["rows"],
            count=len(sess["doc_map"]),
            empty=empty,
            dups=dups,
            failed=sess["failed"],
            short=_esc(sess["short"]),
        )

    async def _job(self, sess):
        sid = sess["sid"]
        try:
            empty, dups = await self._run(sess)
            text = self._done_text(sess, empty, dups)
            if self.config["SEND_GRID"]:
                try:
                    await self._send_grid(sess)
                    text += self.strings["grid_sent"]
                except Exception as e:
                    logger.warning("[Pic2Emoji] grid send failed: %s", e)
                    text += self.strings["grid_failed"].format(error=_esc(e))
            sess["done"] = True
            sess["done_text"] = self._done_text(sess, empty, dups)
            sess["img"] = None
            sess["uploaded"] = {}
            sess["data"]["tiles"] = OrderedDict()
            await self._edit(sess, text, self._done_markup(sid))
        except _Stopped:
            if sess.get("created"):
                sess["img"] = None
                await self._edit(
                    sess,
                    self.strings["stopped_created"].format(count=len(sess.get("added") or []), short=_esc(sess["short"])),
                    self._close_markup(sid),
                )
            else:
                await self._edit(sess, self.strings["stopped"], self._back_markup(sid))
        except Exception as e:
            logger.exception("[Pic2Emoji] job failed")
            markup = self._close_markup(sid) if sess.get("created") else self._back_markup(sid)
            await self._edit(sess, self.strings["failed"].format(error=_esc(e)), markup)
        finally:
            sess["busy"] = False
            sess["stop"] = asyncio.Event()
            sess["task"] = None

    async def _cb_side(self, call: InlineCall, sid, delta):
        sess = await self._get(call, sid)
        if not sess or sess["img"] is None:
            return
        side = min(MAX_COLS, max(MIN_SIDE, sess["side"] + delta))
        cols, rows = _auto_grid(*sess["img"].size, side)
        if (cols, rows) == (sess["cols"], sess["rows"]):
            side = min(MAX_COLS, max(MIN_SIDE, side + delta))
            cols, rows = _auto_grid(*sess["img"].size, side)
        if (cols, rows) == (sess["cols"], sess["rows"]):
            await call.answer()
            return
        sess["side"], sess["cols"], sess["rows"] = side, cols, rows
        await self._analyze(sess)
        await self._show(sess, call)

    async def _cb_mode(self, call: InlineCall, sid):
        sess = await self._get(call, sid)
        if not sess or sess["img"] is None:
            return
        sess["mode"] = "crop" if sess["mode"] == "fit" else "fit"
        await self._analyze(sess)
        await self._show(sess, call)

    async def _in_grid(self, call: InlineCall, query, sid):
        sess = await self._get(call, sid)
        if not sess or sess["img"] is None:
            return
        parsed = self._parse_grid(query, *sess["img"].size)
        if not parsed:
            sess["note"] = self.strings["note_grid"]
        else:
            side, (cols, rows) = parsed
            sess["side"] = side or max(cols, rows)
            sess["cols"], sess["rows"] = cols, rows
            await self._analyze(sess)
        await self._show(sess, call)

    async def _in_title(self, call: InlineCall, query, sid):
        sess = await self._get(call, sid)
        if not sess or sess["img"] is None:
            return
        title = (query or "").strip()
        if not 1 <= len(title) <= 64:
            sess["note"] = self.strings["note_title"]
        else:
            sess["title"] = title
        await self._show(sess, call)

    async def _in_short(self, call: InlineCall, query, sid):
        sess = await self._get(call, sid)
        if not sess or sess["img"] is None:
            return
        m = SHORT_RE.match((query or "").strip())
        short = m.group(1) if m else None
        if not short or "__" in short:
            sess["note"] = self.strings["note_short_invalid"]
            await self._show(sess, call)
            return
        try:
            free = await self._client(CheckShortNameRequest(short_name=short))
        except ShortNameOccupiedError:
            free = False
        except ShortNameInvalidError:
            sess["note"] = self.strings["note_short_invalid"]
            await self._show(sess, call)
            return
        except Exception as e:
            logger.debug("[Pic2Emoji] check short name failed: %s", e)
            free = True
        if not free:
            sess["note"] = self.strings["note_short_taken"]
        else:
            sess["short"] = short
        await self._show(sess, call)

    async def _cb_create(self, call: InlineCall, sid):
        sess = await self._get(call, sid)
        if not sess or sess["img"] is None:
            return
        if not sess["data"]["tiles"]:
            sess["note"] = self.strings["err_empty_picture"]
            await self._show(sess, call)
            return
        sess["busy"] = True
        sess["last_edit"] = 0
        await self._progress(sess, self.strings["st_prepare"], force=True)
        sess["task"] = asyncio.ensure_future(self._job(sess))

    async def _cb_stop(self, call: InlineCall, sid):
        sess = self._sessions.get(sid)
        if not sess:
            await call.answer(self.strings["expired"], show_alert=True)
            return
        sess["stop"].set()
        await call.answer(self.strings["stopping"])

    async def _cb_back(self, call: InlineCall, sid):
        sess = await self._get(call, sid)
        if not sess or sess["img"] is None or sess.get("created"):
            return
        await self._show(sess, call)

    async def _cb_send(self, call: InlineCall, sid, saved):
        sess = await self._get(call, sid)
        if not sess or not sess.get("doc_map"):
            return
        text = sess["done_text"]
        try:
            await self._send_grid(sess, saved)
            text += self.strings["grid_sent"]
        except Exception as e:
            text += self.strings["grid_failed"].format(error=_esc(e))
        await self._edit(sess, text, self._done_markup(sid), call)

    async def _cb_cancel(self, call: InlineCall, sid):
        sess = self._sessions.pop(sid, None)
        if sess:
            sess["stop"].set()
        try:
            await call.delete()
        except Exception:
            pass

    @loader.command(
        ru_doc="Реплай на картинку - создать набор кастом-эмодзи из сетки до 10x10. Можно сразу задать сетку: pte 8 или pte 7x13",
        en_doc="Reply to a picture - create a custom emoji pack from a grid of up to 10x10. Grid can be set right away: pte 8 or pte 7x13",
    )
    async def ptecmd(self, message: Message):
        """Reply to a picture - create a custom emoji pack from a grid of up to 10x10. Grid can be set right away: pte 8 or pte 7x13"""
        args = (utils.get_args_raw(message) or "").strip()
        reply = await message.get_reply_message()
        kind, doc = self._source_doc(reply)
        if not kind:
            await utils.answer(message, self._s("no_image"))
            return
        if doc is not None and (doc.size or 0) > SRC_MAX_BYTES:
            await utils.answer(message, self._s("too_big", max=SRC_MAX_BYTES // 1024 // 1024))
            return
        message = await utils.answer(message, self._s("loading"))
        try:
            raw = await reply.download_media(file=bytes)
            img, fmt, alpha, size = await asyncio.to_thread(_open_image, raw)
        except Exception as e:
            await utils.answer(message, self._s("bad_image", error=_esc(e)))
            return
        del raw
        side = int(self.config["DEFAULT_GRID"])
        cols, rows = _auto_grid(*img.size, side)
        if args:
            parsed = self._parse_grid(args, *img.size)
            if not parsed:
                await utils.answer(message, self._s("bad_grid"))
                return
            parsed_side, (cols, rows) = parsed
            side = parsed_side or max(cols, rows)
        sid = uuid.uuid4().hex[:10]
        sess = {
            "sid": sid,
            "chat_id": message.chat_id,
            "reply_id": reply.id,
            "img": img,
            "fmt": fmt,
            "alpha": alpha,
            "size": size,
            "side": side,
            "cols": cols,
            "rows": rows,
            "mode": self.config["FILL_MODE"],
            "title": f"Pic2Emoji {sid[:4]}",
            "short": f"pic2emoji_{sid}",
            "uploaded": {},
            "busy": False,
            "done": False,
            "created": False,
            "stop": asyncio.Event(),
            "last_edit": 0,
            "note": None,
            "task": None,
            "form": None,
        }
        await self._analyze(sess)
        text, markup = self._render(sess)
        form = await self.inline.form(text=text, message=message, reply_markup=markup, silent=True)
        if not form:
            return
        sess["form"] = form
        self._sessions[sid] = sess
        while len(self._sessions) > 10:
            _, old = self._sessions.popitem(last=False)
            old["stop"].set()