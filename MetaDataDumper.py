# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (1, 2, 0)
# meta developer: Execute_forge.t.me

import asyncio
import html
import io
import json
import logging
import math
import mimetypes
import os
import plistlib
import re
import shutil
import struct
import tarfile
import tempfile
import time
import contextlib
import hashlib
import urllib.parse
import urllib.request
import uuid
import zipfile
import zlib
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

from telethon.tl.types import (
    DocumentAttributeAnimated,
    DocumentAttributeAudio,
    DocumentAttributeFilename,
    DocumentAttributeSticker,
    DocumentAttributeVideo,
    MessageMediaDocument,
    MessageMediaPhoto,
)

from .. import loader, utils
from ..inline.types import InlineCall

logger = logging.getLogger(__name__)

CHUNK = 512 * 1024
PAGE_LIMIT = 3300
ET_GROUP_CAP = 120
MOOV_CAP = 96 * 1024 * 1024
EXIFTOOL_URLS = (
    "https://sourceforge.net/projects/exiftool/files/Image-ExifTool-{v}.tar.gz/download",
    "https://github.com/exiftool/exiftool/archive/refs/tags/{v}.tar.gz",
)
EXIFTOOL_VER_URL = "https://exiftool.org/ver.txt"
EXIFTOOL_FALLBACK_VER = "13.59"
EXIFTOOL_HOSTS = ("sourceforge.net", "github.com", "codeload.github.com", "exiftool.org")
EXIFTOOL_SHA256 = ""
MP4_EPOCH = datetime(1904, 1, 1, tzinfo=timezone.utc)
WAVE_BARS = "\u2581\u2582\u2583\u2584\u2585\u2586\u2587\u2588"

NATURAL_ORDER = (
    0, 1, 8, 16, 9, 2, 3, 10, 17, 24, 32, 25, 18, 11, 4, 5,
    12, 19, 26, 33, 40, 48, 41, 34, 27, 20, 13, 6, 7, 14, 21, 28,
    35, 42, 49, 56, 57, 50, 43, 36, 29, 22, 15, 23, 30, 37, 44, 51,
    58, 59, 52, 45, 38, 31, 39, 46, 53, 60, 61, 54, 47, 55, 62, 63,
)

ANNEX_K_LUMA = (
    16, 11, 10, 16, 24, 40, 51, 61,
    12, 12, 14, 19, 26, 58, 60, 55,
    14, 13, 16, 24, 40, 57, 69, 56,
    14, 17, 22, 29, 51, 87, 80, 62,
    18, 22, 37, 56, 68, 109, 103, 77,
    24, 35, 55, 64, 81, 104, 113, 92,
    49, 64, 78, 87, 103, 121, 120, 101,
    72, 92, 95, 98, 112, 100, 103, 99,
)

ANNEX_K_CHROMA = (
    17, 18, 24, 47, 99, 99, 99, 99,
    18, 21, 26, 66, 99, 99, 99, 99,
    24, 26, 56, 99, 99, 99, 99, 99,
    47, 66, 99, 99, 99, 99, 99, 99,
    99, 99, 99, 99, 99, 99, 99, 99,
    99, 99, 99, 99, 99, 99, 99, 99,
    99, 99, 99, 99, 99, 99, 99, 99,
    99, 99, 99, 99, 99, 99, 99, 99,
)

EXIF_TAGS = {
    0x0100: "ImageWidth", 0x0101: "ImageHeight", 0x010E: "ImageDescription",
    0x010F: "Make", 0x0110: "Model", 0x0112: "Orientation",
    0x011A: "XResolution", 0x011B: "YResolution", 0x0128: "ResolutionUnit",
    0x0131: "Software", 0x0132: "ModifyDate", 0x013B: "Artist",
    0x013C: "HostComputer", 0x0213: "YCbCrPositioning", 0x8298: "Copyright",
    0x829A: "ExposureTime", 0x829D: "FNumber", 0x8822: "ExposureProgram",
    0x8827: "ISO", 0x8830: "SensitivityType", 0x9000: "ExifVersion",
    0x9003: "DateTimeOriginal", 0x9004: "CreateDate", 0x9010: "OffsetTime",
    0x9011: "OffsetTimeOriginal", 0x9012: "OffsetTimeDigitized",
    0x9201: "ShutterSpeedValue", 0x9202: "ApertureValue", 0x9203: "BrightnessValue",
    0x9204: "ExposureCompensation", 0x9205: "MaxApertureValue", 0x9207: "MeteringMode",
    0x9209: "Flash", 0x920A: "FocalLength", 0x9214: "SubjectArea",
    0x927C: "MakerNote", 0x9286: "UserComment", 0x9290: "SubSecTime",
    0x9291: "SubSecTimeOriginal", 0x9292: "SubSecTimeDigitized",
    0xA000: "FlashpixVersion", 0xA001: "ColorSpace", 0xA002: "ExifImageWidth",
    0xA003: "ExifImageHeight", 0xA217: "SensingMethod", 0xA301: "SceneType",
    0xA401: "CustomRendered", 0xA402: "ExposureMode", 0xA403: "WhiteBalance",
    0xA404: "DigitalZoomRatio", 0xA405: "FocalLengthIn35mmFormat",
    0xA406: "SceneCaptureType", 0xA420: "ImageUniqueID", 0xA430: "OwnerName",
    0xA431: "SerialNumber", 0xA432: "LensInfo", 0xA433: "LensMake",
    0xA434: "LensModel", 0xA435: "LensSerialNumber", 0xA460: "CompositeImage",
    0x8769: "ExifOffset", 0x8825: "GPSInfo", 0xA005: "InteropOffset",
    0x0103: "Compression", 0x0201: "ThumbnailOffset", 0x0202: "ThumbnailLength",
    0x9C9B: "XPTitle", 0x9C9C: "XPComment", 0x9C9D: "XPAuthor",
    0x9C9E: "XPKeywords", 0x9C9F: "XPSubject", 0x4746: "Rating",
}

GPS_TAGS = {
    0x00: "GPSVersionID", 0x01: "GPSLatitudeRef", 0x02: "GPSLatitude",
    0x03: "GPSLongitudeRef", 0x04: "GPSLongitude", 0x05: "GPSAltitudeRef",
    0x06: "GPSAltitude", 0x07: "GPSTimeStamp", 0x08: "GPSSatellites",
    0x09: "GPSStatus", 0x0A: "GPSMeasureMode", 0x0B: "GPSDOP",
    0x0C: "GPSSpeedRef", 0x0D: "GPSSpeed", 0x0E: "GPSTrackRef", 0x0F: "GPSTrack",
    0x10: "GPSImgDirectionRef", 0x11: "GPSImgDirection", 0x12: "GPSMapDatum",
    0x17: "GPSDestBearingRef", 0x18: "GPSDestBearing", 0x1B: "GPSProcessingMethod",
    0x1D: "GPSDateStamp", 0x1F: "GPSHPositioningError",
}

QT_KEYS = {
    "\xa9too": "Encoder", "\xa9swr": "Software", "\xa9mak": "Make",
    "\xa9mod": "Model", "\xa9xyz": "GPSCoordinates", "\xa9day": "Date",
    "\xa9nam": "Title", "\xa9ART": "Artist", "\xa9cmt": "Comment",
    "\xa9des": "Description", "\xa9enc": "EncodedBy", "\xa9fmt": "Format",
    "\xa9inf": "Information", "\xa9src": "Source", "\xa9swf": "Creator",
    "\xa9wrt": "Composer", "\xa9alb": "Album", "\xa9gen": "Genre",
    "\xa9cpy": "Copyright", "cprt": "Copyright", "auth": "Author",
    "desc": "Description", "ldes": "LongDescription", "\xa9lyr": "Lyrics",
}

ISO_CONTAINERS = {
    b"moov", b"trak", b"mdia", b"minf", b"stbl", b"udta", b"edts",
    b"dinf", b"mvex", b"tref", b"gmhd", b"sinf", b"schi",
}

UUID_NAMES = {
    "be7acfcb97a942e89c71999491e3afac": "XMP",
    "50524f4621d24fcebb88695cfac9c740": "Sony PROF",
    "55534d5421d24fcebb88695cfac9c740": "Sony USMT",
    "85c0b687820f11e08111f4ce462b6a48": "Canon CNTH",
    "ffcc8263f8554a938814587a02521fdd": "Spherical Video",
    "05370497f4d94f8d8e1f3e8cf8f0e5fa": "GoPro GPMF",
}

HDLR_HINTS = (
    ("VideoHandle", "Android MP4 writer (VideoHandle)"),
    ("SoundHandle", "Android MP4 writer (SoundHandle)"),
    ("Core Media", "Apple AVFoundation / CoreMedia"),
    ("VideoHandler", "FFmpeg / libavformat default handler"),
    ("SoundHandler", "FFmpeg / libavformat default handler"),
    ("ISO Media file produced by Google", "Google / YouTube pipeline"),
    ("L-SMASH", "L-SMASH muxer"),
    ("GPAC", "GPAC / MP4Box"),
    ("Mainconcept", "MainConcept (Adobe Premiere / Media Encoder)"),
    ("Apple Video Media Handler", "Apple QuickTime"),
    ("Apple Sound Media Handler", "Apple QuickTime"),
    ("Bento4", "Bento4"),
    ("DJI", "DJI camera"),
    ("GoPro", "GoPro camera"),
    ("Ambarella", "Ambarella camera SoC (action cam / dashcam)"),
    ("Video Media Handler", "Generic QuickTime writer"),
)

SOFTWARE_RULES = (
    (r"adobe photoshop lightroom|lightroom", "Adobe Lightroom", 3),
    (r"adobe photoshop", "Adobe Photoshop", 3),
    (r"photoshop express", "Photoshop Express", 3),
    (r"adobe illustrator", "Adobe Illustrator", 3),
    (r"adobe indesign", "Adobe InDesign", 3),
    (r"adobe premiere", "Adobe Premiere", 3),
    (r"after effects", "Adobe After Effects", 3),
    (r"adobe firefly", "Adobe Firefly (AI)", 3),
    (r"\bgimp\b", "GIMP", 3),
    (r"snapseed", "Snapseed", 3),
    (r"picsart", "PicsArt", 3),
    (r"\bvsco\b", "VSCO", 3),
    (r"meitu", "Meitu", 3),
    (r"facetune", "Facetune", 3),
    (r"capcut", "CapCut", 3),
    (r"inshot", "InShot", 3),
    (r"\bcanva\b", "Canva", 3),
    (r"pixelmator", "Pixelmator", 3),
    (r"affinity", "Affinity", 3),
    (r"paint\.net", "Paint.NET", 3),
    (r"darktable", "darktable", 3),
    (r"rawtherapee", "RawTherapee", 3),
    (r"capture one", "Capture One", 3),
    (r"luminar", "Luminar", 3),
    (r"instagram", "Instagram", 3),
    (r"hdr\+", "Google Pixel camera (HDR+)", 3),
    (r"^picasa", "Picasa", 2),
    (r"windows photo editor|microsoft windows photo", "Windows Photos", 2),
    (r"^microsoft windows", "Microsoft Windows", 2),
    (r"^macos|^mac os x", "macOS", 2),
    (r"handbrake", "HandBrake", 3),
    (r"davinci resolve", "DaVinci Resolve", 3),
    (r"final cut", "Final Cut Pro", 3),
    (r"imovie", "iMovie", 3),
    (r"kinemaster", "KineMaster", 3),
    (r"obs studio|\bobs\b", "OBS Studio", 2),
    (r"^lavf|^lavc|ffmpeg", "FFmpeg", 2),
    (r"stable diffusion|automatic1111|comfyui|invokeai|novelai|midjourney|dall-?e", "AI image generator", 3),
    (r"chatgpt|openai", "OpenAI (AI)", 3),
)

PDF_RULES = (
    (r"skia/pdf", "Chromium print-to-PDF (Chrome / Edge / Electron)", 3),
    (r"ios version .*quartz pdfcontext", "Apple iOS (Quartz PDFContext)", 3),
    (r"mac os x .*quartz pdfcontext|macos .*quartz pdfcontext", "Apple macOS (Quartz PDFContext)", 3),
    (r"quartz pdfcontext", "Apple Quartz (iOS / macOS)", 2),
    (r"microsoft. word", "Microsoft Word", 3),
    (r"microsoft. excel", "Microsoft Excel", 3),
    (r"microsoft. powerpoint", "Microsoft PowerPoint", 3),
    (r"microsoft: print to pdf", "Windows Print to PDF", 3),
    (r"libreoffice", "LibreOffice", 3),
    (r"openoffice", "OpenOffice", 3),
    (r"google docs renderer|google docs", "Google Docs", 3),
    (r"wps office|kingsoft", "WPS Office", 3),
    (r"adobe acrobat|acrobat distiller|acrobat pdfmaker", "Adobe Acrobat", 3),
    (r"adobe pdf library", "Adobe PDF Library", 2),
    (r"adobe indesign", "Adobe InDesign", 3),
    (r"pdftex|luatex|xetex|latex|dvipdfm", "LaTeX", 3),
    (r"wkhtmltopdf", "wkhtmltopdf", 3),
    (r"ghostscript", "Ghostscript", 3),
    (r"itext", "iText (Java / .NET library)", 3),
    (r"pdfsharp", "PDFsharp (.NET)", 3),
    (r"reportlab", "ReportLab (Python)", 3),
    (r"fpdf|tcpdf|mpdf|dompdf", "PHP PDF library", 3),
    (r"weasyprint", "WeasyPrint", 3),
    (r"princexml|prince", "Prince", 2),
    (r"pdf-lib", "pdf-lib (JavaScript)", 3),
    (r"pypdf|pdfminer|pikepdf|qpdf", "Python / qpdf toolchain", 2),
    (r"camscanner", "CamScanner (scanner app)", 3),
    (r"adobe scan", "Adobe Scan (scanner app)", 3),
    (r"microsoft lens|office lens", "Microsoft Lens (scanner app)", 3),
    (r"genius scan", "Genius Scan (scanner app)", 3),
    (r"scanbot", "Scanbot (scanner app)", 3),
    (r"tiny ?scanner", "Tiny Scanner (scanner app)", 3),
    (r"samsung notes", "Samsung Notes", 3),
    (r"pdfium", "PDFium", 2),
    (r"\bqt\b", "Qt framework", 2),
    (r"foxit", "Foxit", 3),
    (r"nitro", "Nitro PDF", 3),
    (r"pages", "Apple Pages", 2),
    (r"keynote", "Apple Keynote", 2),
    (r"canva", "Canva", 3),
)

OPUS_RULES = (
    (r"^libopus unknown-fixed$", "Telegram Android", 3),
    (r"^libopus unknown$", "Telegram X (Android)", 3),
    (r"^libopus \d+\.\d+(\.\d+)?-fixed$", "Telegram macOS (native)", 3),
    (r"^libopus \d+\.\d+(\.\d+)?$", "Telegram iOS", 3),
    (r"^lavf", "Telegram Desktop / FFmpeg", 2),
    (r"^tweb", "Telegram Web K", 3),
    (r"^telegram-web-a", "Telegram Web A", 3),
    (r"whatsapp", "WhatsApp", 3),
    (r"^libopus", "libopus (unknown build)", 1),
    (r"^xiph\.org libvorbis", "Xiph libvorbis (Vorbis)", 2),
)

FILENAME_RULES = (
    (r"^ptt-\d{8}-wa\d{4}", "WhatsApp voice note", 3),
    (r"^(img|vid|aud|doc|stk)-\d{8}-wa\d{4}", "WhatsApp media", 3),
    (r"^pxl_\d{8}_\d+", "Google Pixel camera", 3),
    (r"^telegram-cloud-(photo|document)", "Telegram iOS (shared from app)", 3),
    (r"^photo_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}", "Telegram Desktop (saved photo)", 3),
    (r"^(video|audio|file|sticker|animation|voice|round)_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}", "Telegram Desktop (saved media)", 3),
    (r"^img_\d{8}_\d{6}_\d{3}\.", "Telegram Android (saved photo)", 2),
    (r"^vid_\d{8}_\d{6}_\d{3}\.", "Telegram Android (saved video)", 2),
    (r"^screenshot_\d{8}[-_]\d{6}(?:_(.+))?\.", "Android screenshot", 3),
    (r"^screenshot_\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}(?:-\d+)?(?:_(.+))?\.", "Android screenshot", 3),
    (r"^screen ?shot \d{4}-\d{2}-\d{2} at ", "macOS screenshot", 3),
    (r"^снимок экрана \d{4}-\d{2}-\d{2} в ", "macOS screenshot (RU)", 3),
    (r"^снимок экрана \d{4}-\d{2}-\d{2} \d{6}", "Windows 11 Snipping Tool (RU)", 3),
    (r"^screenshot \d{4}-\d{2}-\d{2} \d{6}", "Windows 11 Snipping Tool", 3),
    (r"^снимок экрана( \(\d+\))?\.png", "Windows screenshot (RU)", 2),
    (r"^img_e?\d{4}\.(heic|jpe?g|png|mov|mp4|dng)$", "iPhone / iPad camera roll", 3),
    (r"^img_\d{8}_\d{6}", "Android camera", 2),
    (r"^vid_\d{8}_\d{6}", "Android camera", 2),
    (r"^(img|vid)_\d{8}_\d{6}_hdr", "Android camera (HDR)", 2),
    (r"^signal-\d{4}-\d{2}-\d{2}-\d{6}", "Signal", 3),
    (r"^received_\d+", "Facebook Messenger", 2),
    (r"^\d+_\d+_\d+_n\.(jpe?g|mp4)", "Facebook / Instagram CDN", 3),
    (r"^snapchat-\d+", "Snapchat", 3),
    (r"^dsc_\d{4}", "Nikon camera", 2),
    (r"^dsc\d{5}", "Sony camera", 2),
    (r"^_?mg_\d{4}", "Canon camera", 2),
    (r"^gopr\d{4}|^g[hx]\d{6}", "GoPro", 3),
    (r"^dji_\d{4}", "DJI drone / camera", 3),
    (r"^new recording( \d+)?\.m4a", "iOS Voice Memos", 3),
    (r"^recording_\d+", "Android voice recorder", 2),
    (r"^(rpreplay|screenrecording)_", "iOS screen recording", 3),
    (r"^screen_recording_\d{8}_\d{6}", "Android screen recording", 3),
    (r"^voice\.ogg$|^audio\.ogg$", "Generic voice file", 1),
)

ANDROID_MAKERS = (
    "samsung", "xiaomi", "redmi", "poco", "huawei", "honor", "oppo", "vivo",
    "oneplus", "realme", "motorola", "nokia", "sony", "lg", "google", "asus",
    "zte", "nothing", "tecno", "infinix", "meizu", "lenovo", "fairphone", "htc",
)


def _esc(text):
    return html.escape(str(text), quote=False)


def _clip(text, n=300):
    text = str(text)
    return text if len(text) <= n else text[: n - 3] + "..."


def _fmt_bytes(b):
    if b is None:
        return "?"
    b = float(b)
    for unit in ("B", "KB", "MB", "GB"):
        if b < 1024 or unit == "GB":
            return f"{int(b)} B" if unit == "B" else f"{b:.2f} {unit}"
        b /= 1024
    return f"{b:.2f} GB"


def _fmt_dur(sec):
    if sec is None:
        return "?"
    sec = float(sec)
    m, s = divmod(sec, 60)
    h, m = divmod(int(m), 60)
    if h:
        return f"{h}:{m:02d}:{s:06.3f}"
    return f"{m}:{s:06.3f}"


def _fmt_dt(dt):
    if not dt:
        return None
    if isinstance(dt, (int, float)):
        dt = datetime.fromtimestamp(dt, tz=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _fmt_delta(sec):
    sign = "-" if sec < 0 else "+"
    sec = abs(int(sec))
    d, rem = divmod(sec, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    parts = []
    if d:
        parts.append(f"{d}d")
    if h:
        parts.append(f"{h}h")
    if m:
        parts.append(f"{m}m")
    parts.append(f"{s}s")
    return sign + " ".join(parts)


def _mp4_time(value):
    if not value or value < 86400 * 365:
        return None
    try:
        return MP4_EPOCH + timedelta(seconds=value)
    except Exception:
        return None


def _dec(data):
    if isinstance(data, str):
        return data
    for enc in ("utf-8", "utf-16"):
        try:
            return data.decode(enc)
        except Exception:
            pass
    return data.decode("latin-1", errors="replace")


def _printable(data, limit=200):
    text = data.decode("latin-1", errors="replace")
    text = "".join(ch if 32 <= ord(ch) < 127 else "." for ch in text)
    return _clip(text, limit)


def _fourcc(b):
    return b.decode("latin-1", errors="replace")


def _sniff(head):
    if head[:3] == b"\xff\xd8\xff":
        return "jpeg"
    if head[:8] == b"\x89PNG\r\n\x1a\n":
        return "png"
    if head[4:8] == b"ftyp" or head[4:8] in (b"moov", b"mdat", b"wide", b"free", b"skip"):
        return "isobmff"
    if head[:4] == b"OggS":
        return "ogg"
    if head[:5] == b"%PDF-":
        return "pdf"
    if head[:4] in (b"PK\x03\x04", b"PK\x05\x06"):
        return "zip"
    if head[:3] == b"ID3" or (len(head) > 1 and head[0] == 0xFF and head[1] & 0xE0 == 0xE0):
        return "mp3"
    if head[:4] == b"\x1a\x45\xdf\xa3":
        return "ebml"
    if head[:4] in (b"RIFF", b"RIFX"):
        return "riff"
    if head[:6] in (b"GIF87a", b"GIF89a"):
        return "gif"
    if head[:4] == b"fLaC":
        return "flac"
    if head[:2] in (b"II", b"MM") and head[2:4] in (b"*\x00", b"\x00*"):
        return "tiff"
    if head[:4] == b"Rar!":
        return "rar"
    if head[:6] == b"7z\xbc\xaf\x27\x1c":
        return "7z"
    if head[:2] == b"\x1f\x8b":
        return "gzip"
    return "unknown"


def _parse_tiff(buf):
    out = OrderedDict()
    if len(buf) < 8:
        return out
    if buf[:2] == b"II":
        e = "<"
    elif buf[:2] == b"MM":
        e = ">"
    else:
        return out
    sizes = {1: 1, 2: 1, 3: 2, 4: 4, 5: 8, 6: 1, 7: 1, 8: 2, 9: 4, 10: 8, 11: 4, 12: 8}

    def value(typ, count, raw, tag):
        try:
            if typ == 2:
                return raw.split(b"\x00", 1)[0].decode("utf-8", errors="replace").strip()
            if typ in (1, 6):
                if count <= 8:
                    return list(raw[:count]) if count > 1 else raw[0]
                return f"<{count} bytes>"
            if typ == 7:
                if tag == 0x9286:
                    code, body = raw[:8], raw[8:]
                    if code.startswith(b"UNICODE"):
                        return body.decode("utf-16-be" if e == ">" else "utf-16-le", errors="replace").strip("\x00 ")
                    return body.decode("utf-8", errors="replace").strip("\x00 ")
                if tag == 0x927C:
                    sig = raw[:16]
                    return f"<{count} bytes, header '{_printable(sig, 16)}'>"
                if tag in (0x9000, 0xA000):
                    return raw[:4].decode("latin-1", errors="replace")
                if count <= 16:
                    return raw.hex()
                return f"<{count} bytes>"
            if typ == 3:
                vals = struct.unpack(e + "H" * count, raw[: 2 * count])
            elif typ == 8:
                vals = struct.unpack(e + "h" * count, raw[: 2 * count])
            elif typ == 4:
                vals = struct.unpack(e + "I" * count, raw[: 4 * count])
            elif typ == 9:
                vals = struct.unpack(e + "i" * count, raw[: 4 * count])
            elif typ in (5, 10):
                fmt = "I" if typ == 5 else "i"
                nums = struct.unpack(e + fmt * (2 * count), raw[: 8 * count])
                vals = tuple(
                    (nums[i] / nums[i + 1]) if nums[i + 1] else 0.0
                    for i in range(0, len(nums), 2)
                )
            elif typ == 11:
                vals = struct.unpack(e + "f" * count, raw[: 4 * count])
            elif typ == 12:
                vals = struct.unpack(e + "d" * count, raw[: 8 * count])
            else:
                return f"<type {typ}>"
            if tag in (0x9C9B, 0x9C9C, 0x9C9D, 0x9C9E, 0x9C9F):
                return bytes(vals).decode("utf-16-le", errors="replace").strip("\x00")
            if count > 16:
                return f"<{count} values>"
            return vals[0] if len(vals) == 1 else list(vals)
        except Exception:
            return None

    def read_ifd(offset, names, prefix, depth=0):
        if depth > 4 or offset <= 0 or offset + 2 > len(buf):
            return 0
        count = struct.unpack(e + "H", buf[offset: offset + 2])[0]
        if count > 512:
            return 0
        for i in range(count):
            p = offset + 2 + i * 12
            if p + 12 > len(buf):
                break
            tag, typ, cnt = struct.unpack(e + "HHI", buf[p: p + 8])
            size = sizes.get(typ, 1) * cnt
            if size <= 4:
                raw = buf[p + 8: p + 8 + size]
            else:
                off = struct.unpack(e + "I", buf[p + 8: p + 12])[0]
                if off + size > len(buf) or size > 4 * 1024 * 1024:
                    continue
                raw = buf[off: off + size]
            if tag == 0x8769 and prefix != "GPS":
                sub = struct.unpack(e + "I", raw[:4])[0] if len(raw) >= 4 else 0
                read_ifd(sub, EXIF_TAGS, "ExifIFD", depth + 1)
                continue
            if tag == 0x8825 and prefix != "GPS":
                sub = struct.unpack(e + "I", raw[:4])[0] if len(raw) >= 4 else 0
                read_ifd(sub, GPS_TAGS, "GPS", depth + 1)
                continue
            if tag == 0xA005:
                continue
            name = names.get(tag, f"Tag0x{tag:04X}")
            val = value(typ, cnt, raw, tag)
            if val is None or val == "":
                continue
            out[f"{prefix}:{name}"] = val
        nxt_pos = offset + 2 + count * 12
        if nxt_pos + 4 <= len(buf):
            return struct.unpack(e + "I", buf[nxt_pos: nxt_pos + 4])[0]
        return 0

    try:
        if struct.unpack(e + "H", buf[2:4])[0] != 42:
            return out
        first = struct.unpack(e + "I", buf[4:8])[0]
        nxt = read_ifd(first, EXIF_TAGS, "IFD0")
        if nxt:
            read_ifd(nxt, EXIF_TAGS, "IFD1")
    except Exception as e2:
        logger.debug("[MDD] tiff parse error: %s", e2)
    return out


def _gps_from_exif(exif):
    def conv(vals, ref):
        if not isinstance(vals, list) or len(vals) < 3:
            return None
        deg = float(vals[0]) + float(vals[1]) / 60 + float(vals[2]) / 3600
        if str(ref).upper() in ("S", "W"):
            deg = -deg
        return deg

    lat = conv(exif.get("GPS:GPSLatitude"), exif.get("GPS:GPSLatitudeRef", "N"))
    lon = conv(exif.get("GPS:GPSLongitude"), exif.get("GPS:GPSLongitudeRef", "E"))
    if lat is None or lon is None or (lat == 0 and lon == 0):
        return None
    return lat, lon


def _parse_icc(data):
    out = OrderedDict()
    if len(data) < 132:
        return out
    try:
        out["CMM"] = _fourcc(data[4:8]).strip("\x00 ")
        out["Version"] = f"{data[8]}.{data[9] >> 4}.{data[9] & 15}"
        out["DeviceClass"] = _fourcc(data[12:16])
        out["ColorSpace"] = _fourcc(data[16:20]).strip()
        y, mo, d, h, mi, s = struct.unpack(">6H", data[24:36])
        if y:
            out["Created"] = f"{y:04d}-{mo:02d}-{d:02d} {h:02d}:{mi:02d}:{s:02d}"
        out["Platform"] = _fourcc(data[40:44]).strip("\x00 ")
        out["Manufacturer"] = _fourcc(data[48:52]).strip("\x00 ")
        out["Creator"] = _fourcc(data[80:84]).strip("\x00 ")
        count = struct.unpack(">I", data[128:132])[0]
        for i in range(min(count, 64)):
            p = 132 + i * 12
            sig, off, size = struct.unpack(">4sII", data[p: p + 12])
            if sig not in (b"desc", b"cprt", b"dmnd", b"dmdd"):
                continue
            body = data[off: off + size]
            text = None
            if body[:4] == b"desc":
                n = struct.unpack(">I", body[8:12])[0]
                text = body[12: 12 + n].split(b"\x00", 1)[0].decode("latin-1", errors="replace")
            elif body[:4] == b"mluc":
                recs = struct.unpack(">I", body[8:12])[0]
                if recs:
                    ln, lo = struct.unpack(">II", body[20:28])
                    text = body[lo: lo + ln].decode("utf-16-be", errors="replace")
            elif body[:4] == b"text":
                text = body[8:].split(b"\x00", 1)[0].decode("latin-1", errors="replace")
            if text:
                key = {b"desc": "Description", b"cprt": "Copyright", b"dmnd": "DeviceMfg", b"dmdd": "DeviceModel"}[sig]
                out[key] = text.strip()
    except Exception:
        pass
    return out


def _parse_xmp(text):
    out = OrderedDict()
    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="replace")
    skip = {"x:xmptk", "xmlns", "rdf:about", "rdf:parseType"}
    for m in re.finditer(r'([A-Za-z][\w\-]*:[A-Za-z][\w\-]*)="([^"]{0,400})"', text):
        k = m.group(1)
        if k.startswith("xmlns") or k in skip:
            continue
        out.setdefault(k, html.unescape(m.group(2)))
    for m in re.finditer(r"<([A-Za-z][\w\-]*:[A-Za-z][\w\-]*)>([^<]{1,400})</\1>", text):
        k = m.group(1)
        if k.startswith("rdf:"):
            continue
        out.setdefault(k, html.unescape(m.group(2).strip()))
    for m in re.finditer(r"<([A-Za-z][\w\-]*:[A-Za-z][\w\-]*)>\s*<rdf:(?:Alt|Seq|Bag)>\s*<rdf:li[^>]*>([^<]{1,400})</rdf:li>", text):
        out.setdefault(m.group(1), html.unescape(m.group(2).strip()))
    x = re.search(r'x:xmptk="([^"]+)"', text)
    if x:
        out["x:xmptk"] = x.group(1)
    return out


def _jpeg_quality(tables):
    luma = tables.get(0)
    if not luma or len(luma) < 64:
        return None
    chroma = tables.get(1)
    nat_l = [0] * 64
    for k, v in enumerate(luma[:64]):
        nat_l[NATURAL_ORDER[k]] = v
    nat_c = None
    if chroma and len(chroma) >= 64:
        nat_c = [0] * 64
        for k, v in enumerate(chroma[:64]):
            nat_c[NATURAL_ORDER[k]] = v
    best = None
    for q in range(1, 101):
        scale = 5000 // q if q < 50 else 200 - 2 * q
        diff = 0
        for i in range(64):
            ref = min(max((ANNEX_K_LUMA[i] * scale + 50) // 100, 1), 255)
            diff += abs(ref - nat_l[i])
        if nat_c:
            for i in range(64):
                ref = min(max((ANNEX_K_CHROMA[i] * scale + 50) // 100, 1), 255)
                diff += abs(ref - nat_c[i])
        if best is None or diff < best[1]:
            best = (q, diff)
    n = 128 if nat_c else 64
    return {"quality": best[0], "exact": best[1] == 0, "mean_error": round(best[1] / n, 2)}


def _parse_jpeg(data):
    if data[:2] != b"\xff\xd8":
        return None
    info = OrderedDict()
    info["markers"] = []
    tables = {}
    icc_parts = []
    exif = OrderedDict()
    xmp = OrderedDict()
    flags = []
    i = 2
    n = len(data)
    sof_names = {
        0xC0: "Baseline DCT", 0xC1: "Extended sequential DCT", 0xC2: "Progressive DCT",
        0xC3: "Lossless", 0xC5: "Differential sequential", 0xC6: "Differential progressive",
        0xC7: "Differential lossless", 0xC9: "Arithmetic sequential", 0xCA: "Arithmetic progressive",
    }
    while i + 4 <= n:
        if data[i] != 0xFF:
            nxt = data.find(b"\xff", i)
            if nxt < 0:
                break
            i = nxt
            continue
        marker = data[i + 1]
        if marker == 0xFF:
            i += 1
            continue
        if marker in (0xD8, 0x01) or 0xD0 <= marker <= 0xD7:
            i += 2
            continue
        if marker == 0xD9:
            break
        seglen = struct.unpack(">H", data[i + 2: i + 4])[0]
        seg = data[i + 4: i + 2 + seglen]
        name = f"APP{marker - 0xE0}" if 0xE0 <= marker <= 0xEF else f"0x{marker:02X}"
        if marker == 0xDA:
            info["markers"].append("SOS")
            break
        if 0xE0 <= marker <= 0xEF:
            ident = seg.split(b"\x00", 1)[0][:40]
            info["markers"].append(f"{name}({_printable(ident, 30)})")
            if marker == 0xE0 and seg[:5] == b"JFIF\x00":
                units = {0: "aspect", 1: "dpi", 2: "dpcm"}.get(seg[7], "?")
                info["JFIF"] = f"v{seg[5]}.{seg[6]:02d}, {struct.unpack('>H', seg[8:10])[0]}x{struct.unpack('>H', seg[10:12])[0]} {units}"
            elif marker == 0xE1 and seg[:6] == b"Exif\x00\x00":
                exif.update(_parse_tiff(seg[6:]))
                info["EXIF bytes"] = len(seg)
            elif marker == 0xE1 and seg.startswith(b"http://ns.adobe.com/xap/1.0/\x00"):
                xmp.update(_parse_xmp(seg[29:]))
                info["XMP bytes"] = len(seg)
            elif marker == 0xE1 and seg.startswith(b"http://ns.adobe.com/xmp/extension/\x00"):
                flags.append("Extended XMP")
            elif marker == 0xE2 and seg[:12] == b"ICC_PROFILE\x00":
                icc_parts.append((seg[12], seg[14:]))
            elif marker == 0xE2 and seg[:4] == b"MPF\x00":
                try:
                    mp = _parse_tiff(seg[4:])
                    info["MPF"] = "Multi-Picture Format"
                    flags.append("MPF (depth map / HDR gain map / multi-frame)")
                    for k, v in mp.items():
                        if k.endswith("Tag0xB001"):
                            info["MPF images"] = v
                except Exception:
                    flags.append("MPF")
            elif marker == 0xEB and (b"c2pa" in seg[:64] or seg[:2] == b"JP"):
                flags.append("C2PA / JUMBF content credentials")
            elif marker == 0xED and seg.startswith(b"Photoshop 3.0"):
                flags.append("Photoshop IRB / IPTC")
            elif marker == 0xEE and seg[:5] == b"Adobe":
                flags.append(f"Adobe APP14 (transform {seg[11] if len(seg) > 11 else '?'})")
            elif marker == 0xEC and seg[:5] == b"Ducky":
                flags.append("Ducky (Photoshop Save for Web)")
        elif marker == 0xDB:
            info["markers"].append("DQT")
            p = 0
            while p < len(seg):
                pq, tq = seg[p] >> 4, seg[p] & 15
                size = 128 if pq else 64
                vals = seg[p + 1: p + 1 + size]
                if pq:
                    tables[tq] = [struct.unpack(">H", vals[j: j + 2])[0] for j in range(0, len(vals), 2)]
                else:
                    tables[tq] = list(vals)
                p += 1 + size
        elif marker in sof_names:
            info["markers"].append(f"SOF{marker - 0xC0}")
            info["Encoding"] = sof_names[marker]
            info["Bits"] = seg[0]
            h, w = struct.unpack(">HH", seg[1:5])
            info["Size"] = f"{w}x{h}"
            comps = seg[5]
            sampling = []
            for c in range(comps):
                b = seg[6 + c * 3 + 1]
                sampling.append((b >> 4, b & 15))
            info["Components"] = comps
            if comps == 3:
                (h0, v0), (h1, v1) = sampling[0], sampling[1]
                sub = {(1, 1): "4:4:4", (2, 1): "4:2:2", (2, 2): "4:2:0", (4, 1): "4:1:1", (1, 2): "4:4:0"}.get(
                    (h0 // max(h1, 1), v0 // max(v1, 1)), f"{h0}x{v0}/{h1}x{v1}")
                info["Subsampling"] = sub
        elif marker == 0xFE:
            info["markers"].append("COM")
            info["Comment"] = _clip(_dec(seg).strip("\x00"), 300)
        elif marker == 0xDD:
            info["Restart interval"] = struct.unpack(">H", seg[:2])[0]
        elif marker == 0xC4:
            info.setdefault("DHT", 0)
            info["DHT"] += 1
        i += 2 + seglen
    q = _jpeg_quality(tables)
    if q:
        info["Quality"] = q
    info["QT count"] = len(tables)
    if icc_parts:
        icc = b"".join(p for _, p in sorted(icc_parts, key=lambda x: x[0]))
        info["ICC"] = _parse_icc(icc)
    tail = data[-64:]
    if tail.endswith(b"SEFT"):
        flags.append("Samsung SEF trailer")
    if b"MotionPhoto" in data[:200000] or b"MicroVideo" in data[:200000]:
        flags.append("Motion Photo (embedded video)")
    eoi = data.rfind(b"\xff\xd9")
    if 0 < eoi < n - 2 and n - eoi - 2 > 16:
        extra = data[eoi + 2:]
        if extra[4:8] == b"ftyp":
            flags.append(f"Embedded MP4 after EOI ({_fmt_bytes(len(extra))})")
        elif "MPF" not in info:
            flags.append(f"Trailing data after EOI ({_fmt_bytes(len(extra))})")
    info["flags"] = flags
    info["tables"] = tables
    return info, exif, xmp


def _parse_png(data):
    if data[:8] != b"\x89PNG\r\n\x1a\n":
        return None
    info = OrderedDict()
    text = OrderedDict()
    exif = OrderedDict()
    xmp = OrderedDict()
    chunks = []
    p = 8
    color_types = {0: "Grayscale", 2: "RGB", 3: "Palette", 4: "Gray+Alpha", 6: "RGBA"}
    while p + 8 <= len(data):
        ln, typ = struct.unpack(">I4s", data[p: p + 8])
        body = data[p + 8: p + 8 + ln]
        t = _fourcc(typ)
        if t not in ("IDAT", "fdAT"):
            chunks.append(t)
        elif not chunks or chunks[-1] != t:
            chunks.append(t)
        try:
            if t == "IHDR":
                w, h, bd, ct, cm, fm, il = struct.unpack(">IIBBBBB", body[:13])
                info["Size"] = f"{w}x{h}"
                info["Bit depth"] = bd
                info["Color"] = color_types.get(ct, ct)
                info["Interlace"] = "Adam7" if il else "None"
            elif t == "tEXt":
                k, v = body.split(b"\x00", 1)
                text[k.decode("latin-1")] = v.decode("latin-1", errors="replace")
            elif t == "zTXt":
                k, rest = body.split(b"\x00", 1)
                text[k.decode("latin-1")] = zlib.decompress(rest[1:]).decode("latin-1", errors="replace")
            elif t == "iTXt":
                k, rest = body.split(b"\x00", 1)
                comp, _method = rest[0], rest[1]
                rest = rest[2:]
                _lang, rest = rest.split(b"\x00", 1)
                _tk, rest = rest.split(b"\x00", 1)
                val = zlib.decompress(rest) if comp else rest
                key = k.decode("latin-1")
                if key == "XML:com.adobe.xmp":
                    xmp.update(_parse_xmp(val))
                else:
                    text[key] = val.decode("utf-8", errors="replace")
            elif t == "eXIf":
                exif.update(_parse_tiff(body))
            elif t == "iCCP":
                name, rest = body.split(b"\x00", 1)
                info["ICC name"] = name.decode("latin-1")
                try:
                    info["ICC"] = _parse_icc(zlib.decompress(rest[1:]))
                except Exception:
                    pass
            elif t == "pHYs":
                x, y, unit = struct.unpack(">IIB", body[:9])
                if unit == 1:
                    info["DPI"] = f"{round(x * 0.0254)}x{round(y * 0.0254)}"
                else:
                    info["Pixel aspect"] = f"{x}:{y}"
            elif t == "sRGB":
                info["sRGB intent"] = body[0]
            elif t == "gAMA":
                info["Gamma"] = struct.unpack(">I", body[:4])[0] / 100000
            elif t == "tIME":
                y, mo, d, h, mi, s = struct.unpack(">HBBBBB", body[:7])
                info["Modified (tIME)"] = f"{y:04d}-{mo:02d}-{d:02d} {h:02d}:{mi:02d}:{s:02d}"
            elif t == "acTL":
                frames, plays = struct.unpack(">II", body[:8])
                info["APNG"] = f"{frames} frames, loops {plays or 'inf'}"
            elif t == "caBX":
                info["C2PA"] = "Content credentials (caBX)"
        except Exception:
            pass
        if t == "IEND":
            break
        p += 12 + ln
    info["Chunks"] = " ".join(chunks)
    return info, text, exif, xmp


def _iter_boxes(data, start, end):
    p = start
    while p + 8 <= end:
        size, typ = struct.unpack(">I4s", data[p: p + 8])
        hdr = 8
        if size == 1:
            if p + 16 > end:
                return
            size = struct.unpack(">Q", data[p + 8: p + 16])[0]
            hdr = 16
        elif size == 0:
            size = end - p
        if size < hdr or p + size > end + 8:
            return
        yield typ, p + hdr, min(p + size, end)
        p += size


def _qt_string(payload):
    if len(payload) >= 4:
        ln = struct.unpack(">H", payload[:2])[0]
        if 0 < ln <= len(payload) - 4:
            return payload[4: 4 + ln].decode("utf-8", errors="replace")
    return payload.decode("utf-8", errors="replace").strip("\x00")


def _ilst_value(data, start, end):
    for typ, s, e in _iter_boxes(data, start, end):
        if typ == b"data":
            kind = struct.unpack(">I", data[s: s + 4])[0] & 0xFFFFFF
            raw = data[s + 8: e]
            if kind in (1, 4, 5):
                return raw.decode("utf-8", errors="replace")
            if kind == 2:
                return raw.decode("utf-16-be", errors="replace")
            if kind in (21, 22) and len(raw) in (1, 2, 4, 8):
                return int.from_bytes(raw, "big", signed=kind == 21)
            if kind == 23 and len(raw) == 4:
                return struct.unpack(">f", raw)[0]
            if kind == 24 and len(raw) == 8:
                return struct.unpack(">d", raw)[0]
            if kind in (13, 14):
                return f"<image {len(raw)} bytes>"
            return _printable(raw, 120) if raw else ""
    return None


def _desc_len(buf, p):
    n = 0
    for _ in range(4):
        if p >= len(buf):
            break
        b = buf[p]
        p += 1
        n = (n << 7) | (b & 0x7F)
        if not b & 0x80:
            break
    return n, p


def _parse_esds(buf):
    out = {}
    try:
        p = 0
        if buf[p] != 0x03:
            return out
        _, p = _desc_len(buf, p + 1)
        flags = buf[p + 2]
        p += 3
        if flags & 0x80:
            p += 2
        if flags & 0x40:
            p += 1 + buf[p]
        if flags & 0x20:
            p += 2
        if buf[p] != 0x04:
            return out
        _, p = _desc_len(buf, p + 1)
        _oti, _st = buf[p], buf[p + 1]
        mx, avg = struct.unpack(">II", buf[p + 5: p + 13])
        if avg:
            out["bitrate"] = f"avg {avg // 1000} kb/s, max {mx // 1000} kb/s"
        p += 13
        if p < len(buf) and buf[p] == 0x05:
            ln, p = _desc_len(buf, p + 1)
            aot = buf[p] >> 3
            if aot == 31 and p + 1 < len(buf):
                aot = 32 + (((buf[p] & 7) << 3) | (buf[p + 1] >> 5))
            out["aac"] = {1: "AAC Main", 2: "AAC LC", 5: "HE-AAC (SBR)", 29: "HE-AACv2 (PS)", 23: "AAC LD", 39: "AAC ELD", 42: "xHE-AAC (USAC)"}.get(aot, f"AOT {aot}")
    except Exception:
        pass
    return out


def _parse_meta_box(data, start, end, tags):
    if end - start >= 8 and data[start + 4: start + 8] != b"hdlr":
        start += 4
    keys = []
    for typ, s, e in _iter_boxes(data, start, end):
        if typ == b"hdlr":
            tags.setdefault("_meta_handler", _fourcc(data[s + 8: s + 12]))
        elif typ == b"keys":
            cnt = struct.unpack(">I", data[s + 4: s + 8])[0]
            p = s + 8
            for _ in range(min(cnt, 256)):
                if p + 8 > e:
                    break
                ksize = struct.unpack(">I", data[p: p + 4])[0]
                keys.append(data[p + 8: p + ksize].decode("utf-8", errors="replace"))
                p += max(ksize, 8)
        elif typ == b"ilst":
            for ityp, isub, iend in _iter_boxes(data, s, e):
                idx = int.from_bytes(ityp, "big")
                if keys and 1 <= idx <= len(keys):
                    name = keys[idx - 1]
                else:
                    name = ityp.decode("latin-1")
                    name = QT_KEYS.get(name, name.replace("\xa9", "(c)"))
                val = _ilst_value(data, isub, iend)
                if val is not None:
                    tags[name] = val


def _parse_moov(data):
    movie = OrderedDict()
    tracks = []
    tags = OrderedDict()

    def walk(start, end, ctx, depth):
        if depth > 12:
            return
        for typ, s, e in _iter_boxes(data, start, end):
            if typ == b"trak":
                ctx2 = OrderedDict()
                tracks.append(ctx2)
                walk(s, e, ctx2, depth + 1)
            elif typ == b"udta":
                parse_udta(s, e)
            elif typ == b"meta" and ctx is movie:
                _parse_meta_box(data, s, e, tags)
            elif typ in ISO_CONTAINERS:
                walk(s, e, ctx, depth + 1)
            elif typ == b"mvhd":
                v = data[s]
                if v == 1:
                    ct, mt, ts, du = struct.unpack(">QQIQ", data[s + 4: s + 32])
                else:
                    ct, mt, ts, du = struct.unpack(">IIII", data[s + 4: s + 20])
                movie["created"] = _mp4_time(ct)
                movie["modified"] = _mp4_time(mt)
                movie["timescale"] = ts
                movie["duration"] = du / ts if ts else None
            elif typ == b"tkhd":
                v = data[s]
                if v == 1:
                    ct, mt, tid = struct.unpack(">QQI", data[s + 4: s + 24])
                    base = s + 4 + 32
                else:
                    ct, mt, tid = struct.unpack(">III", data[s + 4: s + 16])
                    base = s + 4 + 20
                ctx["id"] = tid
                ctx["created"] = _mp4_time(ct)
                m = struct.unpack(">9i", data[base + 16: base + 52])
                a, b = m[0] / 65536, m[1] / 65536
                angle = round(math.degrees(math.atan2(b, a))) % 360
                if angle:
                    ctx["rotation"] = angle
                w, h = struct.unpack(">II", data[base + 52: base + 60])
                if w or h:
                    ctx["dims"] = f"{w >> 16}x{h >> 16}"
            elif typ == b"mdhd":
                v = data[s]
                if v == 1:
                    ct, mt, ts, du = struct.unpack(">QQIQ", data[s + 4: s + 32])
                    lang = struct.unpack(">H", data[s + 32: s + 34])[0]
                else:
                    ct, mt, ts, du = struct.unpack(">IIII", data[s + 4: s + 20])
                    lang = struct.unpack(">H", data[s + 20: s + 22])[0]
                ctx["timescale"] = ts
                ctx["duration"] = du / ts if ts else None
                code = "".join(chr(((lang >> sh) & 31) + 0x60) for sh in (10, 5, 0)) if lang else ""
                if code.isalpha():
                    ctx["language"] = code
            elif typ == b"hdlr":
                comp = data[s + 4: s + 8]
                htype = _fourcc(data[s + 8: s + 12])
                raw = data[s + 24: e]
                if raw and raw[0] == len(raw) - 1:
                    raw = raw[1:]
                name = raw.split(b"\x00", 1)[0].decode("utf-8", errors="replace").strip()
                if ctx is movie:
                    continue
                if comp == b"dhlr" or "handler_type" in ctx:
                    if name:
                        ctx["data_handler"] = name
                    continue
                ctx["handler_type"] = htype
                ctx["handler_name"] = name
            elif typ == b"stsd":
                p = s + 8
                if p + 8 <= e:
                    esize, fmt = struct.unpack(">I4s", data[p: p + 8])
                    entry = data[p: p + esize]
                    ctx["codec"] = _fourcc(fmt)
                    if fmt in (b"avc1", b"avc3", b"hvc1", b"hev1", b"mp4v", b"vp09", b"av01", b"jpeg", b"apcn", b"apch", b"s263", b"dvh1", b"dvhe"):
                        w, h = struct.unpack(">HH", entry[32:36])
                        ctx["coded"] = f"{w}x{h}"
                        cn = entry[50:82]
                        cname = cn[1: 1 + cn[0]].decode("latin-1", errors="replace") if cn and cn[0] < 32 else ""
                        if cname.strip():
                            ctx["compressor"] = cname.strip()
                        for ctyp, cs, ce in _iter_boxes(entry, 86, len(entry)):
                            if ctyp == b"avcC" and ce - cs >= 4:
                                prof = {66: "Baseline", 77: "Main", 88: "Extended", 100: "High", 110: "High 10", 122: "High 4:2:2", 244: "High 4:4:4"}.get(entry[cs + 1], entry[cs + 1])
                                ctx["profile"] = f"H.264 {prof} L{entry[cs + 3] / 10:g}"
                            elif ctyp == b"hvcC" and ce - cs >= 13:
                                prof = {1: "Main", 2: "Main 10", 3: "Main Still"}.get(entry[cs + 1] & 31, entry[cs + 1] & 31)
                                ctx["profile"] = f"HEVC {prof} L{entry[cs + 12] / 30:g}"
                            elif ctyp == b"colr" and entry[cs: cs + 4] == b"nclx":
                                pr, tr, mx = struct.unpack(">HHH", entry[cs + 4: cs + 10])
                                ctx["color"] = f"primaries {pr}, transfer {tr}, matrix {mx}"
                            elif ctyp == b"pasp":
                                hs, vs = struct.unpack(">II", entry[cs: cs + 8])
                                if hs != vs:
                                    ctx["pixel_aspect"] = f"{hs}:{vs}"
                            elif ctyp == b"btrt" and ce - cs >= 12:
                                _, mx, avg = struct.unpack(">III", entry[cs: cs + 12])
                                ctx["bitrate"] = f"avg {avg // 1000} kb/s, max {mx // 1000} kb/s"
                            elif ctyp in (b"dvcC", b"dvvC"):
                                ctx["dolby_vision"] = True
                    elif fmt in (b"mp4a", b"Opus", b"alac", b"ac-3", b"ec-3", b"sowt", b"twos", b"lpcm", b"fLaC", b"samr"):
                        ch, ss = struct.unpack(">HH", entry[24:28])
                        rate = struct.unpack(">I", entry[32:36])[0] >> 16
                        ctx["audio"] = f"{ch} ch, {rate} Hz, {ss} bit"
                        esds = entry.find(b"esds")
                        if fmt == b"mp4a" and esds > 4:
                            esize = struct.unpack(">I", entry[esds - 4: esds])[0]
                            ctx.update(_parse_esds(entry[esds + 8: esds - 4 + esize]))
            elif typ == b"stts":
                cnt = struct.unpack(">I", data[s + 4: s + 8])[0]
                total_s, total_d = 0, 0
                for k in range(min(cnt, 100000)):
                    sc, sd = struct.unpack(">II", data[s + 8 + k * 8: s + 16 + k * 8])
                    total_s += sc
                    total_d += sc * sd
                ctx["samples"] = total_s
                ctx["_stts_dur"] = total_d
                ctx["stts_entries"] = cnt
            elif typ == b"stss":
                ctx["keyframes"] = struct.unpack(">I", data[s + 4: s + 8])[0]
            elif typ == b"elst":
                ctx["edit_entries"] = struct.unpack(">I", data[s + 4: s + 8])[0]

    def parse_udta(start, end):
        for typ, s, e in _iter_boxes(data, start, end):
            name = typ.decode("latin-1")
            if typ == b"meta":
                _parse_meta_box(data, s, e, tags)
            elif name.startswith("\xa9") or name in ("auth", "cprt", "desc"):
                tags[QT_KEYS.get(name, name.replace("\xa9", "(c)"))] = _qt_string(data[s:e])
            elif typ == b"loci":
                tags["Location (loci)"] = _printable(data[s + 6: e], 120)
            elif typ in (b"Xtra", b"XMP_", b"smta", b"SDLN", b"manu", b"modl", b"date", b"FIRM", b"LENS", b"CAME", b"MUID", b"HMMT", b"meta"):
                tags[f"udta:{name}"] = _printable(data[s:e], 120)
            elif typ not in (b"free", b"skip"):
                tags.setdefault("_udta_atoms", [])
                tags["_udta_atoms"].append(name)

    walk(0, len(data), movie, 0)
    for t in tracks:
        ts = t.get("timescale")
        if ts and t.get("samples") and t.get("_stts_dur") and t.get("handler_type") == "vide":
            t["fps"] = round(t["samples"] * ts / t["_stts_dur"], 3)
        t.pop("_stts_dur", None)
    return movie, tracks, tags


def _scan_sei(data):
    out = OrderedDict()
    for sig, name in ((b"x264 - core", "x264"), (b"x265 (build", "x265"), (b"HandBrake", "HandBrake"), (b"Lavc", "Lavc")):
        pos = data.find(sig)
        if pos >= 0:
            endp = data.find(b"\x00", pos)
            if endp < 0 or endp - pos > 900:
                endp = pos + 900
            out[name] = _printable(data[pos:endp], 900)
    return out


def _parse_isobmff_file(path, size):
    res = OrderedDict()
    top = []
    moov = None
    with open(path, "rb") as f:
        pos = 0
        guard = 0
        while pos + 8 <= size and guard < 256:
            guard += 1
            f.seek(pos)
            hdr = f.read(16)
            if len(hdr) < 8:
                break
            bsize, typ = struct.unpack(">I4s", hdr[:8])
            hlen = 8
            if bsize == 1:
                bsize = struct.unpack(">Q", hdr[8:16])[0]
                hlen = 16
            elif bsize == 0:
                bsize = size - pos
            if bsize < 8 or not re.match(rb"^[\x20-\x7e\xa9]{4}$", typ):
                break
            name = _fourcc(typ)
            top.append((name, pos, bsize))
            if typ == b"ftyp":
                f.seek(pos + hlen)
                body = f.read(min(bsize - hlen, 256))
                res["major_brand"] = _fourcc(body[:4])
                res["minor_version"] = struct.unpack(">I", body[4:8])[0]
                res["compatible"] = [_fourcc(body[k: k + 4]) for k in range(8, len(body) - 3, 4)]
            elif typ == b"moov" and bsize <= MOOV_CAP:
                f.seek(pos + hlen)
                moov = f.read(bsize - hlen)
            elif typ == b"mdat":
                f.seek(pos + hlen)
                res["sei"] = _scan_sei(f.read(min(bsize - hlen, 2 * 1024 * 1024)))
                res["mdat_size"] = bsize
            elif typ == b"uuid":
                f.seek(pos + hlen)
                u = f.read(16).hex()
                res.setdefault("uuid", []).append(UUID_NAMES.get(u, u))
                if u == "be7acfcb97a942e89c71999491e3afac" and bsize < 4 * 1024 * 1024:
                    res["xmp"] = _parse_xmp(f.read(bsize - hlen - 16))
            elif typ == b"meta" and bsize < 4 * 1024 * 1024:
                f.seek(pos + hlen)
                res["top_meta"] = f.read(bsize - hlen)
            elif typ in (b"free", b"skip", b"wide") and 8 < bsize < 4096:
                f.seek(pos + hlen)
                blob = f.read(bsize - hlen)
                if blob.strip(b"\x00"):
                    res.setdefault("free_text", []).append(_printable(blob.strip(b"\x00"), 120))
            pos += bsize
    res["top"] = top
    if moov:
        movie, tracks, tags = _parse_moov(moov)
        res["movie"] = movie
        res["tracks"] = tracks
        res["tags"] = tags
        res["moov_first"] = any(n == "moov" for n, _, _ in top[: next((i for i, (n, _, _) in enumerate(top) if n == "mdat"), len(top))])
    return res


def _ogg_packets(data, limit=3):
    packets = []
    cur = b""
    p = 0
    last_granule = None
    serial = None
    while p + 27 <= len(data) and len(packets) < limit:
        if data[p: p + 4] != b"OggS":
            nxt = data.find(b"OggS", p + 1)
            if nxt < 0:
                break
            p = nxt
            continue
        nseg = data[p + 26]
        table = data[p + 27: p + 27 + nseg]
        serial = struct.unpack("<I", data[p + 14: p + 18])[0]
        last_granule = struct.unpack("<q", data[p + 6: p + 14])[0]
        q = p + 27 + nseg
        for lace in table:
            cur += data[q: q + lace]
            q += lace
            if lace < 255:
                packets.append(cur)
                cur = b""
                if len(packets) >= limit:
                    break
        p = q
    return packets, serial


def _parse_ogg(head, tail, size):
    info = OrderedDict()
    comments = OrderedDict()
    packets, serial = _ogg_packets(head, 3)
    if not packets:
        return info, comments
    info["Stream serial"] = serial
    first = packets[0]
    codec = None
    if first[:8] == b"OpusHead":
        codec = "Opus"
        ver, ch, preskip, rate, gain, fam = struct.unpack("<BBHIhB", first[8:19])
        info["Codec"] = "Opus"
        info["OpusHead version"] = ver
        info["Channels"] = ch
        info["Pre-skip"] = preskip
        info["Input sample rate"] = rate
        info["Output gain"] = gain
        info["Mapping family"] = fam
    elif first[:7] == b"\x01vorbis":
        codec = "Vorbis"
        ch, rate = struct.unpack("<BI", first[11:16])
        info["Codec"] = "Vorbis"
        info["Channels"] = ch
        info["Sample rate"] = rate
    elif first[:5] == b"\x7fFLAC":
        info["Codec"] = "FLAC in Ogg"
    elif first[:8] == b"Speex   ":
        info["Codec"] = "Speex"
    if len(packets) > 1:
        tags = packets[1]
        off = None
        if tags[:8] == b"OpusTags":
            off = 8
        elif tags[:7] == b"\x03vorbis":
            off = 7
        if off is not None:
            try:
                vlen = struct.unpack("<I", tags[off: off + 4])[0]
                info["Vendor"] = tags[off + 4: off + 4 + vlen].decode("utf-8", errors="replace")
                p = off + 4 + vlen
                cnt = struct.unpack("<I", tags[p: p + 4])[0]
                p += 4
                for _ in range(min(cnt, 128)):
                    ln = struct.unpack("<I", tags[p: p + 4])[0]
                    item = tags[p + 4: p + 4 + ln].decode("utf-8", errors="replace")
                    p += 4 + ln
                    if "=" in item:
                        k, v = item.split("=", 1)
                        if k.upper() == "METADATA_BLOCK_PICTURE":
                            v = f"<picture {len(v)} b64 chars>"
                        comments[k.upper()] = _clip(v, 300)
                info["Comments count"] = cnt
            except Exception:
                pass
    pos = tail.rfind(b"OggS")
    if pos >= 0 and pos + 14 <= len(tail):
        gran = struct.unpack("<q", tail[pos + 6: pos + 14])[0]
        if codec == "Opus" and gran > 0:
            dur = (gran - info.get("Pre-skip", 0)) / 48000
            info["Duration"] = _fmt_dur(dur)
            if dur > 0:
                info["Avg bitrate"] = f"{size * 8 / dur / 1000:.1f} kb/s"
        elif codec == "Vorbis" and gran > 0 and info.get("Sample rate"):
            info["Duration"] = _fmt_dur(gran / info["Sample rate"])
    return info, comments


def _pdf_string(raw):
    raw = raw.strip()
    if raw.startswith(b"<") and raw.endswith(b">"):
        try:
            b = bytes.fromhex(re.sub(rb"\s", b"", raw[1:-1]).decode())
        except Exception:
            return _printable(raw)
    else:
        body = raw[1:-1] if raw.startswith(b"(") else raw
        b = re.sub(rb"\\([nrtbf()\\]|[0-7]{1,3})", lambda m: {
            b"n": b"\n", b"r": b"\r", b"t": b"\t", b"b": b"\b", b"f": b"\f",
            b"(": b"(", b")": b")", b"\\": b"\\",
        }.get(m.group(1), bytes([int(m.group(1), 8) & 255]) if m.group(1)[:1].isdigit() else m.group(1)), body)
    if b[:2] == b"\xfe\xff":
        return b[2:].decode("utf-16-be", errors="replace")
    if b[:3] == b"\xef\xbb\xbf":
        return b[3:].decode("utf-8", errors="replace")
    try:
        return b.decode("utf-8")
    except Exception:
        return b.decode("latin-1", errors="replace")


def _pdf_date(s):
    m = re.match(r"D?:?(\d{4})(\d{2})?(\d{2})?(\d{2})?(\d{2})?(\d{2})?([Zz+\-])?(\d{2})?'?(\d{2})?", s)
    if not m:
        return s
    y, mo, d, h, mi, se, z, th, tm = m.groups()
    out = f"{y}-{mo or '01'}-{d or '01'} {h or '00'}:{mi or '00'}:{se or '00'}"
    if z in ("Z", "z"):
        out += " UTC"
    elif z and th:
        out += f" {z}{th}:{tm or '00'}"
    return out


def _parse_pdf(head, tail):
    info = OrderedDict()
    m = re.match(rb"%PDF-(\d\.\d)", head)
    if m:
        info["Version"] = m.group(1).decode()
    blob = head + b"\n" + tail
    for key in ("Producer", "Creator", "Author", "Title", "Subject", "Keywords", "CreationDate", "ModDate", "Trapped", "Company", "SourceModified"):
        for mm in re.finditer(rb"/" + key.encode() + rb"\s*(\((?:\\.|[^\\)])*\)|<[0-9A-Fa-f\s]*>)", blob):
            val = _pdf_string(mm.group(1)).strip("\x00 ")
            if val:
                if "Date" in key or key == "SourceModified":
                    val = _pdf_date(val)
                info[key] = _clip(val, 300)
    if b"/Encrypt" in blob:
        info["Encrypted"] = "yes"
    if b"/Linearized" in head[:2048]:
        info["Linearized"] = "yes (fast web view)"
    if b"/JavaScript" in blob or b"/JS" in blob:
        info["JavaScript"] = "present"
    if b"/EmbeddedFile" in blob:
        info["Embedded files"] = "present"
    if b"/AcroForm" in blob:
        info["Forms"] = "AcroForm"
    sigs = blob.count(b"/Sig")
    if sigs:
        info["Signature refs"] = sigs
    pages = len(re.findall(rb"/Type\s*/Page[^s]", blob))
    if pages:
        info["Pages (visible objects)"] = pages
    upd = blob.count(b"%%EOF")
    if upd > 1:
        info["Incremental updates"] = upd - 1
    xmp = OrderedDict()
    xs = blob.find(b"<x:xmpmeta")
    if xs >= 0:
        xe = blob.find(b"</x:xmpmeta>", xs)
        if xe > 0:
            xmp = _parse_xmp(blob[xs: xe + 12])
    return info, xmp


def _parse_zip(path):
    info = OrderedDict()
    props = OrderedDict()
    hosts = {0: "MS-DOS / Windows (FAT)", 3: "Unix / Linux / Android", 7: "Macintosh", 10: "Windows NTFS", 11: "MVS", 19: "macOS (OS X)", 14: "VFAT"}
    with zipfile.ZipFile(path) as z:
        items = z.infolist()
        info["Entries"] = len(items)
        systems = OrderedDict()
        for it in items:
            systems[hosts.get(it.create_system, f"host {it.create_system}")] = 1
        info["Created on"] = ", ".join(systems)
        versions = sorted({it.create_version for it in items})
        info["Zip version made by"] = ", ".join(f"{v / 10:g}" for v in versions[:6])
        dates = [datetime(*it.date_time) for it in items if it.date_time[0] >= 1980]
        if dates:
            info["Entry dates (local)"] = f"{min(dates):%Y-%m-%d %H:%M:%S} .. {max(dates):%Y-%m-%d %H:%M:%S}"
        if z.comment:
            info["Comment"] = _clip(z.comment.decode("utf-8", errors="replace"), 300)
        if any(it.flag_bits & 1 for it in items):
            info["Encrypted entries"] = sum(1 for it in items if it.flag_bits & 1)
        names = [it.filename for it in items]
        if any(n.startswith("__MACOSX/") for n in names):
            info["macOS resource forks"] = "yes (__MACOSX), archived in Finder"
        if any(n.endswith(".DS_Store") for n in names):
            info[".DS_Store"] = "present (macOS)"
        if any(n.lower().endswith("thumbs.db") for n in names):
            info["Thumbs.db"] = "present (Windows)"
        kind = None
        if "[Content_Types].xml" in names:
            kind = "OOXML"
        elif "mimetype" in names:
            try:
                kind = "ODF " + z.read("mimetype").decode("latin-1", errors="replace").strip()
            except Exception:
                kind = "ODF"
        elif "AndroidManifest.xml" in names:
            kind = "APK"
        elif any(re.match(r"Payload/[^/]+\.app/Info\.plist$", n) for n in names):
            kind = "IPA"
        elif "META-INF/MANIFEST.MF" in names:
            kind = "JAR"
        if kind:
            info["Package"] = kind

        def rd(name, cap=2 * 1024 * 1024):
            try:
                with z.open(name) as fh:
                    return fh.read(cap).decode("utf-8", errors="replace")
            except Exception:
                return ""

        if kind == "OOXML":
            for part in ("docProps/core.xml", "docProps/app.xml", "docProps/custom.xml"):
                txt = rd(part)
                for m in re.finditer(r"<(?:[\w]+:)?(\w+)(?:\s[^>]*)?>([^<]{1,300})</", txt):
                    k, v = m.group(1), m.group(2).strip()
                    if v and k not in ("vt", "lpstr", "i4", "variant"):
                        props.setdefault(k, html.unescape(v))
                for m in re.finditer(r'<property[^>]*name="([^"]+)"[^>]*>\s*<vt:\w+>([^<]{0,300})</vt', txt):
                    props.setdefault("custom:" + m.group(1), html.unescape(m.group(2)))
            for part in ("word/settings.xml",):
                txt = rd(part)
                m = re.search(r'<w:attachedTemplate r:id="[^"]+"', txt)
                if m:
                    props.setdefault("attachedTemplate", "present")
            rels = rd("word/_rels/settings.xml.rels") + rd("word/_rels/document.xml.rels")
            for m in re.finditer(r'Target="((?:file:|[A-Za-z]:\\|\\\\)[^"]{1,250})"', rels):
                props.setdefault("External target", html.unescape(m.group(1)))
            rsids = len(re.findall(r"<w:rsid ", rd("word/settings.xml")))
            if rsids:
                props["Revision sessions (rsid)"] = rsids
        elif kind and kind.startswith("ODF"):
            txt = rd("meta.xml")
            for m in re.finditer(r"<(?:[\w]+:)?([\w\-]+)(?:\s[^>]*)?>([^<]{1,300})</", txt):
                props.setdefault(m.group(1), html.unescape(m.group(2).strip()))
        elif kind == "IPA":
            plist_name = next(n for n in names if re.match(r"Payload/[^/]+\.app/Info\.plist$", n))
            try:
                pl = plistlib.loads(z.read(plist_name))
                for k in ("CFBundleIdentifier", "CFBundleDisplayName", "CFBundleName", "CFBundleShortVersionString", "CFBundleVersion", "MinimumOSVersion", "DTXcode", "DTPlatformVersion", "DTSDKName"):
                    if k in pl:
                        props[k] = str(pl[k])
            except Exception:
                pass
        elif kind == "APK":
            certs = [n for n in names if re.match(r"META-INF/[^/]+\.(RSA|DSA|EC)$", n)]
            props["v1 signature files"] = ", ".join(certs) or "none"
            props["DEX files"] = sum(1 for n in names if re.match(r"classes\d*\.dex$", n))
            libs = sorted({n.split("/")[1] for n in names if n.startswith("lib/") and n.count("/") >= 2})
            if libs:
                props["Native ABIs"] = ", ".join(libs)
        top = OrderedDict()
        for n in names:
            root = n.split("/", 1)[0]
            top[root] = top.get(root, 0) + 1
        info["Top level"] = ", ".join(f"{k}({v})" if v > 1 else k for k, v in list(top.items())[:12])
    return info, props


def _syncsafe(b):
    return (b[0] << 21) | (b[1] << 14) | (b[2] << 7) | b[3]


def _parse_id3(data):
    info = OrderedDict()
    frames = OrderedDict()
    if data[:3] == b"ID3":
        ver = data[3]
        size = _syncsafe(data[6:10])
        info["ID3"] = f"v2.{ver}.{data[4]}"
        p = 10
        end = min(10 + size, len(data))
        if data[5] & 0x40:
            ext = _syncsafe(data[10:14]) if ver == 4 else struct.unpack(">I", data[10:14])[0] + 4
            p += ext
        while p + 10 <= end:
            if ver == 2:
                fid = data[p: p + 3].decode("latin-1", errors="replace")
                fsize = int.from_bytes(data[p + 3: p + 6], "big")
                body = data[p + 6: p + 6 + fsize]
                p += 6 + fsize
            else:
                fid = data[p: p + 4].decode("latin-1", errors="replace")
                fsize = _syncsafe(data[p + 4: p + 8]) if ver == 4 else struct.unpack(">I", data[p + 4: p + 8])[0]
                body = data[p + 10: p + 10 + fsize]
                p += 10 + fsize
            if not fid.strip("\x00") or fsize <= 0:
                break
            enc = body[:1]
            encs = {b"\x00": "latin-1", b"\x01": "utf-16", b"\x02": "utf-16-be", b"\x03": "utf-8"}

            def txt(b):
                return b.decode(encs.get(enc, "latin-1"), errors="replace").replace("\x00", " / ").strip(" /")

            try:
                if fid in ("APIC", "PIC"):
                    frames[fid] = f"<picture {len(body)} bytes>"
                elif fid in ("COMM", "COM", "USLT"):
                    rest = body[4:]
                    sep = b"\x00\x00" if enc in (b"\x01", b"\x02") else b"\x00"
                    parts = rest.split(sep, 1)
                    frames[f"{fid}"] = _clip(txt(parts[-1]), 300)
                elif fid in ("TXXX", "TXX"):
                    sep = b"\x00\x00" if enc in (b"\x01", b"\x02") else b"\x00"
                    parts = body[1:].split(sep, 1)
                    if len(parts) == 2:
                        frames[f"TXXX:{txt(parts[0])}"] = _clip(txt(parts[1]), 300)
                elif fid.startswith("T"):
                    frames[fid] = _clip(txt(body[1:]), 300)
                elif fid in ("PRIV", "GEOB", "UFID"):
                    owner = body.split(b"\x00", 1)[0]
                    frames[f"{fid}:{_printable(owner, 60)}"] = f"<{len(body)} bytes>"
                elif fid in ("WXXX", "WOAR", "WOAF", "WCOM"):
                    frames[fid] = _printable(body, 200)
            except Exception:
                pass
    lame = data.find(b"LAME")
    if lame >= 0:
        info["LAME tag"] = _printable(data[lame: lame + 9], 9)
    for sig in (b"Xing", b"Info"):
        if data.find(sig, 0, 64 * 1024) >= 0:
            info["VBR header"] = sig.decode()
    return info, frames


def _ebml_vint(data, p, keep_marker=False):
    if p >= len(data):
        return None, p
    first = data[p]
    length = 1
    mask = 0x80
    while length <= 8 and not first & mask:
        mask >>= 1
        length += 1
    if length > 8:
        return None, p + 1
    val = first if keep_marker else first & (mask - 1)
    for k in range(1, length):
        if p + k >= len(data):
            return None, p + length
        val = (val << 8) | data[p + k]
    return val, p + length


def _parse_ebml(data):
    info = OrderedDict()
    tracks = []
    masters = {0x1A45DFA3, 0x18538067, 0x1549A966, 0x1654AE6B, 0xAE, 0xE0, 0xE1, 0x1254C367, 0x7373, 0x67C8}
    names = {
        0x4282: "DocType", 0x4287: "DocTypeVersion", 0x4D80: "MuxingApp", 0x5741: "WritingApp",
        0x7BA9: "Title", 0x4461: "DateUTC", 0x4489: "Duration", 0x2AD7B1: "TimecodeScale",
        0x86: "CodecID", 0xB0: "PixelWidth", 0xBA: "PixelHeight", 0x536E: "Name",
        0x22B59C: "Language", 0xB5: "SamplingFrequency", 0x9F: "Channels", 0x83: "TrackType",
        0x45A3: "TagName", 0x4487: "TagString",
    }
    track = None
    tag_name = None

    def walk(p, end, depth):
        nonlocal track, tag_name
        while p < end and depth < 8:
            eid, p2 = _ebml_vint(data, p, keep_marker=True)
            size, p3 = _ebml_vint(data, p2)
            if eid is None or size is None:
                return
            unknown = size == (1 << (7 * (p3 - p2))) - 1
            body_end = end if unknown else min(p3 + size, end)
            if eid == 0x1F43B675:
                return
            if eid in masters:
                if eid == 0xAE:
                    track = OrderedDict()
                    tracks.append(track)
                walk(p3, body_end, depth + 1)
            elif eid in names:
                raw = data[p3:body_end]
                key = names[eid]
                if eid in (0x4D80, 0x5741, 0x7BA9, 0x86, 0x536E, 0x4282, 0x22B59C, 0x45A3, 0x4487):
                    val = raw.decode("utf-8", errors="replace").strip("\x00")
                elif eid == 0x4489 or eid == 0xB5:
                    val = struct.unpack(">f", raw)[0] if len(raw) == 4 else struct.unpack(">d", raw)[0] if len(raw) == 8 else None
                elif eid == 0x4461:
                    ns = int.from_bytes(raw, "big", signed=True)
                    val = _fmt_dt(datetime(2001, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=ns / 1000))
                else:
                    val = int.from_bytes(raw, "big")
                if key == "TagName":
                    tag_name = val
                elif key == "TagString" and tag_name:
                    info[f"Tag:{tag_name}"] = _clip(val, 200)
                elif key in ("CodecID", "PixelWidth", "PixelHeight", "Name", "Language", "SamplingFrequency", "Channels", "TrackType") and track is not None:
                    track[key] = val
                else:
                    info[key] = val
            if unknown:
                return
            p = body_end

    walk(0, len(data), 0)
    if "Duration" in info and isinstance(info["Duration"], float):
        scale = info.get("TimecodeScale", 1000000)
        info["Duration"] = _fmt_dur(info["Duration"] * scale / 1e9)
    return info, tracks


def _parse_riff(data):
    info = OrderedDict()
    tags = OrderedDict()
    exif = OrderedDict()
    xmp = OrderedDict()
    info["Form"] = _fourcc(data[8:12])
    p = 12
    chunks = []
    while p + 8 <= len(data):
        cid, size = struct.unpack("<4sI", data[p: p + 8])
        body = data[p + 8: p + 8 + size]
        c = _fourcc(cid)
        chunks.append(c)
        try:
            if c == "fmt ":
                fmt, ch, rate, _br, _ba, bits = struct.unpack("<HHIIHH", body[:16])
                info["Audio"] = f"format 0x{fmt:04X}, {ch} ch, {rate} Hz, {bits} bit"
            elif c == "LIST" and body[:4] in (b"INFO", b"exif"):
                q = 4
                while q + 8 <= len(body):
                    sid, ssz = struct.unpack("<4sI", body[q: q + 8])
                    tags[_fourcc(sid)] = body[q + 8: q + 8 + ssz].split(b"\x00", 1)[0].decode("utf-8", errors="replace")
                    q += 8 + ssz + (ssz & 1)
            elif c == "VP8X":
                fl = body[0]
                w = int.from_bytes(body[4:7], "little") + 1
                h = int.from_bytes(body[7:10], "little") + 1
                info["Size"] = f"{w}x{h}"
                info["VP8X flags"] = ", ".join(n for b, n in ((0x20, "ICC"), (0x10, "alpha"), (0x08, "EXIF"), (0x04, "XMP"), (0x02, "animation")) if fl & b) or "none"
            elif c == "VP8 ":
                w, h = struct.unpack("<HH", body[6:10])
                info.setdefault("Size", f"{w & 0x3FFF}x{h & 0x3FFF}")
                info["WebP"] = "lossy (VP8)"
            elif c == "VP8L":
                b = int.from_bytes(body[1:5], "little")
                info.setdefault("Size", f"{(b & 0x3FFF) + 1}x{((b >> 14) & 0x3FFF) + 1}")
                info["WebP"] = "lossless (VP8L)"
            elif c == "EXIF":
                exif.update(_parse_tiff(body[6:] if body[:6] == b"Exif\x00\x00" else body))
            elif c == "XMP ":
                xmp.update(_parse_xmp(body))
            elif c == "ICCP":
                info["ICC"] = _parse_icc(body)
            elif c == "avih":
                usec, _, _, _, frames = struct.unpack("<IIIII", body[:20])
                w, h = struct.unpack("<II", body[32:40])
                info["AVI"] = f"{w}x{h}, {frames} frames, {1e6 / usec if usec else 0:.2f} fps"
        except Exception:
            pass
        if c == "LIST" and body[:4] in (b"hdrl", b"strl"):
            p += 12
            continue
        p += 8 + size + (size & 1)
    info["Chunks"] = " ".join(chunks[:40])
    return info, tags, exif, xmp


def _parse_gif(data):
    info = OrderedDict()
    info["Version"] = data[:6].decode("latin-1")
    w, h = struct.unpack("<HH", data[6:10])
    info["Size"] = f"{w}x{h}"
    comments = []
    apps = []
    p = 13
    if data[10] & 0x80:
        p += 3 * (2 << (data[10] & 7))
    frames = 0
    while p < len(data):
        b = data[p]
        if b == 0x21 and p + 2 < len(data):
            label = data[p + 1]
            p += 2
            blocks = []
            while p < len(data) and data[p]:
                blocks.append(data[p + 1: p + 1 + data[p]])
                p += 1 + data[p]
            p += 1
            if label == 0xFE:
                comments.append(_clip(b"".join(blocks).decode("latin-1", errors="replace"), 200))
            elif label == 0xFF and blocks:
                apps.append(_printable(blocks[0][:11], 11))
        elif b == 0x2C:
            frames += 1
            if p + 10 > len(data):
                break
            flags = data[p + 9]
            p += 10
            if flags & 0x80:
                p += 3 * (2 << (flags & 7))
            p += 1
            while p < len(data) and data[p]:
                p += 1 + data[p]
            p += 1
        else:
            break
    info["Frames"] = frames
    if comments:
        info["Comment"] = " | ".join(comments)
    if apps:
        info["Application ext"] = ", ".join(apps)
    return info


def _decode_waveform(wf):
    if not wf:
        return None
    bits = int.from_bytes(wf, "little")
    count = len(wf) * 8 // 5
    vals = [(bits >> (i * 5)) & 31 for i in range(count)]
    if not vals:
        return None
    step = max(1, len(vals) // 50)
    sampled = [max(vals[i: i + step]) for i in range(0, len(vals), step)][:50]
    return "".join(WAVE_BARS[min(v * len(WAVE_BARS) // 32, len(WAVE_BARS) - 1)] for v in sampled)


def _gps_link(lat, lon):
    return f"https://maps.google.com/?q={lat:.6f},{lon:.6f}"


def _parse_iso6709(text):
    m = re.match(r"([+\-]\d+(?:\.\d+)?)([+\-]\d+(?:\.\d+)?)", text or "")
    if not m:
        return None
    lat, lon = float(m.group(1)), float(m.group(2))
    if lat == 0 and lon == 0:
        return None
    return lat, lon


def _parse_et_gps(value):
    m = re.findall(r"(\d+(?:\.\d+)?) deg (\d+)' (\d+(?:\.\d+)?)\"\s*([NSEW])", str(value))
    if len(m) < 2:
        return None
    res = []
    for d, mi, s, ref in m[:2]:
        v = float(d) + float(mi) / 60 + float(s) / 3600
        res.append(-v if ref in "SW" else v)
    return tuple(res)


class Report:
    def __init__(self, title):
        self.title = title
        self.pages = OrderedDict()

    def add(self, page, title, lines, expandable=True):
        lines = [l for l in lines if l]
        if not lines:
            return
        self.pages.setdefault(page, []).append((title, lines, expandable))


def _kv(key, value, n=300):
    if value is None or value == "" or value == []:
        return None
    if isinstance(value, float):
        value = f"{value:.6g}"
    elif isinstance(value, (list, tuple)):
        value = ", ".join(f"{v:.6g}" if isinstance(v, float) else str(v) for v in value)
    elif isinstance(value, datetime):
        value = _fmt_dt(value)
    return f"{_esc(key)}: <code>{_esc(_clip(value, n))}</code>"


def _visible_len(text):
    return len(html.unescape(re.sub(r"<[^>]+>", "", text)))


def _render_pages(blocks, header):
    pages = []
    cur = header
    cur_len = _visible_len(header)
    for title, lines, expandable in blocks:
        tag_open = "<blockquote expandable>" if expandable else "<blockquote>"
        chunk_lines = []
        chunk_len = 0
        parts = []
        for line in lines:
            ll = _visible_len(line) + 1
            if chunk_lines and chunk_len + ll > PAGE_LIMIT - 200:
                parts.append(chunk_lines)
                chunk_lines, chunk_len = [], 0
            chunk_lines.append(line)
            chunk_len += ll
        if chunk_lines:
            parts.append(chunk_lines)
        for idx, part in enumerate(parts):
            t = title if idx == 0 else f"{title} ({idx + 1})"
            block = f"<b>{_esc(t)}</b>\n{tag_open}" + "\n".join(part) + "</blockquote>\n"
            bl = _visible_len(block)
            if cur_len + bl > PAGE_LIMIT and cur.strip() != header.strip():
                pages.append(cur.rstrip())
                cur = header
                cur_len = _visible_len(header)
            cur += block
            cur_len += bl
    if cur.strip() != header.strip() or not pages:
        pages.append(cur.rstrip())
    return pages


@loader.tds
class MetaDataDumper(loader.Module):
    """Deep metadata dumper for files, photos, videos and video notes with source fingerprinting"""

    strings = {
        "name": "MetaDataDumper",
        "no_reply": (
            "<b>No reply</b>\n"
            "<blockquote>Reply to a message with media</blockquote>"
        ),
        "unsupported": (
            "<b>Unsupported media</b>\n"
            "<blockquote>Reply to a file, voice, photo, video, video note, sticker or GIF</blockquote>"
        ),
        "downloading": (
            "<b>Downloading</b>\n"
            "<blockquote>{what}</blockquote>"
        ),
        "dl_full": "Full file, {size}",
        "dl_partial": "Partial: header, index and tail of {size}",
        "analyzing": (
            "<b>Analyzing</b>\n"
            "<blockquote>Parsing containers, running ExifTool, matching fingerprints...</blockquote>"
        ),
        "failed": (
            "<b>Dump failed</b>\n"
            "<blockquote><code>{error}</code></blockquote>"
        ),
        "expired": "Report expired, run the command again",
        "header": "<b>MetaDataDumper</b> | <code>{title}</code>\n",
        "page_info": "<i>{page}</i>\n",
        "btn_summary": "Summary",
        "btn_telegram": "Telegram",
        "btn_tech": "Container",
        "btn_tags": "Tags",
        "btn_exiftool": "ExifTool",
        "btn_close": "Close",
        "btn_prev": "Prev",
        "btn_next": "Next",
        "sec_verdict": "Source verdict",
        "sec_evidence": "Evidence",
        "sec_key": "Key facts",
        "sec_privacy": "Privacy leaks",
        "sec_time": "Time analysis",
        "sec_notes": "Notes",
        "sec_message": "Message",
        "sec_forward": "Forward header",
        "sec_media": "Media object",
        "sec_attrs": "Attributes",
        "sec_sizes": "Server sizes / thumbs",
        "sec_engine": "Engine",
        "verdict_none": "No reliable fingerprint found",
        "conf_3": "high",
        "conf_2": "medium",
        "conf_1": "low",
        "confidence": "confidence",
        "also": "Other candidates",
        "note_photo": "Compressed photos are re-encoded by Telegram servers. EXIF, GPS and the client encoder are stripped, so the sending platform cannot be derived from photo bytes. Only geometry, quality and server data are available.",
        "note_photo_q": "Luminance/chroma tables match IJG Annex K at quality {q}. Telegram server output is typically about q87.",
        "note_partial": "File is larger than the download limit, only header, moov/index and tail were fetched. Content-dependent tags may be missing.",
        "note_et_missing": "ExifTool is unavailable ({reason}). Built-in parsers were used. Install perl or set EXIFTOOL_PATH for the full dump.",
        "note_hd": "Largest side {side}px: {mode}",
        "mode_hd": "sent in HD mode (2560 limit)",
        "mode_sd": "sent in standard mode (1280 limit)",
        "mode_small": "original was smaller than the compression limit",
        "note_reused": "File was uploaded {delta} before this message: it is a forward, resend or reused cached file.",
        "note_encoded": "Container creation time is {delta} relative to the message. For Telegram clients this is the moment of on-device encoding (device clock).",
        "note_tz": "Device timezone offset from metadata: {tz}",
        "et_installing": "ExifTool is being installed in background, built-in parsers used for now",
        "engine_et": "ExifTool {ver} ({src})",
        "et_more": "... and {n} more tags in this group (truncated)",
        "engine_builtin": "Built-in parsers: {list}",
        "ai_generated": "AI generation markers found",
        "_cls_doc": "Deep metadata dumper for files, photos, videos and video notes with source fingerprinting",
        "_cfg_limit": "Max size in MB to download fully. Larger files are fetched partially (header, index, tail)",
        "_cfg_auto": "Automatically download portable ExifTool (needs perl)",
        "_cfg_path": "Custom path to exiftool binary or script (empty = auto)",
        "_cfg_raw": "Show raw ExifTool dump page",
    }

    strings_ru = {
        "no_reply": (
            "<b>Нет реплая</b>\n"
            "<blockquote>Ответьте на сообщение с медиа</blockquote>"
        ),
        "unsupported": (
            "<b>Неподдерживаемое медиа</b>\n"
            "<blockquote>Ответьте на файл, голосовое, фото, видео, кружок, стикер или GIF</blockquote>"
        ),
        "downloading": (
            "<b>Скачивание</b>\n"
            "<blockquote>{what}</blockquote>"
        ),
        "dl_full": "Полностью, {size}",
        "dl_partial": "Частично: заголовок, индекс и хвост из {size}",
        "analyzing": (
            "<b>Анализ</b>\n"
            "<blockquote>Разбор контейнеров, ExifTool, сверка отпечатков...</blockquote>"
        ),
        "failed": (
            "<b>Ошибка дампа</b>\n"
            "<blockquote><code>{error}</code></blockquote>"
        ),
        "expired": "Отчет устарел, запустите команду снова",
        "header": "<b>MetaDataDumper</b> | <code>{title}</code>\n",
        "page_info": "<i>{page}</i>\n",
        "btn_summary": "Сводка",
        "btn_telegram": "Telegram",
        "btn_tech": "Контейнер",
        "btn_tags": "Теги",
        "btn_exiftool": "ExifTool",
        "btn_close": "Закрыть",
        "btn_prev": "Назад",
        "btn_next": "Далее",
        "sec_verdict": "Вердикт по источнику",
        "sec_evidence": "Доказательства",
        "sec_key": "Ключевые данные",
        "sec_privacy": "Утечки приватных данных",
        "sec_time": "Анализ времени",
        "sec_notes": "Примечания",
        "sec_message": "Сообщение",
        "sec_forward": "Заголовок пересылки",
        "sec_media": "Медиа объект",
        "sec_attrs": "Атрибуты",
        "sec_sizes": "Серверные размеры / превью",
        "sec_engine": "Движок",
        "verdict_none": "Надежный отпечаток не найден",
        "conf_3": "высокая",
        "conf_2": "средняя",
        "conf_1": "низкая",
        "confidence": "уверенность",
        "also": "Другие кандидаты",
        "note_photo": "Сжатые фото пережимаются серверами Telegram. EXIF, GPS и следы клиентского энкодера удаляются, поэтому платформу отправителя по байтам фото определить нельзя. Доступны только геометрия, качество и серверные данные.",
        "note_photo_q": "Таблицы квантования совпадают с IJG Annex K при качестве {q}. Серверный выход Telegram обычно около q87.",
        "note_partial": "Файл больше лимита загрузки, скачаны только заголовок, moov/индекс и хвост. Часть тегов, зависящих от содержимого, может отсутствовать.",
        "note_et_missing": "ExifTool недоступен ({reason}). Использованы встроенные парсеры. Установите perl или задайте EXIFTOOL_PATH для полного дампа.",
        "note_hd": "Наибольшая сторона {side}px: {mode}",
        "mode_hd": "отправлено в HD режиме (лимит 2560)",
        "mode_sd": "отправлено в стандартном режиме (лимит 1280)",
        "mode_small": "оригинал был меньше лимита сжатия",
        "note_reused": "Файл загружен на сервер за {delta} до этого сообщения: это пересылка, повторная отправка или кешированный файл.",
        "note_encoded": "Время создания контейнера: {delta} относительно сообщения. Для клиентов Telegram это момент кодирования на устройстве (по часам устройства).",
        "note_tz": "Часовой пояс устройства из метаданных: {tz}",
        "et_installing": "ExifTool устанавливается в фоне, пока использованы встроенные парсеры",
        "engine_et": "ExifTool {ver} ({src})",
        "et_more": "... и ещё {n} тегов в этой группе (обрезано)",
        "engine_builtin": "Встроенные парсеры: {list}",
        "ai_generated": "Найдены маркеры генерации ИИ",
        "_cls_doc": "Глубокий дамп метаданных файлов, фото, видео и кружков с определением источника по отпечаткам",
        "_cfg_limit": "Максимальный размер в МБ для полной загрузки. Большие файлы качаются частично (заголовок, индекс, хвост)",
        "_cfg_auto": "Автоматически скачивать портативный ExifTool (нужен perl)",
        "_cfg_path": "Свой путь к exiftool (пусто = авто)",
        "_cfg_raw": "Показывать страницу сырого дампа ExifTool",
    }

    def __init__(self):
        self.config = loader.ModuleConfig(
            loader.ConfigValue(
                "FULL_DOWNLOAD_LIMIT_MB",
                40,
                lambda: self.strings("_cfg_limit"),
                validator=loader.validators.Integer(minimum=1, maximum=2000),
            ),
            loader.ConfigValue(
                "AUTO_INSTALL_EXIFTOOL",
                True,
                lambda: self.strings("_cfg_auto"),
                validator=loader.validators.Boolean(),
            ),
            loader.ConfigValue(
                "EXIFTOOL_PATH",
                "",
                lambda: self.strings("_cfg_path"),
                validator=loader.validators.String(),
            ),
            loader.ConfigValue(
                "SHOW_RAW_EXIFTOOL",
                True,
                lambda: self.strings("_cfg_raw"),
                validator=loader.validators.Boolean(),
            ),
        )
        self._reports = OrderedDict()
        self._et_cmd = None
        self._et_ver = None
        self._et_src = None
        self._et_reason = "not checked"
        self._et_lock = asyncio.Lock()
        self._et_task = None

    @property
    def _data_dir(self):
        if path := getattr(self, "_data_dir_path", None):
            return path

        uid = getattr(self, "_tg_id", None)
        if not uid and getattr(self, "_client", None):
            uid = getattr(self._client, "tg_id", None)
        if not uid:
            uid = "common"
        path = os.path.join(tempfile.gettempdir(), f"MetaDataDumper-{uid}")
        try:
            os.makedirs(path, mode=0o700, exist_ok=True)
        except PermissionError:
            path = tempfile.mkdtemp(
                prefix=f"MetaDataDumper-{uid}-",
                dir=tempfile.gettempdir(),
            )
        self._data_dir_path = path
        return self._data_dir_path

    async def client_ready(self, client, db):
        self._client = client
        self._owner = await client.get_me()
        self._tg_id = self._owner.id if self._owner else getattr(client, "tg_id", None)
        try:
            for name in os.listdir(self._data_dir):
                if name.startswith(("dl_", "tmp", "blob")):
                    p = os.path.join(self._data_dir, name)
                    if os.path.isdir(p):
                        shutil.rmtree(p, ignore_errors=True)
                    else:
                        with contextlib.suppress(Exception):
                            os.remove(p)
        except Exception:
            pass
        self._et_task = asyncio.ensure_future(self._ensure_exiftool())

    async def on_unload(self):
        if self._et_task and not self._et_task.done():
            self._et_task.cancel()

    def _s(self, key, **kwargs):
        text = self.strings[key]
        try:
            return text.format(prefix=self.get_prefix(), **kwargs)
        except (KeyError, IndexError):
            return text

    async def _run(self, *args, timeout=60):
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:
                pass
            raise
        return proc.returncode, out, err

    async def _probe_et(self, cmd):
        try:
            code, out, _ = await self._run(*cmd, "-ver", timeout=30)
            ver = out.decode(errors="replace").strip()
            if code == 0 and re.match(r"^\d+\.\d+", ver):
                return ver
        except Exception:
            pass
        return None

    def _download_exiftool(self, version):
        os.makedirs(self._data_dir, exist_ok=True)
        target = os.path.join(self._data_dir, "exiftool")
        last_error = None
        for tpl in EXIFTOOL_URLS:
            url = tpl.format(v=version)
            parts = urllib.parse.urlsplit(url)
            if parts.scheme != "https" or (parts.hostname or "").lower() not in EXIFTOOL_HOSTS:
                last_error = "the archive host is not allowed"
                continue
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "curl/8.5.0"})
                data = urllib.request.urlopen(req, timeout=90).read()
                if EXIFTOOL_SHA256 and hashlib.sha256(data).hexdigest() != EXIFTOOL_SHA256:
                    last_error = "the archive digest does not match the pin"
                    continue
                tf = tarfile.open(fileobj=io.BytesIO(data))
                tmp = tempfile.mkdtemp(dir=self._data_dir)
                for member in tf.getmembers():
                    parts = member.name.split("/", 1)
                    if len(parts) < 2:
                        continue
                    rel = parts[1]
                    if not (rel == "exiftool" or rel.startswith("lib/")):
                        continue
                    if ".." in rel.split("/") or member.issym() or member.islnk():
                        continue
                    dest = os.path.join(tmp, rel)
                    if member.isdir():
                        os.makedirs(dest, exist_ok=True)
                        continue
                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    src = tf.extractfile(member)
                    if src is None:
                        continue
                    with open(dest, "wb") as fh:
                        shutil.copyfileobj(src, fh)
                if not os.path.isfile(os.path.join(tmp, "exiftool")):
                    shutil.rmtree(tmp, ignore_errors=True)
                    last_error = "archive without exiftool script"
                    continue
                if os.path.isdir(target):
                    shutil.rmtree(target, ignore_errors=True)
                os.replace(tmp, target)
                return os.path.join(target, "exiftool")
            except Exception as e:
                last_error = str(e)
        raise RuntimeError(last_error or "download failed")

    def _latest_version(self):
        try:
            req = urllib.request.Request(EXIFTOOL_VER_URL, headers={"User-Agent": "curl/8.5.0"})
            ver = urllib.request.urlopen(req, timeout=20).read().decode().strip()
            if re.match(r"^\d+\.\d+$", ver):
                return ver
        except Exception:
            pass
        return EXIFTOOL_FALLBACK_VER

    async def _ensure_exiftool(self):
        async with self._et_lock:
            if self._et_cmd:
                return True
            perl = shutil.which("perl")
            custom = (self.config["EXIFTOOL_PATH"] or "").strip()
            candidates = []
            if custom:
                if os.access(custom, os.X_OK):
                    candidates.append(([custom], "custom"))
                if perl:
                    candidates.append(([perl, custom], "custom"))
            system = shutil.which("exiftool")
            if system:
                candidates.append(([system], "system"))
            portable = os.path.join(self._data_dir, "exiftool", "exiftool")
            if perl and os.path.isfile(portable):
                candidates.append(([perl, portable], "portable"))
            for cmd, src in candidates:
                ver = await self._probe_et(cmd)
                if ver:
                    self._et_cmd, self._et_ver, self._et_src = cmd, ver, src
                    return True
            if not perl:
                self._et_reason = "perl not found"
                return False
            if not self.config["AUTO_INSTALL_EXIFTOOL"]:
                self._et_reason = "auto install disabled"
                return False
            try:
                version = await asyncio.to_thread(self._latest_version)
                script = await asyncio.to_thread(self._download_exiftool, version)
                ver = await self._probe_et([perl, script])
                if ver:
                    self._et_cmd, self._et_ver, self._et_src = [perl, script], ver, "portable"
                    return True
                self._et_reason = "portable build does not start"
            except Exception as e:
                self._et_reason = f"install failed: {_clip(e, 120)}"
                logger.warning("[MDD] ExifTool install failed: %s", e)
            return False

    async def _exiftool(self, path):
        if not self._et_cmd:
            if self._et_task and not self._et_task.done():
                try:
                    await asyncio.wait_for(asyncio.shield(self._et_task), timeout=45)
                except Exception:
                    pass
            if not self._et_cmd:
                await self._ensure_exiftool()
        if not self._et_cmd:
            return None
        try:
            code, out, err = await self._run(
                *self._et_cmd,
                "-j", "-a", "-G1", "-u", "-s",
                "-api", "LargeFileSupport=1",
                "-charset", "filename=utf8",
                path,
                timeout=120,
            )
            data = json.loads(out.decode("utf-8", errors="replace") or "[]")
            if not data:
                return None
            return {k: v for k, v in data[0].items() if k != "SourceFile" and not k.startswith("System:") and k != "ExifTool:ExifToolVersion"}
        except Exception as e:
            logger.warning("[MDD] exiftool run failed: %s", e)
            return None

    async def _fetch(self, client, doc, path, size, wanted):
        have = set()
        last = (size - 1) // CHUNK

        async def get(chunks):
            chunks = sorted(c for c in chunks if 0 <= c <= last and c not in have)
            if not chunks:
                return
            runs = []
            for c in chunks:
                if runs and c == runs[-1][1] + 1:
                    runs[-1][1] = c
                else:
                    runs.append([c, c])
            with open(path, "r+b") as fh:
                for a, b in runs:
                    pos = a * CHUNK
                    async for part in client.iter_download(
                        doc,
                        offset=a * CHUNK,
                        request_size=CHUNK,
                        chunk_size=CHUNK,
                        limit=b - a + 1,
                        file_size=size,
                    ):
                        fh.seek(pos)
                        fh.write(part)
                        pos += len(part)
                    have.update(range(a, b + 1))

        def span(off, n):
            off = max(0, off)
            end = min(size, off + max(n, 1)) - 1
            return set(range(off // CHUNK, end // CHUNK + 1))

        with open(path, "wb") as fh:
            fh.truncate(size)
        await get(span(0, wanted) | span(size - wanted, wanted))
        with open(path, "rb") as fh:
            head = fh.read(16)
        if head[4:8] in (b"ftyp", b"moov", b"mdat", b"wide", b"free", b"skip"):
            pos = 0
            guard = 0
            while pos + 8 <= size and guard < 128:
                guard += 1
                await get(span(pos, 16))
                with open(path, "rb") as fh:
                    fh.seek(pos)
                    hdr = fh.read(16)
                bsize, typ = struct.unpack(">I4s", hdr[:8])
                if bsize == 1:
                    bsize = struct.unpack(">Q", hdr[8:16])[0]
                elif bsize == 0:
                    bsize = size - pos
                if bsize < 8:
                    break
                if typ in (b"moov", b"meta", b"uuid") and bsize <= MOOV_CAP:
                    await get(span(pos, bsize))
                elif typ == b"mdat":
                    await get(span(pos, 2 * 1024 * 1024))
                pos += bsize

    async def _download(self, client, message, media_obj, size, form, kind, ext):
        os.makedirs(self._data_dir, exist_ok=True)
        tmpdir = tempfile.mkdtemp(prefix="dl_", dir=self._data_dir)
        path = os.path.join(tmpdir, "blob" + ext)
        with open(path, "wb"):
            pass
        limit = int(self.config["FULL_DOWNLOAD_LIMIT_MB"]) * 1024 * 1024
        partial = bool(size and size > limit and kind != "photo")
        what = self._s("dl_partial", size=_fmt_bytes(size)) if partial else self._s("dl_full", size=_fmt_bytes(size))
        await self._edit(form, self._s("downloading", what=what))
        if partial:
            await self._fetch(client, media_obj, path, size, 2 * 1024 * 1024)
        else:
            result = await message.download_media(file=path)
            if isinstance(result, str) and os.path.isfile(result):
                path = result
        return tmpdir, path, partial

    async def _edit(self, form, text, markup=None):
        try:
            if markup is None:
                await form.edit(text)
            else:
                await form.edit(text, reply_markup=markup)
        except Exception as e:
            logger.debug("[MDD] edit failed: %s", e)

    def _analyze_local(self, path, size, partial):
        facts = OrderedDict()
        tech = []
        tags = []
        with open(path, "rb") as fh:
            head = fh.read(8 * 1024 * 1024)
            if size > len(head):
                fh.seek(max(0, size - 2 * 1024 * 1024))
                tail = fh.read()
            else:
                tail = head
        fmt = _sniff(head)
        facts["format"] = fmt
        parsers = [fmt]
        try:
            if fmt == "jpeg":
                with open(path, "rb") as fh:
                    data = fh.read(min(size, 64 * 1024 * 1024))
                res = _parse_jpeg(data)
                if res:
                    info, exif, xmp = res
                    facts["jpeg"] = info
                    facts["exif"] = exif
                    facts["xmp"] = xmp
                    lines = [_kv(k, v) for k, v in info.items() if k not in ("markers", "tables", "flags", "ICC", "Quality")]
                    q = info.get("Quality")
                    if q:
                        lines.append(_kv("Quality estimate", f"q{q['quality']} ({'exact Annex K' if q['exact'] else 'custom tables, mean error ' + str(q['mean_error'])})"))
                    lines.append(_kv("Markers", " ".join(info["markers"][:40])))
                    for fl in info["flags"]:
                        lines.append(f"<code>{_esc(fl)}</code>")
                    tech.append(("JPEG", lines))
                    for tq, vals in info.get("tables", {}).items():
                        rows = [" ".join(f"{v:3d}" for v in vals[r * 8: r * 8 + 8]) for r in range(8)]
                        tech.append((f"DQT #{tq} (zigzag)", ["<code>" + _esc(r) + "</code>" for r in rows]))
                    if info.get("ICC"):
                        tech.append(("ICC", [_kv(k, v) for k, v in info["ICC"].items()]))
                    if exif:
                        tags.append(("EXIF", [_kv(k, v) for k, v in exif.items()]))
                    if xmp:
                        tags.append(("XMP", [_kv(k, v) for k, v in xmp.items()]))
            elif fmt == "png":
                with open(path, "rb") as fh:
                    data = fh.read(min(size, 64 * 1024 * 1024))
                res = _parse_png(data)
                if res:
                    info, text, exif, xmp = res
                    facts["png"] = info
                    facts["png_text"] = text
                    facts["exif"] = exif
                    facts["xmp"] = xmp
                    tech.append(("PNG", [_kv(k, v) for k, v in info.items() if k != "ICC"]))
                    if info.get("ICC"):
                        tech.append(("ICC", [_kv(k, v) for k, v in info["ICC"].items()]))
                    if text:
                        tags.append(("PNG text", [_kv(k, v, 900) for k, v in text.items()]))
                    if exif:
                        tags.append(("EXIF", [_kv(k, v) for k, v in exif.items()]))
                    if xmp:
                        tags.append(("XMP", [_kv(k, v) for k, v in xmp.items()]))
            elif fmt == "isobmff":
                res = _parse_isobmff_file(path, size)
                facts["mp4"] = res
                lines = [
                    _kv("Major brand", res.get("major_brand")),
                    _kv("Minor version", res.get("minor_version")),
                    _kv("Compatible", " ".join(res.get("compatible", []))),
                    _kv("Top level", " ".join(f"{n}({_fmt_bytes(s)})" for n, _, s in res.get("top", [])[:16])),
                    _kv("Fast start (moov before mdat)", "yes" if res.get("moov_first") else "no"),
                    _kv("UUID boxes", ", ".join(res.get("uuid", []))),
                    _kv("Free box text", " | ".join(res.get("free_text", []))),
                ]
                mv = res.get("movie", {})
                lines += [
                    _kv("Created (mvhd)", _fmt_dt(mv.get("created"))),
                    _kv("Modified (mvhd)", _fmt_dt(mv.get("modified"))),
                    _kv("Timescale", mv.get("timescale")),
                    _kv("Duration", _fmt_dur(mv["duration"]) if mv.get("duration") else None),
                ]
                tech.append(("ISO BMFF", lines))
                for t in res.get("tracks", []):
                    tl = [_kv(k, _fmt_dt(v) if isinstance(v, datetime) else (_fmt_dur(v) if k == "duration" and v else v)) for k, v in t.items()]
                    tech.append((f"Track {t.get('id', '?')} ({t.get('handler_type', '?')})", tl))
                if res.get("sei"):
                    tech.append(("Encoder SEI / strings", [_kv(k, v, 900) for k, v in res["sei"].items()]))
                if res.get("tags"):
                    tags.append(("QuickTime / iTunes tags", [_kv(k, v) for k, v in res["tags"].items()]))
                if res.get("xmp"):
                    tags.append(("XMP", [_kv(k, v) for k, v in res["xmp"].items()]))
            elif fmt == "ogg":
                info, comments = _parse_ogg(head, tail, size)
                facts["ogg"] = info
                facts["ogg_comments"] = comments
                tech.append(("Ogg", [_kv(k, v) for k, v in info.items()]))
                if comments:
                    tags.append(("Vorbis comments", [_kv(k, v) for k, v in comments.items()]))
            elif fmt == "pdf":
                pdf_head = head
                if not partial:
                    with open(path, "rb") as fh:
                        pdf_head = fh.read(min(size, 48 * 1024 * 1024))
                info, xmp = _parse_pdf(pdf_head, tail)
                facts["pdf"] = info
                facts["xmp"] = xmp
                tags.append(("PDF Info", [_kv(k, v) for k, v in info.items()]))
                if xmp:
                    tags.append(("XMP", [_kv(k, v) for k, v in xmp.items()]))
            elif fmt == "zip":
                info, props = _parse_zip(path)
                facts["zip"] = info
                facts["office"] = props
                tech.append(("ZIP", [_kv(k, v) for k, v in info.items()]))
                if props:
                    tags.append((info.get("Package", "Properties"), [_kv(k, v) for k, v in props.items()]))
            elif fmt == "mp3":
                info, frames = _parse_id3(head)
                facts["id3"] = frames
                facts["mp3"] = info
                tech.append(("MP3", [_kv(k, v) for k, v in info.items()]))
                if frames:
                    tags.append(("ID3", [_kv(k, v) for k, v in frames.items()]))
            elif fmt == "ebml":
                info, tracks = _parse_ebml(head)
                facts["ebml"] = info
                tech.append(("Matroska / WebM", [_kv(k, v) for k, v in info.items()]))
                for n, t in enumerate(tracks, 1):
                    tech.append((f"Track {n}", [_kv(k, v) for k, v in t.items()]))
            elif fmt == "riff":
                info, rtags, exif, xmp = _parse_riff(head)
                facts["riff"] = info
                facts["riff_tags"] = rtags
                facts["exif"] = exif
                facts["xmp"] = xmp
                tech.append(("RIFF", [_kv(k, v) for k, v in info.items() if k != "ICC"]))
                if rtags:
                    tags.append(("RIFF INFO", [_kv(k, v) for k, v in rtags.items()]))
                if exif:
                    tags.append(("EXIF", [_kv(k, v) for k, v in exif.items()]))
                if xmp:
                    tags.append(("XMP", [_kv(k, v) for k, v in xmp.items()]))
            elif fmt == "gif":
                info = _parse_gif(head)
                facts["gif"] = info
                tech.append(("GIF", [_kv(k, v) for k, v in info.items()]))
            elif fmt == "tiff":
                exif = _parse_tiff(head)
                facts["exif"] = exif
                tags.append(("TIFF / EXIF", [_kv(k, v) for k, v in exif.items()]))
        except Exception as e:
            logger.exception("[MDD] builtin parser failed")
            tech.append(("Parser error", [_kv(fmt, str(e))]))
        if fmt == "unknown":
            tech.append(("Magic", [_kv("First bytes", head[:32].hex(" ")), _kv("ASCII", _printable(head[:64], 64))]))
        return facts, tech, tags, parsers

    def _fingerprint(self, facts, et, kind, filename):
        ev = []

        def add(label, score, why):
            ev.append((label, score, why))

        et = et or {}
        etl = {k.lower(): v for k, v in et.items()}

        def etv(*names):
            for n in names:
                for k, v in et.items():
                    if k.split(":", 1)[-1].lower() == n.lower():
                        return v
            return None

        ogg = facts.get("ogg") or {}
        vendor = ogg.get("Vendor") or (etv("Vendor") if facts.get("format") == "ogg" else None)
        comments = facts.get("ogg_comments") or {}
        if vendor:
            v = str(vendor).strip()
            enc = comments.get("ENCODER", "")
            if "opusenc" in enc.lower():
                add("opus-tools (opusenc)", 3, f"ENCODER = '{enc}'")
            else:
                for rx, label, score in OPUS_RULES:
                    if re.search(rx, v, re.I):
                        add(label, score, f"OpusTags vendor = '{v}'")
                        break
                else:
                    add(f"Encoder '{v}'", 1, f"OpusTags vendor = '{v}'")
            if ogg.get("Input sample rate"):
                add("_info", 0, f"OpusHead input rate = {ogg['Input sample rate']} Hz")
            if enc:
                add("_info", 0, f"ENCODER = '{enc}'")
        mp4 = facts.get("mp4") or {}
        if mp4:
            major = mp4.get("major_brand", "")
            minor = mp4.get("minor_version")
            compat = mp4.get("compatible", [])
            tracks = mp4.get("tracks", [])
            mtags = mp4.get("tags", {})
            hdlrs = [t.get("handler_name", "") for t in tracks if t.get("handler_name")]
            comps = [t.get("compressor", "") for t in tracks if t.get("compressor")]
            encoder = mtags.get("Encoder") or etv("Encoder")
            brand = f"ftyp {major}/{minor} [{' '.join(compat)}]"
            hd = ", ".join(f"'{h}'" for h in hdlrs)
            apple_make = mtags.get("com.apple.quicktime.make") or mtags.get("Make")
            apple_model = mtags.get("com.apple.quicktime.model") or mtags.get("Model")
            apple_sw = mtags.get("com.apple.quicktime.software") or mtags.get("Software")
            android_ver = mtags.get("com.android.version")
            ff_label = "Telegram Desktop (FFmpeg)" if kind == "round" else "FFmpeg (Telegram Desktop, bots, converters)"
            if any(h == "VideoHandle" or h == "SoundHandle" for h in hdlrs):
                if major == "isom" and minor == 512 and "iso2" in compat and not encoder:
                    label = "Telegram Android" if kind in ("round", "video") else "Telegram Android (MP4Builder)"
                    if comps:
                        add(label, 2, f"{brand} + hdlr {hd}, no encoder tag, but compressorname '{comps[0]}' (MP4Builder leaves it empty)")
                    else:
                        add(label, 3, f"{brand} + hdlr {hd}, no encoder tag, empty compressorname (Telegram MP4Builder)")
                elif android_ver:
                    add(f"Android {android_ver} camera / MediaMuxer (original)", 3, f"com.android.version = {android_ver}, hdlr {hd}")
                else:
                    add("Android MediaMuxer (system camera, Telegram X or other app)", 2, f"{brand} + hdlr {hd}")
            if any(h.startswith("Core Media") for h in hdlrs):
                if apple_model or str(apple_make).lower() == "apple":
                    add(f"Apple {apple_model or 'device'} camera original (iOS {apple_sw or '?'})", 3, f"com.apple.quicktime make/model = {apple_make} / {apple_model}")
                elif kind == "round":
                    add("Telegram iOS / macOS (AVFoundation)", 3, f"{brand} + hdlr {hd}")
                else:
                    add("Apple AVFoundation export (Telegram iOS / macOS or iOS app)", 2, f"{brand} + hdlr {hd}")
            if any(c in ("H.264", "HEVC") for c in comps):
                add("Apple VideoToolbox encoder", 2, f"compressorname = {', '.join(comps)}")
            if any(c.startswith("Lavc") for c in comps):
                add(ff_label, 2, f"compressorname = {', '.join(comps)}")
            if any(c in ("AVC Coding", "HEVC Coding") for c in comps):
                add("mp4parser / GPAC style muxer", 1, f"compressorname = {', '.join(comps)}")
            if encoder and re.match(r"lav[fc]", str(encoder), re.I):
                add(ff_label, 2, f"encoder = '{encoder}'")
            elif encoder:
                for rx, label, score in SOFTWARE_RULES:
                    if re.search(rx, str(encoder), re.I):
                        add(label, score, f"encoder = '{encoder}'")
                        break
                else:
                    add(f"Encoder '{encoder}'", 2, f"encoder tag = '{encoder}'")
            if any(h in ("VideoHandler", "SoundHandler") for h in hdlrs) and not encoder:
                add(ff_label, 1, f"hdlr {hd} without encoder tag")
            for h in hdlrs:
                for key, label in HDLR_HINTS:
                    if key in h and key not in ("VideoHandle", "SoundHandle", "Core Media", "VideoHandler", "SoundHandler"):
                        add(label, 3 if "Google" in key else 2, f"hdlr '{h}'")
                        break
            if major == "qt  ":
                add("Apple QuickTime container", 1, f"{brand}")
            if major in ("3gp4", "3gp5", "3gp6", "3g2a"):
                add("3GPP (old phone / MMS / messenger)", 1, brand)
            sei = mp4.get("sei", {})
            if sei.get("x264"):
                core = re.search(r"core (\d+)", sei["x264"])
                add(f"libx264{' core ' + core.group(1) if core else ''}", 2, "x264 SEI string in bitstream")
            if sei.get("x265"):
                add("libx265", 2, "x265 SEI string in bitstream")
            if sei.get("Lavc") and not (encoder and re.match(r"lav[fc]", str(encoder), re.I)):
                add(ff_label, 1, f"'{_clip(sei['Lavc'], 40)}' string in stream data")
            if sei.get("HandBrake"):
                add("HandBrake", 3, "HandBrake string in bitstream")
            cmt = str(mtags.get("Comment", ""))
            if cmt.startswith("vid:"):
                add("TikTok", 2, f"comment '{_clip(cmt, 40)}'")
        ebml = facts.get("ebml") or {}
        if ebml:
            mux, wr = str(ebml.get("MuxingApp", "")), str(ebml.get("WritingApp", ""))
            if "chrome" in wr.lower() or "chrome" in mux.lower():
                add("Chromium MediaRecorder (browser, Telegram Web)", 2, f"MuxingApp '{mux}', WritingApp '{wr}'")
            elif mux.lower().startswith("lavf") or wr.lower().startswith("lavf"):
                add("FFmpeg", 2, f"MuxingApp '{mux}', WritingApp '{wr}'")
            elif mux or wr:
                add(f"Matroska writer '{wr or mux}'", 2, f"MuxingApp '{mux}', WritingApp '{wr}'")
        exif = facts.get("exif") or {}
        xmp = facts.get("xmp") or {}
        make = str(exif.get("IFD0:Make") or etv("Make") or "").strip() or None
        model = str(exif.get("IFD0:Model") or etv("Model") or "").strip() or None
        software = str(exif.get("IFD0:Software") or etv("Software") or "").strip() or None
        maker_note = str(exif.get("ExifIFD:MakerNote", ""))
        if make:
            ml = str(make).lower()
            if ml.startswith("apple"):
                ios = f", iOS {software}" if software and re.match(r"^\d+(\.\d+)*$", str(software)) else ""
                add(f"Apple {model or ''}{ios}".strip(), 3, f"EXIF Make/Model = {make} / {model or '-'}")
            elif any(ml.startswith(m) for m in ANDROID_MAKERS):
                add(f"Android camera: {make} {model or ''}".strip(), 3, f"EXIF Make/Model = {make} / {model or '-'}")
            else:
                add(f"Camera: {make} {model or ''}".strip(), 3, f"EXIF Make/Model = {make} / {model or '-'}")
        if any(k.startswith("Google:HDRPlusMakerNote") for k in et):
            add("Google Pixel camera (HDR+)", 3, "Google HDR+ maker note present")
        if "Apple iOS" in maker_note:
            add("Apple iOS camera", 3, "MakerNote header 'Apple iOS'")
        creator_tool = xmp.get("xmp:CreatorTool") or etv("CreatorTool")
        for src_name, val in (("Software", software), ("CreatorTool", creator_tool), ("HistorySoftwareAgent", xmp.get("stEvt:softwareAgent") or etv("HistorySoftwareAgent"))):
            if not val:
                continue
            for rx, label, score in SOFTWARE_RULES:
                if re.search(rx, str(val), re.I):
                    add(label, score, f"{src_name} = '{val}'")
                    break
        user_comment = str(exif.get("ExifIFD:UserComment", "") or etv("UserComment") or xmp.get("exif:UserComment", ""))
        if "screenshot" in user_comment.lower():
            add("Screenshot (Apple iOS / macOS)", 3, f"UserComment = '{user_comment}'")
        png = facts.get("png") or {}
        if png:
            chunks = png.get("Chunks", "")
            if "iDOT" in chunks:
                add("Apple ImageIO (iOS / macOS screenshot or export)", 2, "private PNG chunk iDOT")
            dpi = png.get("DPI")
            if dpi == "144x144":
                add("macOS Retina screenshot", 1, "pHYs 144 dpi")
        ptext = facts.get("png_text") or {}
        if "parameters" in ptext:
            add("Stable Diffusion (AUTOMATIC1111 / Forge)", 3, "PNG text 'parameters' with generation settings")
        if "prompt" in ptext or "workflow" in ptext:
            add("ComfyUI", 3, "PNG text 'prompt' / 'workflow'")
        if "invokeai_metadata" in ptext or "Dream" in ptext or "sd-metadata" in ptext:
            add("InvokeAI", 3, "PNG text InvokeAI metadata")
        if ptext.get("Software", "").lower().startswith("novelai") or "NovelAI" in ptext.get("Source", ""):
            add("NovelAI", 3, "PNG text Software / Source NovelAI")
        dst = str(xmp.get("Iptc4xmpExt:DigitalSourceType", "") or etv("DigitalSourceType") or "")
        if "trainedAlgorithmicMedia" in dst or "compositeWithTrainedAlgorithmicMedia" in dst:
            add("AI-generated (IPTC DigitalSourceType)", 3, f"DigitalSourceType = {dst.rsplit('/', 1)[-1]}")
        jpeg = facts.get("jpeg") or {}
        if jpeg:
            if any("C2PA" in f for f in jpeg.get("flags", [])) or png.get("C2PA"):
                add("C2PA content credentials (AI tool / camera signing)", 2, "JUMBF / C2PA manifest")
            if any("Samsung" in f for f in jpeg.get("flags", [])):
                add("Samsung camera", 2, "Samsung SEF trailer")
            q = jpeg.get("Quality") or {}
            if q and q.get("exact") and not exif and "JFIF" in jpeg and kind == "file":
                if 85 <= q["quality"] <= 89:
                    add("Re-saved Telegram photo (server JPEG)", 2, f"JFIF, no EXIF, Annex K q{q['quality']}")
                else:
                    add(f"libjpeg-compatible encoder (q{q['quality']})", 1, f"Annex K exact q{q['quality']}, no EXIF")
        pdf = facts.get("pdf") or {}
        producer = pdf.get("Producer") or et.get("PDF:Producer")
        creator = pdf.get("Creator") or et.get("PDF:Creator")
        for src_name, val in (("Producer", producer), ("Creator", creator), ("CreatorTool", creator_tool if facts.get("format") == "pdf" else None)):
            if not val:
                continue
            for rx, label, score in PDF_RULES:
                if re.search(rx, str(val), re.I):
                    add(label, score, f"PDF {src_name} = '{val}'")
                    break
            else:
                add(f"'{_clip(val, 40)}'", 1, f"PDF {src_name} = '{val}'")
        office = facts.get("office") or {}
        app = office.get("Application") or office.get("generator")
        if app:
            ver = office.get("AppVersion", "")
            m = re.search(r"\$(\w+)", str(app))
            os_hint = f", OS {m.group(1)}" if m else ""
            add(f"{_clip(str(app).split('$')[0], 50)} {ver}{os_hint}".strip(), 3, f"Application = '{app}'")
        zipinfo = facts.get("zip") or {}
        if zipinfo.get("Created on") and not app:
            add(f"Archived on {zipinfo['Created on']}", 1, "ZIP central directory 'version made by' host")
        if zipinfo.get("macOS resource forks"):
            add("macOS Finder archive", 2, "__MACOSX entries")
        id3 = facts.get("id3") or {}
        for key in ("TSSE", "TENC", "TSS", "TEN"):
            if id3.get(key):
                val = id3[key]
                for rx, label, score in SOFTWARE_RULES:
                    if re.search(rx, val, re.I):
                        add(label, score, f"ID3 {key} = '{val}'")
                        break
                else:
                    add(f"Encoder '{_clip(val, 40)}'", 2, f"ID3 {key} = '{val}'")
        mp3 = facts.get("mp3") or {}
        if mp3.get("LAME tag"):
            add(mp3["LAME tag"], 1, "LAME header in first frame")
        riff_tags = facts.get("riff_tags") or {}
        if riff_tags.get("ISFT"):
            val = riff_tags["ISFT"]
            for rx, label, score in SOFTWARE_RULES:
                if re.search(rx, val, re.I):
                    add(label, score, f"RIFF ISFT = '{val}'")
                    break
            else:
                add(f"Software '{_clip(val, 40)}'", 2, f"RIFF ISFT = '{val}'")
        if filename:
            fl = filename.lower()
            for rx, label, score in FILENAME_RULES:
                m = re.search(rx, fl, re.I)
                if m:
                    extra = ""
                    if m.groups() and m.group(m.lastindex or 1):
                        extra = f" ({m.group(m.lastindex or 1)})"
                    add(label + extra, score, f"file name pattern '{filename}'")
                    break
        return ev

    def _privacy(self, facts, et, filename):
        out = []
        gps = None
        exif = facts.get("exif") or {}
        if exif:
            gps = _gps_from_exif(exif)
        mp4 = facts.get("mp4") or {}
        mtags = mp4.get("tags", {}) if mp4 else {}
        for key in ("GPSCoordinates", "com.apple.quicktime.location.ISO6709", "location", "com.android.capture.location"):
            if not gps and mtags.get(key):
                gps = _parse_iso6709(str(mtags[key]))
        et = et or {}
        if not gps:
            for k, v in et.items():
                if k.endswith(":GPSPosition") or k.endswith(":GPSCoordinates"):
                    gps = _parse_et_gps(v) or _parse_iso6709(str(v))
                    if gps:
                        break
        if gps:
            out.append(_kv("GPS", f"{gps[0]:.6f}, {gps[1]:.6f}"))
            out.append(f"<a href=\"{_gps_link(*gps)}\">{_esc(_gps_link(*gps))}</a>")
        sensitive = (
            "SerialNumber", "BodySerialNumber", "LensSerialNumber", "InternalSerialNumber",
            "CameraSerialNumber", "OwnerName", "CameraOwnerName", "Artist", "Author",
            "LastModifiedBy", "Creator", "creator", "lastModifiedBy", "initial-creator",
            "Company", "HostComputer", "Manager", "XPAuthor", "By-line", "Copyright",
            "com.apple.quicktime.author", "Contact", "Owner",
        )
        seen = set()
        pools = [
            ("EXIF", exif),
            ("Office", facts.get("office") or {}),
            ("PDF", facts.get("pdf") or {}),
            ("XMP", facts.get("xmp") or {}),
            ("QuickTime", mtags),
            ("ExifTool", et),
        ]
        for src, pool in pools:
            for k, v in pool.items():
                group, _, base = k.rpartition(":")
                if base not in sensitive or not v or str(v) in seen:
                    continue
                if base == "Creator" and (src == "PDF" or group == "PDF"):
                    continue
                seen.add(str(v))
                out.append(_kv(k, v))
        paths = set()
        blobs = [json.dumps(et, ensure_ascii=False)] if et else []
        for _src, pool in pools[:5]:
            blobs.append(json.dumps({k: str(v) for k, v in pool.items()}, ensure_ascii=False))
        for blob in blobs:
            for m in re.finditer(r"([A-Za-z]:\\\\Users\\\\[^\\\"]+|/Users/[^/\"]+|/home/[^/\"]+|/storage/emulated/\d+/[^\"]{0,60})", blob):
                p = m.group(1).replace("\\\\", "\\")
                if "/home/" in p and ("mdd_" in p or "tmp" in p):
                    continue
                paths.add(p)
        for p in sorted(paths)[:8]:
            out.append(_kv("User path", p))
        return out

    def _verdict_lines(self, ev):
        scored = OrderedDict()
        for label, score, why in ev:
            if label == "_info":
                continue
            item = scored.setdefault(label, [0, []])
            item[0] += score
            item[1].append(why)
        lines = []
        if not scored:
            return [f"<i>{_esc(self.strings['verdict_none'])}</i>"], scored
        ranked = sorted(scored.items(), key=lambda x: -x[1][0])
        top_label, (top_score, _) = ranked[0]
        conf = self.strings["conf_3"] if top_score >= 3 else self.strings["conf_2"] if top_score == 2 else self.strings["conf_1"]
        lines.append(f"<b>{_esc(top_label)}</b>")
        lines.append(f"{_esc(self.strings['confidence'])}: <code>{_esc(conf)}</code>")
        others = [f"{_esc(l)} <code>{s}</code>" for l, (s, _) in ranked[1:6]]
        if others:
            lines.append(f"{_esc(self.strings['also'])}: " + "; ".join(others))
        return lines, scored

    def _telegram_blocks(self, message, media, report):
        msg_lines = [
            _kv("Message ID", message.id),
            _kv("Chat ID", message.chat_id),
            _kv("Sender ID", message.sender_id),
            _kv("Date", _fmt_dt(message.date)),
            _kv("Edited", _fmt_dt(message.edit_date)),
            _kv("Grouped ID (album)", message.grouped_id),
            _kv("Via bot ID", message.via_bot_id),
            _kv("Post author", getattr(message, "post_author", None)),
            _kv("Views", getattr(message, "views", None)),
            _kv("Forwards", getattr(message, "forwards", None)),
            _kv("TTL period", getattr(message, "ttl_period", None)),
            _kv("No forwards (protected)", True if getattr(message, "noforwards", False) else None),
            _kv("From scheduled", True if getattr(message, "from_scheduled", False) else None),
            _kv("Caption length", len(message.message) if message.message else None),
            _kv("Spoiler", True if getattr(media, "spoiler", False) else None),
            _kv("Self-destruct (ttl_seconds)", getattr(media, "ttl_seconds", None)),
        ]
        report.add("telegram", self.strings["sec_message"], msg_lines)
        fwd = message.fwd_from
        if fwd:
            f = fwd.to_dict()
            fl = []
            for k, v in f.items():
                if k == "_" or v in (None, False):
                    continue
                if isinstance(v, datetime):
                    v = _fmt_dt(v)
                elif isinstance(v, dict):
                    v = ", ".join(f"{a}={b}" for a, b in v.items() if a != "_")
                fl.append(_kv(k, v))
            report.add("telegram", self.strings["sec_forward"], fl)

    def _media_blocks(self, media, report):
        if isinstance(media, MessageMediaPhoto) and media.photo:
            p = media.photo
            lines = [
                _kv("Photo ID", p.id),
                _kv("DC", p.dc_id),
                _kv("Uploaded", _fmt_dt(p.date)),
                _kv("Has stickers", True if getattr(p, "has_stickers", False) else None),
            ]
            report.add("telegram", self.strings["sec_media"], lines)
            sl = []
            for s in p.sizes or []:
                sl.append(self._size_line(s))
            for v in getattr(p, "video_sizes", None) or []:
                sl.append(self._size_line(v, video=True))
            report.add("telegram", self.strings["sec_sizes"], sl)
            return
        doc = getattr(media, "document", None)
        if not doc:
            return
        lines = [
            _kv("Document ID", doc.id),
            _kv("DC", doc.dc_id),
            _kv("MIME", doc.mime_type),
            _kv("Size", f"{_fmt_bytes(doc.size)} ({doc.size} B)"),
            _kv("Uploaded", _fmt_dt(doc.date)),
        ]
        alts = getattr(media, "alt_documents", None) or []
        if alts:
            lines.append(_kv("Server alt qualities", ", ".join(self._alt_desc(a) for a in alts)))
        report.add("telegram", self.strings["sec_media"], lines)
        al = []
        for a in doc.attributes or []:
            d = a.to_dict()
            name = d.pop("_", type(a).__name__).replace("DocumentAttribute", "")
            parts = []
            for k, v in d.items():
                if v in (None, False, b""):
                    continue
                if k == "waveform" and isinstance(v, (bytes, bytearray)):
                    wave = _decode_waveform(v)
                    parts.append(f"waveform[{len(v)}B]")
                    if wave:
                        al.append(f"<b>{_esc(name)}</b> waveform: <code>{wave}</code>")
                    continue
                if isinstance(v, (bytes, bytearray)):
                    v = f"<{len(v)} bytes>"
                elif isinstance(v, dict):
                    v = v.get("_", "obj")
                parts.append(f"{k}={v}")
            al.append(f"<b>{_esc(name)}</b>: <code>{_esc(_clip(', '.join(parts) or '-', 400))}</code>")
        report.add("telegram", self.strings["sec_attrs"], al)
        sl = [self._size_line(s) for s in doc.thumbs or []]
        sl += [self._size_line(v, video=True) for v in getattr(doc, "video_thumbs", None) or []]
        report.add("telegram", self.strings["sec_sizes"], sl)

    def _alt_desc(self, alt):
        w = h = None
        for a in getattr(alt, "attributes", []) or []:
            if isinstance(a, DocumentAttributeVideo):
                w, h = a.w, a.h
                codec = getattr(a, "video_codec", None)
                return f"{w}x{h} {codec or ''} {_fmt_bytes(alt.size)}".strip()
        return _fmt_bytes(getattr(alt, "size", 0))

    def _size_line(self, s, video=False):
        typ = getattr(s, "type", "?")
        cls = type(s).__name__
        w, h = getattr(s, "w", None), getattr(s, "h", None)
        dims = f"{w}x{h}" if w else "-"
        if hasattr(s, "sizes") and isinstance(getattr(s, "sizes"), list):
            size = f"progressive {', '.join(_fmt_bytes(x) for x in s.sizes)}"
        elif getattr(s, "size", None):
            size = _fmt_bytes(s.size)
        elif getattr(s, "bytes", None):
            size = f"inline {len(s.bytes)} B"
        else:
            size = ""
        extra = ""
        if video and getattr(s, "video_start_ts", None):
            extra = f", start {s.video_start_ts}s"
        return f"<code>{_esc(typ)}</code> {_esc(cls)}: <code>{_esc(dims)}</code> {_esc(size)}{_esc(extra)}"

    def _et_pages(self, et, report):
        if not et:
            return
        groups = OrderedDict()
        for k, v in et.items():
            g, _, t = k.partition(":")
            if not t:
                g, t = "Main", g
            if isinstance(v, (dict, list)):
                v = json.dumps(v, ensure_ascii=False)
            groups.setdefault(g, []).append(_kv(t, v, 500))
        for g, lines in groups.items():
            if len(lines) > ET_GROUP_CAP:
                extra = len(lines) - ET_GROUP_CAP
                lines = lines[:ET_GROUP_CAP] + [f"<i>{_esc(self._s('et_more', n=extra))}</i>"]
            report.add("exiftool", f"[{g}]", lines)

    def _round_attr_hints(self, info):
        ev = []
        fn = (info.get("filename") or "").strip()
        lo = fn.lower()
        w = info.get("vw") or 0
        h = info.get("vh") or 0
        dimsig = None
        if w and h and w == h:
            dimsig = {
                (300, 300): ("Telegram macOS", "native macOS app rounds video notes to 300px"),
                (384, 384): ("Telegram Android", "Telegram for Android processes video notes at 384px"),
                (240, 240): ("Telegram X", "Telegram X sticks to legacy 240px video notes"),
            }.get((w, h))
        if lo.startswith("round_"):
            ev.append(("Telegram Desktop", 3, f"documentAttributeFilename = '{fn}' (round_* server pattern marks Telegram Desktop video notes)"))
            if dimsig:
                ev.append((dimsig[0], 1, f"DocumentAttributeVideo {w}x{h} ({dimsig[1]}), but filename already flags Desktop"))
            return ev
        ios_name = lo in ("video.mp4", "file.mp4", "video note.mp4", "videonote.mp4")
        if dimsig:
            ev.append((dimsig[0], 3, f"DocumentAttributeVideo {w}x{h} ({dimsig[1]})"))
        if ios_name:
            score = 2 if dimsig else 3
            ev.append(("Telegram iOS", score, f"documentAttributeFilename = '{fn}' (server default when the client sends no name: iOS video notes)"))
            ev.append(("Telegram Web (A/K)", 2, "same server metadata as iOS: generic name, indistinguishable before download"))
        return ev

    def _detect_media(self, reply):
        media = reply.media
        doc = getattr(media, "document", None) if isinstance(media, MessageMediaDocument) else None
        info = {"media": media, "doc": doc, "round": False, "voice": False, "video": False, "photo": isinstance(media, MessageMediaPhoto) and bool(getattr(media, "photo", None)), "filename": None, "animated": False, "sticker": False, "vw": 0, "vh": 0}
        if doc:
            for a in doc.attributes or []:
                if isinstance(a, DocumentAttributeVideo):
                    info["video"] = True
                    info["vw"] = a.w or 0
                    info["vh"] = a.h or 0
                    if a.round_message:
                        info["round"] = True
                elif isinstance(a, DocumentAttributeAudio) and a.voice:
                    info["voice"] = True
                elif isinstance(a, DocumentAttributeFilename):
                    info["filename"] = a.file_name
                elif isinstance(a, DocumentAttributeAnimated):
                    info["animated"] = True
                elif isinstance(a, DocumentAttributeSticker):
                    info["sticker"] = True
            if not info["video"] and (doc.mime_type or "").startswith("video/") and not info["filename"]:
                info["video"] = True
        return info

    def _ext_for(self, info, title):
        if info["photo"]:
            return ".jpg"
        ext = os.path.splitext(title or "")[1].lower()
        if re.match(r"^\.[a-z0-9]{1,6}$", ext):
            return ext
        mime = (info["doc"].mime_type or "").split(";")[0] if info["doc"] else ""
        fixed = {"audio/ogg": ".ogg", "audio/opus": ".opus", "video/mp4": ".mp4", "video/quicktime": ".mov", "audio/mpeg": ".mp3", "image/webp": ".webp", "application/x-tgsticker": ".tgs", "video/webm": ".webm", "audio/mp4": ".m4a", "audio/x-m4a": ".m4a"}
        return fixed.get(mime) or mimetypes.guess_extension(mime or "") or ".bin"

    async def _deny(self, message, key):
        await utils.answer(message, self._s(key))

    async def _dump(self, message):
        reply = await message.get_reply_message()
        if not reply or not reply.media:
            return await self._deny(message, "no_reply")
        info = self._detect_media(reply)
        if info["photo"]:
            kind = "photo"
        elif not info["doc"]:
            return await self._deny(message, "unsupported")
        elif info["round"]:
            kind = "round"
        elif info["video"] and not info["sticker"]:
            kind = "video"
        else:
            kind = "file"
        media = info["media"]
        if info["photo"]:
            largest = None
            for s in media.photo.sizes or []:
                sz = getattr(s, "size", None) or (max(s.sizes) if getattr(s, "sizes", None) else None)
                if sz and (largest is None or sz > largest):
                    largest = sz
            size = largest or 0
            title = f"photo_{media.photo.id}.jpg"
        else:
            size = info["doc"].size
            title = info["filename"] or ("voice.ogg" if info["voice"] else "round.mp4" if info["round"] else f"document_{info['doc'].id}")
        form = await self.inline.form(
            text=self._s("downloading", what=_esc(title)),
            message=message,
            reply_markup=[[{"text": self.strings["btn_close"], "callback": self._cb_close, "args": ("",), "style": "danger"}]],
            silent=True,
        )
        if not form:
            return
        tmpdir = None
        try:
            tmpdir, path, partial = await self._download(message.client, reply, info["doc"] or media.photo, size, form, kind if kind == "photo" else "doc", self._ext_for(info, title))
            real_size = size or os.path.getsize(path)
            await self._edit(form, self._s("analyzing"))
            facts, tech, tags, parsers = await asyncio.to_thread(self._analyze_local, path, real_size, partial)
            et = await self._exiftool(path)
            report = self._build_report(reply, info, kind, title, real_size, facts, tech, tags, parsers, et, partial)
        except Exception as e:
            logger.exception("[MDD] dump failed")
            await self._edit(
                form,
                self._s("failed", error=_esc(_clip(e, 300))),
                [[{"text": self.strings["btn_close"], "callback": self._cb_close, "args": ("",), "style": "danger"}]],
            )
            return
        finally:
            if tmpdir:
                shutil.rmtree(tmpdir, ignore_errors=True)
        uid = uuid.uuid4().hex[:12]
        self._reports[uid] = report
        while len(self._reports) > 25:
            self._reports.popitem(last=False)
        text, markup = self._page(uid, "summary", 0)
        await self._edit(form, text, markup)

    def _build_report(self, reply, info, kind, title, size, facts, tech, tags, parsers, et, partial):
        report = Report(title)
        media = info["media"]
        doc = info["doc"]
        filename = info["filename"]
        ev = (self._round_attr_hints(info) if kind == "round" else []) + self._fingerprint(facts, et, kind, filename)
        verdict, scored = self._verdict_lines(ev)
        report.add("summary", self.strings["sec_verdict"], verdict, expandable=False)
        evl = []
        for label, (score, whys) in scored.items():
            for w in whys:
                evl.append(f"<b>{_esc(label)}</b> [{score}]: <code>{_esc(_clip(w, 300))}</code>")
        for label, score, why in ev:
            if label == "_info":
                evl.append(f"<code>{_esc(why)}</code>")
        report.add("summary", self.strings["sec_evidence"], evl)
        key = [
            _kv("Type", {"file": "File", "photo": "Photo", "video": "Video", "round": "Video note"}[kind] + (" (voice)" if info["voice"] else "")),
            _kv("Name", filename),
            _kv("Container", facts.get("format")),
            _kv("MIME", doc.mime_type if doc else "image/jpeg"),
            _kv("Size", f"{_fmt_bytes(size)} ({size} B)"),
        ]
        dims = None
        duration = None
        mp4 = facts.get("mp4") or {}
        for t in mp4.get("tracks", []):
            if t.get("handler_type") == "vide":
                dims = t.get("dims") or t.get("coded")
                key.append(_kv("Video", " | ".join(str(x) for x in (t.get("codec"), t.get("profile"), t.get("dims"), f"{t['fps']} fps" if t.get("fps") else None, f"rot {t['rotation']}" if t.get("rotation") else None) if x)))
            elif t.get("handler_type") == "soun":
                key.append(_kv("Audio", " | ".join(str(x) for x in (t.get("codec"), t.get("aac"), t.get("audio")) if x)))
        if mp4.get("movie", {}).get("duration"):
            duration = mp4["movie"]["duration"]
        ogg = facts.get("ogg") or {}
        if ogg:
            key.append(_kv("Audio", " | ".join(str(x) for x in (ogg.get("Codec"), f"{ogg.get('Channels')} ch" if ogg.get("Channels") else None, ogg.get("Avg bitrate")) if x)))
            key.append(_kv("Vendor", ogg.get("Vendor")))
            key.append(_kv("Duration", ogg.get("Duration")))
        jpeg = facts.get("jpeg") or {}
        if jpeg:
            dims = jpeg.get("Size")
            q = jpeg.get("Quality") or {}
            key.append(_kv("JPEG", " | ".join(str(x) for x in (jpeg.get("Size"), jpeg.get("Encoding"), jpeg.get("Subsampling"), f"q{q['quality']}{' exact' if q.get('exact') else '~'}" if q else None) if x)))
        png = facts.get("png") or {}
        if png:
            key.append(_kv("PNG", " | ".join(str(x) for x in (png.get("Size"), png.get("Color"), png.get("DPI")) if x)))
        if duration:
            key.append(_kv("Duration", _fmt_dur(duration)))
        exif = facts.get("exif") or {}
        for k in ("IFD0:Make", "IFD0:Model", "IFD0:Software", "ExifIFD:LensModel", "ExifIFD:DateTimeOriginal", "ExifIFD:OffsetTimeOriginal"):
            if exif.get(k):
                key.append(_kv(k.split(":", 1)[1], exif[k]))
        pdf = facts.get("pdf") or {}
        for k in ("Producer", "Creator", "Author", "CreationDate", "ModDate"):
            if pdf.get(k):
                key.append(_kv(k, pdf[k]))
        office = facts.get("office") or {}
        for k in ("Application", "AppVersion", "creator", "lastModifiedBy", "created", "modified", "TotalTime", "generator", "initial-creator"):
            if office.get(k):
                key.append(_kv(k, office[k]))
        mtags = mp4.get("tags", {}) if mp4 else {}
        for k in ("Encoder", "com.apple.quicktime.make", "com.apple.quicktime.model", "com.apple.quicktime.software", "com.apple.quicktime.creationdate", "com.android.version", "com.android.manufacturer", "com.android.model"):
            if mtags.get(k):
                key.append(_kv(k, mtags[k]))
        if et:
            for tagname in ("Composite:Megapixels", "Composite:ImageSize", "Composite:Duration", "QuickTime:Duration", "PDF:PageCount", "XMP-xmp:CreatorTool"):
                if et.get(tagname) and not any(tagname.split(":", 1)[1] in (l or "") for l in key):
                    key.append(_kv(tagname.split(":", 1)[1], et[tagname]))
        report.add("summary", self.strings["sec_key"], key)
        priv = self._privacy(facts, et, filename)
        ai = [l for l, s, w in ev if "AI" in l or "Stable Diffusion" in l or "ComfyUI" in l or "NovelAI" in l]
        if ai:
            priv.insert(0, f"<b>{_esc(self.strings['ai_generated'])}</b>: <code>{_esc(', '.join(ai))}</code>")
        report.add("summary", self.strings["sec_privacy"], priv, expandable=False)
        tl = []
        msg_date = reply.date
        up = None
        if doc:
            up = doc.date
        elif info["photo"]:
            up = media.photo.date
        if up and msg_date:
            delta = (msg_date - up).total_seconds()
            tl.append(_kv("Uploaded to server", _fmt_dt(up)))
            tl.append(_kv("Message date", _fmt_dt(msg_date)))
            if delta > 120:
                tl.append(_esc(self.strings["note_reused"].format(delta=_fmt_delta(delta)[1:])))
        created = mp4.get("movie", {}).get("created") if mp4 else None
        if created and msg_date:
            d = (created - msg_date).total_seconds()
            tl.append(_kv("Container created", _fmt_dt(created)))
            tl.append(_esc(self.strings["note_encoded"].format(delta=_fmt_delta(d))))
        tz = exif.get("ExifIFD:OffsetTimeOriginal") or exif.get("ExifIFD:OffsetTime")
        if not tz and mtags.get("com.apple.quicktime.creationdate"):
            m = re.search(r"([+\-]\d{2}:?\d{2})$", str(mtags["com.apple.quicktime.creationdate"]))
            tz = m.group(1) if m else None
        if not tz and pdf.get("CreationDate"):
            m = re.search(r" ([+\-]\d{2}:\d{2})$", pdf["CreationDate"])
            tz = m.group(1) if m else None
        if tz:
            tl.append(_esc(self.strings["note_tz"].format(tz=tz)))
        if exif.get("ExifIFD:DateTimeOriginal"):
            tl.append(_kv("Shot (DateTimeOriginal)", f"{exif['ExifIFD:DateTimeOriginal']} {tz or ''}".strip()))
        report.add("summary", self.strings["sec_time"], tl)
        notes = []
        if kind == "photo":
            notes.append(_esc(self.strings["note_photo"]))
            q = jpeg.get("Quality") or {}
            if q:
                notes.append(_esc(self.strings["note_photo_q"].format(q=q["quality"])))
            if dims and "x" in str(dims):
                side = max(int(x) for x in str(dims).split("x"))
                mode = self.strings["mode_hd"] if side >= 2000 and side <= 2560 else self.strings["mode_sd"] if side == 1280 else self.strings["mode_small"] if side < 1280 else self.strings["mode_hd"]
                notes.append(_esc(self.strings["note_hd"].format(side=side, mode=mode)))
        if partial:
            notes.append(_esc(self.strings["note_partial"]))
        if not et:
            if self._et_task and not self._et_task.done():
                notes.append(_esc(self.strings["et_installing"]))
            else:
                notes.append(_esc(self.strings["note_et_missing"].format(reason=self._et_reason)))
        report.add("summary", self.strings["sec_notes"], notes)
        eng = [self._s("engine_builtin", list=", ".join(parsers))]
        if et:
            eng.append(self._s("engine_et", ver=self._et_ver, src=self._et_src))
        report.add("summary", self.strings["sec_engine"], [_esc(e) for e in eng])
        self._telegram_blocks(reply, media, report)
        self._media_blocks(media, report)
        for title2, lines in tech:
            report.add("tech", title2, lines)
        for title2, lines in tags:
            report.add("tags", title2, lines)
        if self.config["SHOW_RAW_EXIFTOOL"]:
            self._et_pages(et, report)
        report.rendered = OrderedDict()
        header = self._s("header", title=_esc(_clip(title, 60)))
        names = {"summary": "btn_summary", "telegram": "btn_telegram", "tech": "btn_tech", "tags": "btn_tags", "exiftool": "btn_exiftool"}
        for page in ("summary", "telegram", "tech", "tags", "exiftool"):
            if page in report.pages:
                head = header + self._s("page_info", page=self.strings[names[page]])
                report.rendered[page] = _render_pages(report.pages[page], head)
        return report

    def _page(self, uid, page, idx):
        report = self._reports[uid]
        chunks = report.rendered.get(page) or report.rendered["summary"]
        idx = max(0, min(idx, len(chunks) - 1))
        names = {"summary": "btn_summary", "telegram": "btn_telegram", "tech": "btn_tech", "tags": "btn_tags", "exiftool": "btn_exiftool"}
        row = []
        rows = []
        for p in report.rendered:
            label = self.strings[names[p]]
            if len(report.rendered[p]) > 1:
                label = f"{label} {len(report.rendered[p])}"
            row.append({
                "text": label,
                "callback": self._cb_page,
                "args": (uid, p, 0),
                "style": "success" if p == page else "primary",
            })
            if len(row) == 3:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        if len(chunks) > 1:
            rows.append([
                {"text": self.strings["btn_prev"], "callback": self._cb_page, "args": (uid, page, (idx - 1) % len(chunks)), "style": "primary"},
                {"text": f"{idx + 1}/{len(chunks)}", "callback": self._cb_page, "args": (uid, page, idx), "style": "primary"},
                {"text": self.strings["btn_next"], "callback": self._cb_page, "args": (uid, page, (idx + 1) % len(chunks)), "style": "primary"},
            ])
        rows.append([{"text": self.strings["btn_close"], "callback": self._cb_close, "args": (uid,), "style": "danger"}])
        return chunks[idx], rows

    async def _cb_page(self, call: InlineCall, uid, page, idx):
        if uid not in self._reports:
            await call.answer(self.strings["expired"], show_alert=True)
            return
        text, markup = self._page(uid, page, idx)
        await call.edit(text, reply_markup=markup)

    async def _cb_close(self, call: InlineCall, uid):
        self._reports.pop(uid, None)
        await call.delete()

    @loader.command(
        ru_doc="Реплай на любое медиа - дамп метаданных и определение источника. Тип (файл, голосовое, фото, видео, кружок) определяется автоматически",
        en_doc="Reply to any media - metadata dump and source fingerprinting. Type (file, voice, photo, video, video note) is detected automatically",
    )
    async def mdd(self, message):
        """Reply to any media - metadata dump and source fingerprinting. Type (file, voice, photo, video, video note) is detected automatically"""
        await self._dump(message)
