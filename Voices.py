__version__ = (1, 0, 1)
# meta developer: I_execute.t.me

import os
import io
import logging
import asyncio
import tempfile
import shutil

from telethon.tl.types import Message, DocumentAttributeAudio
from .. import loader, utils

logger = logging.getLogger(__name__)

E_GEAR = '<tg-emoji emoji-id=5258511597898340942>😵</tg-emoji>'
E_MIC = '<tg-emoji emoji-id=5255888339248125403>😎</tg-emoji>'
E_ERR = '<tg-emoji emoji-id=5258084738278658226>😠</tg-emoji>'


def _log(tag: str, msg: str):
    logger.info(f"[Voices][{tag}] {msg}")


@loader.tds
class Voices(loader.Module):
    """Convert video/video note to voice message"""

    strings = {
        "name": "Voices",
        "no_reply": "<b>Reply to a video or video note</b>",
        "no_reply_prem": f"{E_ERR} <b>Reply to a video or video note</b>",
        "processing": "<b>Processing...</b>",
        "processing_prem": f"{E_GEAR} <b>Processing...</b>",
        "converting": "<b>Converting to voice...</b>",
        "converting_prem": f"{E_GEAR} <b>Converting to voice...</b>",
        "uploading": "<b>Uploading voice message...</b>",
        "uploading_prem": f"{E_GEAR} <b>Uploading voice message...</b>",
        "error": "<b>Error:</b> {msg}",
        "error_prem": f"{E_ERR} <b>Error:</b> {{msg}}",
        "download_error": "<b>Failed to download media</b>",
        "download_error_prem": f"{E_ERR} <b>Failed to download media</b>",
        "convert_error": "<b>Conversion failed</b>",
        "convert_error_prem": f"{E_ERR} <b>Conversion failed</b>",
    }

    strings_ru = {
        "no_reply": "<b>Ответь на видео или кружок</b>",
        "no_reply_prem": f"{E_ERR} <b>Ответь на видео или кружок</b>",
        "processing": "<b>Обработка...</b>",
        "processing_prem": f"{E_GEAR} <b>Обработка...</b>",
        "converting": "<b>Конвертирую в голосовое...</b>",
        "converting_prem": f"{E_GEAR} <b>Конвертирую в голосовое...</b>",
        "uploading": "<b>Загружаю голосовое сообщение...</b>",
        "uploading_prem": f"{E_GEAR} <b>Загружаю голосовое сообщение...</b>",
        "error": "<b>Ошибка:</b> {msg}",
        "error_prem": f"{E_ERR} <b>Ошибка:</b> {{msg}}",
        "download_error": "<b>Не удалось скачать медиа</b>",
        "download_error_prem": f"{E_ERR} <b>Не удалось скачать медиа</b>",
        "convert_error": "<b>Ошибка конвертации</b>",
        "convert_error_prem": f"{E_ERR} <b>Ошибка конвертации</b>",
    }

    def __init__(self):
        self._tmp = None
        self._me_id = None
        self._premium = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        me = await client.get_me()
        self._me_id = me.id
        self._premium = getattr(me, "premium", False)
        self._tmp = os.path.join(tempfile.gettempdir(), f"Voices_{me.id}")
        if os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)
        os.makedirs(self._tmp, exist_ok=True)

    async def on_unload(self):
        if self._tmp and os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)

    def _s(self, key, **kwargs):
        if self._premium:
            prem_key = f"{key}_prem"
            try:
                val = self.strings(prem_key)
                if val and "Unknown string" not in val:
                    return val.format(**kwargs) if kwargs else val
            except Exception:
                pass
        base = self.strings(key)
        return base.format(**kwargs) if kwargs else base

    async def _is_forum_chat(self, message):
        try:
            chat = await message.get_chat()
            return getattr(chat, "forum", False)
        except Exception:
            return False

    def _get_topic_id(self, message):
        reply_to = getattr(message, "reply_to", None)
        if reply_to:
            top_id = getattr(reply_to, "reply_to_top_id", None)
            if top_id:
                return top_id
            msg_id = getattr(reply_to, "reply_to_msg_id", None)
            if msg_id and getattr(reply_to, "forum_topic", False):
                return msg_id
        return None

    async def _convert_to_voice(self, input_path: str, output_path: str) -> bool:
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffmpeg",
                "-hide_banner",
                "-loglevel", "error",
                "-y",
                "-i", input_path,
                "-vn",
                "-ac", "1",
                "-c:a", "libopus",
                "-b:a", "64k",
                output_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)

            if proc.returncode != 0:
                _log("CONVERT", f"ffmpeg failed: {stderr.decode()}")
                return False

            if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
                _log("CONVERT", "Output file is empty or does not exist")
                return False

            return True
        except asyncio.TimeoutError:
            _log("CONVERT", "Conversion timeout")
            return False
        except FileNotFoundError:
            _log("CONVERT", "ffmpeg not found")
            return False
        except Exception as e:
            _log("CONVERT", f"Exception: {e}")
            return False

    async def _get_audio_duration(self, filepath: str) -> int:
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                filepath,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
            return int(float(stdout.decode().strip()))
        except Exception:
            return 0

    async def _safe_edit(self, msg, text: str):
        try:
            await utils.answer(msg, text)
        except Exception:
            pass

    @loader.command(
        ru_doc="Конвертировать видео/кружок в голосовое (реплай)",
        en_doc="Convert video/video note to voice (reply)",
    )
    async def vtv(self, message: Message):
        """Convert video/video note to voice message"""
        reply = await message.get_reply_message()

        if not reply or not (reply.video or reply.video_note or reply.voice or reply.audio):
            await utils.answer(message, self._s("no_reply"))
            return

        is_forum = await self._is_forum_chat(message)
        topic_id = self._get_topic_id(message) if is_forum else None

        status = await utils.answer(message, self._s("processing"))

        work_dir = tempfile.mkdtemp(dir=self._tmp)
        input_file = os.path.join(work_dir, "input")
        output_file = os.path.join(work_dir, "voice.ogg")

        try:
            try:
                await reply.download_media(input_file)
            except Exception as e:
                _log("DOWNLOAD", f"Failed: {e}")
                await self._safe_edit(status, self._s("download_error"))
                return

            if not os.path.exists(input_file) or os.path.getsize(input_file) == 0:
                await self._safe_edit(status, self._s("download_error"))
                return

            await self._safe_edit(status, self._s("converting"))

            if not await self._convert_to_voice(input_file, output_file):
                await self._safe_edit(status, self._s("convert_error"))
                return

            duration = await self._get_audio_duration(output_file)

            await self._safe_edit(status, self._s("uploading"))

            with open(output_file, "rb") as f:
                voice_buf = io.BytesIO(f.read())
            voice_buf.name = "voice.ogg"

            try:
                await status.delete()
            except Exception:
                pass

            await self._client.send_file(
                message.chat_id,
                voice_buf,
                voice_note=True,
                attributes=[
                    DocumentAttributeAudio(
                        duration=duration,
                        voice=True,
                    )
                ],
                reply_to=topic_id if (is_forum and topic_id) else None,
            )

            _log("VTV", f"Voice sent: {duration}s")

        except Exception as e:
            _log("VTV", f"Error: {e}")
            try:
                await self._client.send_message(
                    message.chat_id,
                    self._s("error", msg=str(e)[:200]),
                    reply_to=topic_id if (is_forum and topic_id) else None,
                )
            except Exception:
                pass
        finally:
            if os.path.exists(work_dir):
                shutil.rmtree(work_dir, ignore_errors=True)