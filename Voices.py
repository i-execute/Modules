__version__ = (1, 0, 0)
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


def _log(tag: str, msg: str):
    logger.info(f"[Voices][{tag}] {msg}")


@loader.tds
class Voices(loader.Module):
    """Convert video/video note to voice message"""

    strings = {
        "name": "Voices",
        "no_reply": "<b>Reply to a video or video note</b>",
        "processing": "<b>Processing...</b>",
        "converting": "<b>Converting to voice...</b>",
        "uploading": "<b>Uploading voice message...</b>",
        "error": "<b>Error:</b> {msg}",
        "no_ffmpeg": "<b>ffmpeg not found</b>",
        "download_error": "<b>Failed to download media</b>",
        "convert_error": "<b>Conversion failed</b>",
        "upload_error": "<b>Upload failed</b>",
    }

    strings_ru = {
        "no_reply": "<b>Ответь на видео или кружок</b>",
        "processing": "<b>Обработка...</b>",
        "converting": "<b>Конвертирую в голосовое...</b>",
        "uploading": "<b>Загружаю голосовое сообщение...</b>",
        "error": "<b>Ошибка:</b> {msg}",
        "no_ffmpeg": "<b>ffmpeg не найден</b>",
        "download_error": "<b>Не удалось скачать медиа</b>",
        "convert_error": "<b>Ошибка конвертации</b>",
        "upload_error": "<b>Ошибка загрузки</b>",
    }

    def __init__(self):
        self._tmp = None
        self._me_id = None

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        me = await client.get_me()
        self._me_id = me.id
        self._tmp = os.path.join(tempfile.gettempdir(), f"Voices_{me.id}")
        if os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)
        os.makedirs(self._tmp, exist_ok=True)

    async def on_unload(self):
        if self._tmp and os.path.exists(self._tmp):
            shutil.rmtree(self._tmp, ignore_errors=True)

    async def _is_forum_chat(self, message):
        try:
            chat = await message.get_chat()
            return getattr(chat, "forum", False)
        except Exception:
            return False

    def _get_topic_id(self, message):
        try:
            if message.reply_to and hasattr(message.reply_to, "reply_to_top_id"):
                return message.reply_to.reply_to_top_id or message.reply_to.reply_to_msg_id
        except Exception:
            pass
        return None

    async def _convert_to_voice(self, input_path: str, output_path: str) -> bool:
        """Convert video/audio to OGG OPUS voice format"""
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
                _log("CONVERT", "Output file is empty or doesn't exist")
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
        """Get audio duration using ffprobe"""
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
            duration = float(stdout.decode().strip())
            return int(duration)
        except Exception:
            return 0

    @loader.command(
        ru_doc="Конвертировать видео/кружок в голосовое (реплай)",
        en_doc="Convert video/video note to voice (reply)",
    )
    async def vtv(self, message: Message):
        """Convert video/video note to voice message"""
        reply = await message.get_reply_message()
        
        if not reply:
            await utils.answer(message, self.strings["no_reply"])
            return
        
        if not (reply.video or reply.video_note or reply.voice or reply.audio):
            await utils.answer(message, self.strings["no_reply"])
            return

        is_forum = await self._is_forum_chat(message)
        topic_id = self._get_topic_id(message) if is_forum else None
        
        msg = await utils.answer(message, self.strings["processing"])
        
        work_dir = tempfile.mkdtemp(dir=self._tmp)
        input_file = os.path.join(work_dir, "input")
        output_file = os.path.join(work_dir, "voice.ogg")
        
        try:
            await utils.answer(msg, self.strings["processing"])
            
            try:
                await reply.download_media(input_file)
            except Exception as e:
                _log("DOWNLOAD", f"Failed: {e}")
                await utils.answer(msg, self.strings["download_error"])
                return
            
            if not os.path.exists(input_file) or os.path.getsize(input_file) == 0:
                await utils.answer(msg, self.strings["download_error"])
                return
            
            await utils.answer(msg, self.strings["converting"])
            
            convert_ok = await self._convert_to_voice(input_file, output_file)
            if not convert_ok:
                await utils.answer(msg, self.strings["convert_error"])
                return
            
            duration = await self._get_audio_duration(output_file)
            
            await utils.answer(msg, self.strings["uploading"])
            
            with open(output_file, "rb") as f:
                voice_bytes = f.read()
            
            voice_buf = io.BytesIO(voice_bytes)
            voice_buf.name = "voice.ogg"
            
            try:
                await msg.delete()
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
                reply_to=topic_id if is_forum and topic_id else None,
            )
            
            _log("VTV", f"Voice sent: {duration}s")
            
        except Exception as e:
            _log("VTV", f"Error: {e}")
            await utils.answer(msg, self.strings["error"].format(msg=str(e)[:200]))
        finally:
            if os.path.exists(work_dir):
                shutil.rmtree(work_dir, ignore_errors=True)