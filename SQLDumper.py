__version__ = (1, 0, 0)
# meta developer: I_execute.t.me

import asyncio
import csv
import io
import logging
import os
import sqlite3
import tempfile
import time

from .. import loader, utils

logger = logging.getLogger(__name__)


def _is_sqlite(data: bytes) -> bool:
    return data[:16] == b"SQLite format 3\x00"


def _is_sql_text(data: bytes) -> bool:
    try:
        text = data[:4096].decode("utf-8", errors="ignore").strip().upper()
    except Exception:
        return False
    keywords = [
        "CREATE TABLE", "INSERT INTO", "DROP TABLE",
        "BEGIN TRANSACTION", "PRAGMA", "SELECT ",
        "ALTER TABLE", "CREATE INDEX",
    ]
    return any(k in text for k in keywords)


def _detect_sql(data: bytes):
    if not data:
        return None, "файл пустой"
    if _is_sqlite(data):
        return "sqlite_binary", None
    if _is_sql_text(data):
        return "sql_text", None
    return None, "нет сигнатуры SQLite и SQL-ключевых слов"


def _tables_to_csvs(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    tables = [r[0] for r in cur.fetchall()]
    result = {}
    for t in tables:
        try:
            cur.execute(f"SELECT * FROM [{t}]")
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(cols)
            w.writerows(rows)
            result[t] = buf.getvalue().encode("utf-8")
        except Exception as e:
            logger.warning(f"[SQLDumper] table '{t}': {e}")
    return result


def _dump_sqlite(data: bytes):
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        f.write(data)
        tmp = f.name
    try:
        conn = sqlite3.connect(tmp)
        result = _tables_to_csvs(conn)
        conn.close()
        if not result:
            raise ValueError("таблиц не найдено")
        return result
    finally:
        try:
            os.unlink(tmp)
        except Exception:
            pass


def _dump_sql_text(data: bytes):
    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript(data.decode("utf-8", errors="replace"))
        result = _tables_to_csvs(conn)
        if not result:
            raise ValueError("таблиц не найдено после импорта")
        return result
    finally:
        conn.close()


@loader.tds
class SQLDumper(loader.Module):
    """SQL/SQLite dumper to CSV"""

    strings = {
        "name": "SQLDumper",
        "no_reply": (
            "<b>SQLDumper</b>\n"
            "<blockquote>Reply to a file with .sqld</blockquote>"
        ),
        "no_file": (
            "<b>SQLDumper</b>\n"
            "<blockquote>No file in that message</blockquote>"
        ),
        "downloading": (
            "<b>SQLDumper</b>\n"
            "<blockquote>Downloading\n"
            "{progress}</blockquote>"
        ),
        "analyzing": (
            "<b>SQLDumper</b>\n"
            "<blockquote>Analyzing <code>{name}</code>\n"
            "{size}</blockquote>"
        ),
        "not_sql": (
            "<b>SQLDumper</b>\n"
            "<blockquote>Not SQL\n"
            "<code>{name}</code>\n"
            "{reason}</blockquote>"
        ),
        "dumping": (
            "<b>SQLDumper</b>\n"
            "<blockquote>Dumping tables\n"
            "Type: {sql_type}\n"
            "Tables: {tables}</blockquote>"
        ),
        "uploading": (
            "<b>SQLDumper</b>\n"
            "<blockquote>Uploading\n"
            "{progress}</blockquote>"
        ),
        "done": (
            "<b>SQLDumper</b>\n"
            "<blockquote>Done\n"
            "File: <code>{name}</code>\n"
            "Tables: {tables}\n"
            "Rows: {rows}</blockquote>"
        ),
        "error": (
            "<b>SQLDumper</b>\n"
            "<blockquote>Error\n"
            "{error}</blockquote>"
        ),
        "empty": (
            "<b>SQLDumper</b>\n"
            "<blockquote>SQL detected, no tables found</blockquote>"
        ),
    }

    strings_ru = {
        "name": "SQLDumper",
        "no_reply": (
            "<b>SQLDumper</b>\n"
            "<blockquote>Ответьте на файл командой .sqld</blockquote>"
        ),
        "no_file": (
            "<b>SQLDumper</b>\n"
            "<blockquote>В сообщении нет файла</blockquote>"
        ),
        "downloading": (
            "<b>SQLDumper</b>\n"
            "<blockquote>Скачиваю\n"
            "{progress}</blockquote>"
        ),
        "analyzing": (
            "<b>SQLDumper</b>\n"
            "<blockquote>Анализирую <code>{name}</code>\n"
            "{size}</blockquote>"
        ),
        "not_sql": (
            "<b>SQLDumper</b>\n"
            "<blockquote>Не SQL\n"
            "<code>{name}</code>\n"
            "{reason}</blockquote>"
        ),
        "dumping": (
            "<b>SQLDumper</b>\n"
            "<blockquote>Дамплю таблицы\n"
            "Тип: {sql_type}\n"
            "Таблиц: {tables}</blockquote>"
        ),
        "uploading": (
            "<b>SQLDumper</b>\n"
            "<blockquote>Отправляю\n"
            "{progress}</blockquote>"
        ),
        "done": (
            "<b>SQLDumper</b>\n"
            "<blockquote>Готово\n"
            "Файл: <code>{name}</code>\n"
            "Таблиц: {tables}\n"
            "Строк: {rows}</blockquote>"
        ),
        "error": (
            "<b>SQLDumper</b>\n"
            "<blockquote>Ошибка\n"
            "{error}</blockquote>"
        ),
        "empty": (
            "<b>SQLDumper</b>\n"
            "<blockquote>SQL опознан, таблиц не найдено</blockquote>"
        ),
    }

    async def client_ready(self, client, db):
        self._client = client
        self._dl_state = {}
        self._ul_state = {}

    def _fmt_size(self, b: int) -> str:
        if b < 1024:
            return f"{b} B"
        if b < 1024 ** 2:
            return f"{b/1024:.1f} KB"
        return f"{b/1024**2:.1f} MB"

    def _fmt_speed(self, bps: float) -> str:
        if bps < 1024:
            return f"{bps:.0f} B/s"
        if bps < 1024 ** 2:
            return f"{bps/1024:.1f} KB/s"
        return f"{bps/1024**2:.1f} MB/s"

    def _dl_cb(self, task_id: str, t0: float):
        def _cb(current: int, total: int):
            s = self._dl_state.get(task_id)
            if not s:
                return
            cur = current / 1024 / 1024
            tot = (total / 1024 / 1024) if total else cur
            elapsed = time.time() - t0
            spd = current / elapsed if elapsed > 0 else 0
            pct = (cur / tot * 100) if tot > 0 else 0.0
            s["text"] = f"{pct:.1f}% ({cur:.1f}/{tot:.1f} MB) {self._fmt_speed(spd)}"
        return _cb

    def _ul_cb(self, task_id: str, t0: float):
        def _cb(current: int, total: int):
            s = self._ul_state.get(task_id)
            if not s:
                return
            cur = current / 1024 / 1024
            tot = (total / 1024 / 1024) if total else cur
            elapsed = time.time() - t0
            spd = current / elapsed if elapsed > 0 else 0
            pct = (cur / tot * 100) if tot > 0 else 0.0
            s["text"] = f"{pct:.1f}% ({cur:.1f}/{tot:.1f} MB) {self._fmt_speed(spd)}"
        return _cb

    async def _render_loop(self, get_text, edit_fn, done: asyncio.Event):
        while not done.is_set():
            try:
                await asyncio.sleep(2)
                if not done.is_set():
                    await edit_fn(get_text())
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    @loader.command(ru_doc="[реплай] — дамп SQL/SQLite в CSV")
    async def sqld(self, message):
        """[reply] — dump SQL/SQLite to CSV"""
        reply = await message.get_reply_message()
        if not reply:
            await utils.answer(message, self.strings["no_reply"])
            return

        doc = reply.document
        if not doc:
            await utils.answer(message, self.strings["no_file"])
            return

        file_name = "file.bin"
        from telethon.tl.types import DocumentAttributeFilename
        for attr in doc.attributes:
            if isinstance(attr, DocumentAttributeFilename):
                file_name = attr.file_name
                break

        status_msg = await self.inline.form(
            text=self.strings["downloading"].format(progress=""),
            message=message,
            reply_markup=[[{"text": "...", "callback": lambda c: None}]],
            silent=True,
        )

        async def edit(text: str):
            try:
                await status_msg.edit(text)
            except Exception:
                pass

        tid_dl = f"dl_{id(message)}"
        self._dl_state[tid_dl] = {"text": ""}
        dl_done = asyncio.Event()
        dl_task = asyncio.create_task(
            self._render_loop(
                lambda: self.strings["downloading"].format(progress=self._dl_state[tid_dl]["text"]),
                edit,
                dl_done,
            )
        )

        buf = io.BytesIO()
        try:
            await self._client.download_file(doc, buf, progress_callback=self._dl_cb(tid_dl, time.time()))
        except Exception as e:
            dl_done.set(); dl_task.cancel(); self._dl_state.pop(tid_dl, None)
            await edit(self.strings["error"].format(error=str(e)))
            return
        finally:
            dl_done.set(); dl_task.cancel(); self._dl_state.pop(tid_dl, None)

        data = buf.getvalue()

        await edit(self.strings["analyzing"].format(name=file_name, size=self._fmt_size(len(data))))
        await asyncio.sleep(0.3)

        sql_type, reason = _detect_sql(data)
        if sql_type is None:
            await edit(self.strings["not_sql"].format(name=file_name, reason=reason))
            return

        type_label = "SQLite binary" if sql_type == "sqlite_binary" else "SQL text"

        try:
            tables_csv = _dump_sqlite(data) if sql_type == "sqlite_binary" else _dump_sql_text(data)
        except ValueError as e:
            await edit(self.strings["error"].format(error=str(e)))
            return
        except Exception as e:
            await edit(self.strings["error"].format(error=str(e)))
            return

        if not tables_csv:
            await edit(self.strings["empty"])
            return

        await edit(self.strings["dumping"].format(sql_type=type_label, tables=len(tables_csv)))

        tmp_files = []
        total_rows = 0
        base = os.path.splitext(file_name)[0]

        try:
            for tname, csv_bytes in tables_csv.items():
                total_rows += max(csv_bytes.count(b"\n") - 1, 0)
                f = tempfile.NamedTemporaryFile(
                    suffix=".csv",
                    prefix=f"{base}_{tname}_",
                    delete=False,
                )
                f.write(csv_bytes)
                f.close()
                tmp_files.append(f.name)

            tid_ul = f"ul_{id(message)}"
            self._ul_state[tid_ul] = {"text": ""}
            ul_done = asyncio.Event()
            ul_task = asyncio.create_task(
                self._render_loop(
                    lambda: self.strings["uploading"].format(progress=self._ul_state[tid_ul]["text"]),
                    edit,
                    ul_done,
                )
            )

            try:
                for chunk in [tmp_files[i:i+10] for i in range(0, len(tmp_files), 10)]:
                    cb = self._ul_cb(tid_ul, time.time())
                    await self._client.send_file(
                        message.chat_id,
                        chunk if len(chunk) > 1 else chunk[0],
                        force_document=True,
                        progress_callback=cb,
                    )
                    await asyncio.sleep(0.5)
            except Exception as e:
                await edit(self.strings["error"].format(error=str(e)))
                return
            finally:
                ul_done.set(); ul_task.cancel(); self._ul_state.pop(tid_ul, None)

        finally:
            for p in tmp_files:
                try:
                    os.unlink(p)
                except Exception:
                    pass

        await edit(self.strings["done"].format(name=file_name, tables=len(tables_csv), rows=total_rows))
