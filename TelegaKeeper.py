# CopyLeft 2026 github.com/i-execute
# Author: I_execute.t.me
# Licensed under AGPLv3.

__version__ = (2, 0, 0)
# meta developer: Execute_forge.t.me

import atexit
import json
import logging
import os
import sys
import tempfile

from .. import loader

logger = logging.getLogger(__name__)

_TG_API_ID = 21882615
_TG_API_HASH = "a55678cc05c1aad2fb0aaccbf9663241"

_DB_ORIG_ID = "orig_api_id"
_DB_ORIG_HASH = "orig_api_hash"
_DB_CFG_PATH = "cfg_path"


def _candidate_dirs():
    dirs = []

    def add(d):
        if d and d not in dirs:
            dirs.append(d)

    try:
        cwd = os.getcwd()
        add(cwd)
        add(os.path.dirname(cwd))
    except Exception:
        pass

    for name in ("heroku", "__main__"):
        try:
            mod = sys.modules.get(name)
            f = getattr(mod, "__file__", None)
            if f:
                d = os.path.dirname(os.path.abspath(f))
                add(d)
                add(os.path.dirname(d))
        except Exception:
            pass

    try:
        if sys.argv and sys.argv[0]:
            d = os.path.dirname(os.path.abspath(sys.argv[0]))
            add(d)
            add(os.path.dirname(d))
    except Exception:
        pass

    try:
        d = os.path.dirname(os.path.abspath(__file__))
        for _ in range(4):
            add(d)
            d = os.path.dirname(d)
    except Exception:
        pass

    return dirs


def _find_config():
    for d in _candidate_dirs():
        path = os.path.join(d, "config.json")
        if not os.path.isfile(path):
            continue
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and "api_id" in data and "api_hash" in data:
                return path, data
        except Exception:
            continue
    return None, None


def _atomic_write(path, data):
    d = os.path.dirname(path)
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".config_", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        try:
            os.chmod(tmp, os.stat(path).st_mode)
        except Exception:
            pass
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass


@loader.tds
class TelegaKeeper(loader.Module):
    """Make ur client unofficial"""

    strings = {"name": "TelegaKeeper"}

    async def client_ready(self, client, db):
        self._client = client
        self._db = db
        self._swapped = False
        try:
            self._swap()
        except Exception as e:
            logger.error(f"[TelegaKeeper] swap error: {e}")
        atexit.register(self._restore)

    def _swap(self):
        path, data = _find_config()
        if not path:
            logger.error("[TelegaKeeper] config.json not found")
            return

        cur_id, cur_hash = data.get("api_id"), data.get("api_hash")
        already = str(cur_id) == str(_TG_API_ID) and str(cur_hash) == _TG_API_HASH
        has_orig = self.get(_DB_ORIG_ID) is not None and self.get(_DB_ORIG_HASH)

        if not already:
            self.set(_DB_ORIG_ID, cur_id)
            self.set(_DB_ORIG_HASH, cur_hash)
            self.set(_DB_CFG_PATH, path)
            try:
                _atomic_write(
                    path + ".tk.bak",
                    {"api_id": cur_id, "api_hash": cur_hash},
                )
            except Exception as e:
                logger.warning(f"[TelegaKeeper] bak write error: {e}")

            data["api_id"] = _TG_API_ID
            data["api_hash"] = _TG_API_HASH
            _atomic_write(path, data)
            logger.info(f"[TelegaKeeper] Keys swapped in {path}")
        elif has_orig:
            logger.info("[TelegaKeeper] Telega keys already set, originals kept")
        else:
            logger.warning("[TelegaKeeper] Telega keys set but originals unknown")

        self._swapped = True

    def _restore(self):
        try:
            orig_id = self.get(_DB_ORIG_ID)
            orig_hash = self.get(_DB_ORIG_HASH)
            path = self.get(_DB_CFG_PATH)

            if (orig_id is None or not orig_hash) and path:
                try:
                    with open(path + ".tk.bak", encoding="utf-8") as f:
                        bak = json.load(f)
                    orig_id, orig_hash = bak.get("api_id"), bak.get("api_hash")
                except Exception:
                    pass

            if orig_id is None or not orig_hash:
                return

            if not path or not os.path.isfile(path):
                path, _ = _find_config()
            if not path:
                return

            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            data["api_id"] = orig_id
            data["api_hash"] = orig_hash
            _atomic_write(path, data)

            self.set(_DB_ORIG_ID, None)
            self.set(_DB_ORIG_HASH, None)
            self.set(_DB_CFG_PATH, None)
            try:
                os.remove(path + ".tk.bak")
            except Exception:
                pass
            self._swapped = False
            logger.info("[TelegaKeeper] Original keys restored")
        except Exception as e:
            logger.error(f"[TelegaKeeper] restore error: {e}")

    async def on_unload(self):
        self._restore()
        try:
            atexit.unregister(self._restore)
        except Exception:
            pass