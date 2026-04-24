__version__ = (2, 0, 0)
# meta developer: I_execute.t.me

import os
import signal
import shutil
import asyncio
import logging
import tempfile
import glob

from .. import loader, utils

logger = logging.getLogger(__name__)

XRAY_PATTERNS = [
    "xray", "x-ray", "xray-core", "xray-linux",
    "xray-freebsd", "xray_", "xray.",
]

XRAY_DB_MODULES = ["XR", "XRay", "XRayProxy", "xray", "xray_proxy"]

XRAY_DB_KEYS = [
    "port", "sni", "dest", "trusted_users",
    "external_ip", "vless_uuid", "private_key",
    "public_key", "short_id", "proxy_autostart",
    "bot_autorun", "tkn", "config", "inbounds",
    "outbounds", "routing", "log_level", "stats",
    "api_port", "dns", "flow", "network",
    "security", "server_name", "fingerprint",
    "reality_dest", "reality_sni", "reality_private_key",
    "reality_public_key", "reality_short_id",
    "subscription_url", "panel_path", "panel_port",
    "cert_path", "key_path", "xray_path", "xray_pid",
    "xray_config_path", "auto_update", "last_update",
]

SCAN_PORTS = [
    80, 443, 1080, 1081, 1082, 1083, 1084, 1085,
    1234, 1443, 2052, 2053, 2082, 2083, 2086, 2087,
    2095, 2096, 2443, 3128, 3129, 4321, 4443,
    5443, 6443, 7443, 7890, 7891, 7892, 7893,
    8080, 8081, 8118, 8443, 8444, 8880, 8888, 8889,
    9050, 9051, 9090, 9091, 9999,
    10000, 10001, 10002, 10080, 10443,
    10808, 10809, 10810, 10811, 10812,
    12345, 15000, 15001, 20000, 20001, 20080,
    23456, 25000, 25001, 30000, 30001,
    40000, 40001, 43567, 44567, 50000, 50001,
    54321, 55555, 60000, 60001, 65535,
    11451, 11452, 11453, 2333, 6666, 6667, 6668,
    7777, 8443, 8444, 8445, 9443, 9444,
    18080, 18443, 28080, 28443, 38080, 38443,
    48080, 48443, 58080, 58443,
    1194, 4500, 5353, 5555, 8008, 8009,
    8180, 8280, 8380, 8480, 8580, 8680, 8780,
    11111, 22222, 33333, 44444,
]

XRAY_DIR_NAMES = [
    ".xray_proxy", "xray", "xray-core", "XRay",
    ".xray", "xray_proxy", "xray_config",
    "xray-linux-64", "xray-linux-arm64",
    "xray_temp", ".xray_temp", "xray_bin",
]

XRAY_CONFIG_NAMES = [
    "config.json", "xray_config.json", "xray.json",
    "config_xray.json", "vless.json", "reality.json",
]

XRAY_BINARY_NAMES = [
    "xray", "xray-linux-amd64", "xray-linux-arm64",
    "xray-linux-64", "xray-linux-32",
    "xray-core", "xray_bin",
]

SYSTEMD_UNIT_NAMES = [
    "xray", "xray-core", "xray.service",
    "xray-core.service", "xray-proxy",
]


@loader.tds
class XRayKiller(loader.Module):
    """if everything went to shit"""

    strings = {
        "name": "XRayKiller",
        "scanning": (
            "<b>started looking for all xray bullshit...</b>\n"
            "<i>processes, files, configs, ports, services, crontab</i>"
        ),
        "report": (
            "<b>Damage report:</b>\n\n"
            "{details}\n\n"
            "<b>now get rid of me too:</b>\n"
            "<code>{prefix}ulm XRayKiller</code>"
        ),
        "nothing": (
            "<b>found jack shit. either clean or totally fucked</b>\n\n"
            "<code>{prefix}ulm XRayKiller</code>"
        ),
        "sec_procs": "<b>murdered processes:</b> {count}",
        "sec_no_procs": "<b>no xray processes, clean as a whistle</b>",
        "sec_dirs": "\n<b>nuked directories/files:</b> {count}",
        "sec_no_dirs": "\n<b>directories fucked off somewhere</b>",
        "sec_db": "\n<b>DB keys wiped:</b> {count}",
        "sec_no_db": "\n<b>database is virgin clean</b>",
        "sec_ports": "\n<b>ports still listening (zombie shit):</b>",
        "port_listening": "port {port}: <b>STILL FUCKING LISTENING</b>",
        "sec_services": "\n<b>systemd services wrecked:</b> {count}",
        "sec_no_services": "\n<b>no xray services found</b>",
        "sec_crontab": "\n<b>crontab entries ripped out:</b> {count}",
        "sec_no_crontab": "\n<b>crontab is clean</b>",
    }

    strings_ru = {
        "scanning": (
            "<b>начал искать всю ненужную ебаторию...</b>\n"
            "<i>процессы, файлы, конфиги, порты, сервисы, кронтаб</i>"
        ),
        "report": (
            "<b>Итоги дня:</b>\n\n"
            "{details}\n\n"
            "<b>а теперь избавься и от меня:</b>\n"
            "<code>{prefix}ulm XRayKiller</code>"
        ),
        "nothing": (
            "<b>нихуя не нашел брат. либо чисто либо пиздец</b>\n\n"
            "<code>{prefix}ulm XRayKiller</code>"
        ),
        "sec_procs": "<b>убито процессов нахуй:</b> {count}",
        "sec_no_procs": "<b>процессов нихуя нету</b>",
        "sec_dirs": "\n<b>выпилено директорий/файлов:</b> {count}",
        "sec_no_dirs": "\n<b>директории куда-то съебались</b>",
        "sec_db": "\n<b>ключей из БД вычищено:</b> {count}",
        "sec_no_db": "\n<b>бдшка девственно чиста</b>",
        "sec_ports": "\n<b>порты всё ещё слушают (зомби хуйня):</b>",
        "port_listening": "порт {port}: <b>СЛУШАЕТ НАХУЙ</b>",
        "sec_services": "\n<b>systemd сервисов уебано:</b> {count}",
        "sec_no_services": "\n<b>сервисов xray нихуя нету</b>",
        "sec_crontab": "\n<b>записей из кронтаба выдрано:</b> {count}",
        "sec_no_crontab": "\n<b>кронтаб чистый</b>",
    }

    @loader.command(
        ru_doc="Уебать все что связано с иксрей",
        en_doc="Destroy everything xray related",
    )
    async def xrkill(self, message):
        """Destroy everything xray related"""
        prefix = self.get_prefix()
        m = await utils.answer(message, self.strings("scanning"))

        details = []
        any_found = False

        killed = await self._kill_all_xray()
        if killed:
            any_found = True
            details.append(self.strings("sec_procs").format(count=len(killed)))
            for pid, info in killed:
                details.append(f"  PID {pid}: <code>{info}</code>")
        else:
            details.append(self.strings("sec_no_procs"))


        services = await self._handle_systemd_services()
        if services:
            any_found = True
            details.append(
                self.strings("sec_services").format(count=len(services))
            )
            for svc in services:
                details.append(f"  <code>{svc}</code>")
        else:
            details.append(self.strings("sec_no_services"))

        removed = await self._remove_all_paths()
        if removed:
            any_found = True
            details.append(self.strings("sec_dirs").format(count=len(removed)))
            for d in removed:
                details.append(f"  <code>{d}</code>")
        else:
            details.append(self.strings("sec_no_dirs"))


        db_cleaned = self._clean_db()
        if db_cleaned:
            any_found = True
            details.append(
                self.strings("sec_db").format(count=len(db_cleaned))
            )
            for k in db_cleaned:
                details.append(f"  <code>{k}</code>")
        else:
            details.append(self.strings("sec_no_db"))


        cron_cleaned = await self._clean_crontab()
        if cron_cleaned:
            any_found = True
            details.append(
                self.strings("sec_crontab").format(count=len(cron_cleaned))
            )
            for entry in cron_cleaned:
                details.append(f"  <code>{entry[:120]}</code>")
        else:
            details.append(self.strings("sec_no_crontab"))

        port_info = await self._check_ports()
        if port_info:
            details.append(self.strings("sec_ports"))
            for line in port_info:
                details.append(f"  {line}")

        if not any_found and not port_info:
            text = self.strings("nothing").format(prefix=prefix)
        else:
            text = self.strings("report").format(
                details="\n".join(details),
                prefix=prefix,
            )

        await self._safe_edit(m, text)

    def _is_xray_related(self, text: str) -> bool:
        lower = text.lower()
        return any(p in lower for p in XRAY_PATTERNS)

    async def _collect_xray_pids(self) -> set:
        pids = set()
        my_pid = os.getpid()

       
        for pattern in ["xray", "x-ray", "xray-core"]:
            try:
                p = await asyncio.create_subprocess_exec(
                    "pgrep", "-f", pattern,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                out, _ = await asyncio.wait_for(p.communicate(), timeout=5)
                if p.returncode == 0 and out:
                    for line in out.decode().strip().split("\n"):
                        line = line.strip()
                        if line.isdigit():
                            pid = int(line)
                            if pid != my_pid:
                                pids.add(pid)
            except (FileNotFoundError, asyncio.TimeoutError):
                pass


        try:
            p = await asyncio.create_subprocess_exec(
                "ps", "aux",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await asyncio.wait_for(p.communicate(), timeout=10)
            if p.returncode == 0 and out:
                for line in out.decode().split("\n"):
                    if self._is_xray_related(line):
                        parts = line.split()
                        if len(parts) >= 2 and parts[1].isdigit():
                            pid = int(parts[1])
                            if pid != my_pid:
                                pids.add(pid)
        except (FileNotFoundError, asyncio.TimeoutError):
            pass


        try:
            for entry in os.listdir("/proc"):
                if not entry.isdigit():
                    continue
                pid = int(entry)
                if pid == my_pid:
                    continue

                is_xray = False

                try:
                    cmdline_path = f"/proc/{pid}/cmdline"
                    if os.path.exists(cmdline_path):
                        with open(cmdline_path, "r") as f:
                            cmdline = f.read().replace("\x00", " ")
                        if self._is_xray_related(cmdline):
                            is_xray = True
                except Exception:
                    pass

                if not is_xray:
                    try:
                        exe_path = os.readlink(f"/proc/{pid}/exe")
                        if self._is_xray_related(exe_path):
                            is_xray = True
                    except Exception:
                        pass

                if not is_xray:
                    try:
                        comm_path = f"/proc/{pid}/comm"
                        if os.path.exists(comm_path):
                            with open(comm_path, "r") as f:
                                comm = f.read().strip()
                            if self._is_xray_related(comm):
                                is_xray = True
                    except Exception:
                        pass

                if not is_xray:
                    try:
                        environ_path = f"/proc/{pid}/environ"
                        if os.path.exists(environ_path):
                            with open(environ_path, "r", errors="ignore") as f:
                                environ = f.read().replace("\x00", " ")
                            if self._is_xray_related(environ):
                                is_xray = True
                    except Exception:
                        pass

                if is_xray:
                    pids.add(pid)
        except Exception:
            pass

        return pids

    def _get_pid_info(self, pid: int) -> str:
        info = "unknown"
        try:
            cmdline_path = f"/proc/{pid}/cmdline"
            if os.path.exists(cmdline_path):
                with open(cmdline_path, "r") as f:
                    info = f.read().replace("\x00", " ").strip()
        except Exception:
            pass

        if info == "unknown":
            try:
                info = os.readlink(f"/proc/{pid}/exe")
            except Exception:
                pass

        return info[:200]

    async def _kill_all_xray(self) -> list:
        killed = []
        pids = await self._collect_xray_pids()

        for pid in pids:
            info = self._get_pid_info(pid)
            try:
                os.kill(pid, signal.SIGTERM)
                await asyncio.sleep(0.5)
                try:
                    os.kill(pid, 0)
                    os.kill(pid, signal.SIGKILL)
                    await asyncio.sleep(0.3)
                except ProcessLookupError:
                    pass
                killed.append((pid, info))
            except ProcessLookupError:
                killed.append((pid, f"{info} (already dead)"))
            except PermissionError:
                killed.append((pid, f"{info} (permission denied)"))
            except Exception as e:
                killed.append((pid, f"{info} (error: {e})"))

        my_pgid = os.getpgid(os.getpid())
        for pid in pids:
            try:
                pgid = os.getpgid(pid)
                if pgid != my_pgid and pgid > 1:
                    os.killpg(pgid, signal.SIGKILL)
            except Exception:
                pass

        return killed

    async def _handle_systemd_services(self) -> list:
        handled = []

        for unit in SYSTEMD_UNIT_NAMES:
            for action in ["stop", "disable"]:
                try:
                    p = await asyncio.create_subprocess_exec(
                        "systemctl", action, unit,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    await asyncio.wait_for(p.communicate(), timeout=10)
                    if p.returncode == 0 and action == "stop":
                        if unit not in handled:
                            handled.append(unit)
                except (FileNotFoundError, asyncio.TimeoutError):
                    pass

        for unit in SYSTEMD_UNIT_NAMES:
            for action in ["stop", "disable"]:
                try:
                    p = await asyncio.create_subprocess_exec(
                        "systemctl", "--user", action, unit,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    await asyncio.wait_for(p.communicate(), timeout=10)
                    if p.returncode == 0 and action == "stop":
                        label = f"{unit} (user)"
                        if label not in handled:
                            handled.append(label)
                except (FileNotFoundError, asyncio.TimeoutError):
                    pass

        systemd_dirs = [
            "/etc/systemd/system",
            "/usr/lib/systemd/system",
            os.path.expanduser("~/.config/systemd/user"),
        ]
        for sd in systemd_dirs:
            if not os.path.isdir(sd):
                continue
            try:
                for f in os.listdir(sd):
                    if self._is_xray_related(f):
                        full = os.path.join(sd, f)
                        try:
                            os.remove(full)
                            handled.append(f"removed: {full}")
                        except Exception:
                            pass
            except Exception:
                pass

        try:
            p = await asyncio.create_subprocess_exec(
                "systemctl", "daemon-reload",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(p.communicate(), timeout=10)
        except Exception:
            pass

        return handled

    async def _remove_all_paths(self) -> list:
        removed = []
        home = os.path.expanduser("~")
        tmp = tempfile.gettempdir()

        search_roots = list(set([home, tmp, "/tmp", "/var/tmp"]))


        for root in search_roots:
            for name in XRAY_DIR_NAMES:
                full = os.path.join(root, name)
                if os.path.exists(full) and full not in removed:
                    try:
                        if os.path.isdir(full):
                            shutil.rmtree(full, ignore_errors=True)
                        else:
                            os.remove(full)
                        removed.append(full)
                    except Exception:
                        pass

       
        for root in search_roots:
            try:
                for name in os.listdir(root):
                    if self._is_xray_related(name):
                        full = os.path.join(root, name)
                        if full not in removed:
                            try:
                                if os.path.isdir(full):
                                    shutil.rmtree(full, ignore_errors=True)
                                else:
                                    os.remove(full)
                                removed.append(full)
                            except Exception:
                                pass
            except Exception:
                pass

        
        for search_dir in search_roots:
            try:
                for root, dirs, files in os.walk(search_dir):
                    depth = root.replace(search_dir, "").count(os.sep)
                    if depth > 3:
                        dirs.clear()
                        continue

                    for f in files:
                        full = os.path.join(root, f)

                        if f.lower() in [b.lower() for b in XRAY_BINARY_NAMES]:
                            if self._is_elf_binary(full):
                                parent = os.path.dirname(full)
                                if parent not in removed:
                                    try:
                                        shutil.rmtree(
                                            parent, ignore_errors=True
                                        )
                                        removed.append(parent)
                                    except Exception:
                                        pass

                        if f in XRAY_CONFIG_NAMES:
                            if self._config_mentions_xray(full):
                                try:
                                    os.remove(full)
                                    if full not in removed:
                                        removed.append(full)
                                except Exception:
                                    pass
            except Exception:
                pass


        standard_paths = [
            "/usr/local/bin/xray",
            "/usr/bin/xray",
            "/usr/local/share/xray",
            "/usr/local/etc/xray",
            "/etc/xray",
            "/opt/xray",
            "/var/log/xray",
        ]
        for sp in standard_paths:
            if os.path.exists(sp) and sp not in removed:
                try:
                    if os.path.isdir(sp):
                        shutil.rmtree(sp, ignore_errors=True)
                    else:
                        os.remove(sp)
                    removed.append(sp)
                except PermissionError:
                    removed.append(f"{sp} (permission denied)")
                except Exception:
                    pass

        
        glob_patterns = [
            os.path.join(home, "**/xray"),
            os.path.join(home, "**/xray-*"),
            os.path.join(tmp, "XRay_*"),
            os.path.join(tmp, "xray_*"),
            os.path.join(tmp, "xray-*"),
        ]
        for pattern in glob_patterns:
            try:
                for match in glob.glob(pattern, recursive=True):
                    if match not in removed:
                        try:
                            if os.path.isdir(match):
                                shutil.rmtree(match, ignore_errors=True)
                            else:
                                os.remove(match)
                            removed.append(match)
                        except Exception:
                            pass
            except Exception:
                pass

        return removed

    def _is_elf_binary(self, path: str) -> bool:
        try:
            with open(path, "rb") as f:
                header = f.read(4)
            return header == b"\x7fELF"
        except Exception:
            return False

    def _config_mentions_xray(self, path: str) -> bool:
        try:
            with open(path, "r", errors="ignore") as f:
                content = f.read(8192)
            lower = content.lower()
            indicators = [
                "xray", "vless", "vmess", "reality",
                "trojan", "shadowsocks", "inbounds", "outbounds",
            ]
            matches = sum(1 for ind in indicators if ind in lower)
            return matches >= 2
        except Exception:
            return False

    def _clean_db(self) -> list:
        cleaned = []
        for module in XRAY_DB_MODULES:
            for key in XRAY_DB_KEYS:
                try:
                    val = self._db.get(module, key)
                    if val is not None:
                        self._db.set(module, key, None)
                        cleaned.append(f"{module}.{key}")
                except Exception:
                    pass

        try:
            db_data = self._db
            if hasattr(db_data, "keys"):
                for mod_key in list(db_data.keys()):
                    if self._is_xray_related(str(mod_key)):
                        try:
                            mod_data = db_data[mod_key]
                            if isinstance(mod_data, dict):
                                for k in list(mod_data.keys()):
                                    self._db.set(mod_key, k, None)
                                    cleaned.append(f"{mod_key}.{k}")
                        except Exception:
                            pass
        except Exception:
            pass

        return cleaned

    async def _clean_crontab(self) -> list:
        removed_entries = []
        try:
            p = await asyncio.create_subprocess_exec(
                "crontab", "-l",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await asyncio.wait_for(p.communicate(), timeout=5)
            if p.returncode != 0 or not out:
                return removed_entries

            lines = out.decode().split("\n")
            new_lines = []
            for line in lines:
                if self._is_xray_related(line):
                    removed_entries.append(line.strip())
                else:
                    new_lines.append(line)

            if removed_entries:
                new_crontab = "\n".join(new_lines)
                if not new_crontab.endswith("\n"):
                    new_crontab += "\n"

                p = await asyncio.create_subprocess_exec(
                    "crontab", "-",
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                await asyncio.wait_for(
                    p.communicate(input=new_crontab.encode()),
                    timeout=5,
                )
        except (FileNotFoundError, asyncio.TimeoutError):
            pass
        except Exception:
            pass

        return removed_entries

    async def _check_ports(self) -> list:
        results = []

        unique_ports = sorted(set(SCAN_PORTS))

        sem = asyncio.Semaphore(30)

        async def check_port(port: int):
            async with sem:
                try:
                    _, writer = await asyncio.wait_for(
                        asyncio.open_connection("127.0.0.1", port),
                        timeout=0.4,
                    )
                    writer.close()
                    await writer.wait_closed()
                    return self.strings("port_listening").format(port=port)
                except Exception:
                    return None

        tasks = [check_port(port) for port in unique_ports]
        checks = await asyncio.gather(*tasks, return_exceptions=True)

        for result in checks:
            if isinstance(result, str):
                results.append(result)

  
        for cmd in [["ss", "-tlnp"], ["netstat", "-tlnp"]]:
            try:
                p = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                out, _ = await asyncio.wait_for(p.communicate(), timeout=5)
                if p.returncode == 0 and out:
                    for line in out.decode().split("\n"):
                        if self._is_xray_related(line):
                            clean_line = " ".join(line.split())
                            entry = f"<code>{clean_line[:150]}</code>"
                            if entry not in results:
                                results.append(entry)
                break
            except (FileNotFoundError, asyncio.TimeoutError):
                continue

        return results

    async def _safe_edit(self, msg, text: str):
        try:
            if isinstance(msg, list):
                msg = msg[0]
            await msg.edit(text)
        except Exception:
            try:
                await self._client.send_message(msg.chat_id, text)
            except Exception:
                pass