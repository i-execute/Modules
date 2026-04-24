__version__ = (1, 4, 0)
# meta developer: I_execute.t.me

import os
import re
import time
import asyncio
import platform
import logging
import socket

from .. import loader, utils

logger = logging.getLogger(__name__)


def _escape(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


@loader.tds
class Test(loader.Module):
    """Network monitoring & speed tests"""

    strings = {
        "name": "Test",
        "help": (
            "<b>Test - Network Monitor</b>\n\n"
            "<b>Commands:</b>\n"
            "<code>{prefix}test speed</code> - internet speed test (Cloudflare)\n"
            "<code>{prefix}test net</code> - network usage statistics\n"
            "<code>{prefix}test ping</code> - ping to popular services\n"
            "<code>{prefix}test dns</code> - DNS resolve speed test\n"
            "<code>{prefix}test ip</code> - external IP, geo, ASN\n"
        ),

        "speed_progress": "<b>Running speed test...</b>",
        "speed_result": (
            "<b>Speed Test Results</b>\n"
            "<b>Download:</b>\n"
            "<blockquote>{download}</blockquote>\n"
            "<b>Upload:</b>\n"
            "<blockquote>{upload}</blockquote>\n"
            "<b>Latency:</b>\n"
            "<blockquote>{latency}</blockquote>"
        ),
        "speed_fail": "<b>Speed test failed</b>\n<code>{error}</code>",

        "net_stats": (
            "<b>Network Statistics</b>\n\n"
            "<b>Interfaces:</b>\n"
            "<blockquote>{interfaces}</blockquote>\n"
            "<b>Totals (excl. loopback):</b>\n"
            "<blockquote>"
            "RX: <code>{total_rx}</code>\n"
            "TX: <code>{total_tx}</code>\n"
            "Total: <code>{total}</code>"
            "</blockquote>\n"
            "<b>System uptime:</b> <code>{uptime}</code>"
        ),
        "net_fail": "<b>Cannot read network stats</b>\n<code>{error}</code>",

        "ping_progress": "<b>Pinging services...</b>",
        "ping_result": (
            "<b>Ping Results</b>\n"
            "<b>Services:</b>\n"
            "<blockquote>{services}</blockquote>\n"
            "<b>Telegram DC:</b>\n"
            "<blockquote>{telegram}</blockquote>"
        ),
        "ping_fail": "<b>Ping failed</b>\n<code>{error}</code>",

        "dns_progress": "<b>Testing DNS resolve speed...</b>",
        "dns_result": (
            "<b>DNS Resolve Test</b>\n"
            "<blockquote>{results}</blockquote>"
        ),
        "dns_fail": "<b>DNS test failed</b>\n<code>{error}</code>",

        "ip_progress": "<b>Getting IP info...</b>",
        "ip_result": (
            "<b>IP Information</b>\n"
            "<blockquote>"
            "IP: <code>{ip}</code>\n"
            "Country: <code>{country}</code>\n"
            "Region: <code>{region}</code>\n"
            "City: <code>{city}</code>\n"
            "ISP: <code>{isp}</code>\n"
            "AS: <code>{as_info}</code>\n"
            "Org: <code>{org}</code>\n"
            "Timezone: <code>{timezone}</code>"
            "</blockquote>"
        ),
        "ip_fail": "<b>Failed to get IP info</b>\n<code>{error}</code>",
    }

    strings_ru = {
        "help": (
            "<b>Test - Сетевой монитор</b>\n\n"
            "<b>Команды:</b>\n"
            "<code>{prefix}test speed</code> - тест скорости интернета (Cloudflare)\n"
            "<code>{prefix}test net</code> - статистика использования сети\n"
            "<code>{prefix}test ping</code> - пинг до популярных сервисов\n"
            "<code>{prefix}test dns</code> - тест скорости DNS резолва\n"
            "<code>{prefix}test ip</code> - внешний IP, гео, ASN\n"
        ),

        "speed_progress": "<b>Запуск теста скорости...</b>",
        "speed_result": (
            "<b>Результаты теста скорости</b>\n"
            "<b>Скачивание:</b>\n"
            "<blockquote>{download}</blockquote>\n"
            "<b>Загрузка:</b>\n"
            "<blockquote>{upload}</blockquote>\n"
            "<b>Задержка:</b>\n"
            "<blockquote>{latency}</blockquote>"
        ),
        "speed_fail": "<b>Тест скорости провалился</b>\n<code>{error}</code>",

        "net_stats": (
            "<b>Сетевая статистика</b>\n\n"
            "<b>Интерфейсы:</b>\n"
            "<blockquote>{interfaces}</blockquote>\n"
            "<b>Итого (без loopback):</b>\n"
            "<blockquote>"
            "RX: <code>{total_rx}</code>\n"
            "TX: <code>{total_tx}</code>\n"
            "Всего: <code>{total}</code>"
            "</blockquote>\n"
            "<b>Аптайм системы:</b> <code>{uptime}</code>"
        ),
        "net_fail": "<b>Не удалось прочитать сетевую статистику</b>\n<code>{error}</code>",

        "ping_progress": "<b>Пингуем сервисы...</b>",
        "ping_result": (
            "<b>Результаты пинга</b>\n"
            "<b>Сервисы:</b>\n"
            "<blockquote>{services}</blockquote>\n"
            "<b>Telegram DC:</b>\n"
            "<blockquote>{telegram}</blockquote>"
        ),
        "ping_fail": "<b>Пинг провалился</b>\n<code>{error}</code>",

        "dns_progress": "<b>Тестируем скорость DNS резолва...</b>",
        "dns_result": (
            "<b>Тест DNS резолва</b>\n"
            "<blockquote>{results}</blockquote>"
        ),
        "dns_fail": "<b>Тест DNS провалился</b>\n<code>{error}</code>",

        "ip_progress": "<b>Получаем информацию об IP...</b>",
        "ip_result": (
            "<b>Информация об IP</b>\n"
            "<blockquote>"
            "IP: <code>{ip}</code>\n"
            "Страна: <code>{country}</code>\n"
            "Регион: <code>{region}</code>\n"
            "Город: <code>{city}</code>\n"
            "Провайдер: <code>{isp}</code>\n"
            "AS: <code>{as_info}</code>\n"
            "Орг: <code>{org}</code>\n"
            "Часовой пояс: <code>{timezone}</code>"
            "</blockquote>"
        ),
        "ip_fail": "<b>Не удалось получить информацию об IP</b>\n<code>{error}</code>",
    }

    def _format_bytes(self, b):
        if b < 1024:
            return f"{b} B"
        if b < 1024 * 1024:
            return f"{b / 1024:.1f} KB"
        if b < 1024 * 1024 * 1024:
            return f"{b / (1024 * 1024):.1f} MB"
        return f"{b / (1024 * 1024 * 1024):.2f} GB"

    def _format_speed(self, bps):
        if bps < 1024:
            return f"{bps:.0f} B/s"
        if bps < 1024 * 1024:
            return f"{bps / 1024:.1f} KB/s"
        if bps < 1024 * 1024 * 1024:
            return f"{bps / (1024 * 1024):.1f} MB/s"
        return f"{bps / (1024 * 1024 * 1024):.2f} GB/s"

    def _get_system_uptime(self):
        try:
            with open("/proc/uptime", "r") as f:
                seconds = float(f.read().split()[0])
            d, rem = divmod(int(seconds), 86400)
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
            return " ".join(parts)
        except Exception:
            return "n/a"

    def _get_net_interfaces(self):
        interfaces = []
        total_rx = 0
        total_tx = 0
        try:
            with open("/proc/net/dev", "r") as f:
                lines = f.readlines()
            for line in lines[2:]:
                line = line.strip()
                if not line:
                    continue
                parts = line.split()
                iface = parts[0].rstrip(":")
                rx_bytes = int(parts[1])
                rx_packets = int(parts[2])
                tx_bytes = int(parts[9])
                tx_packets = int(parts[10])
                interfaces.append({
                    "name": iface,
                    "rx_bytes": rx_bytes,
                    "rx_packets": rx_packets,
                    "tx_bytes": tx_bytes,
                    "tx_packets": tx_packets,
                })
                if iface != "lo":
                    total_rx += rx_bytes
                    total_tx += tx_bytes
        except Exception as e:
            return None, 0, 0, str(e)
        return interfaces, total_rx, total_tx, None

    async def _safe_edit(self, msg, text):
        try:
            if isinstance(msg, list):
                msg = msg[0]
            await msg.edit(text)
        except Exception:
            pass

    async def _run_download_test(self):
        test_files = [
            ("Cloudflare", "https://speed.cloudflare.com/__down?bytes=10000000"),
            ("Cloudflare", "https://speed.cloudflare.com/__down?bytes=25000000"),
            ("Hetzner", "http://speed.hetzner.de/10MB.bin"),
        ]
        results = []
        tmp_file = "/tmp/test_dl.tmp"

        for name, url in test_files:
            try:
                p = await asyncio.create_subprocess_exec(
                    "curl", "-sL", "--max-time", "30",
                    "-o", tmp_file,
                    "-w", "%{speed_download} %{time_total} %{size_download}",
                    url,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                out, _ = await asyncio.wait_for(p.communicate(), timeout=35)
                if p.returncode == 0 and out:
                    parts = out.decode().strip().split()
                    if len(parts) >= 3:
                        elapsed = float(parts[1])
                        size = int(float(parts[2]))
                        if size > 0 and elapsed > 0:
                            speed = size / elapsed
                            results.append((name, speed, elapsed, size))
            except FileNotFoundError:
                try:
                    start = time.time()
                    p = await asyncio.create_subprocess_exec(
                        "wget", "-q", "--timeout=30", "-O", tmp_file, url,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    await asyncio.wait_for(p.communicate(), timeout=35)
                    elapsed = time.time() - start
                    if p.returncode == 0 and os.path.exists(tmp_file):
                        size = os.path.getsize(tmp_file)
                        if size > 0 and elapsed > 0:
                            results.append((name, size / elapsed, elapsed, size))
                except Exception:
                    pass
            except Exception:
                continue
            finally:
                try:
                    os.remove(tmp_file)
                except Exception:
                    pass

            if results:
                break

        return results

    async def _run_upload_test(self):
        results = []
        upload_size = 5 * 1024 * 1024
        upload_file = "/tmp/test_ul.tmp"

        try:
            with open(upload_file, "wb") as f:
                f.write(os.urandom(upload_size))

            try:
                p = await asyncio.create_subprocess_exec(
                    "curl", "-sL", "--max-time", "30",
                    "-X", "POST",
                    "-F", f"file=@{upload_file}",
                    "-w", "%{speed_upload} %{time_total} %{size_upload}",
                    "-o", "/dev/null",
                    "https://speed.cloudflare.com/__up",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                out, _ = await asyncio.wait_for(p.communicate(), timeout=35)
                if p.returncode == 0 and out:
                    parts = out.decode().strip().split()
                    if len(parts) >= 3:
                        elapsed = float(parts[1])
                        size = int(float(parts[2]))
                        if size > 0 and elapsed > 0:
                            results.append(("Cloudflare", size / elapsed, elapsed, size))
            except Exception:
                pass

            if not results:
                try:
                    start = time.time()
                    p = await asyncio.create_subprocess_exec(
                        "curl", "-sL", "--max-time", "30",
                        "-T", upload_file, "-o", "/dev/null",
                        "https://speed.cloudflare.com/__up",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    await asyncio.wait_for(p.communicate(), timeout=35)
                    elapsed = time.time() - start
                    if elapsed > 0:
                        results.append(("Cloudflare", upload_size / elapsed, elapsed, upload_size))
                except Exception:
                    pass

        except Exception:
            pass
        finally:
            try:
                os.remove(upload_file)
            except Exception:
                pass

        return results

    async def _ping_host(self, host, count=4):
        try:
            p = await asyncio.create_subprocess_exec(
                "ping", "-c", str(count), "-W", "3", host,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await asyncio.wait_for(p.communicate(), timeout=20)

            if p.returncode == 0 and out:
                text = out.decode()
                rtt = re.search(
                    r"min/avg/max.*?=\s*([\d.]+)/([\d.]+)/([\d.]+)", text
                )
                loss = re.search(r"(\d+)% packet loss", text)
                loss_val = int(loss.group(1)) if loss else 0

                if rtt:
                    return {
                        "min": float(rtt.group(1)),
                        "avg": float(rtt.group(2)),
                        "max": float(rtt.group(3)),
                        "loss": loss_val,
                    }
                return {"min": -1, "avg": -1, "max": -1, "loss": loss_val}

            return {"min": -1, "avg": -1, "max": -1, "loss": 100}

        except FileNotFoundError:
            return {"min": -1, "avg": -1, "max": -1, "loss": -1, "error": "ping not found"}
        except asyncio.TimeoutError:
            return {"min": -1, "avg": -1, "max": -1, "loss": 100, "error": "timeout"}
        except Exception as e:
            return {"min": -1, "avg": -1, "max": -1, "loss": -1, "error": str(e)}

    def _format_ping_line(self, name, host, data):
        if data.get("error"):
            return (
                f"[FAIL] <b>{name}</b> ({host}): "
                f"<code>{_escape(data['error'])}</code>"
            )
        if data["avg"] < 0:
            return (
                f"[FAIL] <b>{name}</b> ({host}): "
                f"<code>{data['loss']}% loss</code>"
            )

        avg = data["avg"]
        if avg < 50:
            tag = "[OK]"
        elif avg < 150:
            tag = "[SLOW]"
        else:
            tag = "[BAD]"

        loss_str = ""
        if data["loss"] > 0:
            loss_str = f" | {data['loss']}% loss"

        return (
            f"{tag} <b>{name}</b> ({host})\n"
            f"   avg: <code>{avg:.1f}ms</code>"
            f" (min {data['min']:.1f} / max {data['max']:.1f})"
            f"{loss_str}"
        )

    async def _run_quick_latency(self):
        targets = [
            ("Cloudflare", "1.1.1.1"),
            ("Google", "8.8.8.8"),
            ("Telegram DC2", "149.154.167.51"),
        ]
        results = []
        for name, host in targets:
            data = await self._ping_host(host, count=3)
            data["name"] = name
            data["host"] = host
            results.append(data)
        return results

    async def _run_dns_test(self):
        dns_servers = [
            ("Cloudflare", "1.1.1.1"),
            ("Google", "8.8.8.8"),
            ("Quad9", "9.9.9.9"),
            ("OpenDNS", "208.67.222.222"),
            ("Yandex", "77.88.8.8"),
            ("AdGuard", "94.140.14.14"),
        ]
        domains = [
            "google.com",
            "telegram.org",
            "github.com",
            "cloudflare.com",
            "youtube.com",
        ]
        results = []

        for dns_name, dns_ip in dns_servers:
            times = []
            failures = 0

            for domain in domains:
                try:
                    p = await asyncio.create_subprocess_exec(
                        "dig", f"@{dns_ip}", domain, "+time=3", "+tries=1", "+stats",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    out, _ = await asyncio.wait_for(p.communicate(), timeout=5)

                    if p.returncode == 0 and out:
                        text = out.decode()
                        qt = re.search(r"Query time:\s*(\d+)\s*msec", text)
                        if qt:
                            times.append(int(qt.group(1)))
                        else:
                            failures += 1
                    else:
                        failures += 1

                except FileNotFoundError:
                    try:
                        start = time.time()
                        p = await asyncio.create_subprocess_exec(
                            "nslookup", domain, dns_ip,
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE,
                        )
                        await asyncio.wait_for(p.communicate(), timeout=5)
                        elapsed_ms = (time.time() - start) * 1000

                        if p.returncode == 0:
                            times.append(int(elapsed_ms))
                        else:
                            failures += 1
                    except FileNotFoundError:
                        try:
                            start = time.time()
                            loop = asyncio.get_running_loop()
                            await asyncio.wait_for(
                                loop.getaddrinfo(domain, 80),
                                timeout=5,
                            )
                            elapsed_ms = (time.time() - start) * 1000
                            times.append(int(elapsed_ms))
                        except Exception:
                            failures += 1
                    except Exception:
                        failures += 1
                except asyncio.TimeoutError:
                    failures += 1
                except Exception:
                    failures += 1

            if times:
                avg = sum(times) / len(times)
                min_t = min(times)
                max_t = max(times)
            else:
                avg = -1
                min_t = -1
                max_t = -1

            results.append({
                "name": dns_name,
                "ip": dns_ip,
                "avg": avg,
                "min": min_t,
                "max": max_t,
                "ok": len(times),
                "fail": failures,
                "total": len(domains),
            })

        return results

    async def _get_external_ip(self):
        for svc in [
            "https://api.ipify.org",
            "https://ifconfig.me/ip",
            "https://icanhazip.com",
            "https://ident.me",
        ]:
            for cmd in [
                ["curl", "-4", "-s", "--max-time", "5", svc],
                ["wget", "-qO-", "--timeout=5", svc],
            ]:
                try:
                    p = await asyncio.create_subprocess_exec(
                        *cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    out, _ = await asyncio.wait_for(p.communicate(), timeout=8)
                    if p.returncode == 0 and out:
                        ip = out.decode().strip()
                        if re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", ip):
                            return ip
                except FileNotFoundError:
                    continue
                except Exception:
                    continue
        return None

    async def _get_ip_info(self, ip):
        url = (
            f"http://ip-api.com/json/{ip}"
            f"?fields=status,message,country,regionName,city,isp,as,org,timezone,query"
        )
        for cmd in [
            ["curl", "-s", "--max-time", "5", url],
            ["wget", "-qO-", "--timeout=5", url],
        ]:
            try:
                p = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                out, _ = await asyncio.wait_for(p.communicate(), timeout=8)
                if p.returncode == 0 and out:
                    import json
                    data = json.loads(out.decode().strip())
                    if data.get("status") == "success":
                        return data
            except FileNotFoundError:
                continue
            except Exception:
                continue
        return None

    @loader.command(
        ru_doc="Мониторинг сети и тесты скорости",
        en_doc="Network monitoring and speed tests",
    )
    async def test(self, message):
        """Network monitoring and speed tests"""
        args = utils.get_args_raw(message).strip()
        prefix = self.get_prefix()

        if not args:
            await utils.answer(
                message,
                self.strings["help"].format(prefix=prefix),
            )
            return

        split = args.split(maxsplit=1)
        cmd = split[0].lower()

        handlers = {
            "speed": self._cmd_speed,
            "net": self._cmd_net,
            "ping": self._cmd_ping,
            "dns": self._cmd_dns,
            "ip": self._cmd_ip,
        }

        handler = handlers.get(cmd)
        if handler:
            try:
                await handler(message, split)
            except Exception as e:
                logger.error("[Test] Command %s error: %s", cmd, e)
                await utils.answer(
                    message,
                    f"<b>Error:</b> <code>{_escape(str(e)[:300])}</code>",
                )
        else:
            await utils.answer(
                message,
                self.strings["help"].format(prefix=prefix),
            )

    async def _cmd_speed(self, message, parts):
        m = await utils.answer(message, self.strings["speed_progress"])
        steps = []

        async def update(text):
            steps.append(text)
            display = self.strings["speed_progress"] + "\n\n"
            display += "\n".join(f"<code>{_escape(s)}</code>" for s in steps)
            await self._safe_edit(m, display)

        try:
            await update("Testing download...")
            dl_results = await self._run_download_test()

            await update("Testing upload...")
            ul_results = await self._run_upload_test()

            await update("Testing latency...")
            quick_targets = await self._run_quick_latency()

            if dl_results:
                dl_lines = []
                for name, speed, elapsed, size in dl_results:
                    dl_lines.append(
                        f"{name}: <code>{self._format_speed(speed)}</code>"
                        f" ({self._format_bytes(size)} in {elapsed:.1f}s)"
                    )
                dl_text = "\n".join(dl_lines)
            else:
                dl_text = "n/a"

            if ul_results:
                ul_lines = []
                for name, speed, elapsed, size in ul_results:
                    ul_lines.append(
                        f"{name}: <code>{self._format_speed(speed)}</code>"
                        f" ({self._format_bytes(size)} in {elapsed:.1f}s)"
                    )
                ul_text = "\n".join(ul_lines)
            else:
                ul_text = "n/a"

            if quick_targets:
                lat_lines = []
                for r in quick_targets:
                    lat_lines.append(
                        self._format_ping_line(r["name"], r["host"], r)
                    )
                lat_text = "\n".join(lat_lines)
            else:
                lat_text = "n/a"

            await self._safe_edit(
                m,
                self.strings["speed_result"].format(
                    download=dl_text,
                    upload=ul_text,
                    latency=lat_text,
                ),
            )
        except Exception as e:
            await self._safe_edit(
                m,
                self.strings["speed_fail"].format(error=_escape(str(e)[:300])),
            )

    async def _cmd_net(self, message, parts):
        interfaces, total_rx, total_tx, error = self._get_net_interfaces()

        if error or interfaces is None:
            await utils.answer(
                message,
                self.strings["net_fail"].format(error=_escape(error or "Unknown")),
            )
            return

        iface_lines = []
        for iface in interfaces:
            name = iface["name"]
            rx = self._format_bytes(iface["rx_bytes"])
            tx = self._format_bytes(iface["tx_bytes"])
            rx_pkt = iface["rx_packets"]
            tx_pkt = iface["tx_packets"]

            if iface["rx_bytes"] == 0 and iface["tx_bytes"] == 0:
                continue

            tag = "[lo]" if name == "lo" else "[net]"
            iface_lines.append(
                f"{tag} <b>{_escape(name)}</b>\n"
                f"   RX: <code>{rx}</code> ({rx_pkt} pkts)\n"
                f"   TX: <code>{tx}</code> ({tx_pkt} pkts)"
            )

        if not iface_lines:
            iface_lines.append("No active interfaces")

        uptime = self._get_system_uptime()

        await utils.answer(
            message,
            self.strings["net_stats"].format(
                interfaces="\n".join(iface_lines),
                total_rx=self._format_bytes(total_rx),
                total_tx=self._format_bytes(total_tx),
                total=self._format_bytes(total_rx + total_tx),
                uptime=uptime,
            ),
        )

    async def _cmd_ping(self, message, parts):
        m = await utils.answer(message, self.strings["ping_progress"])

        try:
            services = [
                ("Cloudflare", "1.1.1.1"),
                ("Google", "8.8.8.8"),
                ("Quad9", "9.9.9.9"),
                ("OpenDNS", "208.67.222.222"),
            ]

            telegram_dcs = [
                ("DC1", "149.154.175.53"),
                ("DC2", "149.154.167.51"),
                ("DC3", "149.154.175.100"),
                ("DC4", "149.154.167.91"),
                ("DC5", "91.108.56.130"),
            ]

            svc_lines = []
            for name, host in services:
                data = await self._ping_host(host)
                svc_lines.append(self._format_ping_line(name, host, data))

            tg_lines = []
            for name, host in telegram_dcs:
                data = await self._ping_host(host)
                tg_lines.append(self._format_ping_line(name, host, data))

            await self._safe_edit(
                m,
                self.strings["ping_result"].format(
                    services="\n".join(svc_lines),
                    telegram="\n".join(tg_lines),
                ),
            )

        except Exception as e:
            await self._safe_edit(
                m,
                self.strings["ping_fail"].format(error=_escape(str(e)[:300])),
            )

    async def _cmd_dns(self, message, parts):
        m = await utils.answer(message, self.strings["dns_progress"])

        try:
            results = await self._run_dns_test()

            if not results:
                await self._safe_edit(
                    m,
                    self.strings["dns_fail"].format(error="No results"),
                )
                return

            results.sort(key=lambda r: r["avg"] if r["avg"] >= 0 else 99999)

            lines = []
            for i, r in enumerate(results):
                rank = i + 1
                if r["avg"] >= 0:
                    if r["avg"] < 10:
                        tag = "[FAST]"
                    elif r["avg"] < 50:
                        tag = "[OK]"
                    elif r["avg"] < 100:
                        tag = "[SLOW]"
                    else:
                        tag = "[BAD]"

                    fail_str = ""
                    if r["fail"] > 0:
                        fail_str = f" | {r['fail']}/{r['total']} failed"

                    lines.append(
                        f"#{rank} {tag} <b>{r['name']}</b> ({r['ip']})\n"
                        f"   avg: <code>{r['avg']:.0f}ms</code>"
                        f" (min {r['min']}ms / max {r['max']}ms)"
                        f" | {r['ok']}/{r['total']} ok"
                        f"{fail_str}"
                    )
                else:
                    lines.append(
                        f"#{rank} [FAIL] <b>{r['name']}</b> ({r['ip']})\n"
                        f"   all {r['total']} queries failed"
                    )

            await self._safe_edit(
                m,
                self.strings["dns_result"].format(results="\n".join(lines)),
            )

        except Exception as e:
            await self._safe_edit(
                m,
                self.strings["dns_fail"].format(error=_escape(str(e)[:300])),
            )

    async def _cmd_ip(self, message, parts):
        m = await utils.answer(message, self.strings["ip_progress"])

        try:
            ip = await self._get_external_ip()
            if not ip:
                await self._safe_edit(
                    m,
                    self.strings["ip_fail"].format(error="Cannot detect external IP"),
                )
                return

            info = await self._get_ip_info(ip)
            if not info:
                await self._safe_edit(
                    m,
                    self.strings["ip_result"].format(
                        ip=ip,
                        country="n/a",
                        region="n/a",
                        city="n/a",
                        isp="n/a",
                        as_info="n/a",
                        org="n/a",
                        timezone="n/a",
                    ),
                )
                return

            await self._safe_edit(
                m,
                self.strings["ip_result"].format(
                    ip=_escape(info.get("query", ip)),
                    country=_escape(info.get("country", "n/a")),
                    region=_escape(info.get("regionName", "n/a")),
                    city=_escape(info.get("city", "n/a")),
                    isp=_escape(info.get("isp", "n/a")),
                    as_info=_escape(info.get("as", "n/a")),
                    org=_escape(info.get("org", "n/a")),
                    timezone=_escape(info.get("timezone", "n/a")),
                ),
            )

        except Exception as e:
            await self._safe_edit(
                m,
                self.strings["ip_fail"].format(error=_escape(str(e)[:300])),
            )