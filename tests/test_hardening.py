from __future__ import annotations

import ast
import asyncio
import hashlib
import json
import os
import re
import stat
import sys
import tempfile
import textwrap
import urllib.parse
from pathlib import Path
from typing import Dict, List, Tuple

import pytest


def _find_root() -> Path:
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        if (parent / "XRay.py").is_file():
            return parent
    raise RuntimeError("XRay.py is not reachable from the test file")


ROOT = _find_root()
MODULE = ROOT / "XRay.py"
SOURCE = MODULE.read_text(encoding="utf-8")
LINES = SOURCE.split("\n")
TREE = ast.parse(SOURCE)

DAEMON_MODULES = ("XRay.py", "WebDeployer.py")

ROOT_REASON = "root"
DOCKER_REASON = "docker"


def _sources(wanted: dict[str, str]) -> list[str]:
    found: list[str] = []
    for node in TREE.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in wanted:
            found.append(textwrap.dedent("\n".join(LINES[node.lineno - 1:node.end_lineno])))
        if isinstance(node, ast.ClassDef) and node.name == "XRay":
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name in wanted:
                    found.append(textwrap.dedent("\n".join(LINES[item.lineno - 1:item.end_lineno])))
    assert len(found) == len(wanted), (len(found), sorted(wanted))
    return found


def _source(name: str) -> str:
    return _sources({name: name})[0]


def _constants(*names: str) -> dict[str, object]:
    found: dict[str, object] = {}
    for node in TREE.body:
        target = getattr(node.targets[0], "id", "") if isinstance(node, ast.Assign) else ""
        if target in names:
            exec("\n".join(LINES[node.lineno - 1:node.end_lineno]), {"os": os}, found)
    return found


def _base_namespace() -> dict[str, object]:
    return {
        "os": os,
        "re": re,
        "sys": sys,
        "json": json,
        "asyncio": asyncio,
        "hashlib": hashlib,
        "tempfile": tempfile,
        "urllib": urllib,
        "Tuple": Tuple,
        "Dict": Dict,
        "List": List,
        "GUARD_ROOT": ROOT_REASON,
        "GUARD_DOCKER": DOCKER_REASON,
        "RELEASE_HOSTS": (
            "github.com",
            "objects.githubusercontent.com",
            "release-assets.githubusercontent.com",
            "api.github.com",
        ),
    }


def _helpers(*names: str) -> dict[str, object]:
    namespace = _base_namespace()
    exec("\n\n".join(_sources({name: name for name in names})), namespace)
    return namespace


def _class(**extra: object) -> dict[str, object]:
    namespace = _base_namespace()
    namespace.update(_constants("XRAY_CORE_VERSION", "XRAY_CORE_SHA256", "CLOUDFLARED_VERSION", "CLOUDFLARED_SHA256"))
    for name in ("_release_allowed", "_sha256_file", "_curl_header_config", "_drop_curl_config", "_write_private"):
        namespace[name] = _helpers(name)[name]
    namespace["_guard_reason"] = lambda: ""
    namespace.update(extra)
    return namespace


def _bind(namespace: dict[str, object], name: str, **attributes: object) -> object:
    owner = type("Owner", (), {})()
    owner.__dict__.update(attributes)
    function = namespace[name]
    assert callable(function)
    return function.__get__(owner, type(owner))


class Stub:
    pass


def test_the_module_keeps_no_root_systemd_branch() -> None:
    assert "_is_root" not in SOURCE
    assert "/etc/systemd/system" not in SOURCE
    assert "multi-user.target" not in SOURCE
    assert "WantedBy=default.target" in SOURCE


def test_systemctl_is_only_ever_the_user_manager() -> None:
    assert SOURCE.count('"systemctl"') == 1
    assert 'cmd = ["systemctl", "--user"]' in SOURCE
    assert "XDG_RUNTIME_DIR" in SOURCE


def test_the_unit_directory_lives_in_the_home_of_the_caller() -> None:
    body = _source("_systemd_user_dir")
    assert 'os.path.expanduser("~")' in body
    assert '".config"' in body
    assert "/etc" not in body


def test_the_guard_reports_root_first_and_container_second() -> None:
    namespace = _base_namespace()
    exec(_source("_in_docker"), namespace)
    exec(_source("_guard_reason"), namespace)

    namespace["os"] = type("OS", (), {"name": "posix", "geteuid": staticmethod(lambda: 0)})()
    assert namespace["_guard_reason"]() == ROOT_REASON

    namespace["os"] = type("OS", (), {"name": "posix", "geteuid": staticmethod(lambda: 1000)})()
    namespace["_in_docker"] = lambda: True
    assert namespace["_guard_reason"]() == DOCKER_REASON

    namespace["_in_docker"] = lambda: False
    assert namespace["_guard_reason"]() == ""


def test_the_guard_is_asked_before_anything_starts() -> None:
    for name in ("_start_user", "_start_websocket_site", "_start_websocket_tunnel", "_sub_start_server"):
        assert "_guard_reason()" in _source(name), name
    assert "_guard_deny(message)" in _source("xr")
    body = _source("_start_user")
    assert body.index("_guard_reason()") < body.index("_start_user_impl")


def test_the_refusal_is_explained_in_both_languages() -> None:
    tables: dict[str, dict[str, str]] = {}
    for node in ast.walk(TREE):
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") in ("strings", "strings_ru"):
            tables[node.targets[0].id] = ast.literal_eval(node.value)
    assert set(tables) == {"strings", "strings_ru"}
    needed = {"guard_title", "guard_root", "guard_docker", "guard_why", "guard_how"}
    for name, table in tables.items():
        assert needed <= set(table), name
        for key in needed:
            value = table[key]
            assert isinstance(value, str) and len(value) > 20, (name, key)
            assert not any(ord(char) > 0x2FFF for char in value), (name, key)
    assert "root" in tables["strings"]["guard_root"]
    assert "container" in tables["strings"]["guard_docker"]
    assert "\u043e\u0442\u043a\u0430\u0437" in tables["strings_ru"]["guard_root"]
    assert "\u043a\u043e\u043d\u0442\u0435\u0439\u043d\u0435\u0440" in tables["strings_ru"]["guard_docker"]
    assert "systemctl --user" in tables["strings"]["guard_how"]
    assert "systemctl --user" in tables["strings_ru"]["guard_how"]


def test_a_curl_header_config_is_private_and_cannot_add_a_directive(tmp_path) -> None:
    namespace = _helpers("_curl_header_config", "_drop_curl_config")
    injected, injected_path = namespace["_curl_header_config"]('abc" \n header = "X-Evil: 1')
    assert injected == [] and injected_path == ""
    assert namespace["_curl_header_config"]("") == ([], "")
    assert namespace["_curl_header_config"]("a" * 4096) == ([], "")
    assert namespace["_curl_header_config"]("bad token\n") == ([], "")
    args, path = namespace["_curl_header_config"]("gho_16CharsTokenValue")
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    text = Path(path).read_text(encoding="utf-8").strip()
    assert text.count("header =") == 1
    assert "\n" not in text
    namespace["_drop_curl_config"](path)
    assert not Path(path).exists()


def test_no_token_ever_reaches_the_process_list() -> None:
    headers = re.findall(r'"-H", "([^"]+)"', SOURCE)
    assert set(headers) == {
        "Accept: application/json",
        "Content-Type: application/x-www-form-urlencoded",
    }, headers
    assert not any("Authorization" in header for header in headers)
    assert "{gh_token}" not in SOURCE
    assert "Authorization: Bearer {clean}" in SOURCE
    assert SOURCE.count("_curl_header_config(") == 3


def test_the_private_writer_creates_a_0600_file(tmp_path) -> None:
    namespace = _helpers("_write_private")
    target = tmp_path / "config.json"
    target.write_text("old", encoding="utf-8")
    os.chmod(target, 0o644)
    namespace["_write_private"](str(target), '{"k": 1}')
    assert stat.S_IMODE(os.stat(target).st_mode) == 0o600
    assert target.read_text(encoding="utf-8") == '{"k": 1}'


def test_the_vpn_config_with_the_private_keys_is_written_private() -> None:
    assert '_write_private(config_path, json.dumps(config, indent=2))' in SOURCE
    assert 'with open(config_path, "w")' not in SOURCE
    assert 'os.makedirs(self._root, mode=0o700, exist_ok=True)' in SOURCE


def test_the_release_hosts_are_enforced() -> None:
    allow = _helpers("_release_allowed")["_release_allowed"]
    assert allow("https://github.com/XTLS/Xray-core/releases/download/v1/x.zip")
    assert allow("https://objects.githubusercontent.com/a?b=c")
    assert not allow("http://github.com/x")
    assert not allow("https://github.com.evil.tld/x")
    assert not allow("https://evil.tld/github.com/")
    assert not allow("file:///etc/passwd")
    assert not allow("")


def test_the_download_is_hashed_before_it_is_used(tmp_path) -> None:
    hasher = _helpers("_sha256_file")["_sha256_file"]
    sample = tmp_path / "blob"
    sample.write_bytes(b"xray")
    assert hasher(str(sample)) == hashlib.sha256(b"xray").hexdigest()
    install = _source("_install_xray")
    assert install.index("_release_allowed(download_url)") < install.index("_sha256_file(dl)")
    assert install.index("_sha256_file(dl)") < install.index("shutil.copy2(binary, self._xray_path)")
    assert "Digest mismatch" in install


def test_the_engine_pins_are_complete() -> None:
    pins = _constants("XRAY_CORE_VERSION", "XRAY_CORE_SHA256", "CLOUDFLARED_VERSION", "CLOUDFLARED_SHA256")
    assert pins["XRAY_CORE_VERSION"] == "v26.10.21"
    assert pins["CLOUDFLARED_VERSION"] == "2026.10.5"
    core = pins["XRAY_CORE_SHA256"]
    assert isinstance(core, dict) and {"64", "arm64-v8a"} <= set(core)
    for arch, digest in core.items():
        assert re.fullmatch(r"[0-9a-f]{64}", digest), arch
    cloudflared = pins["CLOUDFLARED_SHA256"]
    assert isinstance(cloudflared, dict) and cloudflared
    for asset, digest in cloudflared.items():
        assert asset.startswith("cloudflared-linux-"), asset
        assert re.fullmatch(r"[0-9a-f]{64}", digest), asset
    assert "releases/latest" not in SOURCE
    assert "cloudflared/releases/latest/download" not in SOURCE


def test_only_the_pinned_core_tag_may_be_installed() -> None:
    namespace = _class()
    exec(_source("_install_xray"), namespace)
    owner = Stub()
    owner._install_xray = namespace["_install_xray"].__get__(owner, Stub)
    ok, reason = asyncio.run(owner._install_xray("v1.0.0"))
    assert ok is False
    assert "v26.10.21" in reason
    body = _source("_install_xray")
    assert body.index("XRAY_CORE_VERSION") < body.index("for name in list(self._processes.keys())")


def test_the_installer_refuses_a_root_or_container_host() -> None:
    namespace = _class(_guard_reason=lambda: ROOT_REASON)
    exec(_source("_install_cloudflared"), namespace)
    owner = Stub()
    owner._cloudflared_path = "/tmp/cloudflared"
    owner._install_cloudflared = namespace["_install_cloudflared"].__get__(owner, Stub)
    ok, reason = asyncio.run(owner._install_cloudflared())
    assert ok is False and reason == ROOT_REASON


def test_the_subscription_server_never_starts_under_a_bad_host() -> None:
    namespace = _class(_guard_reason=lambda: DOCKER_REASON)
    exec(_source("_sub_start_server"), namespace)
    owner = Stub()
    owner._sub_server = None
    owner._sub = {"port": 2096}
    owner._sub_start_server = namespace["_sub_start_server"].__get__(owner, Stub)
    ok, reason = owner._sub_start_server()
    assert ok is False and reason == DOCKER_REASON


def test_the_socks_inbound_stays_behind_a_password() -> None:
    body = _source("_build_config")
    assert '"auth": "password"' in body
    assert '"udp": True' in body
    assert 'user.get("socks_user", "")' in body


def test_the_management_api_only_listens_on_the_loopback() -> None:
    found = re.findall(r'"listen": "([^"]+)"', SOURCE)
    assert found.count("0.0.0.0") == 2
    assert found.count("127.0.0.1") == 3
    for match in re.finditer(r'"listen": "0\.0\.0\.0"', SOURCE):
        window = SOURCE[match.start():match.start() + 400]
        assert '"protocol": "socks"' in window or '"protocol": "vless"' in window


def test_no_shell_is_ever_spawned() -> None:
    assert "shell=True" not in SOURCE
    assert "os.system(" not in SOURCE
    assert "create_subprocess_shell" not in SOURCE
    assert "subprocess.Popen" not in SOURCE


def test_the_subscription_token_is_compared_in_constant_time() -> None:
    assert "secrets.compare_digest" in SOURCE
    assert "==" in SOURCE


def test_the_subscription_server_is_bounded_and_rate_limited() -> None:
    start = SOURCE.index("class _SubServer")
    server = SOURCE[start:start + 1200]
    assert "SUB_SLOTS.acquire(blocking=False)" in server
    assert "settimeout(SUB_SOCKET_TIMEOUT)" in server
    assert "request_queue_size" in server
    assert "_sub_allowed(self.client_address[0])" in SOURCE


def test_the_module_ships_no_comment_lines_after_the_header() -> None:
    offenders = []
    for number, line in enumerate(LINES, start=1):
        stripped = line.strip()
        if not stripped.startswith("#"):
            continue
        if number <= 8:
            continue
        offenders.append(number)
    assert offenders == []


def test_the_module_holds_no_emoji_and_no_long_dash() -> None:
    forbidden = {"\u2014", "\u2013", "\u2015"}
    for char in SOURCE:
        assert char not in forbidden, hex(ord(char))
        assert not 0x1F000 <= ord(char) <= 0x1FAFF, hex(ord(char))
        assert not 0x2600 <= ord(char) <= 0x27BF, hex(ord(char))


def test_every_daemon_module_refuses_root_and_containers() -> None:
    for name in DAEMON_MODULES:
        path = ROOT / name
        text = path.read_text(encoding="utf-8")
        assert "_is_root" not in text, name
        assert "/etc/systemd/system" not in text, name
        assert "multi-user.target" not in text, name
        assert "GUARD_ROOT" in text and "GUARD_DOCKER" in text, name
        assert "def _guard_reason()" in text, name
        assert "def _in_docker()" in text, name
        for line in text.split("\n"):
            if '"systemctl"' in line:
                assert '["systemctl", "--user"]' in line, (name, line.strip())
        tables = {}
        for node in ast.walk(ast.parse(text)):
            if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") in ("strings", "strings_ru"):
                tables[node.targets[0].id] = ast.literal_eval(node.value)
        assert set(tables) == {"strings", "strings_ru"}, name
        for key in ("guard_title", "guard_root", "guard_docker", "guard_why", "guard_how"):
            assert key in tables["strings"] and key in tables["strings_ru"], (name, key)
        assert "\u043e\u0442\u043a\u0430\u0437" in tables["strings_ru"]["guard_root"], name


def test_the_daemon_modules_carry_no_emoji_and_no_long_dash() -> None:
    for name in DAEMON_MODULES:
        text = (ROOT / name).read_text(encoding="utf-8")
        assert "\u2014" not in text and "\u2013" not in text, name
        assert not any(0x1F000 <= ord(char) <= 0x1FAFF for char in text), name


def test_the_token_module_never_builds_a_url_from_an_unchecked_token() -> None:
    text = (ROOT / "Token.py").read_text(encoding="utf-8")
    assert 'f"https://api.telegram.org/bot{token}/getMe"' in text
    body = text[text.index("def _validate_token_api"):]
    body = body[:body.index('url = f"https://api.telegram.org')]
    assert "BOT_TOKEN_PATTERN.fullmatch" in body
    pattern = _constants_from(ROOT / "Token.py", "BOT_TOKEN_PATTERN")
    assert pattern is not None
    assert pattern.fullmatch("123456789:AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsawX")
    assert not pattern.fullmatch("1:x@evil.example/a")
    assert not pattern.fullmatch("123456789:short")


def _constants_from(path: Path, *names: str):
    import re as _re

    source = path.read_text(encoding="utf-8")
    for node in ast.parse(source).body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") in names:
            expression = ast.unparse(node.value)
            return eval(expression, {"re": _re})
    return None


def test_the_token_module_keeps_its_work_in_a_private_home_directory() -> None:
    text = (ROOT / "Token.py").read_text(encoding="utf-8")
    assert '"/tmp/Token' not in text
    assert 'os.path.join(os.path.expanduser("~"), ".token_on_userbot")' in text
    assert "os.chmod(base, 0o700)" in text


def test_the_exiftool_download_is_limited_to_known_hosts() -> None:
    text = (ROOT / "MetaDataDumper.py").read_text(encoding="utf-8")
    assert "EXIFTOOL_HOSTS" in text
    body = text[text.index("def _download_exiftool"):]
    body = body[:body.index("tf = tarfile.open")] if "tf = tarfile.open" in body else body[:2000]
    assert "parts.hostname or \"\").lower() not in EXIFTOOL_HOSTS" in body
    assert "EXIFTOOL_SHA256" in body
    assert "import contextlib" in text


def test_the_module_compiles() -> None:
    for name in DAEMON_MODULES:
        compile((ROOT / name).read_text(encoding="utf-8"), name, "exec")
