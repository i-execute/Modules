from __future__ import annotations

import ast
import asyncio
import json
import re
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest


def _find_root() -> Path:
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        if (parent / "ServerlessChecker.py").is_file():
            return parent
    raise RuntimeError("ServerlessChecker.py is not reachable from the test file")


ROOT = _find_root()
CHECKER = ROOT / "ServerlessChecker.py"
SOURCE = CHECKER.read_text(encoding="utf-8")
LINES = SOURCE.split("\n")


class _Loader:
    class Module:
        def get_prefix(self) -> str:
            return "."

    @staticmethod
    def tds(cls):
        return cls

    @staticmethod
    def command(*args, **kwargs):
        def wrap(func):
            return func

        return wrap


class _Utils:
    @staticmethod
    def get_args_raw(message) -> str:
        return message.text

    @staticmethod
    async def answer(message, text):
        message.answers.append(text)
        return message


def _module(webhook=None):
    body = SOURCE.replace("from .. import loader, utils", "")
    body = body.replace("from telethon.tl.types import Message", "Message = object")
    namespace = {"loader": _Loader, "utils": _Utils}
    exec(compile(body, str(CHECKER), "exec"), namespace)
    if webhook is not None:
        namespace["_webhook_status"] = webhook
    return namespace


class _Bot:
    def __init__(self, bot_id: int, username: str = "", bot: bool = True) -> None:
        self.id = bot_id
        self.username = username
        self.bot = bot

    def __repr__(self) -> str:
        return f"User(id={self.id}, username={self.username!r}, bot={self.bot})"


class _Client:
    def __init__(self, entities: dict) -> None:
        self._entities = dict(entities)
        for entity in list(entities.values()):
            if getattr(entity, "username", ""):
                self._entities[entity.username] = entity

    async def get_entity(self, target):
        key = target if isinstance(target, str) else int(target)
        entity = self._entities.get(key)
        if entity is None:
            raise ValueError("no entity")
        return entity


class _Message:
    def __init__(self, text: str = "") -> None:
        self.text = text
        self.answers: list[str] = []


@pytest.fixture()
def hook_server():
    state = {"status": 401, "type": "INTERNAL_ERROR", "seen": []}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            state["seen"].append(self.path)
            payload = json.dumps(
                {
                    "ok": False,
                    "description": "x",
                    "error_code": state["status"],
                    "error_type": state["type"],
                }
            ).encode()
            self.send_response(state["status"])
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield state, f"http://127.0.0.1:{server.server_port}/app{{app_id}}/webhook"
    server.shutdown()


def test_the_verdict_follows_the_webhook_error_type(hook_server):
    state, url = hook_server
    namespace = _module()
    namespace["WEBHOOK_URL"] = url
    namespace["WEBHOOK_HOST"] = "127.0.0.1"
    namespace["WEBHOOK_SCHEME"] = "http"
    status = namespace["_webhook_status"]
    verdict = namespace["_verdict"]

    code, kind = status(8627643530)
    assert (code, kind) == (401, "INTERNAL_ERROR")
    assert verdict(code, kind) == "serverless"

    state["status"], state["type"] = 404, "APP_NOT_FOUND"
    code, kind = status(1)
    assert (code, kind) == (404, "APP_NOT_FOUND")
    assert verdict(code, kind) == "missing"

    state["status"], state["type"] = 500, ""
    code, kind = status(1)
    assert verdict(code, kind) == "unknown"


@pytest.mark.parametrize(
    "raw,expected",
    (
        ("@Firefly_agent", "Firefly_agent"),
        ("Firefly_agent", "Firefly_agent"),
        ("https://t.me/Grabber_testflight_bot", "Grabber_testflight_bot"),
        ("t.me/Nous_Guard_bot", "Nous_Guard_bot"),
        ("8627643530", 8627643530),
        ("@bad name", None),
        ("", None),
    ),
)
def test_a_target_is_parsed(raw, expected):
    assert _module()["_parse_target"](raw) == expected


def test_a_serverless_bot_returns_the_entity():
    namespace = _module(webhook=lambda bot_id: (401, "INTERNAL_ERROR"))
    module = namespace["ServerlessChecker"]()
    module._client = _Client({8627643530: _Bot(8627643530, "Nous_Guard_bot")})
    message = _Message("@Nous_Guard_bot")

    asyncio.run(module.slch(message))

    assert len(message.answers) == 2
    final = message.answers[-1]
    assert "Nous_Guard_bot" in final
    assert "8627643530" in final
    assert "serverless" in final
    assert "User(id=8627643530" in final
    assert "<code>401</code>" in final


def test_a_plain_bot_is_reported_as_not_serverless():
    namespace = _module(webhook=lambda bot_id: (404, "APP_NOT_FOUND"))
    module = namespace["ServerlessChecker"]()
    module._client = _Client({555000111: _Bot(555000111, "Grabber_testflight_bot")})
    message = _Message("Grabber_testflight_bot")

    asyncio.run(module.slch(message))

    final = message.answers[-1]
    assert "no serverless" in final
    assert "<code>404</code>" in final


def test_several_targets_are_reported_as_lines():
    namespace = _module(webhook=lambda bot_id: (
        (401, "INTERNAL_ERROR") if bot_id == 1 else (404, "APP_NOT_FOUND")
    ))
    module = namespace["ServerlessChecker"]()
    module._client = _Client({1: _Bot(1, "Firefly_agent"), 2: _Bot(2, "Grabber_testflight_bot")})
    message = _Message("@Firefly_agent @Grabber_testflight_bot")

    asyncio.run(module.slch(message))

    final = message.answers[-1].split("\n")
    assert len(final) == 2
    assert "Firefly_agent" in final[0] and "serverless" in final[0]
    assert "Grabber_testflight_bot" in final[1] and "no serverless" in final[1]


def test_a_non_bot_and_an_unknown_target_are_rejected():
    namespace = _module(webhook=lambda bot_id: (401, "INTERNAL_ERROR"))
    module = namespace["ServerlessChecker"]()
    module._client = _Client({7: _Bot(7, "just_a_user", bot=False)})

    message = _Message("@just_a_user")
    asyncio.run(module.slch(message))
    assert "not a bot" in message.answers[-1]

    missing = _Message("@nobody_here")
    asyncio.run(module.slch(missing))
    assert "could not be resolved" in missing.answers[-1]


def test_a_missing_argument_and_an_overlong_list_are_rejected():
    namespace = _module(webhook=lambda bot_id: (401, "INTERNAL_ERROR"))
    module = namespace["ServerlessChecker"]()
    module._client = _Client({})

    empty = _Message("  ")
    asyncio.run(module.slch(empty))
    assert "no target" in empty.answers[-1]

    crowded = _Message(" ".join(f"@bot{index:03d}" for index in range(11)))
    asyncio.run(module.slch(crowded))
    assert "too many targets" in crowded.answers[-1]


def test_a_failed_request_is_shown_as_an_error():
    def boom(bot_id):
        raise OSError("network is unreachable")

    namespace = _module(webhook=boom)
    module = namespace["ServerlessChecker"]()
    module._client = _Client({8627643530: _Bot(8627643530, "Nous_Guard_bot")})

    message = _Message("@Nous_Guard_bot")
    asyncio.run(module.slch(message))
    assert "request failed" in message.answers[-1]
    assert "network is unreachable" in message.answers[-1]


def test_the_module_follows_the_house_style():
    tree = ast.parse(SOURCE)
    header = LINES[:6]
    assert header[0].startswith("# CopyLeft")
    assert header[1].startswith("# Author")
    assert header[2].startswith("# Licensed")
    for line in LINES[6:]:
        assert not line.strip().startswith("#"), line

    cls = [node for node in tree.body if isinstance(node, ast.ClassDef)]
    assert [node.name for node in cls] == ["ServerlessChecker"]
    decorators = [
        f"{d.value.id}.{d.attr}" for d in cls[0].decorator_list if isinstance(d, ast.Attribute)
    ]
    assert decorators == ["loader.tds"]

    dicts = {}
    for node in cls[0].body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") in ("strings", "strings_ru"):
            dicts[node.targets[0].id] = {
                key.value: ast.literal_eval(value)
                for key, value in zip(node.value.keys, node.value.values)
            }
    assert set(dicts) == {"strings", "strings_ru"}
    assert dicts["strings"]["name"] == "ServerlessChecker"
    assert set(dicts["strings"]) - set(dicts["strings_ru"]) == {"name"}
    for key, value in list(dicts["strings"].items()) + list(dicts["strings_ru"].items()):
        assert "\n\n" not in value, key

    commands = [
        node.name
        for node in cls[0].body
        if isinstance(node, ast.AsyncFunctionDef)
    ]
    assert "slch" in commands
    command = [node for node in cls[0].body if isinstance(node, ast.AsyncFunctionDef) and node.name == "slch"][0]
    assert ast.get_docstring(command)

    emoji = re.compile(r"[\U0001F000-\U0001FAFF\u2600-\u27BF]")
    assert not emoji.search(SOURCE)
    assert "\u2014" not in SOURCE and "\u2013" not in SOURCE


def test_the_webhook_url_is_fixed():
    assert 'https://cloud.telegram.org/app{app_id}/webhook' in SOURCE
    assert SOURCE.count("cloud.telegram.org") == 2
    assert SOURCE.count("https://cloud.telegram.org") == 1
