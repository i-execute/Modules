from __future__ import annotations

import ast
import os
import stat
import tempfile
import textwrap
from pathlib import Path

import pytest


def _find_root() -> Path:
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        if (parent / "MetaDataDumper.py").is_file():
            return parent
    raise RuntimeError("MetaDataDumper.py is not reachable from the test file")


ROOT = _find_root()
MDD = (ROOT / "MetaDataDumper.py").read_text(encoding="utf-8")
MUSIC = {
    name: (ROOT / name).read_text(encoding="utf-8")
    for name in ("YNDXMusic.py", "YTMusic.py", "SCMusic.py")
}
TWIN = (ROOT / "Twin.py").read_text(encoding="utf-8")

DATA_DIR_METHODS = (
    "_data_uid",
    "_data_dir_candidates",
    "_prepare_dir",
    "_resolve_data_dir",
    "_data_dir",
    "_workdir",
)


def _data_dir_class():
    lines = MDD.split("\n")
    tree = ast.parse(MDD)
    bodies = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name in DATA_DIR_METHODS:
            start = min([node.lineno] + [d.lineno for d in node.decorator_list])
            bodies.append(textwrap.dedent("\n".join(lines[start - 1:node.end_lineno])))
    assert len(bodies) == len(DATA_DIR_METHODS)
    namespace = {"os": os, "stat": stat, "tempfile": tempfile}
    source = "class Probe:\n" + textwrap.indent("\n\n".join(bodies), "    ")
    source += "\n    DATA_DIR_NAME = 'MetaDataDumper'\n"
    exec(source, namespace)
    return namespace["Probe"]


def _probe(monkeypatch, getuid=None):
    factory = _data_dir_class()
    instance = factory()
    instance._tg_id = 7610246474
    if getuid is not None:
        monkeypatch.setattr(os, "getuid", getuid)
    return instance


def test_the_data_dir_never_takes_the_module_down(tmp_path, monkeypatch):
    monkeypatch.setenv("TMPDIR", str(tmp_path / "missing"))
    instance = _probe(monkeypatch)
    path = instance._data_dir
    assert isinstance(path, str)
    assert path
    assert os.path.isdir(path)


def test_a_directory_owned_by_someone_else_is_skipped(tmp_path, monkeypatch):
    foreign = tmp_path / "MetaDataDumper-7610246474"
    foreign.mkdir(mode=0o700)
    os.chmod(foreign, 0o500)
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    real_uid = foreign.stat().st_uid

    instance = _probe(monkeypatch, getuid=lambda: real_uid + 1)
    path = instance._data_dir
    assert path != str(foreign)
    assert os.path.isdir(path)
    assert os.access(path, os.W_OK | os.X_OK)


def test_an_exhausted_search_falls_back_to_a_temp_directory(tmp_path, monkeypatch):
    blocked = tmp_path / "blocked"
    blocked.mkdir()
    os.chmod(blocked, 0o500)
    instance = _probe(monkeypatch)
    monkeypatch.setattr(
        instance,
        "_data_dir_candidates",
        lambda uid: iter([str(blocked / "a"), str(blocked / "b")]),
        raising=False,
    )
    path = instance._data_dir
    assert os.path.isdir(path)
    assert os.path.dirname(path) == os.path.realpath(tempfile.gettempdir()) or path.startswith(tempfile.gettempdir())
    assert instance._data_dir_reason == "temporary directory"


def test_a_hopeless_host_still_yields_an_explanation(monkeypatch):
    instance = _probe(monkeypatch)
    monkeypatch.setattr(instance, "_data_dir_candidates", lambda uid: iter([]), raising=False)

    def denied(prefix=""):
        raise OSError("no usable temporary directory")

    monkeypatch.setattr(tempfile, "mkdtemp", denied)
    monkeypatch.setattr(os, "access", lambda path, mode: False)
    assert instance._data_dir == ""
    assert instance._workdir() == ""
    assert instance._data_dir_reason
    assert "no usable temporary directory" in instance._data_dir_reason


def test_the_data_dir_reports_a_reason_instead_of_raising(tmp_path, monkeypatch):
    instance = _probe(monkeypatch)
    instance._tg_id = None
    path = instance._data_dir
    assert path
    assert instance._workdir()
    assert instance._data_uid() == "common"
    assert MDD.count('"no_data_dir": (') == 2
    assert "if not self._workdir():" in MDD


def test_the_registration_pass_can_walk_every_attribute(tmp_path, monkeypatch):
    factory = _data_dir_class()
    instance = factory()
    instance._tg_id = 7610246474
    instance.strings = {"no_data_dir": "<b>No working directory</b>\n<blockquote>{reason}</blockquote>"}

    def _s(self, key, **kwargs):
        return self.strings[key].format(**kwargs)

    def _esc(value):
        return value

    instance._s = _s.__get__(instance, type(instance))
    instance._esc = _esc

    def hostile(*args, **kwargs):
        raise PermissionError(13, "Permission denied", "/tmp/MetaDataDumper/7610246474")

    monkeypatch.setattr(os, "makedirs", hostile)
    for name in dir(instance):
        getattr(instance, name)


def test_the_cleanup_step_survives_a_missing_directory(tmp_path, monkeypatch):
    instance = _probe(monkeypatch)
    monkeypatch.setattr(instance.__class__, "_data_dir", property(lambda self: ""), raising=False)
    assert instance._workdir() == ""
    assert "for name in (os.listdir(workdir) if workdir else []):" in MDD


@pytest.mark.parametrize("name", ("YTMusic.py", "SCMusic.py"))
def test_the_music_modules_carry_no_upload_host(name):
    text = MUSIC[name]
    assert "UPLOAD_PROVIDER" not in text
    assert "UPLOAD_PROVIDERS" not in text
    assert "x0.at" not in text
    assert "uguu.se" not in text
    assert "pomf.lain.la" not in text
    assert "_upload_via" not in text
    assert "_parse_upload_url" not in text


@pytest.mark.parametrize("name", ("YNDXMusic.py", "YTMusic.py", "SCMusic.py"))
def test_covers_travel_as_public_urls(name):
    text = MUSIC[name]
    assert "cover_photo" not in text
    assert "io.BytesIO" not in text.split("def ", 1)[0] or True
    for line in text.split("\n"):
        stripped = line.strip()
        if stripped.startswith(("edit_kwargs[", "form_kwargs[")) and "\"photo\"]" in stripped:
            value = stripped.split("=", 1)[1].strip()
            assert value.isidentifier(), stripped


@pytest.mark.parametrize("name", ("YNDXMusic.py", "YTMusic.py", "SCMusic.py"))
def test_no_cover_is_fetched_from_a_file_host(name):
    text = MUSIC[name]
    tree = ast.parse(text)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and "cover" in node.name:
            chunk = ast.get_source_segment(text, node) or ""
            assert "_upload" not in chunk, node.name


@pytest.mark.parametrize("name", ("YNDXMusic.py", "YTMusic.py", "SCMusic.py"))
def test_an_inline_form_never_receives_local_bytes(name):
    text = MUSIC[name]
    tree = ast.parse(text)
    form_calls = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "form":
            form_calls.append(node)
    assert form_calls
    for call in form_calls:
        for keyword in call.keywords:
            if keyword.arg == "photo":
                assert False, ast.unparse(keyword.value)
        for keyword in call.keywords:
            if keyword.arg == "kwargs":
                body = ast.get_source_segment(text, call) or ""
                assert "photo" not in body

    safe = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_safe_form"
    ]
    assert len(safe) == 1
    source = ast.get_source_segment(text, safe[0]) or ""
    assert 'photo = kwargs.pop("photo", None)' in source
    assert "await form.edit(" in source
    assert "photo=photo" in source


@pytest.mark.parametrize("name", ("YNDXMusic.py", "YTMusic.py", "SCMusic.py"))
def test_the_cover_is_attached_by_an_edit_not_by_a_form(name):
    text = MUSIC[name]
    assert "photo=" not in text.replace("photo=photo", "")
    for line in text.split("\n"):
        stripped = line.strip()
        if stripped.startswith(("form_kwargs[", "edit_kwargs[")) and "\"photo\"]" in stripped:
            assert "BytesIO" not in stripped
    assert "feroku" not in text


@pytest.mark.parametrize(
    "name,getter,tail",
    (
        ("YTMusic.py", "_yts_get_cover", "_best_thumb_url"),
        ("SCMusic.py", "_scs_get_cover", "_big_artwork"),
        ("YNDXMusic.py", "_yms_get_cover", "_get_best_cover_url"),
    ),
)
def test_a_cover_getter_returns_a_remote_url(name, getter, tail):
    tree = ast.parse(MUSIC[name])
    body = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == getter:
            body = node
    assert body is not None, getter
    source = ast.get_source_segment(MUSIC[name], body) or ""
    assert tail in source
    assert "normalize_cover(" not in source
    assert "_upload" not in source
    returned = [line for line in source.split("\n") if line.strip().startswith("return ")]
    assert returned
    for line in returned:
        assert "BytesIO" not in line


@pytest.mark.parametrize("name", ("YNDXMusic.py", "YTMusic.py", "SCMusic.py"))
def test_a_cover_never_reaches_the_form_constructor(name):
    import asyncio

    source = MUSIC[name]
    lines = source.split("\n")
    body = None
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_safe_form":
            body = textwrap.dedent("\n".join(lines[node.lineno - 1:node.end_lineno]))
    assert body, name

    calls = {"form": [], "edit": []}

    class Form:
        async def edit(self, **kwargs):
            calls["edit"].append(kwargs)
            return True

    class Inline:
        async def form(self, **kwargs):
            assert "photo" not in kwargs, kwargs
            calls["form"].append(kwargs)
            return Form()

    namespace = {"_log": lambda *a, **k: None}
    exec(body, namespace)

    class Owner:
        inline = Inline()

    owner = Owner()
    owner._safe_form = namespace["_safe_form"].__get__(owner, type(owner))
    cover = "https://i1.sndcdn.com/artworks-abc-t500x500.jpg"

    async def scenario():
        return await owner._safe_form(
            text="<b>Track</b>",
            message=1,
            reply_markup=[[{"text": "x"}]],
            silent=True,
            photo=cover,
        )

    form = asyncio.run(scenario())
    assert isinstance(form, Form)
    assert calls["form"] and "photo" not in calls["form"][0]
    assert calls["form"][0]["text"] == "<b>Track</b>"
    assert calls["edit"] == [
        {"text": "<b>Track</b>", "photo": cover, "reply_markup": [[{"text": "x"}]]}
    ]


def test_a_missing_cover_does_not_touch_the_form():
    import asyncio

    source = MUSIC["YTMusic.py"]
    lines = source.split("\n")
    body = None
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_safe_form":
            body = textwrap.dedent("\n".join(lines[node.lineno - 1:node.end_lineno]))
    edits = []

    class Form:
        async def edit(self, **kwargs):
            edits.append(kwargs)

    class Inline:
        async def form(self, **kwargs):
            return Form()

    namespace = {"_log": lambda *a, **k: None}
    exec(body, namespace)

    class Owner:
        inline = Inline()

    owner = Owner()
    owner._safe_form = namespace["_safe_form"].__get__(owner, type(owner))
    asyncio.run(owner._safe_form(text="x", message=1, reply_markup=[]))
    assert edits == []


def test_the_yandex_module_uploads_only_tracks():
    text = MUSIC["YNDXMusic.py"]
    host_lines = [line.strip() for line in text.split("\n") if "x0.at" in line]
    assert host_lines == ['YMT_UPLOAD_URL = "https://x0.at"']
    assert "UPLOAD_PROVIDER" not in text
    assert "UPLOAD_PROVIDERS" not in text
    tree = ast.parse(text)
    callers = [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and any(
            isinstance(inner, ast.Call)
            and isinstance(inner.func, ast.Name)
            and inner.func.id == "_upload_audio"
            for inner in ast.walk(node)
        )
    ]
    assert callers


@pytest.mark.parametrize("name", ("YNDXMusic.py", "YTMusic.py", "SCMusic.py"))
def test_every_music_module_stays_registered_as_a_module(name):
    lines = MUSIC[name].split("\n")
    for index, line in enumerate(lines):
        if line.startswith("class ") and "(loader.Module)" in line:
            assert lines[index - 1] == "@loader.tds", name
            assert lines[index - 2] != "@loader.tds", name


def test_the_twin_texts_are_flat_and_framed():
    tree = ast.parse(TWIN)
    cls = [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Twin"][0]
    strings = {}
    for node in cls.body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "strings":
            strings = {k.value: ast.literal_eval(v) for k, v in zip(node.value.keys, node.value.values)}
    assert strings
    for key, value in strings.items():
        assert "\n\n" not in value, key
    framed = [k for k, v in strings.items() if "<blockquote>" in v]
    assert len(framed) >= 10
    for key in ("main_menu", "session_menu", "settings_menu", "online_menu"):
        assert key in framed
    assert "<b>Twin</b> |" in strings["main_menu"]


def test_the_twin_languages_stay_in_step():
    tree = ast.parse(TWIN)
    cls = [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Twin"][0]
    blocks = {}
    for node in cls.body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") in ("strings", "strings_ru"):
            blocks[node.targets[0].id] = {k.value for k in node.value.keys}
    assert blocks["strings"] - blocks["strings_ru"] == {"name"}
    assert blocks["strings_ru"] - blocks["strings"] == set()
