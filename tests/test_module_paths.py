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
def test_covers_travel_as_local_bytes(name):
    text = MUSIC[name]
    assert "def cover_photo(data: bytes):" in text
    assert 'buf.name = "cover.jpg"' in text
    for line in text.split("\n"):
        stripped = line.strip()
        if stripped.startswith('edit_kwargs["photo"]') or stripped.startswith('form_kwargs["photo"]'):
            assert stripped.endswith("= photo"), stripped


@pytest.mark.parametrize("name", ("YNDXMusic.py", "YTMusic.py", "SCMusic.py"))
def test_no_cover_is_fetched_from_a_file_host(name):
    text = MUSIC[name]
    tree = ast.parse(text)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and "cover" in node.name:
            chunk = ast.get_source_segment(text, node) or ""
            assert "_upload" not in chunk, node.name


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
