import ast
import asyncio
import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Optional
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent
XRAY = ROOT / "XRay.py"
WEB = ROOT / "Storage/XRay/WEB"


def load_method():
    source = XRAY.read_text(encoding="utf-8")
    tree = ast.parse(source)
    cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "XRay")
    fn = next(node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == "_get_active_connections")
    module = ast.Module(body=[fn], type_ignores=[])
    ast.fix_missing_locations(module)
    ns = {"subprocess": __import__("subprocess"), "ipaddress": __import__("ipaddress"), "Optional": Optional}
    exec(compile(module, str(XRAY), "exec"), ns)
    return ns["_get_active_connections"]


def test_assets_and_theme_references():
    assert (WEB / "Evil_Cat.jsx").is_file()
    assert (WEB / "Loading.html").is_file()
    assert (ROOT / "Storage/XRay/Animations/Evil_Cat.json").is_file()
    assert (ROOT / "Storage/XRay/Media/Evil_Cat_theme.mp3").stat().st_size > 1_000_000
    assert not (ROOT / "Storage/Media/Halloween_theme.mp3").exists()
    assert not (ROOT / "Storage/XRay/Animations/Pumpkin.json").exists()
    assert "Evil_Cat.json" in (WEB / "Evil_Cat.jsx").read_text()
    assert "Evil_Cat_theme.mp3" in (WEB / "Evil_Cat.jsx").read_text()
    assert "loader__mark" not in (WEB / "Loading.html").read_text()
    assert "Loading.html" in (WEB / "websocket_site.py").read_text()
    json.loads((ROOT / "Storage/XRay/Animations/Evil_Cat.json").read_text())


def test_status_labels_are_single_word_and_cat_is_default():
    source = XRAY.read_text(encoding="utf-8")
    assert '"user_started": "<b>Started</b>"' in source
    assert '"user_stopped": "<b>Stoped</b>"' in source
    assert '"Evil Cat": "https://raw.githubusercontent.com/i-execute/Modules/main/Storage/XRay/WEB/Evil_Cat.jsx' in source
    assert '"mask_site": "Evil Cat"' in source


def test_websocket_count_uses_mask_site_public_peers():
    method = load_method()
    stdout = "ESTAB 0 0 127.0.0.1:48000 198.51.100.20:54001\nESTAB 0 0 127.0.0.1:48000 203.0.113.7:54002\nESTAB 0 0 127.0.0.1:48000 127.0.0.1:31000\nESTAB 0 0 127.0.0.1:48000 198.51.100.20:54001\n"
    with patch("subprocess.run", return_value=SimpleNamespace(returncode=0, stdout=stdout)) as run:
        assert method(SimpleNamespace(), 443, "websocket", 48000) == 2
    assert "sport = :48000" in run.call_args.args[0]


def test_websocket_site_template_has_valid_python():
    spec = importlib.util.spec_from_file_location("websocket_site", WEB / "websocket_site.py")
    # Template placeholders intentionally prevent import, but must compile after expansion.
    text = (WEB / "websocket_site.py").read_text()
    compile(text.replace("__PATH__", "/ws").replace("__BACKEND_PORT__", "18080").replace("__SITE_PORT__", "18081").replace("__MASK_URL__", "https://example.invalid/gate.jsx"), "websocket_site.py", "exec")
