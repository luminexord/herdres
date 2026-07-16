"""Release-readiness guards for the source-only Herdres branch.

These lock the service model (herdres.service, no herdres.timer), keep the
install docs and shipped unit files in agreement with config.SOURCE_SERVICES,
and assert no private/pseudo pane identifiers leak into source-mode state.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from herdres_connector import config, doctor, state
from herdres_connector.source_sync import SyncRuntime, sync_once

from test_source_only import FakeTelegram, FakeTendwire, _store


REPO_ROOT = Path(__file__).resolve().parent.parent
UNIT_DIR = REPO_ROOT / "systemd" / "user"

# Units owned and installed by this repo (tendwired.service ships with Tendwire).
HERDRES_OWNED_SERVICES = ("herdres.service", "herdres-gateway.service")

_PSEUDO_PANE_ID_RE = re.compile(r"\bw[0-9a-f]+:(?:p|t)[0-9a-f]+\b", re.IGNORECASE)
_FORBIDDEN_STATE_KEYS = {"pane_id", "terminal_id", "send_keys", "backend_target", "raw_target"}


def test_source_services_are_the_installed_units_without_timer():
    assert config.SOURCE_SERVICES == ("tendwired.service", "herdres-gateway.service", "herdres.service")
    assert "herdres.timer" not in config.SOURCE_SERVICES
    # Every herdres-owned unit named in SOURCE_SERVICES ships as a file.
    shipped = {path.name for path in UNIT_DIR.glob("*.service")}
    for unit in HERDRES_OWNED_SERVICES:
        assert unit in config.SOURCE_SERVICES
        assert unit in shipped


def test_no_timer_unit_is_shipped():
    assert list(UNIT_DIR.glob("*.timer")) == []


def test_install_docs_agree_with_shipped_units():
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    install = (REPO_ROOT / "INSTALL.md").read_text(encoding="utf-8")
    for doc in (readme, install):
        # The documented enable line must start the real services and never the
        # retired timer.
        assert "enable --now herdres.service herdres-gateway.service" in doc
        assert "enable --now herdres.timer" not in doc


def test_rc_docs_use_explicit_low_minute_paired_gate() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    release = (REPO_ROOT / "RELEASE.md").read_text(encoding="utf-8")
    launcher = (REPO_ROOT / "herdres.py").read_text(encoding="utf-8")
    assert 'VERSION = "0.7.0rc4-tendwired-source-only"' in launcher
    assert "Herdres `0.7.0rc4`" in readme
    assert "Tendwire `0.1.0rc5`" in readme
    assert "Python 3.13" in readme
    assert "Herdres deliberately has no duplicate automatic workflow" in release
    assert "HERDRES_PAIRED_TENDWIRE_SOURCE_DIR=/absolute/tendwire/src" in release
    assert "Never restart Herdr" in release


def test_doctor_checks_exactly_the_source_services(monkeypatch):
    checked: list[str] = []

    def fake_active(unit: str):
        checked.append(unit)
        return {"unit": unit, "active": True, "status": "active", "returncode": 0}

    monkeypatch.setattr(doctor, "_systemctl_is_active", fake_active)
    result = doctor.source_services()

    assert result["ok"] is True
    assert tuple(checked) == config.SOURCE_SERVICES
    assert set(result["services"]) == set(config.SOURCE_SERVICES)


def _iter_strings(value):
    if isinstance(value, dict):
        for key, item in value.items():
            yield ("key", str(key))
            yield from _iter_strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _iter_strings(item)
    elif isinstance(value, str):
        yield ("value", value)


def test_no_pseudo_pane_ids_or_private_keys_in_source_state(monkeypatch):
    monkeypatch.setenv("HERDRES_TENDWIRE_MODE", "source")
    store = _store()
    telegram = FakeTelegram()
    tendwire = FakeTendwire(
        turns={
            "turns": [
                {
                    "id": "turn-1",
                    "worker_id": "worker-1",
                    "space_id": "space-1",
                    "user_text": "please help",
                    "assistant_final_text": "done",
                    "complete": True,
                }
            ]
        },
        workers=[
            {"id": "worker-1", "name": "codex", "status": "idle", "space_id": "space-1", "fingerprint": "fp-1"}
        ],
        spaces=[{"id": "space-1", "name": "Project", "status": "active", "fingerprint": "space-fp"}],
    )

    sync_once(store, SyncRuntime(tendwire, telegram, with_outbox=False))

    for kind, text in _iter_strings(store):
        if kind == "key":
            assert text not in _FORBIDDEN_STATE_KEYS, f"private key leaked into state: {text}"
        else:
            assert not _PSEUDO_PANE_ID_RE.search(text), f"pseudo pane id leaked into state: {text!r}"


def test_runtime_imports_no_herdr_backend_client_and_reports_zero_direct_calls():
    """Source mode: Herdres talks to Tendwire only. It must not import a Herdr
    backend client or invoke a bare `herdr` binary, which is what keeps
    direct_herdr_calls at 0."""
    runtime_files = [
        REPO_ROOT / "herdres.py",
        REPO_ROOT / "herdres_gateway.py",
        *(REPO_ROOT / "herdres_connector").glob("*.py"),
    ]
    text = "\n".join(path.read_text(encoding="utf-8") for path in runtime_files)
    for needle in (
        "herdr_socket",
        "herdr_cli",
        "herdr_events",
        "HerdrSocketClient",
        "from tendwire.backends",
        "import tendwire.backends",
    ):
        assert needle not in text, f"herdres runtime must not reference {needle}"
    # No subprocess invocation of a bare `herdr` binary (herdres/tendwire/systemctl are fine).
    assert not re.search(r"""[\[(]\s*["']herdr["']""", text), "herdres runtime must not spawn a bare herdr binary"
