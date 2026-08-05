"""Release-readiness guards for the source-only Herdres branch.

These lock the service model (herdres.service, no herdres.timer), keep the
install docs and shipped unit files in agreement with config.SOURCE_SERVICES,
and assert no private/pseudo pane identifiers leak into source-mode state.
"""

from __future__ import annotations

import ast
import json
import os
import re
import stat
import subprocess
import sys
import tomllib
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
_H8_SLOC_MARKER_RE = re.compile(
    r"\s*# H8-SLOC-(BEGIN|END) ([a-z][a-z0-9-]*)\s*"
)
_H8_SYMBOL_MANIFEST = {
    "herdres_connector/state.py": {
        "_bounded_text", "StateToken", "PhysicalOwner", "ProviderMutationGuard",
        "SecretStr", "IngressReceiver", "IngressPolicy", "RouteStatus",
        "StableOwner", "IngressRouteQuery", "IngressReplyQuery",
        "IngressRouteResult", "DecisionIngressQuery", "DecisionOption",
        "DecisionStatus", "DecisionIngressResult", "DecisionMutationKind",
        "DecisionMutation", "DecisionMutationStatus", "DecisionMutationResult",
        "_opaque_digest", "_state_token", "_load_schema2_locked",
        "_open_guard_node", "provider_mutation_guard", "_entry_route",
        "_route_result", "_route_matches", "_bound_entry",
        "_binding_evidence_matches", "read_ingress_policy",
        "read_ingress_receivers", "_resolve_ingress_route_locked",
        "resolve_ingress_route", "resolve_ingress_reply", "_decision_result",
        "read_decision_ingress", "_mutation_result", "apply_decision_ingress",
    },
    "herdres_connector/decisions.py": {
        "_legacy_provider_mutation_guard", "_decision_state_fingerprint",
        "_guarded_decision_target_is_current", "render_ingress_markup",
        "_project_ingress_record", "_ingress_mutation_digest",
        "_reduce_ingress_mutation",
    },
}


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


def test_rc_docs_use_explicit_paired_release_gate() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    release = (REPO_ROOT / "RELEASE.md").read_text(encoding="utf-8")
    launcher = (REPO_ROOT / "herdres.py").read_text(encoding="utf-8")
    assert 'VERSION = "0.7.0rc4-tendwired-source-only"' in launcher
    assert "Herdres `0.7.0rc4`" in readme
    assert "Tendwire `0.1.0rc5`" in readme
    assert "Python 3.13" in readme
    assert "complete paired" in release
    assert "paired socket probe" in release
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


def test_tendwire_transport_has_no_cli_fallback_or_database_watcher() -> None:
    client = (REPO_ROOT / "herdres_connector/tendwire_client.py").read_text(
        encoding="utf-8"
    )
    launcher = (REPO_ROOT / "herdres.py").read_text(encoding="utf-8")

    assert not (REPO_ROOT / "herdres_connector/outbound_dispatcher.py").exists()
    for forbidden in (
        "subprocess",
        "HERDRES_TENDWIRE_BIN",
        "TENDWIRE_BIN",
        "tendwire.cli",
    ):
        assert forbidden not in client
    for forbidden in (
        "OutboundDispatcher",
        "_DatabaseWakeWatcher",
        "inotify",
        "tendwire_db_path",
    ):
        assert forbidden not in launcher


def test_h8_has_one_ingress_runtime_and_no_legacy_ingress_surfaces() -> None:
    connector = REPO_ROOT / "herdres_connector"
    assert (connector / "ingress.py").is_file()
    assert (connector / "ingress_queue.py").is_file()
    assert not (connector / "ingress_lanes.py").exists()
    assert not (connector / "ingress_requests.py").exists()

    production = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (
            REPO_ROOT / "herdres.py",
            REPO_ROOT / "herdres_gateway.py",
            *connector.glob("*.py"),
        )
    )
    for forbidden in (
        "tendwire_ingress_command_requests",
        "run_herdres_command",
        "_private_retry_child_result",
        "_validated_child_response",
        "_checkpoint_for_command_result",
        "_preflight_ingress_request",
        "def handle_update(",
        "def command_reply(",
        "deliver_submission_working_card",
        "HERDRES_TENDWIRE_COMMAND_RESPONSE_SCHEMA_VERSION",
        "HERDRES_INBOUND_LANE_DEPTH",
        "HERDRES_INBOUND_LANE_BACKOFF_SECONDS",
        "HERDRES_INBOUND_HOLD_SECONDS",
        "HERDRES_INBOUND_LANE_STALL_SECONDS",
    ):
        assert forbidden not in production


def test_h8_gateway_is_only_the_installed_executable_wrapper() -> None:
    path = REPO_ROOT / "herdres_gateway.py"
    text = path.read_text(encoding="utf-8")
    tree = ast.parse(text)
    assert 70 <= len(text.splitlines()) <= 100
    assert path.stat().st_mode & stat.S_IXUSR
    assert text.count("IngressQueue.open_writer(") == 1
    assert text.count("ingress.run_gateway(") == 1
    assert "reveal_for_telegram_client()" in text
    functions = {
        node.name for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    assert functions == {
        "_log",
        "_install_stop_handlers",
        "_restore_stop_handlers",
        "_telegram_clients",
        "main",
        "_entrypoint",
    }
    for forbidden in (
        "subprocess",
        "sqlite3",
        "source_sync",
        "speech",
        "load_state",
        "save_state",
        "state_lock",
        "command_reply",
        "handle_update",
        "command_json",
    ):
        assert forbidden not in text


def test_h8_launchers_remain_executable() -> None:
    for relative in ("herdres.py", "herdres_gateway.py"):
        assert (REPO_ROOT / relative).stat().st_mode & stat.S_IXUSR, relative


def test_h8_shipped_configuration_has_no_removed_ingress_knobs() -> None:
    shipped = "\n".join(
        (REPO_ROOT / relative).read_text(encoding="utf-8")
        for relative in (".env.example", "INSTALL.md")
    )
    for obsolete in (
        "HERDRES_INBOUND_LANE_DEPTH",
        "HERDRES_INBOUND_LANE_BACKOFF_SECONDS",
        "HERDRES_INBOUND_HOLD_SECONDS",
        "HERDRES_INBOUND_LANE_STALL_SECONDS",
        "HERDRES_TENDWIRE_COMMAND_RESPONSE_SCHEMA_VERSION",
    ):
        assert obsolete not in shipped


def test_h8_ingress_isolated_from_presenter_and_generic_state() -> None:
    ingress_text = (REPO_ROOT / "herdres_connector/ingress.py").read_text(
        encoding="utf-8"
    )
    presenter = (REPO_ROOT / "herdres_connector/source_sync.py").read_text(
        encoding="utf-8"
    )
    launcher = (REPO_ROOT / "herdres.py").read_text(encoding="utf-8")
    for forbidden in (
        "source_sync",
        "speech",
        "subprocess",
        "sqlite3",
        "load_state",
        "save_state",
        "state_lock",
        "released_lock",
    ):
        assert forbidden not in ingress_text
    assert "command_json(" in ingress_text
    assert "provider_mutation_guard(" in ingress_text
    assert "IngressQueue" not in presenter
    assert "inbound_spool" not in presenter
    assert "from herdres_connector.source_sync import" in launcher
    assert "IngressQueue.open_writer" not in launcher


def test_h8_queue_and_provider_guard_security_primitives_are_pinned() -> None:
    queue_text = (REPO_ROOT / "herdres_connector/ingress_queue.py").read_text(
        encoding="utf-8"
    )
    state_text = (REPO_ROOT / "herdres_connector/state.py").read_text(
        encoding="utf-8"
    )
    decisions_text = (REPO_ROOT / "herdres_connector/decisions.py").read_text(
        encoding="utf-8"
    )
    for required in (
        "O_NOFOLLOW",
        "st_nlink",
        "st_uid",
        "fchmod",
        "PRAGMA synchronous=FULL",
        "PRAGMA journal_mode=WAL",
        "PRAGMA integrity_check",
        "PRAGMA trusted_schema=OFF",
        "PRAGMA query_only=ON",
    ):
        assert required in queue_text
    assert 'b"herdres-provider-owner-v1\\0"' in state_text
    assert '".provider-mutation-locks"' in state_text
    assert '"pg1."' in state_text
    assert "def provider_mutation_guard(" in state_text
    assert "_legacy_provider_mutation_guard(" in decisions_text

    revealers = []
    for path in (REPO_ROOT, REPO_ROOT / "herdres_connector"):
        for source in path.glob("*.py"):
            if "reveal_for_telegram_client()" in source.read_text(encoding="utf-8"):
                revealers.append(source.relative_to(REPO_ROOT).as_posix())
    assert revealers == ["herdres_gateway.py"]


def _canonical_line_numbers(path: Path) -> tuple[set[int], ast.Module]:
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    tree = ast.parse(text, filename=str(path))
    excluded: set[int] = set()
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if (
            isinstance(body, list)
            and body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            excluded.update(
                range(body[0].lineno, (body[0].end_lineno or body[0].lineno) + 1)
            )
    return {
        line_number
        for line_number, line in enumerate(lines, 1)
        if line_number not in excluded
        and bool(line.strip())
        and not line.lstrip().startswith("#")
    }, tree


def _canonical_sloc(path: Path) -> int:
    lines, _tree = _canonical_line_numbers(path)
    return len(lines)


def _assert_no_statement_packing(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    canonical, tree = _canonical_line_numbers(path)
    statement_lines: dict[int, list[str]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.stmt) and node.lineno in canonical:
            statement_lines.setdefault(node.lineno, []).append(type(node).__name__)
    packed = {
        line_number: kinds
        for line_number, kinds in statement_lines.items()
        if len(kinds) > 1
    }
    assert not packed, f"co-located AST statements in {path}: {packed}"
    if path.name == "ingress.py":
        long_lines = {
            line_number: len(line)
            for line_number, line in enumerate(text.splitlines(), 1)
            if len(line) > 120
        }
        assert not long_lines, f"overlong ingress lines: {long_lines}"


def _h8_marked_sloc(path: Path, symbols: set[str]) -> int:
    source_lines = path.read_text(encoding="utf-8").splitlines()
    canonical, tree = _canonical_line_numbers(path)
    marked: set[int] = set()
    names: set[str] = set()
    active: tuple[str, int] | None = None
    for line_number, line in enumerate(source_lines, 1):
        marker = _H8_SLOC_MARKER_RE.fullmatch(line)
        if "H8-SLOC-" in line:
            assert marker is not None, f"malformed H8 SLOC marker: {path}:{line_number}"
        if marker is None:
            continue
        direction, name = marker.groups()
        if direction == "BEGIN":
            assert active is None, f"nested H8 SLOC marker: {path}:{line_number}"
            assert name not in names, f"duplicate H8 SLOC marker: {path}:{line_number}"
            names.add(name)
            active = (name, line_number)
            continue
        assert active is not None, f"unmatched H8 SLOC end: {path}:{line_number}"
        assert active[0] == name, f"mismatched H8 SLOC marker: {path}:{line_number}"
        marked.update(range(active[1] + 1, line_number))
        active = None
    assert active is None, f"unterminated H8 SLOC marker: {path}:{active}"
    assert names, f"missing H8 SLOC markers: {path}"

    nodes = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
    }
    assert symbols <= nodes.keys(), f"missing H8 symbol manifest entries: {path}"
    for name in symbols:
        node = nodes[name]
        decorators = getattr(node, "decorator_list", ())
        first_line = min(
            [node.lineno, *(decorator.lineno for decorator in decorators)]
        )
        span = set(range(first_line, (node.end_lineno or node.lineno) + 1))
        assert span <= marked, f"H8 symbol is not completely marked: {path}:{name}"
    return len(canonical & marked)


def test_h8_canonical_sloc_budgets_are_pinned() -> None:
    whole_budgets = {
        "herdres_connector/ingress.py": (1_350, 1_500),
        "herdres_connector/ingress_queue.py": (1_050, 1_150),
        "herdres_gateway.py": (70, 100),
        "herdres_connector/ingress_identity.py": (80, 100),
    }
    marked_budgets = {
        "herdres.py": (15, 25),
        "herdres_connector/state.py": (500, 570),
        "herdres_connector/decisions.py": (180, 250),
        "herdres_connector/managed_bots.py": (20, 30),
    }
    measured = {
        relative: _canonical_sloc(REPO_ROOT / relative)
        for relative in whole_budgets
    }
    for relative in whole_budgets:
        _assert_no_statement_packing(REPO_ROOT / relative)
    measured.update(
        {
            relative: _h8_marked_sloc(
                REPO_ROOT / relative, _H8_SYMBOL_MANIFEST.get(relative, set())
            )
            for relative in marked_budgets
        }
    )
    for relative, (minimum, maximum) in {**whole_budgets, **marked_budgets}.items():
        assert minimum <= measured[relative] <= maximum, (
            relative, measured[relative], minimum, maximum
        )
    assert 3_265 <= sum(measured.values()) <= 3_725, measured


def test_h8_public_docs_describe_independent_state_without_deploy_claim() -> None:
    docs = "\n".join(
        (REPO_ROOT / name).read_text(encoding="utf-8")
        for name in ("README.md", "RELEASE.md", "SECURITY.md")
    )
    for required in (
        "Independent H8",
        "IngressQueue",
        "AF_UNIX",
        "pg1.<43>",
        "old presenter remains",
        "not evidence that a cutover",
    ):
        assert required in docs
    for stale in (
        "### Durable inbound lanes",
        "HERDRES_TENDWIRE_COMMAND_RESPONSE_SCHEMA_VERSION",
        "HERDRES_INBOUND_LANE_DEPTH",
        "HERDRES_INBOUND_LANE_BACKOFF_SECONDS",
        "HERDRES_INBOUND_HOLD_SECONDS",
        "HERDRES_INBOUND_LANE_STALL_SECONDS",
        "In legacy mode it creates",
        "submission receipt immediately renders",
    ):
        assert stale not in docs


def test_herdr_plugin_manifest_has_safe_non_delivery_actions() -> None:
    manifest = tomllib.loads(
        (REPO_ROOT / "herdr-plugin.toml").read_text(encoding="utf-8")
    )

    assert manifest["id"] == "luminexord.herdres"
    assert manifest["version"] == "0.7.0rc4"
    assert manifest["min_herdr_version"] == "0.7.0"
    assert manifest["platforms"] == ["linux"]
    actions = {item["id"]: item for item in manifest["actions"]}
    assert set(actions) == {"init-config", "doctor"}
    assert all(
        item["command"][:2] == ["python3", "scripts/herdr_plugin.py"]
        for item in actions.values()
    )
    serialized = json.dumps(manifest, sort_keys=True)
    assert "/home/" not in serialized
    assert "token" not in serialized.lower()


def test_herdr_plugin_init_config_is_private_and_non_overwriting(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    env = os.environ.copy()
    env["HERDR_PLUGIN_CONFIG_DIR"] = str(config_dir)
    env.pop("HERDRES_ENV_FILE", None)
    command = [sys.executable, str(REPO_ROOT / "scripts/herdr_plugin.py"), "init-config"]

    first = subprocess.run(command, env=env, capture_output=True, text=True, check=False)
    assert first.returncode == 0
    config_path = config_dir / "herdres.env"
    original = config_path.read_bytes()
    assert stat.S_IMODE(config_path.stat().st_mode) == 0o600

    config_path.write_bytes(original + b"\nHERDRES_TEST_SENTINEL=keep\n")
    second = subprocess.run(command, env=env, capture_output=True, text=True, check=False)
    assert second.returncode == 0
    assert config_path.read_bytes().endswith(b"HERDRES_TEST_SENTINEL=keep\n")
