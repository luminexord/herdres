"""Tests for the Tendwire daemon Unix-socket client (herdres_connector.daemon_socket) and the
herdres.tendwire_snapshot/turns fast-path + CLI fallback."""
from __future__ import annotations

import json
import os
import socket
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import herdres
from herdres_connector import daemon_socket


class _OneShotServer:
    """A temp Unix-socket server that accepts one connection, reads a newline-framed request, and
    replies with a fixed newline-framed response. Mirrors tendwire's UnixSocketJSONServer framing."""

    def __init__(self, response_bytes: bytes):
        self._response = response_bytes
        self._dir = tempfile.mkdtemp()
        self.path = os.path.join(self._dir, "tendwire.sock")
        self.request: dict | None = None
        self._srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._srv.bind(self.path)
        self._srv.listen(1)
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self):
        try:
            conn, _ = self._srv.accept()
            with conn:
                buf = bytearray()
                while b"\n" not in buf:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    buf.extend(chunk)
                try:
                    self.request = json.loads(bytes(buf).split(b"\n", 1)[0])
                except ValueError:
                    self.request = None
                conn.sendall(self._response)
        except OSError:
            pass

    def close(self):
        try:
            self._srv.close()
        except OSError:
            pass


class DaemonSocketClientTests(unittest.TestCase):
    def test_request_roundtrip_unwraps_result(self):
        srv = _OneShotServer(json.dumps({"ok": True, "result": {"workers": [{"id": "w1"}]}}).encode() + b"\n")
        self.addCleanup(srv.close)
        out = daemon_socket.snapshot(srv.path, timeout=3)
        self.assertEqual(out, {"workers": [{"id": "w1"}]})
        # the request used the right method + framing
        self.assertEqual(srv.request.get("method"), "snapshot.get")
        self.assertEqual(sorted(srv.request.keys()), ["id", "method", "params"])

    def test_turns_uses_turn_list_method(self):
        srv = _OneShotServer(json.dumps({"ok": True, "result": {"turns": []}}).encode() + b"\n")
        self.addCleanup(srv.close)
        out = daemon_socket.turns(srv.path, timeout=3)
        self.assertEqual(out, {"turns": []})
        self.assertEqual(srv.request.get("method"), "turn.list")

    def test_error_response_raises(self):
        srv = _OneShotServer(json.dumps({"ok": False, "error": {"code": "boom"}}).encode() + b"\n")
        self.addCleanup(srv.close)
        with self.assertRaises(daemon_socket.DaemonUnavailable):
            daemon_socket.snapshot(srv.path, timeout=3)

    def test_malformed_response_raises(self):
        srv = _OneShotServer(b"not json\n")
        self.addCleanup(srv.close)
        with self.assertRaises(daemon_socket.DaemonUnavailable):
            daemon_socket.snapshot(srv.path, timeout=3)

    def test_missing_socket_raises(self):
        with self.assertRaises(daemon_socket.DaemonUnavailable):
            daemon_socket.snapshot("/tmp/definitely-not-a-real.sock", timeout=1)

    def test_empty_path_raises(self):
        with self.assertRaises(daemon_socket.DaemonUnavailable):
            daemon_socket.request("", "snapshot.get", timeout=1)

    def test_result_non_dict_is_empty(self):
        srv = _OneShotServer(json.dumps({"ok": True, "result": None}).encode() + b"\n")
        self.addCleanup(srv.close)
        self.assertEqual(daemon_socket.snapshot(srv.path, timeout=3), {})


class TendwireSnapshotFastPathTests(unittest.TestCase):
    def test_socket_path_override_and_derivation(self):
        with patch.dict(os.environ, {"HERDRES_TENDWIRE_SOCKET": "/x/y.sock"}, clear=False):
            self.assertEqual(herdres.tendwire_daemon_socket_path(), "/x/y.sock")
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("HERDRES_TENDWIRE_SOCKET", None)
            with patch.object(herdres.herdres_tendwire, "db_path", return_value="/a/b/tendwire.db"):
                self.assertEqual(herdres.tendwire_daemon_socket_path(), "/a/b/tendwire.sock")

    def test_snapshot_prefers_socket(self):
        with patch.object(herdres, "tendwire_daemon_socket_path", return_value="/s.sock"), \
                patch.object(herdres.tendwire_daemon_socket, "snapshot", return_value={"workers": [1]}) as sock, \
                patch.object(herdres, "_tendwire_client") as cli:
            out = herdres.tendwire_snapshot()
        self.assertEqual(out, {"workers": [1]})
        sock.assert_called_once()
        cli.assert_not_called()  # never touched the CLI

    def test_snapshot_falls_back_to_cli_when_socket_down(self):
        client = Mock()
        client.snapshot.return_value = {"workers": [2]}
        with patch.object(herdres, "tendwire_daemon_socket_path", return_value="/s.sock"), \
                patch.object(herdres.tendwire_daemon_socket, "snapshot",
                             side_effect=daemon_socket.DaemonUnavailable("down")), \
                patch.object(herdres, "_tendwire_client", return_value=client):
            out = herdres.tendwire_snapshot()
        self.assertEqual(out, {"workers": [2]})
        client.snapshot.assert_called_once()  # fell back

    def test_snapshot_no_socket_configured_uses_cli(self):
        client = Mock()
        client.snapshot.return_value = {"workers": [3]}
        with patch.object(herdres, "tendwire_daemon_socket_path", return_value=""), \
                patch.object(herdres.tendwire_daemon_socket, "snapshot") as sock, \
                patch.object(herdres, "_tendwire_client", return_value=client):
            out = herdres.tendwire_snapshot()
        self.assertEqual(out, {"workers": [3]})
        sock.assert_not_called()
        client.snapshot.assert_called_once()

    def test_snapshot_fails_open_on_non_daemon_error(self):
        # A non-DaemonUnavailable error on the socket path must still fall back to the CLI, never
        # escape as a backend degrade.
        client = Mock()
        client.snapshot.return_value = {"workers": [9]}
        with patch.object(herdres, "tendwire_daemon_socket_path", return_value="/s.sock"), \
                patch.object(herdres.tendwire_daemon_socket, "snapshot", side_effect=ValueError("bug")), \
                patch.object(herdres, "_tendwire_client", return_value=client):
            out = herdres.tendwire_snapshot()
        self.assertEqual(out, {"workers": [9]})
        client.snapshot.assert_called_once()

    def test_turns_socket_loader_is_cached_once_per_sync(self):
        # tendwire_turns is called once per pane; the socket loader must run at most once per sync.
        herdres.herdres_tendwire.clear_turns_payload_cache()
        self.addCleanup(herdres.herdres_tendwire.clear_turns_payload_cache)
        sock_turns = Mock(return_value={"turns": ["t1"]})
        with patch.object(herdres, "tendwire_daemon_socket_path", return_value="/s.sock"), \
                patch.object(herdres.tendwire_daemon_socket, "turns", sock_turns):
            first = herdres.tendwire_turns()
            second = herdres.tendwire_turns()   # per-pane call #2 -> cache hit
        self.assertEqual(first, {"turns": ["t1"]})
        self.assertEqual(second, {"turns": ["t1"]})
        sock_turns.assert_called_once()          # NOT once-per-pane

    def test_turns_loader_falls_back_to_cli(self):
        herdres.herdres_tendwire.clear_turns_payload_cache()
        self.addCleanup(herdres.herdres_tendwire.clear_turns_payload_cache)
        with patch.object(herdres, "tendwire_daemon_socket_path", return_value="/s.sock"), \
                patch.object(herdres.tendwire_daemon_socket, "turns",
                             side_effect=daemon_socket.DaemonUnavailable("down")), \
                patch.object(herdres.herdres_tendwire, "turns_payload", return_value={"turns": ["cli"]}) as cli:
            out = herdres.tendwire_turns()
        self.assertEqual(out, {"turns": ["cli"]})
        cli.assert_called_once()

    def test_connector_maps_action_to_method(self):
        srv = _OneShotServer(json.dumps({"ok": True, "result": {"items": []}}).encode() + b"\n")
        self.addCleanup(srv.close)
        out = daemon_socket.connector(srv.path, "Poll", {"name": "attention"}, timeout=3)
        self.assertEqual(out, {"items": []})
        self.assertEqual(srv.request.get("method"), "connector.poll")  # normalized + prefixed
        self.assertEqual(srv.request.get("params"), {"name": "attention"})

    def test_connector_call_prefers_socket_and_defaults_name(self):
        sock = Mock(return_value={"items": [], "status": "ok"})
        with patch.object(herdres, "tendwire_daemon_socket_path", return_value="/s.sock"), \
                patch.object(herdres.tendwire_daemon_socket, "connector", sock), \
                patch.object(herdres.herdres_tendwire, "connector_name", return_value="attention"), \
                patch.object(herdres, "_tendwire_client") as cli:
            out = herdres.tendwire_connector_call("poll", {"limit": 2})
        self.assertEqual(out["status"], "ok")
        # name defaulted into params; CLI never touched
        self.assertEqual(sock.call_args.args[1], "poll")
        self.assertEqual(sock.call_args.args[2].get("name"), "attention")
        cli.assert_not_called()

    def test_connector_call_falls_back_to_cli(self):
        client = Mock()
        client.connector_call.return_value = {"items": ["cli"], "status": "ok"}
        with patch.object(herdres, "tendwire_daemon_socket_path", return_value="/s.sock"), \
                patch.object(herdres.tendwire_daemon_socket, "connector",
                             side_effect=daemon_socket.DaemonUnavailable("down")), \
                patch.object(herdres.herdres_tendwire, "connector_name", return_value="attention"), \
                patch.object(herdres, "_tendwire_client", return_value=client):
            out = herdres.tendwire_connector_call("poll", {"limit": 2})
        self.assertEqual(out, {"items": ["cli"], "status": "ok"})
        client.connector_call.assert_called_once()


if __name__ == "__main__":
    unittest.main()
