import os
import unittest
from unittest.mock import Mock, patch

import herdres
from conftest import make_pane as pane


class PinnedStatusTests(unittest.TestCase):
    def enabled_env(self, **extra):
        env = {"HERDR_TELEGRAM_TOPICS_PINNED_STATUS": "1"}
        env.update(extra)
        return patch.dict(os.environ, env, clear=False)

    def test_status_dot_mapping(self):
        self.assertEqual(herdres.pinned_status_dot(pane("Idle", "idle")), "🟢")
        self.assertEqual(herdres.pinned_status_dot(pane("Done", "done")), "🟢")
        self.assertEqual(herdres.pinned_status_dot(pane("Working", "working")), "🟡")
        self.assertEqual(herdres.pinned_status_dot(pane("Blocked", "blocked")), "🔴")
        self.assertEqual(herdres.pinned_status_dot(pane("Error", "error")), "🔴")

    def test_goal_dot_only_when_idle(self):
        # The 🧠 goal dot shows only for an IDLE pane pursuing a goal — a more urgent
        # status (blocked/working) must NOT be masked by the goal marker (council finding 3).
        self.assertEqual(herdres.pinned_status_dot(pane("Idle", "idle", _goal_active=True)), "🧠")
        self.assertEqual(herdres.pinned_status_dot(pane("Blocked", "blocked", _goal_active=True)), "🔴")
        self.assertEqual(herdres.pinned_status_dot(pane("Working", "working", _goal_active=True)), "🟡")

    def test_render_overview_text(self):
        panes = [
            pane("Codex", "idle"),
            pane("Claude", "working"),
            pane("herdres", "error"),
            pane("Closed", "closed"),
        ]
        state = {"panes": {}}
        self.assertEqual(
            herdres.render_pinned_status(state, panes, label_fn=lambda p: herdres.pinned_status_pane_label(state, p)),
            "herdres 🔴 | Claude 🟡 | Codex 🟢",
        )

    def test_second_sync_no_resend(self):
        calls = []

        def api(method, payload):
            calls.append((method, payload))
            if method == "sendMessage":
                return {"ok": True, "result": {"message_id": 12}}
            return {"ok": True, "result": True}

        state = {"telegram": {"pinned_status_topic_id": "99"}, "panes": {}}
        panes = [pane("Codex", "idle")]
        with self.enabled_env(), patch.object(herdres, "telegram_api", side_effect=api):
            herdres.sync_pinned_status_overview(state, "TOKEN", "-1001", panes)
            calls.clear()
            herdres.sync_pinned_status_overview(state, "TOKEN", "-1001", panes)

        self.assertEqual(calls, [])

    def test_changed_status_edits(self):
        calls = []

        def api(method, payload):
            calls.append((method, payload))
            return {"ok": True, "result": {"message_id": 12}}

        state = {
            "telegram": {
                "pinned_status_topic_id": "99",
                "pinned_status_msg_id": "12",
                "pinned_status_text": "Codex 🟢",
            },
            "panes": {},
        }
        with self.enabled_env(), patch.object(herdres, "telegram_api", side_effect=api):
            herdres.sync_pinned_status_overview(state, "TOKEN", "-1001", [pane("Codex", "working")])

        self.assertEqual([method for method, _payload in calls], ["editMessageText"])
        self.assertEqual(state["telegram"]["pinned_status_text"], "Codex 🟡")

    def test_missing_pin_rights_no_crash(self):
        def api(method, payload):
            if method == "sendMessage":
                return {"ok": True, "result": {"message_id": 12}}
            if method == "pinChatMessage":
                return {"ok": False, "error_code": 400, "description": "not enough rights"}
            return {"ok": True, "result": True}

        state = {"telegram": {"pinned_status_topic_id": "99"}, "panes": {}}
        with self.enabled_env(), patch.object(herdres, "telegram_api", side_effect=api):
            result = herdres.sync_pinned_status_overview(state, "TOKEN", "-1001", [pane("Codex", "idle")])

        self.assertTrue(result["ok"])
        self.assertEqual(state["telegram"]["pinned_status_msg_id"], "12")
        self.assertIn("pinned_status_pin_error", state["telegram"])

    def test_disabled_by_default_noop(self):
        api = Mock(return_value={"ok": True, "result": True})
        state = {"telegram": {}, "panes": {}}
        with patch.dict(os.environ, {"HERDR_TELEGRAM_TOPICS_PINNED_STATUS": "0"}, clear=False), patch.object(herdres, "telegram_api", api):
            result = herdres.sync_pinned_status_overview(state, "TOKEN", "-1001", [pane("Codex", "idle")])

        self.assertTrue(result["ok"])
        api.assert_not_called()

    def test_general_topic_never_targeted(self):
        api = Mock(return_value={"ok": True, "result": True})
        state = {"telegram": {}, "panes": {}}
        with self.enabled_env(HERDR_TELEGRAM_TOPICS_PINNED_STATUS_TOPIC_ID="0"), patch.object(herdres, "telegram_api", api):
            result = herdres.sync_pinned_status_overview(state, "TOKEN", "-1001", [pane("Codex", "idle")])

        self.assertTrue(result["ok"])
        api.assert_not_called()

    def test_general_thread_id_one_never_targeted(self):
        # "1" is the General topic in this repo; pinning there must be rejected.
        api = Mock(return_value={"ok": True, "result": True})
        state = {"telegram": {}, "panes": {}}
        with self.enabled_env(HERDR_TELEGRAM_TOPICS_PINNED_STATUS_TOPIC_ID="1"), patch.object(herdres, "telegram_api", api):
            result = herdres.sync_pinned_status_overview(state, "TOKEN", "-1001", [pane("Codex", "idle")])

        self.assertTrue(result["ok"])
        api.assert_not_called()

    def test_no_topic_configured_is_noop(self):
        # enabled but no env id and none stored -> skip (no auto-create).
        api = Mock(return_value={"ok": True, "result": True})
        state = {"telegram": {}, "panes": {}}
        with self.enabled_env(), patch.object(herdres, "telegram_api", api):
            result = herdres.sync_pinned_status_overview(state, "TOKEN", "-1001", [pane("Codex", "idle")])

        self.assertTrue(result["ok"])
        api.assert_not_called()

    def test_changed_env_topic_resends_even_with_same_text(self):
        # Operator moves the dashboard to a new topic; even if the rendered text is
        # identical, the message must be recreated in the new topic (not skipped).
        calls = []

        def api(method, payload):
            calls.append((method, payload))
            if method == "sendMessage":
                return {"ok": True, "result": {"message_id": 77}}
            return {"ok": True, "result": True}

        state = {
            "telegram": {
                "pinned_status_topic_id": "99",
                "pinned_status_msg_id": "12",
                "pinned_status_text": "Codex 🟢",
            },
            "panes": {},
        }
        with self.enabled_env(HERDR_TELEGRAM_TOPICS_PINNED_STATUS_TOPIC_ID="55"), patch.object(herdres, "telegram_api", side_effect=api):
            herdres.sync_pinned_status_overview(state, "TOKEN", "-1001", [pane("Codex", "idle")])

        methods = [m for m, _ in calls]
        self.assertIn("sendMessage", methods)
        self.assertIn("pinChatMessage", methods)
        self.assertEqual(state["telegram"]["pinned_status_topic_id"], "55")
        self.assertEqual(state["telegram"]["pinned_status_msg_id"], "77")
        send = next(p for m, p in calls if m == "sendMessage")
        self.assertEqual(str(send.get("message_thread_id")), "55")

    def test_unknown_status_dot(self):
        self.assertEqual(herdres.pinned_status_dot(pane("Mystery", "unknown")), "⬜")


if __name__ == "__main__":
    unittest.main()
