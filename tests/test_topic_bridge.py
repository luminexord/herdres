import asyncio
import importlib.util
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


MODULE_PATH = Path(__file__).resolve().parents[1] / "herdr_topic_bridge.py"
SPEC = importlib.util.spec_from_file_location("herdr_topic_bridge", MODULE_PATH)
bridge = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(bridge)


def _msg(**kw):
    defaults = dict(
        text=None,
        caption=None,
        document=None,
        photo=None,
        chat=types.SimpleNamespace(id=-1001, is_forum=True),
        message_thread_id=77,
        message_id=9,
        from_user=types.SimpleNamespace(id=42, is_bot=False),
        reply_to_message=None,
        edit_date=None,
    )
    defaults.update(kw)
    return types.SimpleNamespace(**defaults)


class AttachmentPayloadTests(unittest.TestCase):
    def test_extracts_document(self):
        doc = types.SimpleNamespace(file_id="D1", file_name="r.pdf", mime_type="application/pdf", file_size=10)
        self.assertEqual(
            bridge._attachment_payload(_msg(document=doc)),
            {"kind": "document", "file_id": "D1", "file_name": "r.pdf", "mime_type": "application/pdf", "file_size": 10},
        )

    def test_picks_largest_photo(self):
        small = types.SimpleNamespace(file_id="S", file_size=100)
        large = types.SimpleNamespace(file_id="L", file_size=9000)
        att = bridge._attachment_payload(_msg(photo=[small, large]))
        self.assertEqual(att["kind"], "photo")
        self.assertEqual(att["file_id"], "L")

    def test_document_preferred_over_photo(self):
        doc = types.SimpleNamespace(file_id="D", file_name="", mime_type="", file_size=1)
        photo = [types.SimpleNamespace(file_id="P", file_size=5)]
        self.assertEqual(bridge._attachment_payload(_msg(document=doc, photo=photo))["kind"], "document")

    def test_none_when_no_attachment(self):
        self.assertIsNone(bridge._attachment_payload(_msg()))

    def test_document_without_file_id_is_none(self):
        doc = types.SimpleNamespace(file_id="", file_name="x", mime_type="", file_size=1)
        self.assertIsNone(bridge._attachment_payload(_msg(document=doc)))

    def test_malformed_object_returns_none(self):
        self.assertIsNone(bridge._attachment_payload(object()))


class BridgeRoutingTests(unittest.TestCase):
    def _state(self):
        return {
            "telegram": {"chat_id": "-1001", "general_thread_id": "1"},
            "panes": {"p": {"topic_id": "77", "pane_id": "pane-1"}},
        }

    def _shared_state(self):
        return {
            "telegram": {"chat_id": "-1001", "general_thread_id": "1", "owner_user_ids": ["42"]},
            "spaces": {
                "workspace:workspace-1": {
                    "space_key": "workspace:workspace-1",
                    "topic_id": "77",
                    "pane_keys": ["p1", "p2"],
                    "message_routes": {"1002": "p2"},
                }
            },
            "panes": {
                "p1": {
                    "pane_key": "p1",
                    "pane_id": "pane-1",
                    "topic_id": "77",
                    "pane_root_message_id": "1001",
                    "last_known_status": "working",
                },
                "p2": {
                    "pane_key": "p2",
                    "pane_id": "pane-2",
                    "topic_id": "77",
                    "pane_root_message_id": "1002",
                    "last_known_status": "working",
                },
            },
        }

    def _run(self, message, state):
        captured = {}

        async def fake_script(payload, mode="command"):
            captured["payload"] = payload
            return {"handled": True, "reply": ""}

        adapter = types.SimpleNamespace(_send_with_retry=Mock())
        with patch.object(bridge, "_load_state", Mock(return_value=state)), patch.object(
            bridge, "_run_command_script", fake_script
        ):
            handled = asyncio.run(bridge.maybe_handle_herdr_topic_message(adapter, message))
        return handled, captured.get("payload")

    def test_document_message_builds_payload(self):
        doc = types.SimpleNamespace(file_id="D1", file_name="r.pdf", mime_type="application/pdf", file_size=10)
        handled, payload = self._run(_msg(document=doc, caption="hi"), self._state())
        self.assertTrue(handled)
        self.assertEqual(payload["attachment"]["file_id"], "D1")
        self.assertEqual(payload["caption"], "hi")
        self.assertEqual(payload["text"], "")

    def test_photo_only_message_builds_payload(self):
        photo = [types.SimpleNamespace(file_id="L", file_size=9000)]
        handled, payload = self._run(_msg(photo=photo), self._state())
        self.assertTrue(handled)
        self.assertEqual(payload["attachment"]["kind"], "photo")

    def test_empty_message_dropped(self):
        handled, payload = self._run(_msg(), self._state())
        self.assertFalse(handled)
        self.assertIsNone(payload)

    def test_plain_text_unchanged(self):
        handled, payload = self._run(_msg(text="hello"), self._state())
        self.assertTrue(handled)
        self.assertEqual(payload["text"], "hello")
        self.assertIsNone(payload["attachment"])
        self.assertEqual(payload["caption"], "")

    def test_unmapped_topic_not_handled(self):
        handled, payload = self._run(_msg(text="hi", message_thread_id=999), self._state())
        self.assertFalse(handled)
        self.assertIsNone(payload)

    def test_missing_configured_chat_id_fails_closed(self):
        state = self._state()
        state["telegram"].pop("chat_id")
        msg = _msg(text="hi", chat=types.SimpleNamespace(id="", is_forum=True))
        handled, payload = self._run(msg, state)
        self.assertFalse(handled)
        self.assertIsNone(payload)

    def test_malformed_telegram_config_fails_closed(self):
        state = self._state()
        state["telegram"] = "not-a-dict"
        handled, payload = self._run(_msg(text="hi"), state)
        self.assertFalse(handled)
        self.assertIsNone(payload)

    def test_malformed_pane_entry_is_ignored(self):
        state = self._state()
        state["panes"] = {"bad": "not-a-dict"}
        handled, payload = self._run(_msg(text="hi"), state)
        self.assertFalse(handled)
        self.assertIsNone(payload)

    def test_foreign_chat_with_matching_legacy_topic_id_is_not_handled(self):
        handled, payload = self._run(
            _msg(text="hi", chat=types.SimpleNamespace(id=-999, is_forum=True), message_thread_id=77),
            self._state(),
        )
        self.assertFalse(handled)
        self.assertIsNone(payload)

    def test_owner_prefilter_drops_non_owner_without_spawning(self):
        state = self._state()
        state["telegram"]["owner_user_ids"] = ["42"]
        msg = _msg(text="hi", from_user=types.SimpleNamespace(id=99, is_bot=False))
        handled, payload = self._run(msg, state)
        self.assertTrue(handled)       # handled (dropped), not routed onward
        self.assertIsNone(payload)     # herdres subprocess not spawned

    def test_owner_prefilter_allows_owner(self):
        state = self._state()
        state["telegram"]["owner_user_ids"] = ["42"]
        handled, payload = self._run(_msg(text="hi"), state)
        self.assertTrue(handled)
        self.assertIsNotNone(payload)

    def test_from_bot_dropped_without_spawning(self):
        msg = _msg(text="hi", from_user=types.SimpleNamespace(id=42, is_bot=True))
        handled, payload = self._run(msg, self._state())
        self.assertTrue(handled)
        self.assertIsNone(payload)

    def test_caption_only_unsupported_media_not_handled(self):
        # caption present but no document/photo/text -> fall through to host bot
        handled, payload = self._run(_msg(caption="look"), self._state())
        self.assertFalse(handled)
        self.assertIsNone(payload)

    def test_shared_topic_reply_routes_to_pane_root(self):
        reply_to = types.SimpleNamespace(message_id=1002)
        handled, payload = self._run(_msg(text="/send hi", reply_to_message=reply_to), self._shared_state())

        self.assertTrue(handled)
        self.assertEqual(payload["pane_key"], "p2")
        self.assertEqual(payload["reply_to_message_id"], "1002")

    def test_shared_topic_top_level_message_is_ambiguous_and_not_spawned(self):
        sent = []

        async def fake_send(**kwargs):
            sent.append(kwargs)

        async def fake_script(payload, mode="command"):
            raise AssertionError("ambiguous top-level text must not spawn herdres")

        adapter = types.SimpleNamespace(_send_with_retry=fake_send)
        with patch.object(bridge, "_load_state", Mock(return_value=self._shared_state())), patch.object(
            bridge, "_run_command_script", fake_script
        ):
            handled = asyncio.run(bridge.maybe_handle_herdr_topic_message(adapter, _msg(text="/send hi")))

        self.assertTrue(handled)
        self.assertEqual(len(sent), 1)
        self.assertIn("Reply inside a pane thread", sent[0]["content"])


if __name__ == "__main__":
    unittest.main()
