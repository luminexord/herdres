from __future__ import annotations

import unittest
import unittest.mock

import herdres


def _pane(status, **extra):
    p = {"pane_id": extra.pop("pane_id", "p"), "agent_status": status, "_goal_active": False}
    p.update(extra)
    return p


class StatusSeverityTests(unittest.TestCase):
    def test_severity_ordering(self) -> None:
        order = ["error", "blocked", "workflow", "working", "goal", "idle", "done", "unknown", "closed"]
        sev = [herdres.status_severity(k) for k in order]
        self.assertEqual(sev, sorted(sev, reverse=True), "severity must strictly decrease in this order")
        self.assertEqual(herdres.status_severity("nonsense"), herdres.status_severity("unknown"))

    def test_done_with_active_goal_reads_as_goal(self) -> None:
        # A finished turn that still has a committed /goal reads as "on a goal" 🧠
        # (work continues in the background), not a plain done/✅.
        self.assertEqual(herdres.status_icon_key(_pane("done", _goal_active=True)), "goal")
        self.assertEqual(herdres.status_icon_key(_pane("done", _goal_active=False)), "done")
        # The reported X-topic scenario: one done+goal pane among idle/done panes ->
        # the aggregate surfaces the goal (🧠), not a coffee break (☕️).
        panes = [_pane("done", _goal_active=True), _pane("idle"), _pane("idle"), _pane("done")]
        self.assertEqual(herdres.topic_status_key(panes), "goal")

    def test_status_icon_key_alias_folding(self) -> None:
        self.assertEqual(herdres.status_icon_key(_pane("failed")), "error")
        self.assertEqual(herdres.status_icon_key(_pane("waiting")), "blocked")
        self.assertEqual(herdres.status_icon_key(_pane("busy")), "working")
        self.assertEqual(herdres.status_icon_key(_pane("succeeded")), "done")
        self.assertEqual(herdres.status_icon_key(_pane("exited")), "closed")
        self.assertEqual(herdres.status_icon_key(_pane("idle")), "idle")


class TopicAggregateTests(unittest.TestCase):
    def test_topic_status_key_picks_worst(self) -> None:
        # A topic with one blocked + several working/idle panes reads as blocked.
        panes = [_pane("idle"), _pane("working"), _pane("blocked"), _pane("done")]
        self.assertEqual(herdres.topic_status_key(panes), "blocked")
        # error beats everything.
        self.assertEqual(herdres.topic_status_key([_pane("working"), _pane("error")]), "error")
        # all idle/done -> the worst of those is idle.
        self.assertEqual(herdres.topic_status_key([_pane("done"), _pane("idle")]), "idle")
        # no open panes -> closed.
        self.assertEqual(herdres.topic_status_key([_pane("closed")]), "closed")
        self.assertEqual(herdres.topic_status_key([]), "closed")

    def test_aggregate_debounce_escalates_now_deescalates_slow(self) -> None:
        telegram = {"forum_topic_icons": {"by_emoji": {
            "⚡️": "icon-working", "☕️": "icon-idle", "❗️": "icon-blocked", "❓": "icon-unknown",
        }, "fetched_at": herdres.utc_now()}}
        space = {"topic_id": "77", "topic_name": "T"}
        with unittest.mock.patch.object(herdres, "STATUS_ICON_ENABLED", True), \
             unittest.mock.patch.object(herdres, "edit_topic_icon_async") as edit:
            # idle -> working is an escalation: applies immediately.
            r1 = herdres.update_topic_status_icon("-1", space, [_pane("idle")], telegram=telegram)
            self.assertEqual(r1["icon_key"], "idle")
            r2 = herdres.update_topic_status_icon("-1", space, [_pane("working")], telegram=telegram)
            self.assertEqual((r2["kind"], r2["icon_key"]), ("updated", "working"))
            # working -> idle is a de-escalation: deferred while the cooldown is fresh.
            r3 = herdres.update_topic_status_icon("-1", space, [_pane("idle")], telegram=telegram)
            self.assertEqual(r3["kind"], "retry_deferred")
            self.assertEqual(space["topic_status_icon_key"], "working")
            # but a NEW escalation (working -> blocked) is never deferred.
            r4 = herdres.update_topic_status_icon("-1", space, [_pane("blocked")], telegram=telegram)
            self.assertEqual((r4["kind"], r4["icon_key"]), ("updated", "blocked"))


class TopicRemapTests(unittest.TestCase):
    def test_clear_space_topic_mapping_reapplies_icon_on_remap(self) -> None:
        # Regression (council review finding 1): icon state lives on the space record, so
        # clearing the topic mapping must drop it — else a remap to a new topic id with the
        # same status dedup-skips and the new topic launches iconless.
        telegram = {"forum_topic_icons": {"by_emoji": {
            "❗️": "icon-blocked", "⚡️": "icon-working", "❓": "icon-unknown",
        }, "fetched_at": herdres.utc_now()}}
        space = {"topic_id": "77", "topic_name": "T", "pane_keys": []}
        with unittest.mock.patch.object(herdres, "STATUS_ICON_ENABLED", True), \
             unittest.mock.patch.object(herdres, "edit_topic_icon_async"):
            herdres.update_topic_status_icon("-1", space, [_pane("blocked")], telegram=telegram)
            self.assertEqual(space["topic_status_icon_custom_emoji_id"], "icon-blocked")
            herdres.clear_space_topic_mapping({"panes": {}}, space, "topic gone")
            self.assertNotIn("topic_status_icon_custom_emoji_id", space)
            # remapped to a NEW topic, same blocked status -> icon must re-apply.
            space["topic_id"] = "999"
            r = herdres.update_topic_status_icon("-1", space, [_pane("blocked")], telegram=telegram)
            self.assertEqual(r["kind"], "updated")
            self.assertEqual(space["topic_status_icon_custom_emoji_id"], "icon-blocked")


class RenderPinnedStatusTests(unittest.TestCase):
    def test_sorted_worst_first_with_unified_dots(self) -> None:
        panes = [
            _pane("idle", agent="codex"),
            _pane("working", agent="claude"),
            _pane("error", agent="kimi"),
            _pane("closed", agent="omp"),
        ]
        text = herdres.render_pinned_status({}, panes, label_fn=lambda p: str(p.get("agent")))
        # worst-first; closed filtered; unified dot vocab (error 🔴, working 🟡, idle 🟢).
        self.assertEqual(text, "kimi 🔴 | claude 🟡 | codex 🟢")

    def test_goal_dot_and_empty(self) -> None:
        self.assertEqual(herdres.render_pinned_status({}, []), "No active panes.")
        goal = _pane("idle", agent="g", _goal_active=True)
        self.assertEqual(herdres.render_pinned_status({}, [goal], label_fn=lambda p: "g"), "g 🧠")

    def test_dot_matches_severity_not_goal_for_working_pane(self) -> None:
        # Finding 3: a WORKING pane with an active goal must show 🟡 (working), not 🧠.
        wp = _pane("working", _goal_active=True)
        self.assertEqual(herdres.status_icon_key(wp), "working")
        self.assertEqual(herdres.pinned_status_dot(wp), "🟡")
        # an IDLE pane with an active goal is 🧠.
        ip = _pane("idle", _goal_active=True)
        self.assertEqual(herdres.pinned_status_dot(ip), "🧠")


if __name__ == "__main__":
    unittest.main()
