from __future__ import annotations

import unittest

import herdres


class PromptCollapseTests(unittest.TestCase):
    def test_should_collapse_semantics(self) -> None:
        long = "x" * 300
        short = "look into the emoji status"
        self.assertFalse(herdres._prompt_should_collapse(long, 0))   # 0 = never
        self.assertFalse(herdres._prompt_should_collapse(short, 0))
        self.assertTrue(herdres._prompt_should_collapse(short, 1))   # 1 = always
        self.assertTrue(herdres._prompt_should_collapse(long, 1))
        self.assertFalse(herdres._prompt_should_collapse(short, 200))  # below threshold
        self.assertTrue(herdres._prompt_should_collapse(long, 200))    # above threshold

    def test_render_collapsed_has_preview_and_no_open(self) -> None:
        text = "look into the emoji status and fix the broadcast across all the spaces please"
        html = herdres.render_user_prompt_quote_html(text, 1)  # always collapse
        self.assertIn("<details>", html)
        self.assertNotIn("<details open>", html)
        self.assertIn("<summary><b>User:</b> ", html)  # preview rides after the label
        self.assertIn("look into the emoji status", html)  # preview text present
        # body still present (one tap away)
        self.assertIn("<blockquote>", html)

    def test_render_expanded_when_below_threshold(self) -> None:
        text = "short prompt"
        html = herdres.render_user_prompt_quote_html(text, 200)
        self.assertIn("<details open>", html)
        self.assertEqual(html.count("<summary><b>User:</b></summary>"), 1)  # bare label, no preview

    def test_render_default_zero_never_collapses(self) -> None:
        text = "x" * 5000
        html = herdres.render_user_prompt_quote_html(text)  # default collapse_chars=0
        self.assertIn("<details open>", html)

    def test_turn_item_threads_collapse_chars(self) -> None:
        item = {"kind": "turn", "user_text": ("Please investigate the broadcast and the prompt rendering thoroughly. " * 6),
                "assistant_final_text": "the answer", "prompt_collapse_chars": 200}
        html = herdres.render_turn_item_html(item)
        # prompt collapsed (long), response expanded
        self.assertIn("<summary><b>User:</b> ", html)
        self.assertNotIn("<details open><summary><b>User:</b>", html)
        self.assertIn("<details open><summary><b>Response</b></summary>", html)

    def test_streaming_matches_final_collapse(self) -> None:
        long = "Please investigate the broadcast and the prompt rendering thoroughly. " * 6
        html = herdres.render_stream_turn_html(long, "working on it", collapse_chars=200)
        self.assertIn("<summary><b>User:</b> ", html)
        self.assertNotIn("<details open><summary><b>User:</b>", html)

    def test_space_prompt_collapse_chars_default_and_override(self) -> None:
        entry = {"space_key": "workspace:w1"}
        state = {"spaces": {"workspace:w1": {"space_key": "workspace:w1"}}}
        self.assertEqual(herdres.space_prompt_collapse_chars(state, entry),
                         herdres.PROMPT_COLLAPSE_CHARS_DEFAULT)
        state["spaces"]["workspace:w1"]["prompt_collapse_chars"] = 1
        self.assertEqual(herdres.space_prompt_collapse_chars(state, entry), 1)
        state["spaces"]["workspace:w1"]["prompt_collapse_chars"] = 0
        self.assertEqual(herdres.space_prompt_collapse_chars(state, entry), 0)


if __name__ == "__main__":
    unittest.main()
