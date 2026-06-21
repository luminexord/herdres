from __future__ import annotations

import re
import unittest

import herdres


def _count(html: str, tag: str) -> int:
    """open minus close count for a tag; 0 means balanced."""
    return len(re.findall(rf"<{tag}\b", html, re.IGNORECASE)) - len(
        re.findall(rf"</{tag}>", html, re.IGNORECASE)
    )


class RichDetailsSplitTests(unittest.TestCase):
    """Regression tests for the oversize <details> turn-block split bug.

    Previously `_hard_split_rich_block` re-wrapped each piece in ONLY the outer
    <details> tag, severing the nested <ol>/<table>/<blockquote> wrapper and
    dropping <summary> from continuation chunks — producing empty bubbles.
    """

    # Void / self-closing tags have no close counterpart, so exclude them from
    # the balance check.
    _BALANCE_TAGS = ("details", "summary", "ol", "ul", "li", "table",
                     "tr", "td", "th", "blockquote", "pre", "b", "p")

    def _balanced(self, s: str) -> bool:
        for tag in self._BALANCE_TAGS:
            if _count(s, tag) != 0:
                return False
        return True

    def test_oversize_details_with_ol_list_balanced_and_nonempty(self) -> None:
        items = "".join(
            f"<li>Step {i}: do something meaningful with a reasonably long line of filler text here to grow the body past the limit.</li>"
            for i in range(80)
        )
        html = (
            "<details open><summary><b>Response</b></summary>"
            f"<ol>{items}</ol>"
            "</details>"
        )
        self.assertGreater(len(html), 6000)
        chunks = herdres.split_rich_html(html, 6000)
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            self.assertTrue(self._balanced(c), f"unbalanced chunk: {c[:120]!r}")
            self.assertEqual(_count(c, "ol"), 0)
            self.assertEqual(_count(c, "li"), 0)
            self.assertTrue(herdres.html_to_plain(c).strip(), f"empty chunk: {c[:120]!r}")
        # <summary> rides the first chunk only.
        self.assertIn("<summary>", chunks[0])
        for c in chunks[1:]:
            self.assertNotIn("<summary>", c)
        # No chunk has an <li> outside an <ol>: every chunk with <li> must wrap
        # it in at least one <ol>...</ol>.
        for c in chunks:
            if re.search(r"<li\b", c, re.IGNORECASE):
                self.assertGreaterEqual(
                    len(re.findall(r"<ol\b", c, re.IGNORECASE)), 1,
                    f"<li> outside <ol> in {c[:120]!r}",
                )

    def test_oversize_details_with_table_balanced(self) -> None:
        rows = "".join(
            f"<tr><td>row {i}</td><td>value {i} with filler text to grow the size past the rich limit</td></tr>"
            for i in range(120)
        )
        html = (
            "<details open><summary>Report</summary>"
            f"<table>{rows}</table>"
            "</details>"
        )
        self.assertGreater(len(html), 6000)
        chunks = herdres.split_rich_html(html, 6000)
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            self.assertTrue(self._balanced(c), f"unbalanced chunk: {c[:120]!r}")
            self.assertEqual(_count(c, "table"), 0)
            self.assertEqual(_count(c, "tr"), 0)
            self.assertTrue(herdres.html_to_plain(c).strip(), f"empty chunk: {c[:120]!r}")
        self.assertIn("<summary>", chunks[0])
        for c in chunks[1:]:
            self.assertNotIn("<summary>", c)
            # every <tr> is wrapped in a <table>
            if re.search(r"<tr\b", c, re.IGNORECASE):
                self.assertGreaterEqual(
                    len(re.findall(r"<table\b", c, re.IGNORECASE)), 1,
                    f"<tr> outside <table> in {c[:120]!r}",
                )

    def test_oversize_details_with_blockquote_balanced(self) -> None:
        lines = "".join(
            f"Worklog entry {i}: did a thing with enough prose to make the line long enough.<br>"
            for i in range(160)
        )
        html = (
            "<details open><summary>Worklog</summary>"
            f"<blockquote>{lines}</blockquote>"
            "</details>"
        )
        self.assertGreater(len(html), 6000)
        chunks = herdres.split_rich_html(html, 6000)
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            self.assertTrue(self._balanced(c), f"unbalanced chunk: {c[:120]!r}")
            self.assertEqual(_count(c, "blockquote"), 0)
            self.assertTrue(herdres.html_to_plain(c).strip(), f"empty chunk: {c[:120]!r}")
        self.assertIn("<summary>", chunks[0])
        for c in chunks[1:]:
            self.assertNotIn("<summary>", c)

    def test_details_without_recognizable_wrapper_still_splits(self) -> None:
        # Plain paragraphs separated by newlines inside <details> — no
        # <ol>/<table>/<blockquote>/<pre> wrapper. The legacy fallback path must
        # still split on the newline boundary without crashing.
        paras = "\n".join(
            f"<p>Paragraph number {i} with some filler text to grow the body past the limit.</p>"
            for i in range(160)
        )
        html = f"<details open><summary>Notes</summary>{paras}</details>"
        self.assertGreater(len(html), 6000)
        chunks = herdres.split_rich_html(html, 6000)
        self.assertGreater(len(chunks), 1)
        # No crash; every chunk is details-balanced and non-empty.
        for c in chunks:
            self.assertEqual(_count(c, "details"), 0)
            self.assertTrue(herdres.html_to_plain(c).strip(), f"empty chunk: {c[:120]!r}")
        self.assertIn("<summary>", chunks[0])


    def test_nested_ol_inside_li_not_severed(self) -> None:
        # GAP 1 (council fix-forward): a nested <ol> inside an <li> must NOT be
        # split at the sub-list's </li> boundaries. The parent item + its whole
        # nested sub-list must stay intact in one tag-balanced chunk.
        subitems = "".join(f"<li>Subitem {i}: {'x' * 90}</li>" for i in range(15))
        siblings = "".join(
            f"<li>Outer item {i}: filler text long enough to grow the list past the limit boundary.</li>"
            for i in range(60)
        )
        html = (
            "<details open><summary><b>Response</b></summary><ol>"
            f"<li>Milestone plan:<ol>{subitems}</ol></li>"
            f"{siblings}"
            "</ol></details>"
        )
        self.assertGreater(len(html), 6000)
        chunks = herdres.split_rich_html(html, 6000)
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            self.assertTrue(self._balanced(c), f"unbalanced chunk: {c[:160]!r}")
            self.assertEqual(_count(c, "ol"), 0)
            self.assertEqual(_count(c, "li"), 0)
            self.assertTrue(herdres.html_to_plain(c).strip(), f"empty chunk: {c[:160]!r}")
        # The parent item and its entire nested sub-list land together in ONE chunk,
        # intact (all 15 subitems present, balanced).
        host = [c for c in chunks if "Milestone plan:" in c]
        self.assertEqual(len(host), 1, "parent item split across chunks")
        for i in range(15):
            self.assertIn(f"Subitem {i}:", host[0])
        # No subitem leaked into any other chunk.
        for c in chunks:
            if c is not host[0]:
                self.assertNotIn("Subitem ", c)
        # Total content preserved: every outer item survives exactly once.
        joined = "".join(chunks)
        for i in range(60):
            self.assertEqual(joined.count(f"Outer item {i}:"), 1)

    def test_single_oversize_li_is_hard_broken(self) -> None:
        # GAP 2 (council fix-forward): one flat <li> larger than the budget must be
        # hard-broken so NO emitted chunk exceeds the limit, with content preserved.
        big = " ".join(f"word{i}" for i in range(1500))  # ~> 9000 chars, no nesting
        html = (
            "<details open><summary>Response</summary>"
            f"<ol><li>{big}</li><li>small tail item</li></ol>"
            "</details>"
        )
        limit = 6000
        self.assertGreater(len(html), limit)
        chunks = herdres.split_rich_html(html, limit)
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            self.assertLessEqual(len(c), limit, f"chunk over limit: len={len(c)}")
            self.assertTrue(self._balanced(c), f"unbalanced chunk: {c[:120]!r}")
            self.assertEqual(_count(c, "ol"), 0)
            self.assertEqual(_count(c, "li"), 0)
        # All words preserved across the hard-broken chunks.
        joined = herdres.html_to_plain("".join(chunks))
        for i in (0, 1, 750, 1499):
            self.assertIn(f"word{i}", joined)
        self.assertIn("small tail item", joined)


    def test_oversize_li_no_internal_whitespace_is_char_sliced(self) -> None:
        # Codex re-review Finding 2: a single token with NO internal whitespace,
        # larger than the budget, must still be sliced so no chunk exceeds the limit.
        blob = "x" * 9000
        html = (
            "<details open><summary>Response</summary>"
            f"<ol><li>{blob}</li><li>tail</li></ol>"
            "</details>"
        )
        limit = 6000
        chunks = herdres.split_rich_html(html, limit)
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            self.assertLessEqual(len(c), limit, f"chunk over limit: len={len(c)}")
            self.assertTrue(self._balanced(c), f"unbalanced chunk: {c[:100]!r}")
        self.assertEqual(herdres.html_to_plain("".join(chunks)).count("x"), 9000)
        self.assertIn("tail", herdres.html_to_plain("".join(chunks)))

    def test_oversize_li_with_inline_tags_not_severed(self) -> None:
        # Codex re-review Finding 3: breaking an oversize <li> must NOT split inside
        # an inline element (<b>/<code>/<a>); every chunk stays tag-balanced.
        words = " ".join(
            (f"<code>tok{i}_{'y' * 12}</code>" if i % 3 == 0 else f"word{i}") for i in range(900)
        )
        html = (
            "<details open><summary>Response</summary>"
            f"<ol><li>Plan: {words}</li></ol>"
            "</details>"
        )
        limit = 6000
        chunks = herdres.split_rich_html(html, limit)
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            self.assertLessEqual(len(c), limit, f"chunk over limit: len={len(c)}")
            self.assertTrue(self._balanced(c), f"unbalanced chunk: {c[:100]!r}")
            self.assertEqual(_count(c, "code"), 0, f"severed <code> in {c[:100]!r}")
        joined = herdres.html_to_plain("".join(chunks))
        for i in range(900):
            self.assertIn(f"tok{i}_" if i % 3 == 0 else f"word{i}", joined)

    def test_oversize_single_tr_cells_not_severed(self) -> None:
        # Codex re-review Finding 3: an oversize single <tr> must break by whole
        # cells into multiple balanced <tr>, never mid-<td>.
        cells = "".join(f"<td>cell {i}: {'z' * 60}</td>" for i in range(100))
        html = (
            "<details open><summary>Report</summary>"
            f"<table><tr>{cells}</tr></table>"
            "</details>"
        )
        limit = 6000
        chunks = herdres.split_rich_html(html, limit)
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            self.assertLessEqual(len(c), limit, f"chunk over limit: len={len(c)}")
            self.assertTrue(self._balanced(c), f"unbalanced chunk: {c[:100]!r}")
            self.assertEqual(_count(c, "td"), 0, f"severed <td> in {c[:100]!r}")
            self.assertEqual(_count(c, "tr"), 0)
        joined = herdres.html_to_plain("".join(chunks))
        for i in range(100):
            self.assertIn(f"cell {i}:", joined)


    def test_hard_break_oversize_token_after_accumulated_content(self) -> None:
        # Codex final re-review: an oversize whitespace-free token must be sliced
        # even when content was already buffered before it (cur non-empty).
        unit = "<li>intro blurb " + ("x" * 9000) + "</li>"
        frags = herdres._hard_break_unit(unit, 6000)
        self.assertGreater(len(frags), 1)
        for f in frags:
            self.assertLessEqual(len(f), 6000, f"frag over budget: {len(f)}")
            self.assertEqual(_count(f, "li"), 0, f"unbalanced: {f[:80]!r}")
        joined = "".join(frags)
        self.assertEqual(joined.count("x"), 9000)
        self.assertIn("intro blurb", joined)

    def test_hard_split_no_outer_wrapper_char_slices(self) -> None:
        # Codex final re-review (new MED): the no-outer-wrapper fallback must
        # char-slice a long whitespace-free token, not emit one over-limit chunk.
        chunks = herdres._hard_split_rich_block("x" * 9000, 6000)
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            self.assertLessEqual(len(c), 6000, f"chunk over limit: {len(c)}")
        self.assertEqual("".join(chunks).count("x"), 9000)


if __name__ == "__main__":
    unittest.main()
