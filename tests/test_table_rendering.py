"""Markdown pipe-tables render as NATIVE Telegram tables (<table bordered striped> → PageBlockTable via
the rich-message path), in both the turn-card engine (_render_final_reply_blocks) and the secondary
markdownish_to_html path. Ported from the pre-tendwire renderer, which the RC rewrite had dropped."""
from __future__ import annotations

from herdres_connector.rendering import html_to_plain, markdownish_to_html, try_render_table
from herdres_connector.rich_delivery import render_final_reply_html, render_turn_item_html

_TABLE = """| Feature | Status | Notes |
|---------|:------:|------:|
| STT | done | #126 |
| Tables | native | this PR |"""


def test_markdownish_renders_native_table():
    html = markdownish_to_html("Intro:\n\n" + _TABLE + "\n\nOutro.")
    assert "<table bordered striped>" in html and "</table>" in html
    assert "<th>Feature</th>" in html and "<th>Status</th>" in html
    assert "<td>STT</td>" in html and "<td>Tables</td>" in html
    assert "| Feature |" not in html and "|---------|" not in html   # no raw markup leaks
    assert "<pre>" not in html                                        # native table, not a monospace box
    assert "Intro:" in html and "Outro." in html


def test_native_table_in_live_turn_card():
    html = render_turn_item_html({"assistant_final_text": _TABLE, "user_text": "q"})
    assert "<table bordered striped>" in html and "<th>" in html and "<td>" in html
    assert "| Feature |" not in html


def test_pipe_inside_code_span_does_not_split_a_cell():
    # Code-span-aware cell parsing (the win over the <pre> approach): a pipe inside `code` stays.
    html = markdownish_to_html("| cmd | desc |\n|---|---|\n| `a | b` | pipe in code |")
    assert "<code>a | b</code>" in html            # one cell, not two
    assert "<td>pipe in code</td>" in html
    body_row = [r for r in html.splitlines() if "pipe in code" in r][0]
    assert body_row.count("<td>") == 2             # exactly 2 columns in the body row


def test_escaped_pipe_stays_in_cell():
    html = markdownish_to_html("| a | b |\n|---|---|\n| x \\| y | z |")
    assert "<td>x | y</td>" in html                # escaped \| is literal, not a column break


def test_rich_cells_render_bold():
    html = markdownish_to_html("| k | v |\n|---|---|\n| name | **bold** |")
    assert "<b>bold</b>" in html


def test_plain_fallback_is_table_aware():
    # If the rich path is unavailable, html_to_plain degrades a <table> to `|`-separated rows.
    plain = html_to_plain(markdownish_to_html(_TABLE))
    assert "Feature | Status | Notes" in plain
    assert "STT | done | #126" in plain
    assert "<td>" not in plain and "<table" not in plain


def test_pipe_line_without_delimiter_stays_inline():
    html = markdownish_to_html("a | b | c")
    assert "<table" not in html
    assert "a | b | c" in html


def test_table_inside_code_fence_stays_literal():
    html = markdownish_to_html("```\n| a | b |\n|---|---|\n```")
    assert "| a | b |" in html and "<table" not in html


def test_ragged_rows_padded():
    md = "| A | B | C |\n|---|---|---|\n| 1 | 2 |\n| x | y | z |"
    html = markdownish_to_html(md)
    short = [r for r in html.splitlines() if "<td>1</td>" in r][0]
    assert short.count("<td>") == 3               # short row padded to 3 cells


def test_html_in_cells_is_escaped():
    html = markdownish_to_html("| tag |\n|-----|\n| <script> & y |")
    assert "&lt;script&gt;" in html and "&amp;" in html
    assert "<script>" not in html


# --- review-hardening regressions (carried from the <pre> version) ----------------------------

def test_prose_pipe_above_bare_rule_is_not_a_table():
    for engine in (markdownish_to_html, render_final_reply_html):
        out = engine("Run `grep foo | wc -l` to count\n---\nNext paragraph.")
        assert "<table" not in out
        assert "Next paragraph" in out


def test_delimiter_without_pipe_is_rejected():
    from herdres_connector.rendering import _looks_like_table_separator
    assert _looks_like_table_separator("---") is False
    assert _looks_like_table_separator("|---|---|") is True
    assert _looks_like_table_separator("---|---") is True


def test_repeated_interior_separator_absorbed():
    md = "| A | B |\n|---|---|\n| 1 | 2 |\n|---|---|\n| 3 | 4 |"
    for engine in (markdownish_to_html, render_final_reply_html):
        out = engine(md)
        assert out.count("<table bordered striped>") == 1
        assert "<td>1</td>" in out and "<td>3</td>" in out       # both sections in one table
        assert "|---|" not in out


def test_all_empty_table_left_as_text():
    for md in ("| |\n|-|", "||||\n|-|"):
        assert "<table" not in markdownish_to_html(md)


def test_try_render_table_returns_none_off_table():
    assert try_render_table(["just a line"], 0) is None
    assert try_render_table(["| h |", "not a delimiter"], 0) is None
