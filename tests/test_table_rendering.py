"""Markdown pipe-tables render as aligned monospace <pre> blocks (Telegram has no <table>), in both
the turn-card engine (_render_final_reply_blocks) and the secondary markdownish_to_html path."""
from __future__ import annotations

from herdres_connector.rendering import markdownish_to_html, try_render_table
from herdres_connector.rich_delivery import render_final_reply_html, render_turn_item_html

_TABLE = """| Feature | Status | Notes |
|---------|:------:|------:|
| STT | done | #126 |
| Tables | new | this PR |"""


def test_markdownish_renders_table_as_pre():
    html = markdownish_to_html("Intro:\n\n" + _TABLE + "\n\nOutro.")
    assert "<pre>" in html and "</pre>" in html
    assert "| Feature |" not in html          # raw header row gone
    assert "|---------|" not in html          # raw delimiter gone
    assert "Feature" in html and "Tables" in html and "#126" in html
    assert "Intro:" in html and "Outro." in html


def test_alignment_markers_respected():
    # Status is centered (:--:), Notes right (--:); check padding on a short cell.
    html = markdownish_to_html(_TABLE)
    pre = html[html.index("<pre>") + 5 : html.index("</pre>")]
    header = pre.splitlines()[0]
    assert " Status " in header or "Status" in header      # centered → surrounded by spaces
    assert header.index("Notes") > header.index("Status")   # column order preserved


def test_table_renders_in_live_turn_card():
    html = render_turn_item_html({"assistant_final_text": _TABLE, "user_text": "q"})
    assert "<pre>" in html
    assert "| Feature |" not in html                        # no raw markup leaks into the Response
    assert "STT" in html and "Tables" in html


def test_pipe_line_without_delimiter_stays_inline():
    # A lone `a | b | c` (no delimiter row) is NOT a table.
    html = markdownish_to_html("a | b | c")
    assert "<pre>" not in html
    assert "a | b | c" in html


def test_table_inside_code_fence_stays_literal():
    html = markdownish_to_html("```\n| a | b |\n|---|---|\n```")
    # the fenced block is rendered verbatim, not re-parsed as a table
    assert "| a | b |" in html


def test_ragged_rows_are_padded_not_crashing():
    md = "| A | B | C |\n|---|---|---|\n| 1 | 2 |\n| x | y | z | extra |"
    html = markdownish_to_html(md)
    assert "<pre>" in html and "extra" not in html.split("<pre>")[1].split("</pre>")[0].splitlines()[0]


def test_html_in_cells_is_escaped():
    md = "| tag |\n|-----|\n| <b>x</b> & y |"
    html = markdownish_to_html(md)
    assert "&lt;b&gt;" in html and "&amp;" in html
    assert "<b>x</b>" not in html.split("<pre>")[1]


def test_try_render_table_returns_none_off_table():
    assert try_render_table(["just a line"], 0) is None
    assert try_render_table(["| h |", "not a delimiter"], 0) is None
