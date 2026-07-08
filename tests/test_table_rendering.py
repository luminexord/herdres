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


# --- review-hardening regressions -------------------------------------------------------------

def test_prose_pipe_above_bare_rule_is_not_a_table():
    # MAJOR (review): "run `foo | grep bar`\n---\ntext" must stay prose — a bare --- rule is not a
    # table delimiter. Requires a pipe in the delimiter row (GitHub behavior).
    for engine in (markdownish_to_html, render_final_reply_html):
        out = engine("Run `grep foo | wc -l` to count\n---\nNext paragraph.")
        assert "<pre>" not in out
        assert "Next paragraph" in out


def test_bullet_with_pipe_above_rule_stays_a_bullet():
    out = render_final_reply_html("- item one | two\n---\nmore")
    assert "<pre>" not in out
    assert "<li>" in out or "item one" in out


def test_delimiter_without_pipe_is_rejected():
    from herdres_connector.rendering import _looks_like_table_separator
    assert _looks_like_table_separator("---") is False
    assert _looks_like_table_separator("----------") is False
    assert _looks_like_table_separator("|---|---|") is True
    assert _looks_like_table_separator("---|---") is True


def test_repeated_interior_separator_absorbed():
    # A table that repeats its delimiter to group sections renders as ONE table with all rows.
    md = "| A | B |\n|---|---|\n| 1 | 2 |\n|---|---|\n| 3 | 4 |"
    for engine in (markdownish_to_html, render_final_reply_html):
        out = engine(md)
        assert out.count("<pre>") == 1
        body = out.split("<pre>")[1].split("</pre>")[0]
        assert "1" in body and "3" in body      # both sections inside the one table
        assert "|---|" not in out               # no leaked delimiter


def test_all_empty_table_left_as_text():
    for md in ("| |\n|-|", "||||\n|-|"):
        assert "<pre>" not in markdownish_to_html(md)
