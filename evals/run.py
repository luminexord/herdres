#!/usr/bin/env python3
"""Behavioral eval runner for the herdres operator skill (stdlib only).

Each scenario in ``evals/scenarios/*.json`` is a small behavioral contract:

    {
      "skills": ["herdres"],
      "query": "...what the user asks...",
      "expected_behavior": ["...", "..."]
    }

Two modes:

* **Print mode (default, no deps, no model key).** Prints the scenario and a
  grading rubric so a human (or a reviewing agent) can eval the transcript by
  hand. This is what CI exercises — it never calls a model.

* **Driver mode (``--driver "<cmd>"``).** Shells out to an agent CLI such as
  ``claude -p`` or ``codex exec``, handing it the herdres skill text plus the
  scenario ``query``, captures the transcript, and grades each
  ``expected_behavior`` line with a coarse substring/keyword heuristic. The
  heuristic is intentionally conservative: it reports PASS / FAIL / UNSURE and
  is an aid to human review, not a verdict. A model key is only needed by the
  driver you point at; ``run.py`` itself never imports an SDK.

Examples::

    python3 evals/run.py                       # print every scenario + rubric
    python3 evals/run.py guided-install        # print one scenario by name
    python3 evals/run.py --list                # list scenario names
    python3 evals/run.py --driver "claude -p"  # drive + grade all scenarios

The scenarios are the committed *source of truth* for how the skill should
behave — eval-driven development per Anthropic's agent-skill best practices.
See ``evals/README.md``.
"""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
from pathlib import Path

EVALS_DIR = Path(__file__).resolve().parent
SCENARIOS_DIR = EVALS_DIR / "scenarios"
REPO_ROOT = EVALS_DIR.parent
# Prefer the self-contained single-file entrypoint; fall back to the packaged copy.
SKILL_CANDIDATES = (
    REPO_ROOT / "SKILL.md",
    REPO_ROOT / "skills" / "herdres" / "SKILL.md",
)

# Heuristic grader verdicts.
PASS = "PASS"
FAIL = "FAIL"
UNSURE = "UNSURE"


class Scenario:
    """One behavioral eval scenario loaded from JSON."""

    def __init__(self, path: Path, data: dict) -> None:
        self.path = path
        self.name = str(data.get("name") or path.stem)
        self.skills: list[str] = list(data.get("skills") or [])
        self.query = str(data.get("query") or "")
        self.expected_behavior: list[str] = list(data.get("expected_behavior") or [])

    @classmethod
    def load(cls, path: Path) -> "Scenario":
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls(path, data)


def load_scenarios(name: str | None = None) -> list[Scenario]:
    """Load every scenario, or just the one whose name/stem matches ``name``."""
    paths = sorted(SCENARIOS_DIR.glob("*.json"))
    scenarios = [Scenario.load(p) for p in paths]
    if name is None:
        return scenarios
    matches = [s for s in scenarios if s.name == name or s.path.stem == name]
    if not matches:
        known = ", ".join(s.name for s in scenarios) or "(none)"
        raise SystemExit(f"no scenario named {name!r}; known scenarios: {known}")
    return matches


def skill_text() -> str:
    """Return the herdres skill markdown to hand a driver, or '' if absent."""
    for candidate in SKILL_CANDIDATES:
        if candidate.is_file():
            return candidate.read_text(encoding="utf-8")
    return ""


# --- keyword heuristic -------------------------------------------------------

# Short, lowercase signal words used as a coarse "did the transcript touch this
# idea" check. Stopwords are dropped; tokens shorter than 3 chars are ignored so
# we key on meaningful terms (commands, env vars, distinctive nouns).
_STOPWORDS = frozenset(
    """
    a an and any are as at be been before but by can cannot copy do does each
    for from has have how i if in into is it its just like may must never new
    not now of off on once one only or other reply runs set so than that the
    their them then they this to use user using via walk want was what when
    which while who with without you your
    """.split()
)


# Distinctive tokens we must not mangle: a leading-/-/embedded-punctuation token
# like ``-100`` (negative chat id), ``getUpdates``, or ``chat-id`` carries signal
# precisely *because* of its shape. Stripping it down to ``100`` / ``chatid``
# would let unrelated transcript text match. Keep such tokens verbatim.
_DISTINCTIVE = re.compile(r"^-?\d+$|[A-Za-z][A-Z]|[A-Za-z0-9]-[A-Za-z0-9]|\.[a-z]")

# Prohibition cues: if an expectation says the agent MUST NOT do something, the
# keyword heuristic cannot tell "did X" from "refused to do X" — they share the
# same content words. Such lines must never auto-PASS on overlap alone.
_PROHIBITION = re.compile(
    r"\b(?:must\s+not|never|do\s+not|don'?t|cannot|may\s+not)\b|\bnot\s+\w+",
    re.IGNORECASE,
)


def is_prohibition(expectation: str) -> bool:
    """True if the expectation is a negative/prohibition contract (MUST NOT ...).

    Keyword overlap is polarity-blind: a transcript that *does* the forbidden
    thing shares the same content words as one that *refuses* it. So a
    prohibition can never be auto-graded PASS by the heuristic.
    """
    return bool(_PROHIBITION.search(expectation))


def _keywords(line: str) -> list[str]:
    raw = re.findall(r"[A-Za-z0-9_./!-]+", line.lower())
    distinctive_raw = re.findall(r"[A-Za-z0-9_./!-]+", line)
    out: list[str] = []
    for tok, original in zip(raw, distinctive_raw):
        # Preserve distinctive tokens (``-100``, ``getUpdates``, ``chat-id``,
        # ``install-user.sh``) verbatim rather than stripping their punctuation;
        # their shape is the signal. Match case-insensitively downstream.
        if _DISTINCTIVE.search(original):
            if len(tok) >= 2 and tok not in _STOPWORDS:
                out.append(tok)
            continue
        tok = tok.strip("./-")
        if len(tok) < 3 or tok in _STOPWORDS:
            continue
        out.append(tok)
    return out


def grade_line(expectation: str, transcript: str) -> str:
    """Coarse PASS/FAIL/UNSURE for one expected_behavior line vs a transcript.

    This is a heuristic aid for human review, not an authoritative judge: it
    measures keyword overlap between the expectation and the transcript.

    Polarity guard: keyword overlap is *blind to negation*. A transcript that
    performs a forbidden action shares the same content words as one that
    refuses it, so for a PROHIBITION line (``MUST NOT`` / ``never`` /
    ``do not`` / ``don't`` / ``not <verb>``) this grader never auto-returns
    PASS — it returns UNSURE (needs human review) on overlap, only FAIL when
    the idea is absent. Callers treat UNSURE as *not passing* for exit codes.
    """
    haystack = transcript.lower()
    kws = _keywords(expectation)
    if not kws:
        return UNSURE
    hits = sum(1 for kw in kws if kw in haystack)
    ratio = hits / len(kws)
    if is_prohibition(expectation):
        # Cannot confirm a refusal from keyword overlap alone: never auto-PASS.
        return FAIL if ratio <= 0.2 else UNSURE
    if ratio >= 0.6:
        return PASS
    if ratio <= 0.2:
        return FAIL
    return UNSURE


# --- print mode --------------------------------------------------------------

def render_rubric(scenario: Scenario) -> str:
    lines = [
        f"=== scenario: {scenario.name} ===",
        f"file:   {scenario.path.relative_to(REPO_ROOT)}",
        f"skills: {', '.join(scenario.skills) or '(none)'}",
        "",
        "QUERY:",
        f"  {scenario.query}",
        "",
        "GRADING RUBRIC (each line should hold in the agent's response):",
    ]
    for i, expectation in enumerate(scenario.expected_behavior, 1):
        lines.append(f"  [{i}] {expectation}")
    lines.append("")
    return "\n".join(lines)


def cmd_print(scenarios: list[Scenario]) -> int:
    for scenario in scenarios:
        print(render_rubric(scenario))
    print(
        "Print mode: no model was called. Eval the agent's transcript against "
        "the rubric above by hand, or pass --driver to drive + auto-grade."
    )
    return 0


def cmd_list(scenarios: list[Scenario]) -> int:
    for scenario in scenarios:
        print(f"{scenario.name}\t({len(scenario.expected_behavior)} checks)\t{scenario.query}")
    return 0


# --- driver mode -------------------------------------------------------------

def build_prompt(scenario: Scenario, skill_md: str) -> str:
    parts = []
    if skill_md:
        parts.append(
            "You have access to the following Agent Skill. Follow it exactly, "
            "including its safety rules.\n"
        )
        parts.append("<skill>\n" + skill_md + "\n</skill>\n")
    parts.append("User request:\n" + scenario.query + "\n")
    return "\n".join(parts)


# Sentinel returncode used when the driver could not be run at all (timeout, …).
_DRIVER_ABORTED = -1


def run_driver(driver: str, prompt: str, timeout: float) -> tuple[str, str, int]:
    """Run the driver command, feeding the prompt on stdin.

    Returns ``(stdout, stderr, returncode)``. A non-zero ``returncode`` means
    the driver itself errored — its stdout must NOT be graded as a real
    transcript (a crashing driver can still emit keyword-rich noise).
    """
    argv = shlex.split(driver)
    try:
        proc = subprocess.run(
            argv,
            input=prompt,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except FileNotFoundError as exc:
        raise SystemExit(f"driver not found: {driver!r} ({exc})")
    except subprocess.TimeoutExpired:
        return "", f"driver timed out after {timeout:.0f}s", _DRIVER_ABORTED
    return proc.stdout, proc.stderr, proc.returncode


def cmd_driver(scenarios: list[Scenario], driver: str, timeout: float) -> int:
    skill_md = skill_text()
    if not skill_md:
        print(
            "warning: no SKILL.md found; driver will run without the skill text.",
            file=sys.stderr,
        )
    total_fail = 0
    total_unsure = 0
    total_error = 0
    for scenario in scenarios:
        print(f"=== driving scenario: {scenario.name} ===")
        prompt = build_prompt(scenario, skill_md)
        out, err, returncode = run_driver(driver, prompt, timeout)
        # A non-zero driver exit means the run errored: do NOT grade its stdout
        # as a real transcript (it may be keyword-rich noise that would falsely
        # PASS). Report it as an ERROR and force a non-zero overall exit.
        if returncode != 0:
            total_error += 1
            print(f"  [ERROR ] driver exited {returncode} — not grading its output")
            if err.strip():
                print(f"  stderr: {err.strip()}")
            print()
            continue
        transcript = out + ("\n" + err if err else "")
        if not transcript.strip():
            print("  (empty transcript)")
            if err:
                print(f"  stderr: {err.strip()}")
        for i, expectation in enumerate(scenario.expected_behavior, 1):
            verdict = grade_line(expectation, transcript)
            if verdict == FAIL:
                total_fail += 1
            elif verdict == UNSURE:
                total_unsure += 1
            print(f"  [{verdict:<6}] {i}. {expectation}")
        print()
    print(
        f"heuristic summary: {total_error} ERROR, {total_fail} FAIL, "
        f"{total_unsure} UNSURE (verdicts are a coarse keyword aid — confirm by "
        "reading the transcript; UNSURE does not count as passing)."
    )
    # Non-zero exit on any driver ERROR or heuristic FAIL so a CI/local driver
    # run can gate. (UNSURE is surfaced for human review but does not gate.)
    return 1 if (total_error or total_fail) else 0


# --- cli ---------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="evals/run.py",
        description="Run herdres behavioral evals (print rubric, or drive + grade).",
    )
    parser.add_argument(
        "scenario",
        nargs="?",
        help="scenario name to run (default: all). See --list.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="list scenario names and exit.",
    )
    parser.add_argument(
        "--driver",
        metavar="CMD",
        help="agent CLI to drive each scenario, e.g. 'claude -p' or 'codex exec'. "
        "The prompt (skill + query) is fed on stdin; the transcript is read from "
        "stdout. Without --driver, runs in print mode (no model call).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=300.0,
        help="per-scenario driver timeout in seconds (default: 300).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    scenarios = load_scenarios(args.scenario)
    if args.list:
        return cmd_list(scenarios)
    if args.driver:
        return cmd_driver(scenarios, args.driver, args.timeout)
    return cmd_print(scenarios)


if __name__ == "__main__":
    raise SystemExit(main())
