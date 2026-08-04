# Herdres reduction baseline

This document freezes the Wave 0 Herdres measurement at commit
`ff000d6c19ba8c24cf21aff301cbba883cc39f07` (`fix shared-topic Telegram
reply ownership`) on branch `wave-0/baseline-herdres`.

## Measurement

The canonical command is:

```console
python3 /home/smith/acp-reduction-goal/tools/sloc_count.py \
  /home/smith/tendwire/.worktrees/wave-0-baseline-herdres --verbose
```

The counter SHA-256 was
`4cea6441d1619b8ce81266318e71f8d80b5fe4f5a0f1cafda1d4143a435ee657`.
It counts non-blank, non-comment, non-docstring physical Python source lines
and excludes tests, docs, tooling, generated code, vendored code, build output,
and virtual environments. No counter changes were needed.

| Measure | Lines |
|---|---:|
| Physical production Python | 39,202 |
| Canonical production SLOC | 34,480 |

The planning baseline was 34,382. The authoritative source at `ff000d6` is
34,480: **98 SLOC higher (0.285%)**. This is the expected movement from the
same-day shared-topic reply-ownership fix, and is within the Wave 0 calibration
tolerance of 3%. Future wave deltas must use **34,480**, not 34,382, as their
Herdres starting point.

## Per-module inventory

Physical lines are from `wc -l`; canonical SLOC is from the counter above.
Both columns cover the same production module set.

| Module | Physical lines | Canonical SLOC |
|---|---:|---:|
| `herdr_turn_adapter.py` | 2,394 | 1,867 |
| `herdres.py` | 2,262 | 2,020 |
| `herdres_gateway.py` | 1,891 | 1,698 |
| `herdres_pending_hook.py` | 89 | 58 |
| `herdres_connector/__init__.py` | 1 | 0 |
| `herdres_connector/accounts.py` | 323 | 241 |
| `herdres_connector/config.py` | 620 | 416 |
| `herdres_connector/decisions.py` | 1,202 | 1,094 |
| `herdres_connector/doctor.py` | 276 | 242 |
| `herdres_connector/ingress_identity.py` | 141 | 103 |
| `herdres_connector/ingress_lanes.py` | 1,087 | 992 |
| `herdres_connector/ingress_requests.py` | 1,886 | 1,688 |
| `herdres_connector/managed_bots.py` | 145 | 111 |
| `herdres_connector/outbound_dispatcher.py` | 199 | 178 |
| `herdres_connector/rendering.py` | 928 | 742 |
| `herdres_connector/rich_delivery.py` | 1,691 | 1,444 |
| `herdres_connector/safe.py` | 263 | 210 |
| `herdres_connector/source_sync.py` | 15,600 | 14,301 |
| `herdres_connector/speech.py` | 730 | 568 |
| `herdres_connector/state.py` | 5,185 | 4,399 |
| `herdres_connector/telegram_delivery.py` | 1,009 | 922 |
| `herdres_connector/tendwire_client.py` | 1,280 | 1,186 |
| **Total** | **39,202** | **34,480** |

The four top-level production entry modules account for 6,636 physical lines
and 5,643 SLOC. The `herdres_connector` package accounts for 32,566 physical
lines and 28,837 SLOC.

## Green-suite proof

The unmodified full suite was run with the shared Tendwire virtual environment,
without installing anything:

```console
PYTHONPATH=. /home/smith/tendwire/.venv/bin/python -m pytest tests/ -x -q
```

Environment: Python 3.13.5 and pytest 9.1.1.

Result: **1,218 passed, 4 skipped, 6 warnings in 155.84 seconds
(0:02:35)**. Shell wall time was 157.468 seconds. All six warnings were the
same Python 3.13 `multiprocessing` fork deprecation warning emitted by the six
parameterizations of
`test_kill_9_restart_submits_each_request_exactly_once`; there were no test
failures or errors.
