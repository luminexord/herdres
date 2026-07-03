"""Small Herdres connector helpers for Tendwire source mode.

These modules keep Tendwire/source-mode plumbing separate from the large
Telegram runtime in ``herdres.py``.  They deliberately avoid importing
``herdres.py``; runtime behavior is injected by the CLI wrapper.
"""

