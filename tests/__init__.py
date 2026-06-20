from __future__ import annotations

import sys

from . import conftest as _conftest

sys.modules.setdefault("conftest", _conftest)
