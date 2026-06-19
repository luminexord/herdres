from __future__ import annotations

import plistlib
import unittest
from pathlib import Path


class LaunchdConfigTests(unittest.TestCase):
    def test_sync_interval_is_short_enough_for_streaming_turns(self) -> None:
        plist_path = Path(__file__).resolve().parents[1] / "launchd/com.gaijinjoe.herdres.plist"

        with plist_path.open("rb") as plist_file:
            config = plistlib.load(plist_file)

        self.assertEqual(config["StartInterval"], 5)


if __name__ == "__main__":
    unittest.main()
