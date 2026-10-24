"""Run with: python3 -m unittest discover tests"""

import fcntl
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import main


class ScrapeLockTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.lock_path = Path(tmp.name) / "scrape.lock"
        patcher = mock.patch.object(main, "SCRAPE_LOCK_PATH", self.lock_path)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_scrape_runs_when_no_other_scrape_is_running(self):
        with mock.patch.object(main, "_scrape") as scrape:
            main.cmd_scrape()
            main.cmd_scrape()  # the lock is released after each run
        self.assertEqual(scrape.call_count, 2)

    def test_second_scrape_exits_while_one_is_running(self):
        with open(self.lock_path, "w") as running:
            fcntl.flock(running, fcntl.LOCK_EX)
            with mock.patch.object(main, "_scrape") as scrape, \
                 mock.patch.object(main, "log") as log:
                main.cmd_scrape()
        scrape.assert_not_called()
        log.assert_called_once_with("Another scrape is still running, skipping this one")


if __name__ == "__main__":
    unittest.main()
