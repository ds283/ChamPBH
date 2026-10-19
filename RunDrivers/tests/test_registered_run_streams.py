# (c) University of Sussex 2026
# Created by David Seery
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The heartbeat and the stage, read off the run's own output (rrk-adoption prompt 03, W3 and W6
test 2).

The subjects are `StageTracker` and `RegisteredStream` in `RunDrivers/registered_run.py`, on a
real `Run` begun in a temporary runs root and a `StringWriter` standing in for the real stream.
Time is a fake clock, `time.monotonic` patched. The lines are RayWorkPool's, made with its own
format strings (`raypoolkit/RayWorkPool.py:206`, `:588`, `:592`, `:606`), and a worker's line as
Ray forwards it, with its `(... pid=...)` prefix. No Ray is initialised, no store is opened, and
nothing under `var/` is read or written.
"""

import sys
import tempfile
import threading
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

import ray
import runregistrykit

from RunDrivers import registered_run
from RunDrivers.registered_run import RegisteredStream, StageTracker
from RunDrivers.tests.stubbed_run import REPO_ROOT, StringWriter

TITLE = "CALCULATE ADIABATIC TRANSGRESSION PARAMETERS"
NOW = datetime(2026, 10, 19, 12, 0, 0)

BANNER = f"\n** {TITLE}"
PROGRESS = (
    f"   -- {NOW:%Y-%m-%d %H:%M:%S%z} (5m 1s running): 3/5 work items remaining = "
    f"{40.0:.2f}% complete"
)
INFLIGHT = (
    "      inflight: 0 lookup, 3 compute, 0 store, 0 validation | completed: 2 lookup"
)
COMPLETE = "   -- ALL WORK ITEMS COMPLETE in time 10m 3s"
WORKER = "\x1b[36m(Datastore pid=89917)\x1b[0m @@ a line a worker printed"


class Clock:
    """A fake `time.monotonic`."""

    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now


class FailingCopy:
    """A copy whose `write` raises, as a full disk would."""

    def write(self, _text):
        raise OSError("no space left on device")

    def flush(self):
        raise OSError("no space left on device")

    def close(self):
        pass


class FailingRun:
    """A run whose heartbeat cannot be written."""

    def heartbeat(self, **_kwargs):
        raise OSError("no space left on device")


class StreamCase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="champbh-registered-run-")
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name).resolve()
        self.run = runregistrykit.begin(
            campaign="rrk-adoption",
            prompt="03",
            slug="streams",
            purpose="a unit test of the stream proxies, in a temporary directory",
            argv=["unittest"],
            root=str(self.root),
            client_root=str(REPO_ROOT),
        )
        self.real = StringWriter()
        self.clock = Clock()
        patcher = mock.patch.object(registered_run.time, "monotonic", self.clock)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.beats = mock.patch.object(
            self.run, "heartbeat", wraps=self.run.heartbeat
        ).start()
        self.addCleanup(mock.patch.stopall)

    def tearDown(self):
        self.assertFalse(ray.is_initialized())

    def stream(self, beats=True, copy_to="stdout"):
        path = self.run.stdout_path if copy_to == "stdout" else self.run.stderr_path
        stream = RegisteredStream(
            self.real, self.run, StageTracker(), copy_to=path, beats=beats
        )
        self.addCleanup(stream.close_copy)
        return stream

    def line(self, stream, text, at=None):
        if at is not None:
            self.clock.now = at
        print(text, file=stream)

    def stage(self):
        return self.run.status().get("stage")


class TestTheBeat(StreamCase):
    def test_lines_closer_than_thirty_seconds_beat_once(self):
        stream = self.stream()
        self.line(stream, WORKER, at=1000.0)
        self.line(stream, WORKER, at=1010.0)
        self.line(stream, WORKER, at=1029.9)
        self.assertEqual(self.beats.call_count, 1)
        self.line(stream, WORKER, at=1030.0)
        self.assertEqual(self.beats.call_count, 2)
        # a worker's line moves no stage
        self.assertIsNone(self.stage())

    def test_a_banner_changes_the_stage_at_once(self):
        stream = self.stream()
        self.line(stream, WORKER, at=1000.0)
        self.assertEqual(self.beats.call_count, 1)
        self.line(stream, BANNER, at=1001.0)
        self.assertEqual(self.beats.call_count, 2)
        self.assertEqual(self.stage(), TITLE)

    def test_the_progress_and_completion_stages(self):
        stream = self.stream()
        self.line(stream, BANNER, at=1000.0)
        self.line(stream, PROGRESS, at=1001.0)
        self.assertEqual(self.stage(), f"{TITLE}, 40.00% (3 of 5 work items remaining)")
        self.line(stream, INFLIGHT, at=1002.0)
        self.assertEqual(self.stage(), f"{TITLE}, 40.00% (3 of 5 work items remaining)")
        self.line(stream, COMPLETE, at=1003.0)
        self.assertEqual(self.stage(), f"{TITLE}, all work items complete")
        # the banner's leading newline ends an empty line, which beat as the first line; then
        # the banner, the progress line and the completion each changed the stage
        self.assertEqual(self.beats.call_count, 4)

    def test_main_pys_own_banners_are_stages(self):
        tracker = StageTracker()
        self.assertTrue(tracker.feed(">> RUNNING PIPELINE FOR MODEL QCD_Cosmology"))
        self.assertEqual(tracker.stage, "RUNNING PIPELINE FOR MODEL QCD_Cosmology")
        self.assertTrue(tracker.feed("** BUILDING GRID OF MODELS TO SAMPLE"))
        self.assertEqual(tracker.stage, "BUILDING GRID OF MODELS TO SAMPLE")
        # its other lines, a warning and a worker's line move nothing
        for line in (
            '   -- using potential type "Exponential"',
            "   -- populated M sample grid with 1 value = [ 0.001 Mp ]",
            "!! WARNING: ScalarModel object with store_id=3 did not validate",
            WORKER,
        ):
            self.assertFalse(tracker.feed(line), line)
        self.assertEqual(tracker.stage, "BUILDING GRID OF MODELS TO SAMPLE")

    def test_stderr_does_not_beat(self):
        stream = self.stream(beats=False, copy_to="stderr")
        self.line(stream, BANNER, at=1000.0)
        self.line(stream, PROGRESS, at=2000.0)
        self.assertEqual(self.beats.call_count, 0)
        self.assertIsNone(self.stage())
        self.assertEqual(
            Path(self.run.stderr_path).read_text(), f"{BANNER}\n{PROGRESS}\n"
        )

    def test_a_stream_that_has_stopped_beating_still_copies(self):
        stream = self.stream()
        self.line(stream, BANNER, at=1000.0)
        beats = self.beats.call_count
        stream.stop_beating()
        self.line(stream, "** driver exit status 0", at=2000.0)
        self.assertEqual(self.beats.call_count, beats)
        self.assertEqual(self.stage(), TITLE)
        self.assertTrue(
            Path(self.run.stdout_path).read_text().endswith("** driver exit status 0\n")
        )


class TestThePassThrough(StreamCase):
    def test_every_write_reaches_the_real_stream_unaltered_and_the_copy(self):
        stream = self.stream()
        pieces = ["half a ", "line\n", BANNER, "\n", "", "two\nlines\n", "no newline"]
        for piece in pieces:
            stream.write(piece)
        stream.writelines(["a\n", "b\n"])
        stream.flush()
        expected = "".join(pieces) + "a\nb\n"
        self.assertEqual(self.real.text(), expected)
        self.assertEqual(Path(self.run.stdout_path).read_text(), expected)
        self.assertEqual(self.stage(), TITLE)

    def test_a_copy_that_fails_costs_neither_the_line_nor_the_beat(self):
        stream = self.stream()
        stream.close_copy()
        stream._copy = FailingCopy()
        self.line(stream, BANNER, at=1000.0)
        self.assertEqual(self.real.text(), f"{BANNER}\n")
        self.assertEqual(self.stage(), TITLE)
        # dropped, not retried, and a later line still gets through
        self.assertIsNone(stream._copy)
        self.line(stream, PROGRESS, at=1001.0)
        self.assertEqual(self.real.text(), f"{BANNER}\n{PROGRESS}\n")
        stream.flush()

    def test_a_heartbeat_that_fails_costs_no_line(self):
        stream = RegisteredStream(self.real, FailingRun(), StageTracker())
        self.line(stream, BANNER, at=1000.0)
        self.line(stream, WORKER, at=2000.0)
        self.assertEqual(self.real.text(), f"{BANNER}\n{WORKER}\n")

    def test_a_line_from_another_thread_reaches_both_and_beats(self):
        """Ray writes the workers' output into the driver's sys.stdout from a thread of its own
        (the orchestrator's note for 03, ruling 6)."""
        stream = self.stream()
        self.clock.now = 1000.0
        thread = threading.Thread(target=lambda: print(WORKER, file=stream))
        thread.start()
        thread.join(timeout=30)
        self.assertFalse(thread.is_alive())
        self.assertEqual(self.real.text(), f"{WORKER}\n")
        self.assertEqual(Path(self.run.stdout_path).read_text(), f"{WORKER}\n")
        self.assertEqual(self.beats.call_count, 1)
        self.assertIsNotNone(self.run.status()["heartbeat"])

    def test_two_threads_writing_at_once_lose_no_line(self):
        stream = self.stream()
        self.clock.now = 1000.0
        lines = {name: [f"{name} {i}" for i in range(200)] for name in ("a", "b")}

        def write(name):
            # one write per line: print's two writes (the text, then the newline) can interleave
            # between threads on any stream, the real one included
            for line in lines[name]:
                stream.write(f"{line}\n")

        threads = [threading.Thread(target=write, args=(n,)) for n in lines]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)
        copied = Path(self.run.stdout_path).read_text().splitlines()
        self.assertEqual(sorted(copied), sorted(lines["a"] + lines["b"]))
        self.assertEqual(sorted(self.real.text().splitlines()), sorted(copied))

    def test_it_knows_when_the_last_write_left_a_line_unfinished(self):
        stream = self.stream()
        self.assertFalse(stream.mid_line)
        stream.write("half a line")
        self.assertTrue(stream.mid_line)
        stream.write("")
        self.assertTrue(stream.mid_line)
        stream.write(" and the rest\n")
        self.assertFalse(stream.mid_line)

    def test_attributes_are_the_real_streams(self):
        stream = RegisteredStream(sys.__stdout__, self.run, StageTracker())
        self.assertEqual(stream.fileno(), sys.__stdout__.fileno())
        self.assertEqual(stream.encoding, sys.__stdout__.encoding)


if __name__ == "__main__":
    unittest.main()
