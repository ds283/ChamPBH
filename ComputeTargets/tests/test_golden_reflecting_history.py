"""
The reflecting golden history (reflection-restart prompt 03 §1.6; README §6.2 P5; orchestrator note 03,
ruling 3): beta = 1.5, M = 1e-12 M_P, alpha = 0.05, tuned, the diagnosis's first failure, which reflects
about 30 times and parks. It is tuned again here through tune_history, with no Ray, and compared exactly
with data/golden_history_rr03_reflecting.json (golden_reflecting_history.py holds the setup and what is
pinned). Since the history reflects, it pins F3's first step at every restart after a reflection and the
guard, which the golden histories at alpha = 0 cannot: they do not restart.

Cost: one tuning of one integration, about 6 s unloaded (5.6 s at a load of about 10 in prompt 01's batch,
under the probe's F3). No Ray, no PRyMordial solve, no store.

A pin of floating-point output holds for this venv on this platform; on a mismatch the test prints the
recorded and the current platform and library versions, with the first difference. It never skips.
"""

import contextlib
import io
import json
import unittest

from ComputeTargets.tests import golden_reflecting_history as grh
from ComputeTargets.tests.golden_history import environment


class TestTheReflectingGoldenHistory(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.golden = json.loads(grh.GOLDEN_REFLECTING_PATH.read_text())
        with contextlib.redirect_stdout(io.StringIO()):
            cls.data = grh.run_tuning()

    def test_it_succeeds_and_is_the_record_bitwise(self):
        self.assertFalse(
            self.data.get("failure", False), self.data.get("failure_reason")
        )
        messages = grh.differences(self.golden, grh.build_record(self.data))
        if messages:
            self.fail(
                f"the reflecting golden history differs from {grh.GOLDEN_REFLECTING_PATH.name}:\n  "
                + "\n  ".join(messages)
                + f"\nrecorded environment: {self.golden['provenance']['environment']}"
                + f"\ncurrent environment:  {environment()}"
                + f"\n(generated at {self.golden['provenance']['generated_at_commit']}; "
                f"regenerate only deliberately: {grh.REGENERATE_COMMAND})"
            )

    def test_the_record_reflects_and_the_guard_never_fired(self):
        """The file itself: a reflecting history, no retake, the counts agreeing with its history."""
        reflecting = self.golden["reflecting"]
        self.assertGreater(reflecting["reflections"], 0)
        self.assertEqual(reflecting["restart_retakes"], 0)
        self.assertEqual(
            reflecting["reflections"], self.golden["history"]["reflections"]
        )
        self.assertEqual(
            reflecting["restart_retakes"], self.golden["history"]["restart_retakes"]
        )
        self.assertEqual(
            reflecting["accepted_steps"], self.golden["history"]["accepted_steps"]
        )


if __name__ == "__main__":
    unittest.main()
