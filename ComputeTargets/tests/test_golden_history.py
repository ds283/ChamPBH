"""
The golden histories: the campaign's regression (jordan-normalization P3).

Two histories (beta = 1.5 and beta = 5.5 at M = 1e-3 M_P, Lambda = 1 meV, n = 1, phi* = 5 M_P,
pi* = 0, T* = 2e4 GeV, T_stop = 1 MeV, atol = rtol = 1e-8, the production sampling axis) are
recomputed through compute_scalar_history._function, with no Ray, and compared exactly with
data/golden_history_03.json: the SHA-256 of the samples and of the sampled ln(1 + z), the counts, the
first bounce, the fixed-temperature values, the step-control scalars, and every excerpted sample
(every 250th and the last) field by field. The setup, and what is and is not pinned, are in
golden_history.py.

Prompt 03 regenerated the record, as golden_history_03.json, when the grid became the ln(1 + z)
axis and each sample gained ln(1 + z_J); 01's golden_history_01.json is kept as the record of
prompts 01 and 02, and is never written.

Cost: about 12 s for both histories at a machine load of 12-15 (0.6 s for beta = 1.5, 10-12 s for
beta = 5.5). It runs a real integration but needs no Ray, no PRyMordial solve and no store.

Every prompt of jordan-normalization runs it. Prompts 01, 02 and 04 must leave it passing unchanged.
Prompts 03 and 05 change values by design: each regenerates the JSON deliberately and logs the
change (the largest relative change per field):

    PYTHONPATH=. ./venv/bin/python -m ComputeTargets.tests.golden_history

A pin of floating-point output holds for this venv on this platform; on a mismatch the test prints
the recorded and the current platform and library versions, with the first excerpted sample and
field that differ. It never skips.
"""

import json
import unittest

from ComputeTargets.tests import golden_history as gh


class TestGoldenHistories(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.golden = json.loads(gh.GOLDEN_PATH.read_text())

    def _check(self, beta: float):
        key = gh.history_key(beta)
        stored = self.golden["histories"][key]
        current = gh.record_of(gh.run_history(beta))

        messages = gh.first_difference(stored, current)
        if messages:
            recorded = self.golden["provenance"]["environment"]
            now = gh.environment()
            self.fail(
                f"golden history {key} differs from {gh.GOLDEN_PATH.name}:\n  "
                + "\n  ".join(messages)
                + f"\nrecorded environment: {recorded}\ncurrent environment:  {now}\n"
                f"(generated at {self.golden['provenance']['generated_at_commit']}; "
                f"regenerate only deliberately: {gh.REGENERATE_COMMAND})"
            )

    def test_history_beta_1_5(self):
        self._check(1.5)

    def test_history_beta_5_5(self):
        self._check(5.5)

    def test_the_record_holds_exactly_the_two_histories_and_their_excerpts(self):
        """The file itself: both histories, an excerpt every 250th sample and the last."""
        self.assertEqual(
            sorted(self.golden["histories"]),
            sorted(gh.history_key(b) for b in gh.BETAS),
        )
        for key, h in self.golden["histories"].items():
            n = h["n_samples"]
            expected = list(range(0, n, gh.EXCERPT_STRIDE))
            if expected[-1] != n - 1:
                expected.append(n - 1)
            self.assertEqual([e["index"] for e in h["excerpts"]], expected, key)
            self.assertEqual(h["sample_fields"], list(gh.SampleValues._fields), key)


if __name__ == "__main__":
    unittest.main()
