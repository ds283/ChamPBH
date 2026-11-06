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

"""
extract_common.history_directory, the history's directory below a row's plots/ and csv/ (one-loop-drain prompt 05,
orchestrator note 05 ruling 9):
- it equals the f-string plot_ScalarHistory.py formatted inline before, on literals;
- plot_ScalarHistory.py's plot_path and csv_path both call it (by ast: that script parses its arguments on import), and
  their strings are those of before.

Instant; no store, no Ray.
"""

import ast
import unittest
from pathlib import Path

from extract_common import history_directory

ROOT = Path(__file__).resolve().parents[2]


def old_strings(beta, M, eV, n):
    """plot_ScalarHistory.py's two lines before this prompt, as f-strings on the same names."""
    return (
        f"plots/beta={beta:.5g}/M={M/eV:.5g}eV_n={n}",
        f"csv/beta={beta:.5g}/M={M/eV:.5g}eV_n={n}",
    )


class TestHistoryDirectory(unittest.TestCase):
    CASES = (
        (1.185, 4.0e-31, 1.0 / 2.435323e27, 1),
        (2.0, 8.0e-18, 1.0 / 2.435323e27, 1),
        (1.1066666, 0.5, 1.0 / 2.435323e27, 2),
        (3.0, 1.0e-12, 1.0, 1),
        (0.99999999, 123456.789, 1.0, 3),
        (1.0e-7, 1.0e12, 1.0, 1),
    )

    def test_equals_the_old_f_string(self):
        for beta, M, eV, n in self.CASES:
            with self.subTest(beta=beta, M=M, n=n):
                plots, csv = old_strings(beta, M, eV, n)
                self.assertEqual(f"plots/{history_directory(beta, M / eV, n)}", plots)
                self.assertEqual(f"csv/{history_directory(beta, M / eV, n)}", csv)
                base = Path("/out/QCD_Cosmology/one-loop-measured-v1/A=1")
                self.assertEqual(
                    base / f"csv/{history_directory(beta, M / eV, n)}", base / csv
                )

    def test_plot_ScalarHistory_calls_it_for_both_paths(self):
        tree = ast.parse((ROOT / "plot_ScalarHistory.py").read_text())
        assigned = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                target = node.targets[0]
                if isinstance(target, ast.Name) and target.id in (
                    "plot_path",
                    "csv_path",
                ):
                    calls = [
                        n
                        for n in ast.walk(node.value)
                        if isinstance(n, ast.Call)
                        and isinstance(n.func, ast.Name)
                        and n.func.id == "history_directory"
                    ]
                    if calls:
                        assigned.setdefault(target.id, []).append(
                            ast.unparse(node.value)
                        )
        self.assertEqual(
            assigned,
            {
                "plot_path": [
                    "base_path / f'plots/{history_directory(beta, M / units.eV, n)}'"
                ],
                "csv_path": [
                    "base_path / f'csv/{history_directory(beta, M / units.eV, n)}'"
                ],
            },
        )
        # no inline copy of the old format remains
        self.assertNotIn("eV_n={n}", (ROOT / "plot_ScalarHistory.py").read_text())


if __name__ == "__main__":
    unittest.main()
