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
`--plots/--no-plots`: the plot scripts write their CSV files alone and draw no figure (2026-11-09).

The parser only: figures are on by default, `--no-plots` turns them off and `--plots` back on, a `--config`
YAML file can set it, and the help names it. The drivers' behaviour under the flag needs a store and Ray,
and was checked against a real store when the flag was added: the CSV files written with and without
figures are byte-identical.
"""

import contextlib
import io
import os
import tempfile
import unittest

from config.argument_parser import create_argument_parser


class TestNoPlotsFlag(unittest.TestCase):
    def parse(self, *argv):
        # --database is required by the shared parser; nothing is opened
        return create_argument_parser().parse_args(["--database", "unused.db", *argv])

    def test_figures_are_drawn_by_default(self):
        self.assertTrue(self.parse().plots)

    def test_no_plots_turns_them_off(self):
        self.assertFalse(self.parse("--no-plots").plots)

    def test_plots_turns_them_back_on(self):
        self.assertTrue(self.parse("--no-plots", "--plots").plots)

    def test_config_file_sets_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "c.yaml")
            with open(path, "w") as f:
                f.write("plots: false\n")
            self.assertFalse(self.parse("--config", path).plots)

    def test_help_names_it(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out), self.assertRaises(SystemExit):
            self.parse("--help")
        self.assertIn("--no-plots", out.getvalue())


if __name__ == "__main__":
    unittest.main()
