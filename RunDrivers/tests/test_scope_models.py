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
The wrapper's scope names the potentials (power-law-wall prompt 06 §1.9; README §6.10 P57).

`describe_scope` adds `models <values>` after the config file's part, for every run: the values as given (duplicates
kept, as the alpha part keeps them), `models exponential` for a run that gives none, since that is the parser's default.
The manifests already on disk are records and are not touched.

No Ray, no store, no registry. Instant.
"""

import unittest

from RunDrivers import registered_run
from config.argument_parser import create_argument_parser

BASE = ["--database", "x.db", "--ray-address", "127.0.0.1:6380"]


def scope(*extra):
    parsed = create_argument_parser().parse_args(BASE + list(extra))
    return registered_run.describe_scope(parsed)


class TestTheScopeNamesThePotentials(unittest.TestCase):
    def test_the_default_names_the_exponential(self):
        self.assertEqual(
            scope(),
            "no config file; models exponential; β 0.1–3.0, 5 per unit; M 10^25–10^26.5 eV, 6 per decade; "
            "n 1; φ* 5 Mp; tuning tolerance 1e-05; α 0; 20 shards; job ChamPBH-test",
        )

    def test_the_potentials_follow_the_config_file(self):
        text = scope("--config", "inverse_power.yaml", "--n-values", "1", "2")
        parts = text.split("; ")
        self.assertEqual(parts[0], "config inverse_power.yaml")
        self.assertEqual(parts[1], "models inverse-power")
        text = scope("--models", "inverse-power", "exponential")
        self.assertEqual(text.split("; ")[1], "models inverse-power, exponential")

    def test_as_given(self):
        self.assertIn(
            "; models exponential, inverse-power, exponential; ",
            scope("--models", "exponential", "inverse-power", "exponential"),
        )


if __name__ == "__main__":
    unittest.main()
