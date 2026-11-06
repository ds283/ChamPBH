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
The wrapper's scope names A (one-loop-drain prompt 04, prompt §1.8, P40).

`describe_scope` adds `A <values>` after the alpha part only when `--A-values` is given, read as given as the alpha
part is (up to `SCOPE_MAX_VALUES`, a count and a range beyond, duplicates kept). A run that gives no `--A-values`
has the scope it had before this prompt, so every scope string `test_scope_alpha` pins is unchanged (that module is
untouched and passes).

No Ray, no store, no registry. Instant.
"""

import unittest

from RunDrivers import registered_run
from config.argument_parser import create_argument_parser

BASE = ["--database", "x.db", "--ray-address", "127.0.0.1:6380"]


def scope(*extra):
    parsed = create_argument_parser().parse_args(BASE + list(extra))
    return registered_run.describe_scope(parsed)


class TestTheScopeNamesA(unittest.TestCase):
    def test_no_A_values_is_the_scope_of_before(self):
        self.assertEqual(
            scope(),
            "no config file; β 0.1–3.0, 5 per unit; M 10^25–10^26.5 eV, 6 per decade; "
            "n 1; φ* 5 Mp; tuning tolerance 1e-05; α 0; 20 shards; job ChamPBH-test",
        )
        self.assertNotIn("; A ", scope("--alpha-values", "0", "0.1"))

    def test_A_follows_alpha(self):
        text = scope("--alpha-values", "0", "0.1", "--A-values", "0.5", "1", "10")
        parts = text.split("; ")
        self.assertEqual(parts[parts.index("α 0, 0.1") + 1], "A 0.5, 1, 10")
        self.assertEqual(parts[parts.index("A 0.5, 1, 10") + 1], "20 shards")
        self.assertEqual(
            scope("--A-values", "1"),
            "no config file; β 0.1–3.0, 5 per unit; M 10^25–10^26.5 eV, 6 per decade; "
            "n 1; φ* 5 Mp; tuning tolerance 1e-05; α 0; A 1; 20 shards; job ChamPBH-test",
        )

    def test_the_order_and_duplicates_are_as_given(self):
        self.assertIn("; A 2, 1, 2; ", scope("--A-values", "2", "1", "2"))

    def test_more_than_six_values_read_as_a_count_and_a_range(self):
        values = ["1", "2", "3", "4", "5", "6", "7"]
        self.assertIn("; A 7 values, 1–7; ", scope("--A-values", *values))


if __name__ == "__main__":
    unittest.main()
