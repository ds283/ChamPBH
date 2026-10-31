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
The wrapper's scope names alpha (inelastic-reflection prompt 10, test 8, P37).

`describe_scope` reads `--alpha-values` as given from `main.py`'s parsed arguments, as it reads
beta's: `α 0` by default, the values up to `SCOPE_MAX_VALUES`, a count and a range beyond, and
duplicates kept, since the scope describes the arguments and not the grid. The alpha follows the
tuning tolerance. The parse is the shared parser's, which refuses an alpha out of range with exit 2.

No Ray, no store, no registry. Instant.
"""

import contextlib
import io
import unittest

from RunDrivers import registered_run
from config.argument_parser import create_argument_parser

BASE = ["--database", "x.db", "--ray-address", "127.0.0.1:6380"]


def scope(*extra):
    parsed = create_argument_parser().parse_args(BASE + list(extra))
    return registered_run.describe_scope(parsed)


class TestTheScopeNamesAlpha(unittest.TestCase):
    def test_8_the_default_reads_alpha_0_after_the_tuning_tolerance(self):
        text = scope()
        parts = text.split("; ")
        self.assertEqual(parts[parts.index("tuning tolerance 1e-05") + 1], "α 0")
        self.assertEqual(
            text,
            "no config file; β 0.1–3.0, 5 per unit; M 10^25–10^26.5 eV, 6 per decade; "
            "n 1; φ* 5 Mp; tuning tolerance 1e-05; α 0; 20 shards; job ChamPBH-test",
        )

    def test_8_three_values(self):
        self.assertIn(
            "tuning tolerance 1e-05; α 0, 0.03, 0.1; 20 shards",
            scope("--alpha-values", "0", "0.03", "0.1"),
        )
        # the order given, and a duplicate as given
        self.assertIn("; α 0.1, 0, 0.1; ", scope("--alpha-values", "0.1", "0", "0.1"))

    def test_8_more_than_six_values_read_as_a_count_and_a_range(self):
        values = ["0.01", "0.02", "0.03", "0.04", "0.05", "0.06", "0.07"]
        self.assertIn("; α 7 values, 0.01–0.07; ", scope("--alpha-values", *values))

    def test_8_an_alpha_out_of_range_is_refused_by_the_parser(self):
        for value in ("1.0", "-0.1", "nan"):
            with self.subTest(value=value), contextlib.redirect_stderr(
                io.StringIO()
            ) as err:
                with self.assertRaises(SystemExit) as ctx:
                    scope("--alpha-values", value)
                self.assertEqual(ctx.exception.code, 2)
                self.assertIn("--alpha-values", err.getvalue())


if __name__ == "__main__":
    unittest.main()
