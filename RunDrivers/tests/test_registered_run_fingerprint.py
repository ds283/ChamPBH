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

"""The fingerprint a registered run takes once its program has returned (rrk-adoption prompt 03,
W5 and W6 test 5; campaign README P6).

The store is rrk-adoption 02's golden store, built by its own builder
(`ObjectFactories.tests.test_store_fingerprint_golden._GoldenStoreCase`, on `_TempStoreCase`'s
harness: a one-shard store in a `tempfile` directory, its actors closed before it is read), and its
fingerprint is the golden file's (the orchestrator's note for 03, ruling 10). With a sidecar, it is
made by `runregistrykit.stores.create_sidecar` before the run, as a person makes one after a first
close. The wrapper's `main` runs in this process on a stand-in that returns without touching the
store, with `ray.init` and `ray.shutdown` replaced by stand-ins (`stubbed_run.in_process`), into a
runs root in the same temporary directory. This is the only test module of this root that builds
a store, and it is subject to the harness's rule that Ray is never initialised. Nothing under
`var/` is read or written.
"""

import json
import unittest

from runregistrykit.stores import create_sidecar, sidecar_path

from ObjectFactories.tests.test_store_fingerprint_golden import (
    GOLDEN,
    _GoldenStoreCase,
    without_taken,
)
from RunDrivers.tests.stubbed_run import (
    REPO_ROOT,
    in_process,
    only_run,
    read_json,
    standin_program,
    wrapper_argv,
)


class TestTheFingerprintAtFinish(_GoldenStoreCase):
    def setUp(self):
        super().setUp()
        self.runs_root = self._dir / "runs"
        self.runs_root.mkdir()
        self.golden = json.loads(GOLDEN.read_text())

    def a_run(self, primary):
        program = standin_program(self._dir, "return", self.runs_root)
        done = in_process(
            self, program, wrapper_argv(primary, slug="golden"), self.runs_root
        )
        self.assertIsNone(done.outcome, repr(done.outcome))
        return read_json(only_run(self.runs_root) / "status.json")

    def test_with_a_sidecar_the_golden_fingerprint_is_written_to_it(self):
        primary = self.build_store()
        create_sidecar(
            primary,
            purpose="the golden store of rrk-adoption 02, in a test of the wrapper",
            client_root=str(REPO_ROOT),
        )
        status = self.a_run(primary)
        self.assertEqual((status["state"], status["exit_code"]), ("done", 0))
        self.assertEqual(status["fingerprint_sidecar"], "written")
        self.assertNotIn("fingerprint_error", status)
        self.assertEqual(status["fingerprint"]["digest"], self.golden["digest"])
        self.assertEqual(
            without_taken(status["fingerprint"]), without_taken(self.golden)
        )
        sidecar = read_json(sidecar_path(primary))
        self.assertEqual(sidecar["fingerprint"]["digest"], self.golden["digest"])

    def test_without_one_it_is_not_written_and_the_run_is_still_done(self):
        primary = self.build_store()
        status = self.a_run(primary)
        self.assertEqual((status["state"], status["exit_code"]), ("done", 0))
        # the text after "not written" is the package's (runregistrykit/__init__.py:865-868)
        self.assertTrue(
            status["fingerprint_sidecar"].startswith("not written"),
            status["fingerprint_sidecar"],
        )
        self.assertNotIn("fingerprint_error", status)
        self.assertEqual(status["fingerprint"]["digest"], self.golden["digest"])
        self.assertFalse(sidecar_path(primary).exists())


if __name__ == "__main__":
    unittest.main()
