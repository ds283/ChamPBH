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
The lock paths of ChamPBH's cluster script,
`.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh` (rpk-v030-adoption prompt 01, item
T1; README section 6.2, P4). From that prompt the script keeps no lock of its own: it calls
`raypoolkit.rehearsal_lock` of the pinned `raypoolkit`, whose contract is RayPoolKit's
`docs/rehearsal-lock.md` at `v0.3.0`.

**No Ray, no real HOME, no real lock, no real cluster, by construction.** Each test runs
`bash -c HARNESS harness <cmd_...> [args]` from the repository root. The harness sources the script
(its main skipped), then overrides the cluster's values from the environment: a temp dir of the
test's own (`tempfile`), four ports found free by binding port 0, and `RAY_BIN` a stub `ray`. `HOME`
is a `tempfile` directory, so the lock the module takes is the test's. No value is on a command
line, so no ancestor of a lock command names the test's temp dir (the contract, section 6). Even a
mutated script can signal only processes naming the test's temp dir, and lock only the test's HOME.

**The stub `ray`** records its arguments and acts by `TEST_STUB_MODE`: `fail` exits 1, `nogcs`
exits 0 and starts nothing, `ok` starts a detached fake `gcs_server` naming the test's temp dir,
records its pid and exits 0 once it runs; with no mode it exits 1 and starts nothing. The fake is
`/usr/bin/perl -e 'sleep 120' gcs_server <temp dir>/session`, not the venv's Python: MacPorts'
framework Python re-executes itself under a path long enough that the script's `describe_pid`, which
cuts at 150 columns, loses the word `gcs_server` (prompt 01's orchestration note, ruling 1). Its
standard streams go to `/dev/null`, so it holds no pipe of the test's (ruling 2). Nothing looks
for a process by name: `tearDown` ends the fake by its recorded pid.

**The other project's lock** is taken through the module's command line, under the test's HOME,
with a third temp dir of its own. **The two locations** come from the module's
`default_lock_dir()` and `old_lock_dir()`, called with the test's HOME, so this file spells
neither.

No PRyMordial solve and no datastore. About a minute: each lock command is a Python start that
imports Ray's package (contract section 2), and the cycle of test 9 waits out a grace period.
"""

import os
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from raypoolkit import rehearsal_lock

REPO = Path(__file__).resolve().parents[2]
SCRIPT = ".documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh"
LOCK_PY = "./venv/bin/python"
TIMEOUT = 60  # seconds: a hang fails a test instead of blocking the suite

HARNESS = r"""
set -u
. ./.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh
CPBH_TEMP_DIR="$TEST_CLUSTER_TEMP_DIR"
CPBH_GCS_PORT="$TEST_GCS_PORT"
CPBH_CLIENT_PORT="$TEST_CLIENT_PORT"
CPBH_DASHBOARD_PORT="$TEST_DASHBOARD_PORT"
CPBH_AGENT_PORT="$TEST_AGENT_PORT"
CPBH_ADDRESS="127.0.0.1:${CPBH_GCS_PORT}"
RAY_BIN="$TEST_RAY_BIN"
"$@"
"""

STUB_RAY = r"""#!/bin/bash
# The test's stand-in for ray: records its arguments, then acts by TEST_STUB_MODE.
printf '%s\n' "$*" >> "$TEST_STUB_CALLS"
dir=""
prev=""
for a in "$@"; do
    [ "$prev" = "--temp-dir" ] && dir="$a"
    prev="$a"
done
case "${TEST_STUB_MODE:-}" in
    fail) exit 1 ;;
    nogcs) exit 0 ;;
    ok)
        [ -n "$dir" ] || exit 1
        nohup /usr/bin/perl -e 'sleep 120' gcs_server "$dir/session" </dev/null >/dev/null 2>&1 &
        pid=$!
        echo "$pid" > "$TEST_STUB_PIDFILE"
        # As ray start does, return once the gcs_server runs (the fork has exec'd perl).
        i=0
        while [ "$i" -lt 100 ]; do
            case "$(ps -o command= -p "$pid" 2>/dev/null)" in
                /usr/bin/perl*) exit 0 ;;
            esac
            sleep 0.1
            i=$((i + 1))
        done
        exit 1 ;;
    *) exit 1 ;;
esac
"""


def _free_ports(n):
    """``n`` distinct ports on 127.0.0.1 that were free a moment ago."""
    socks = []
    try:
        for _ in range(n):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("127.0.0.1", 0))
            socks.append(s)
        return [s.getsockname()[1] for s in socks]
    finally:
        for s in socks:
            s.close()


def _pid_exists(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class RayClusterLockPaths(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.mkdtemp(prefix="cpbh-lock-home-")
        self.cluster_dir = tempfile.mkdtemp(prefix="cpbh-lock-ray-")
        self.si_dir = tempfile.mkdtemp(prefix="cpbh-lock-si-")
        self.addCleanup(shutil.rmtree, self.home, ignore_errors=True)
        self.addCleanup(shutil.rmtree, self.cluster_dir, ignore_errors=True)
        self.addCleanup(shutil.rmtree, self.si_dir, ignore_errors=True)

        self.stub = Path(self.home) / "stub-ray"
        self.stub.write_text(STUB_RAY)
        self.stub.chmod(0o755)
        self.calls = Path(self.home) / "stub-calls"
        self.pidfile = Path(self.home) / "stub-gcs-pid"

        (
            self.gcs_port,
            self.client_port,
            self.dashboard_port,
            self.agent_port,
            si_port,
        ) = _free_ports(5)
        self.address = f"127.0.0.1:{self.gcs_port}"
        self.si_address = f"127.0.0.1:{si_port}"

        with mock.patch.dict(os.environ, {"HOME": self.home}):
            self.lock_dir = rehearsal_lock.default_lock_dir()
            self.old_dir = rehearsal_lock.old_lock_dir()

        self.children = []

    def tearDown(self):
        # The test's own children: killed and reaped.
        for proc in self.children:
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=TIMEOUT)
        # The stub's fake gcs_server, by its recorded pid, if it is still the fake.
        if self.pidfile.exists():
            text = self.pidfile.read_text().strip()
            if text.isdigit():
                pid = int(text)
                command = subprocess.run(
                    ["ps", "-ww", "-o", "command=", "-p", str(pid)],
                    capture_output=True,
                    text=True,
                    timeout=TIMEOUT,
                ).stdout
                if self.cluster_dir in command:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    # ---- helpers ----

    def _env(self, mode=None):
        env = dict(os.environ)
        env.update(
            HOME=self.home,
            TEST_CLUSTER_TEMP_DIR=self.cluster_dir,
            TEST_GCS_PORT=str(self.gcs_port),
            TEST_CLIENT_PORT=str(self.client_port),
            TEST_DASHBOARD_PORT=str(self.dashboard_port),
            TEST_AGENT_PORT=str(self.agent_port),
            TEST_RAY_BIN=str(self.stub),
            TEST_STUB_CALLS=str(self.calls),
            TEST_STUB_PIDFILE=str(self.pidfile),
            RAY_CLUSTER_GRACE="2",
        )
        env.pop("TEST_STUB_MODE", None)
        if mode is not None:
            env["TEST_STUB_MODE"] = mode
        return env

    def _script(self, *args, mode=None):
        """A function of the sourced script, with the test's values; no value on its command line."""
        return subprocess.run(
            ["bash", "-c", HARNESS, "harness", *args],
            cwd=REPO,
            env=self._env(mode),
            capture_output=True,
            text=True,
            timeout=TIMEOUT,
        )

    def _module(self, *args):
        """A command of raypoolkit.rehearsal_lock, under the test's HOME."""
        return subprocess.run(
            [LOCK_PY, "-m", "raypoolkit.rehearsal_lock", *args],
            cwd=REPO,
            env=self._env(),
            capture_output=True,
            text=True,
            timeout=TIMEOUT,
        )

    def _other_project_holds_the_lock(self):
        r = self._module(
            "acquire",
            "--project",
            "StochasticInstantons",
            "--purpose",
            "another project's rehearsal",
            "--ray-temp-dir",
            self.si_dir,
            "--ray-address",
            self.si_address,
        )
        self.assertEqual(r.returncode, 0, r.stderr)
        return (self.lock_dir / "owner").read_bytes()

    def _cluster_process(self):
        """A process of the test's cluster: it names the temp dir, as a gcs_server does."""
        proc = subprocess.Popen(
            [
                "/usr/bin/perl",
                "-e",
                "sleep 120",
                "gcs_server",
                f"{self.cluster_dir}/session",
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.children.append(proc)
        return proc

    def _stub_calls(self):
        if not self.calls.exists():
            return []
        return [line for line in self.calls.read_text().splitlines() if line]

    def _owner(self):
        return rehearsal_lock.parse_owner((self.lock_dir / "owner").read_text())

    # ---- the tests ----

    def test_01_the_scripts_values_and_pin(self):
        """Guards against the script's cluster values drifting (the lock compares them as given),
        against the script keeping a lock of its own or naming the old location, and against the
        pin falling back below v0.3.0."""
        text = (REPO / SCRIPT).read_text()
        self.assertIn('LOCK_PROJECT="ChamPBH"\n', text)
        self.assertIn('CPBH_TEMP_DIR="/tmp/ray-cpbh"\n', text)
        self.assertIn("CPBH_GCS_PORT=6380\n", text)
        self.assertIn('CPBH_ADDRESS="127.0.0.1:${CPBH_GCS_PORT}"\n', text)
        self.assertIn("raypoolkit.rehearsal_lock", text)
        for part in self.old_dir.relative_to(self.home).parts:
            self.assertNotIn(part, text)
        for name in ("acquire_lock", "release_lock", "write_owner"):
            self.assertNotIn(name, text)
        pins = [
            line
            for line in (REPO / "requirements.txt").read_text().splitlines()
            if re.match(r"raypoolkit\b", line)
        ]
        self.assertEqual(
            pins, ["raypoolkit @ git+https://github.com/ds283/RayPoolKit@v0.3.0"]
        )

    def test_02_status_on_a_free_lock(self):
        """Guards against status not being the module's (its layout, the contract's section 8),
        or looking at another temp dir than the cluster's."""
        r = self._script("cmd_status")
        self.assertEqual(r.returncode, 0, r.stderr)
        lines = r.stdout.splitlines()
        self.assertEqual(lines[0], f"lock: free ({self.lock_dir} does not exist)")
        self.assertIn(f"Ray processes of {self.cluster_dir}: none", lines)

    def test_03_start_while_another_project_holds_the_lock(self):
        """Guards against a start that goes on past a held lock (exit 3), or touches it."""
        before = self._other_project_holds_the_lock()
        r = self._script("cmd_start", "test 03")
        self.assertEqual(r.returncode, 3, r.stdout + r.stderr)
        self.assertEqual((self.lock_dir / "owner").read_bytes(), before)
        self.assertEqual(self._stub_calls(), [])

    def test_04_start_while_the_old_location_exists(self):
        """Guards against a start that takes the new lock while the old one may be held."""
        self.old_dir.mkdir(parents=True)
        r = self._script("cmd_start", "test 04")
        self.assertEqual(r.returncode, 3, r.stdout + r.stderr)
        self.assertFalse(os.path.lexists(self.lock_dir))
        self.assertEqual(self._stub_calls(), [])

    def test_05_stop_while_another_project_holds_the_lock(self):
        """Guards against a stop that signals ChamPBH's cluster while the lock is another
        project's: it must touch nothing, the lock and the processes included."""
        before = self._other_project_holds_the_lock()
        proc = self._cluster_process()
        r = self._script("cmd_stop")
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertEqual((self.lock_dir / "owner").read_bytes(), before)
        self.assertIsNone(proc.poll())
        out = r.stdout + r.stderr
        for line in ("no process names", "SIGTERM to", "grace period", "are free"):
            self.assertNotIn(line, out)

    def test_06_start_while_a_process_of_the_cluster_is_alive(self):
        """Guards against a start on top of a stale cluster: exit 4, the lock released again,
        nothing started and nothing signalled."""
        proc = self._cluster_process()
        r = self._script("cmd_start", "test 06")
        self.assertEqual(r.returncode, 4, r.stdout + r.stderr)
        self.assertFalse(os.path.lexists(self.lock_dir))
        self.assertEqual(self._stub_calls(), [])
        self.assertIsNone(proc.poll())

    def test_07_start_whose_ray_start_fails(self):
        """Guards against a failed ray start that leaves the lock held, and against ray start's
        temp dir not being the cluster's."""
        r = self._script("cmd_start", "test 07", mode="fail")
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        calls = self._stub_calls()
        self.assertEqual(len(calls), 1, calls)
        self.assertTrue(
            calls[0].startswith(f"start --head --temp-dir {self.cluster_dir} "), calls
        )
        self.assertFalse(os.path.lexists(self.lock_dir))

    def test_08_start_with_no_gcs_server(self):
        """Guards against a start that leaves the lock held when ray start finds no gcs_server."""
        r = self._script("cmd_start", "test 08", mode="nogcs")
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertFalse(os.path.lexists(self.lock_dir))

    def test_09_the_cycle_through_the_module(self):
        """Guards against a start that records no gcs_server pid (so release would not wait for
        it), a status that does not see the cluster, and a stop that leaves the cluster or the
        lock behind."""
        r = self._script("cmd_start", "test 09", mode="ok")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        pid = int(self.pidfile.read_text().strip())

        owner = self._owner()
        self.assertEqual(owner["project"], "ChamPBH")
        self.assertEqual(owner["ray_temp_dir"], self.cluster_dir)
        self.assertEqual(owner["ray_address"], self.address)
        self.assertEqual(owner["pid"], str(pid))

        r = self._module(
            "release", "--project", "ChamPBH", "--ray-temp-dir", self.cluster_dir
        )
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn(f"the recorded pid {pid} is alive", r.stderr)

        r = self._script("cmd_status")
        self.assertEqual(r.returncode, 0, r.stderr)
        lines = r.stdout.splitlines()
        self.assertEqual(lines[0], f"lock: HELD ({self.lock_dir})")
        self.assertIn(f"owner pid {pid}: alive", lines)
        self.assertIn(f"Ray processes of {self.cluster_dir}: 1", lines)
        self.assertTrue(any(line.startswith(f"    {pid} ") for line in lines), lines)

        r = self._script("cmd_stop")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("released the rehearsal lock", r.stdout)
        deadline = time.monotonic() + 2 + 5
        while _pid_exists(pid) and time.monotonic() < deadline:
            time.sleep(0.1)
        self.assertFalse(_pid_exists(pid))
        self.assertFalse(os.path.lexists(self.lock_dir))


if __name__ == "__main__":
    unittest.main()
