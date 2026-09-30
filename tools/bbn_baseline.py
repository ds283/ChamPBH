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
Print the Standard-Model BBN baseline: rho_NP = 0 through the same PRyMordial
settings compute_BBN_data uses (ComputeTargets.BBNData.compute_SM_baseline).
One PRyMordial solve, about 10 s. No Ray cluster, no datastore; nothing is
stored.

Run from the repository root, since PRyMordial reads PRyMrates/ from the
working directory:

    ./venv/bin/python tools/bbn_baseline.py
    ./venv/bin/python tools/bbn_baseline.py --small-network

Written for review-remediation prompt 04 (item R3).
"""

import argparse
import os
import sys
import time
from pathlib import Path

# make the repository root importable when run as `python tools/bbn_baseline.py`
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ComputeTargets.BBNData import compute_SM_baseline  # noqa: E402


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--small-network",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="the small_network value passed to compute_SM_baseline (default: False, the full network, as main.py passes)",
    )
    args = parser.parse_args(argv)

    if not Path(os.getcwd(), "PRyMrates").is_dir():
        print(
            f"!! bbn_baseline: run from the repository root; PRyMordial reads PRyMrates/ "
            f"from the working directory ({os.getcwd()})"
        )
        return 2

    start = time.perf_counter()
    baseline = compute_SM_baseline(small_network=args.small_network)
    wall = time.perf_counter() - start

    print(
        f"SM baseline (rho_NP = 0), PRyM_version={baseline['PRyM_version']}, "
        f"small_network={baseline['small_network']}, {wall:.1f} s"
    )
    print(f"  Yp           = {baseline['Yp_BBN']:.10g}")
    print(f"  D/H   x 1e5  = {baseline['DOverH']:.10g}")
    print(f"  3He/H x 1e5  = {baseline['He3OverH']:.10g}")
    print(f"  7Li/H x 1e10 = {baseline['Li7OverH']:.10g}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
