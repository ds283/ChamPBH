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
Planning probe for the science-readiness campaign (README §6.2): reference abundances that prompt
01's patched Hubble-only route must reproduce, measured on the shipped tree, small network.

  zero-off     rho_NP = p_NP = drho_NP/dT = 0, NP_thermo_flag = False (no new physics at all)
  const-honly  the CONSTANT family rho_NP = 0.08 rho_SM(T) of prym_fixtures, with
               p_NP = -rho_NP and drho_NP/dT = 0, NP_thermo_flag = True: the Hubble-only route
               through the shipped callbacks (cham03 makes T_NP inert, so this is well defined)
  const-shipped the CONSTANT family as prym_fixtures defines it (p_NP = rho_NP/3), for scale

Each case is one PRyMordial solve, about 10 s. Run from the repository root, one at a time:

    PYTHONPATH=. ./venv/bin/python prompts/science-readiness/planning-probes/honly_constant_reference.py CASE

After prompt 01 the first two cannot be re-run (the NP_thermo_flag route is removed); the
numbers recorded in README §6.2 are the reference.
"""

import sys
import time

from ComputeTargets.tests.prym_fixtures import (
    CONSTANT,
    ZERO,
    run_prym,
    RES_YP_BBN,
    RES_D_OVER_H_E5,
    RES_HE3_OVER_H_E5,
    RES_LI7_OVER_H_E10,
)


def _zero(T):
    return 0.0


def _minus_rho(T):
    return -CONSTANT.rho(T)


case = sys.argv[1]
t0 = time.time()
if case == "zero-off":
    res = run_prym(
        ZERO.rho, ZERO.p, ZERO.drho_dT, small_network=True, NP_thermo_flag=False
    )
elif case == "const-honly":
    res = run_prym(CONSTANT.rho, _minus_rho, _zero, small_network=True)
elif case == "const-shipped":
    res = run_prym(CONSTANT.rho, CONSTANT.p, CONSTANT.drho_dT, small_network=True)
else:
    raise ValueError(case)
print(
    f"{case}: Yp={res[RES_YP_BBN]:.10g} DoH={res[RES_D_OVER_H_E5]:.10g} "
    f"He3oH={res[RES_HE3_OVER_H_E5]:.10g} Li7oH={res[RES_LI7_OVER_H_E10]:.10g} "
    f"wall={time.time() - t0:.1f} s"
)
