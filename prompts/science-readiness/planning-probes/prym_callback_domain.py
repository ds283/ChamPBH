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
Planning probe for the science-readiness campaign (README §6.0): at which temperatures does
PRyMordial call the new-physics callbacks? One small-network solve with rho_NP = p_NP =
drho_NP/dT = 0, through the flags compute_BBN_data sets, recording every T. About 18 s. Run from
the repository root:

    PYTHONPATH=. ./venv/bin/python prompts/science-readiness/planning-probes/prym_callback_domain.py

On 6aaa706 it printed: calls=4804 min_T_pos=0.3628 keV max_T=10 MeV n_negative=0; 351 calls
below 1 keV, 767 below 3 keV. PRyMordial's thermodynamic solve runs to t_end = 1e7 s, past
T_end = 1 keV, which is why the lowest query is below 1 keV.
"""

import time

from ComputeTargets.BBNData import _configure_PRyMordial

Ts = []


def rec(T):
    Ts.append(T)
    return 0.0


PRyMmain = _configure_PRyMordial(small_network=True)
t0 = time.time()
res = PRyMmain.PRyMclass(rec, rec, rec).PRyMresults()
pos = [T for T in Ts if T > 0]
print(
    f"calls={len(Ts)} min_T_pos={min(pos)*1e3:.4g} keV max_T={max(Ts):.4g} MeV "
    f"n_negative={sum(1 for T in Ts if T<0)} wall={time.time()-t0:.1f}s"
)
print(
    f"calls below 1 keV: {sum(1 for T in pos if T < 1e-3)}; below 3 keV: {sum(1 for T in pos if T < 3e-3)}"
)
