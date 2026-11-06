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
The dark-radiation component rho_chi of a scalar history (inelastic-reflection prompt 01;
README §6.2 P1).

The quanta that a reflection produces are carried as dark radiation: massless, collisionless,
homogeneous and isotropic, so that their Einstein-frame energy density obeys

    rho_chi,E(N) = K(N) e^{-4N},

with N = N_E = ln a_E the integrator's own variable, counted from N_start, and K(N) piecewise
constant. K changes only at an injection (inelastic-reflection prompt 02), where the integrator
restarts Radau, so the state vector stays five-dimensional.

    K(N) = K_0 + sum_{N_j <= N} Delta K_j

K_0 is the history's initial condition: present at every N, below N_start as well
(inelastic-reflection prompt 01, orchestrator ruling 1). The only way to give a history a
nonzero K_0 in prompt 01 is the test harness `rho_chi_ratio_init` of integrate_history and
tune_history. The injections (N_j, Delta K_j), with Delta K_j = Delta rho_chi,E,j e^{4 N_j}, are
added by add() in increasing N_j.

The rule N_j <= N is deliberate: after an injection the new Radau instance evaluates the RHS at
N_j itself, with the new K. A dense-output interpolant that ends at N_j holds the state before
the injection, and evaluated at N_j it would be paired with the new K; that edge belongs to
prompt 02.

The ratio r_chi = rho_chi,E / rho_R,E is the same in both frames (both densities carry A^{-4}),
and is computed in logs, exp(ln K - 4N - ln rho_R,E), so that no intermediate overflows.
"""

from bisect import bisect_right
from math import exp, isfinite, log
from typing import Optional, Tuple


class DarkRadiationLedger:
    """
    The ledger of the dark-radiation component: K_0 and the injections (N_j, Delta K_j), in
    increasing N_j. It returns K(N), rho_chi,E(N) and r_chi(N), and answers whether K(N) is zero
    without an exp or a log, so that a caller can take the old code path exactly.

    It holds Python floats and lists only, so it pickles (Ray returns it in the payload).
    """

    def __init__(self, K_initial: float = 0.0):
        """
        :param K_initial: K_0, the constant part of K: rho_chi,E e^{4N}, present at every N. Zero
                          (the default) for a history with no initial rho_chi. Must be finite and
                          non-negative.
        """
        K_initial = float(K_initial)
        if not isfinite(K_initial) or K_initial < 0.0:
            raise ValueError(
                f"DarkRadiationLedger: K_initial must be finite and non-negative, not {K_initial!r}"
            )

        self._K_initial: float = K_initial
        self._log_K_initial: Optional[float] = (
            log(K_initial) if K_initial > 0.0 else None
        )

        # the injections, in increasing N_j; _K[j] and _log_K[j] are K and ln K on
        # [N_j, N_{j+1}), i.e. K_0 plus the injections 0 ... j
        self._N: list = []
        self._delta_K: list = []
        self._K: list = []
        self._log_K: list = []

    # ---- read-only views (02 checks sum Delta K_j against its drains) ----

    @property
    def K_initial(self) -> float:
        """K_0."""
        return self._K_initial

    @property
    def injections(self) -> Tuple[Tuple[float, float], ...]:
        """The injections, as (N_j, Delta K_j) pairs in increasing N_j."""
        return tuple(zip(self._N, self._delta_K))

    @property
    def cumulative_K(self) -> Tuple[float, ...]:
        """
        K after each injection, in the order of injections: entry j is K_0 plus the injections 0 ... j, the
        float add() computed for it (one-loop-drain prompt 03; orchestrator note 03, ruling 6), so that a
        caller reads K just before or after an entry by position, never re-summed.
        """
        return tuple(self._K)

    @property
    def is_empty(self) -> bool:
        """True iff K_0 = 0 and there are no injections: K(N) = 0 at every N."""
        return self._K_initial == 0.0 and not self._N

    # ---- the interface ----

    def add(self, N: float, delta_K: float) -> None:
        """
        Append an injection Delta K at N. N below the last injection's N is a ValueError (equal
        is allowed); so is a Delta K that is negative or not finite, or an N that is not finite.
        """
        N = float(N)
        delta_K = float(delta_K)
        if not isfinite(N):
            raise ValueError(f"DarkRadiationLedger.add: N must be finite, not {N!r}")
        if not isfinite(delta_K) or delta_K < 0.0:
            raise ValueError(
                f"DarkRadiationLedger.add: delta_K must be finite and non-negative, not {delta_K!r}"
            )
        if self._N and N < self._N[-1]:
            raise ValueError(
                f"DarkRadiationLedger.add: injection at N={N!r} is below the last one, "
                f"at N={self._N[-1]!r}"
            )

        K_before = self._K[-1] if self._K else self._K_initial
        K = K_before + delta_K

        self._N.append(N)
        self._delta_K.append(delta_K)
        self._K.append(K)
        self._log_K.append(log(K) if K > 0.0 else None)

    def _segment(self, N: float) -> int:
        """The number of injections with N_j <= N."""
        return bisect_right(self._N, N) if self._N else 0

    def K(self, N: float) -> float:
        """K(N) = K_0 + sum_{N_j <= N} Delta K_j."""
        j = self._segment(N)
        return self._K_initial if j == 0 else self._K[j - 1]

    def is_zero(self, N: float) -> bool:
        """True iff K(N) = 0. No exp and no log."""
        if not self._N:
            return self._K_initial == 0.0
        return self.K(N) == 0.0

    def _log_K_at(self, N: float) -> Optional[float]:
        j = self._segment(N)
        return self._log_K_initial if j == 0 else self._log_K[j - 1]

    def rho_chi(self, N: float) -> float:
        """rho_chi,E(N) = K(N) e^{-4N}, computed as exp(ln K - 4N); 0.0 where K(N) = 0."""
        log_K = self._log_K_at(N)
        if log_K is None:
            return 0.0
        return exp(log_K - 4.0 * N)

    def ratio(self, N: float, log_rhorad_Einstein: float) -> float:
        """
        r_chi(N) = rho_chi,E(N) / rho_R,E, given ln rho_R,E, computed as
        exp(ln K - 4N - ln rho_R,E); 0.0 where K(N) = 0.
        """
        log_K = self._log_K_at(N)
        if log_K is None:
            return 0.0
        return exp(log_K - 4.0 * N - log_rhorad_Einstein)

    def __repr__(self) -> str:
        return (
            f"DarkRadiationLedger(K_initial={self._K_initial!r}, "
            f"injections={list(self.injections)!r})"
        )
