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

import time
from collections import namedtuple
from datetime import datetime
from math import log, exp
from typing import Optional, Dict, List

from CosmologyConcepts import TemperatureLike, GetTemperature
from Quadrature.supervisors.base import IntegrationSupervisor, DEFAULT_UPDATE_INTERVAL
from Units.base import UnitsLike
from utilities import format_time, to_float

# use named tuples ensures that we never get the fields of the state vector in the wrong order
StateVector = namedtuple(
    "StateVector",
    [
        "phi_Einstein",
        "pi_Einstein",
        "log_rhorad_Einstein",
        "log_fm",
        "log_T_Jordan",
    ],
)


class ScalarFieldIntegrationSupervisor(IntegrationSupervisor):
    def __init__(
        self,
        units: UnitsLike,
        T_init: TemperatureLike,
        T_stop: TemperatureLike,
        max_step_size,
        label: str,
        notify_interval: int = DEFAULT_UPDATE_INTERVAL,
        collect_full_statistics: bool = False,
    ):
        super().__init__(notify_interval)

        self._units: UnitsLike = units

        self._label: str = label
        self._collect_full_statistics: bool = collect_full_statistics
        self._max_step_size: float = max_step_size

        self._T_init: float = GetTemperature(T_init)
        self._T_stop: float = GetTemperature(T_stop)
        self._log_T_stop: float = log(self._T_stop)

        self._T_stop_GeV: float = self._T_stop / units.GeV
        self._T_stop_Kelvin: float = self._T_stop / units.Kelvin

        self._log_T_init_GeV: float = log(self._T_init / units.GeV)
        self._log_T_stop_GeV: float = log(self._T_stop / units.GeV)
        self._log_T_GeV_range: float = self._log_T_init_GeV - self._log_T_stop_GeV

        self._last_log_T_GeV: Optional[float] = None

        ## TRACK EVOLVING STATE VARIABLES REPORTED BY EVENT FINDERS

        self._event_finder_last_log_T_Jordan: Optional[float] = None

        ## TRACK REFLECTION EVENTS AND LEVEL 1/2 REGIONS

        # track when we impose a "manual" hard reflection - this happens when we detect phi_E crossing zero
        self._hard_reflection_data = {"all": [], "new": []}
        self._level_1_data = {
            "entry": {"all": [], "new": []},
            "exit": {"all": [], "new": []},
        }
        self._level_2_data = {
            "entry": {"all": [], "new": []},
            "exit": {"all": [], "new": []},
        }

        self._in_level_1: bool = False
        self._in_level_2: bool = False

        ## TRACK STATISTICS OF RHS REPORTS

        self._largest_RHS_values: StateVector = StateVector(
            None, None, None, None, None
        )
        self._smallest_RHS_values: StateVector = StateVector(
            None, None, None, None, None
        )
        self._total_RHS_values: StateVector = StateVector(0.0, 0.0, 0.0, 0.0, 0.0)

        ## UNITS CONVERSIONS

        self._GeV = units.GeV
        self._Kelvin = units.Kelvin

    def __enter__(self):
        super().__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)

    def message(self, T_Jordan: TemperatureLike, msg: str):
        current_time = time.time()

        since_last_notify = current_time - self._last_notify
        since_start = current_time - self._start_time

        update_number = self.report_notify()

        T_Jordan_float = GetTemperature(T_Jordan)

        T_GeV = T_Jordan_float / self._GeV
        T_Kelvin = T_Jordan_float / self._Kelvin

        log_T_GeV = log(T_GeV)
        log_T_GeV_remain = log_T_GeV - self._log_T_stop_GeV
        percent_remain = log_T_GeV_remain / self._log_T_GeV_range

        level_state = None
        if self._in_level_2:
            level_state = "level 2"
        elif self._in_level_1:
            level_state = "level 1"

        print(
            f"** STATUS UPDATE #{update_number} - {datetime.now().strftime("%a %d %b %Y %H:%M:%S")} - {self._label}"
        )
        print(
            f"|    integration has been running for {format_time(since_start)} ({format_time(since_last_notify)} since last notification) | current max step size dN={self._max_step_size:.5g}{" | in " + level_state if level_state is not None else ""}"
        )
        print(
            f"|    current T_Jordan = {T_GeV:.5g} GeV or {T_Kelvin:.5g} K, log(T_Jordan/GeV) = {log_T_GeV:.5g} | {1.0-percent_remain:.3%} complete measured in T_Jordan"
        )
        print(
            f"|    target T_Jordan = {self._T_stop_GeV:.5g} GeV or {self._T_stop_Kelvin:.5g} K, log(T_Jordan/GeV) = {self._log_T_stop_GeV:.5g}"
        )

        def events_status(label: str, events: Dict[str, List[float]]):
            num_events = len(events["all"])

            if num_events > 0:
                num_new_events = len(events["new"])

                if num_new_events > 0:
                    if num_new_events <= 20:
                        formatted_event_times = [f"{N:.5g}" for N in events["new"]]
                    else:
                        early_event_times = [f"{N:.5g}" for N in events["new"][:10]]
                        late_event_times = [f"{N:.5g}" for N in events["new"][-10:]]
                        formatted_event_times = (
                            early_event_times + ["..."] + late_event_times
                        )
                    print(
                        f"|    {num_events} {label} events, {num_new_events} since last update: N = [{', '.join(formatted_event_times)}]"
                    )
                else:
                    print(f"|    {num_events} {label} events, none since last update")

        events_status("hard reflection", self._hard_reflection_data)
        events_status("level 1 entry", self._level_1_data["entry"])
        events_status("level 1 exit", self._level_1_data["exit"])
        events_status("level 2 entry", self._level_2_data["entry"])
        events_status("level 2 exit", self._level_2_data["exit"])

        if self._last_log_T_GeV is not None:
            log_T_GeV_delta = self._last_log_T_GeV - log_T_GeV
            print(
                f"|    log_T_GeV advance since last update: Delta(log(T_Jordan/GeV)) = {log_T_GeV_delta:.5g}"
            )
        print(
            f"|    {self.RHS_evaluations} RHS evaluations, mean {self.mean_RHS_time:.5g}s per evaluation, min RHS time = {self.min_RHS_time:.5g}s, max RHS time = {self.max_RHS_time:.5g}s"
        )
        print(f"|    {msg}")

        if self._collect_full_statistics:
            mean_values = self.mean_RHS_values
            largest_values = self.largest_RHS_values
            smallest_values = self.smallest_RHS_values

            print(f"|    MEAN VALUES OF RHS VECTOR:")
            print(
                f"|      d(phi_E)/dN={mean_values.phi_Einstein / self._units.PlanckMass:.5g} Mp, d(pi_E)/dN={mean_values.pi_Einstein / self._units.PlanckMass:.5g} Mp, d(log_rhorad_E)/dN={mean_values.log_rhorad_Einstein:.5g}, d(log_fm)/dN={mean_values.log_fm:.5g}, d(log_T_Jordan)/dN={mean_values.log_T_Jordan:.5g}"
            )
            print(
                f'|      d(phi_E)/dN={mean_values.phi_Einstein:.5g} raw, d(pi_E)/dN={mean_values.pi_Einstein:.5g} raw | values in the current units system "{self._units.system_name}"'
            )
            print(f"|    LARGEST VALUES OF RHS VECTOR:")
            print(
                f"|      d(phi_E)/dN={largest_values.phi_Einstein/self._units.PlanckMass:.5g} Mp, d(pi_E)/dN={largest_values.pi_Einstein/self._units.PlanckMass:.5g} Mp, d(log_rhorad_E)/dN={largest_values.log_rhorad_Einstein:.5g}, d(log_fm)/dN={largest_values.log_fm:.5g}, d(log_T_J)/dN={largest_values.log_T_Jordan:.5g}"
            )
            print(f"|    SMALLEST VALUES OF RHS VECTOR:")
            print(
                f"|      d(phi_E)/dN={smallest_values.phi_Einstein/self._units.PlanckMass:.5g} Mp, d(pi_E)/dN={smallest_values.pi_Einstein/self._units.PlanckMass:.5g} Mp, d(log_rhorad_E)/dN={smallest_values.log_rhorad_Einstein:.5g}, d(log_fm)/dN={smallest_values.log_fm:.5g}, d(log_T_J)/dN={smallest_values.log_T_Jordan:.5g}"
            )

    def notify_hard_reflection(self, N):
        N_as_float = to_float(N)
        self._hard_reflection_data["all"].append(N_as_float)
        self._hard_reflection_data["new"].append(N_as_float)

    def notify_level_1_entry(self, N, max_step_size):
        N_as_float = to_float(N)
        self._level_1_data["entry"]["all"].append(N_as_float)
        self._level_1_data["entry"]["new"].append(N_as_float)

        self._max_step_size = max_step_size

        self._in_level_1 = True
        self._in_level_2 = False

    def notify_level_1_exit(self, N, max_step_size):
        N_as_float = to_float(N)
        self._level_1_data["exit"]["all"].append(N_as_float)
        self._level_1_data["exit"]["new"].append(N_as_float)

        self._max_step_size = max_step_size

        self._in_level_1 = False
        self._in_level_2 = False

    def notify_level_2_entry(self, N, max_step_size):
        N_as_float = to_float(N)
        self._level_2_data["entry"]["all"].append(N_as_float)
        self._level_2_data["entry"]["new"].append(N_as_float)

        self._max_step_size = max_step_size

        self._in_level_1 = True
        self._in_level_2 = True

    def notify_level_2_exit(self, N, max_step_size):
        N_as_float = to_float(N)
        self._level_2_data["exit"]["all"].append(N_as_float)
        self._level_2_data["exit"]["new"].append(N_as_float)

        self._max_step_size = max_step_size

        self._in_level_1 = True
        self._in_level_2 = False

    @property
    def number_hard_reflections(self) -> int:
        return len(self._hard_reflection_data["all"])

    @property
    def number_level_1_entries(self) -> int:
        return len(self._level_1_data["entry"]["all"])

    @property
    def number_level_1_exits(self) -> int:
        return len(self._level_1_data["exit"]["all"])

    @property
    def number_level_2_entries(self) -> int:
        return len(self._level_2_data["entry"]["all"])

    @property
    def number_level_2_exits(self) -> int:
        return len(self._level_2_data["exit"]["all"])

    @property
    def collect_full_statistics(self) -> bool:
        return self._collect_full_statistics

    @property
    def largest_RHS_values(self) -> StateVector:
        return self._largest_RHS_values

    @property
    def smallest_RHS_values(self) -> StateVector:
        return self._smallest_RHS_values

    @property
    def mean_RHS_values(self) -> StateVector:
        if self._RHS_evaluations == 0:
            return StateVector(0.0, 0.0, 0.0, 0.0, 0.0)

        return StateVector(
            phi_Einstein=self._total_RHS_values.phi_Einstein / self._RHS_evaluations,
            pi_Einstein=self._total_RHS_values.pi_Einstein / self._RHS_evaluations,
            log_rhorad_Einstein=self._total_RHS_values.log_rhorad_Einstein
            / self._RHS_evaluations,
            log_fm=self._total_RHS_values.log_fm / self._RHS_evaluations,
            log_T_Jordan=self._total_RHS_values.log_T_Jordan / self._RHS_evaluations,
        )

    def reset_notify_time(self, T_Jordan: TemperatureLike):
        super().reset_notify_time()

        self._hard_reflection_data["new"] = []
        self._level_1_data["entry"]["new"] = []
        self._level_1_data["exit"]["new"] = []
        self._level_2_data["entry"]["new"] = []
        self._level_2_data["exit"]["new"] = []

        T_Jordan_float = GetTemperature(T_Jordan)
        T_GeV = T_Jordan_float / self._GeV
        log_T_GeV = log(T_GeV)

        self._last_log_T_GeV = log_T_GeV

    def notify_new_RHS(self, RHS: StateVector):
        if not self._collect_full_statistics:
            return

        phi_Einstein_float = to_float(RHS.phi_Einstein)
        pi_Einstein_float = to_float(RHS.pi_Einstein)
        log_rhorad_Einstein_float = to_float(RHS.log_rhorad_Einstein)
        log_fm_float = to_float(RHS.log_fm)
        log_T_Jordan_float = to_float(RHS.log_T_Jordan)

        self._largest_RHS_values = StateVector(
            phi_Einstein=(
                phi_Einstein_float
                if self._largest_RHS_values.phi_Einstein is None
                else max(self._largest_RHS_values.phi_Einstein, phi_Einstein_float)
            ),
            pi_Einstein=(
                pi_Einstein_float
                if self._largest_RHS_values.pi_Einstein is None
                else max(self._largest_RHS_values.pi_Einstein, pi_Einstein_float)
            ),
            log_rhorad_Einstein=(
                log_rhorad_Einstein_float
                if self._largest_RHS_values.log_rhorad_Einstein is None
                else max(
                    self._largest_RHS_values.log_rhorad_Einstein,
                    log_rhorad_Einstein_float,
                )
            ),
            log_fm=(
                log_fm_float
                if self._largest_RHS_values.log_fm is None
                else max(self._largest_RHS_values.log_fm, log_fm_float)
            ),
            log_T_Jordan=(
                log_T_Jordan_float
                if self._largest_RHS_values.log_T_Jordan is None
                else max(self._largest_RHS_values.log_T_Jordan, log_T_Jordan_float)
            ),
        )

        self._smallest_RHS_values = StateVector(
            phi_Einstein=(
                phi_Einstein_float
                if self._smallest_RHS_values.phi_Einstein is None
                else min(self._smallest_RHS_values.phi_Einstein, phi_Einstein_float)
            ),
            pi_Einstein=(
                pi_Einstein_float
                if self._smallest_RHS_values.pi_Einstein is None
                else min(self._smallest_RHS_values.pi_Einstein, pi_Einstein_float)
            ),
            log_rhorad_Einstein=(
                log_rhorad_Einstein_float
                if self._smallest_RHS_values.log_rhorad_Einstein is None
                else min(
                    self._smallest_RHS_values.log_rhorad_Einstein,
                    log_rhorad_Einstein_float,
                )
            ),
            log_fm=(
                log_fm_float
                if self._smallest_RHS_values.log_fm is None
                else min(self._smallest_RHS_values.log_fm, log_fm_float)
            ),
            log_T_Jordan=(
                log_T_Jordan_float
                if self._smallest_RHS_values.log_T_Jordan is None
                else min(self._smallest_RHS_values.log_T_Jordan, log_T_Jordan_float)
            ),
        )

        self._total_RHS_values = StateVector(
            phi_Einstein=(self._total_RHS_values.phi_Einstein + phi_Einstein_float),
            pi_Einstein=(self._total_RHS_values.pi_Einstein + pi_Einstein_float),
            log_rhorad_Einstein=(
                self._total_RHS_values.log_rhorad_Einstein + log_rhorad_Einstein_float
            ),
            log_fm=(self._total_RHS_values.log_fm + log_fm_float),
            log_T_Jordan=(self._total_RHS_values.log_T_Jordan + log_T_Jordan_float),
        )

    def event_finder_notify_new_log_T_Jordan(self, log_T_Jordan: float):
        if log_T_Jordan < self._log_T_stop:
            T_Jordan = exp(log_T_Jordan)
            print(
                f"!! ScalarFieldIntegrationSupervisor: notified of log_T_Jordan={log_T_Jordan:.5g} (T_Jordan={T_Jordan/self._GeV:.5g} GeV={T_Jordan/self._Kelvin:.5g} K), which is smaller than terminal value log_T_stop={self._log_T_stop:.5g}"
            )

            if self._event_finder_last_log_T_Jordan is not None:
                last_T_Jordan = exp(self._event_finder_last_log_T_Jordan)
                print(
                    f"   -- NOTE: previously notified value was log_T_Jordan={self._event_finder_last_log_T_Jordan:.5g} (T_Jordan={last_T_Jordan / self._GeV:.5g} GeV={last_T_Jordan / self._Kelvin:.5g} K)"
                )

        self._event_finder_last_log_T_Jordan = log_T_Jordan
