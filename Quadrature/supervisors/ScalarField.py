import time
from collections import namedtuple
from math import log
from typing import Optional

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
        label: str,
        notify_interval: int = DEFAULT_UPDATE_INTERVAL,
        collect_full_statistics: bool = False,
    ):
        super().__init__(notify_interval)

        self._units = units

        self._label: str = label
        self._collect_full_statistics: bool = collect_full_statistics

        self._T_init = GetTemperature(T_init)
        self._T_stop = GetTemperature(T_stop)

        self._T_stop_GeV = self._T_stop / units.GeV
        self._T_stop_Kelvin = self._T_stop / units.Kelvin

        self._log_T_init_GeV = log(self._T_init / units.GeV)
        self._log_T_stop_GeV = log(self._T_stop / units.GeV)
        self._log_T_GeV_range = self._log_T_init_GeV - self._log_T_stop_GeV

        self._last_log_T_GeV: Optional[float] = None

        # track when we impose a "manual" hard reflection - this happens when we detect phi_E crossing zero
        self._hard_reflection_events = []
        self._new_hard_reflection_events = []

        self._GeV = units.GeV
        self._Kelvin = units.Kelvin

        self._largest_RHS_values: StateVector = StateVector(
            None, None, None, None, None
        )
        self._smallest_RHS_values: StateVector = StateVector(
            None, None, None, None, None
        )
        self._total_RHS_values: StateVector = StateVector(0.0, 0.0, 0.0, 0.0, 0.0)

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

        print(
            f"** STATUS UPDATE #{update_number} - {self._label}: integration has been running for {format_time(since_start)} ({format_time(since_last_notify)} since last notification)"
        )
        print(
            f"|    current T_Jordan = {T_GeV:.5g} GeV or {T_Kelvin:.5g} K, log(T_Jordan/GeV) = {log_T_GeV:.5g} | {1.0-percent_remain:.3%} complete measured in T_Jordan"
        )
        print(
            f"|    target T_Jordan = {self._T_stop_GeV:.5g} GeV or {self._T_stop_Kelvin:.5g} K, log(T_Jordan/GeV) = {self._log_T_stop_GeV:.5g}"
        )
        num_hard_reflection_events = len(self._hard_reflection_events)
        if num_hard_reflection_events > 0:
            num_new_hard_reflection_events = len(self._new_hard_reflection_events)
            if num_new_hard_reflection_events > 0:
                formatted_event_times = [
                    f"{N:.5g}" for N in self._new_hard_reflection_events
                ]
                print(
                    f"|    {num_hard_reflection_events} hard reflection events, {num_new_hard_reflection_events} since last update: N = [{", ".join(formatted_event_times)}]"
                )
            else:
                print(
                    f"|    {num_hard_reflection_events} hard reflection events, none since last update"
                )
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
                f"|      phi_E={largest_values.phi_Einstein/self._units.PlanckMass:.5g} Mp, pi_E={largest_values.pi_Einstein/self._units.PlanckMass:.5g} Mp, log_rhorad_E={largest_values.log_rhorad_Einstein:.5g}, log_fm={largest_values.log_fm:.5g}, log_T_J={largest_values.log_T_Jordan:.5g}"
            )
            print(f"|    SMALLEST VALUES OF RHS VECTOR:")
            print(
                f"|      phi_E={smallest_values.phi_Einstein/self._units.PlanckMass:.5g} Mp, pi_E={smallest_values.pi_Einstein/self._units.PlanckMass:.5g} Mp, log_rhorad_E={smallest_values.log_rhorad_Einstein:.5g}, log_fm={smallest_values.log_fm:.5g}, log_T_J={smallest_values.log_T_Jordan:.5g}"
            )

    def notify_hard_reflection(self, N: float):
        self._hard_reflection_events.append(N)
        self._new_hard_reflection_events.append(N)

    @property
    def number_hard_reflections(self) -> int:
        return len(self._hard_reflection_events)

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
        self._new_hard_reflection_events = []

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
