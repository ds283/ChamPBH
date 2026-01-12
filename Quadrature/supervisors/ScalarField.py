import time
from collections import namedtuple
from math import log
from typing import Optional

from CosmologyConcepts import TemperatureLike, GetTemperature
from Quadrature.supervisors.base import IntegrationSupervisor, DEFAULT_UPDATE_INTERVAL
from Units.base import UnitsLike
from utilities import format_time

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
            print(f"|    MEAN VALUES OF RHS VECTOR:")
            print(
                f"|      phi_E={self._total_RHS_values.phi_Einstein / self._RHS_evaluations / self._units.PlanckMass:.5g}, pi_E={self._total_RHS_values.pi_Einstein / self._RHS_evaluations / self._units.PlanckMass}, log_rhorad_E={self._total_RHS_values.log_rhorad_Einstein/self._RHS_evaluations:.5g}, log_fm={self._total_RHS_values.log_fm/self._RHS_evaluations:.5g}, log_T_J={self._total_RHS_values.log_T_Jordan/self._RHS_evaluations:.5g}"
            )
            print(f"|    LARGEST VALUES OF RHS VECTOR:")
            print(
                f"|      phi_E={self._largest_RHS_values.phi_Einstein/self._units.PlanckMass:.5g}, pi_E={self._largest_RHS_values.pi_Einstein/self._units.PlanckMass}, log_rhorad_E={self._largest_RHS_values.log_rhorad_Einstein:.5g}, log_fm={self._largest_RHS_values.log_fm:.5g}, log_T_J={self._largest_RHS_values.log_T_Jordan:.5g}"
            )
            print(f"|    SMALLEST VALUES OF RHS VECTOR:")
            print(
                f"|      phi_E={self._smallest_RHS_values.phi_Einstein/self._units.PlanckMass:.5g}, pi_E={self._smallest_RHS_values.pi_Einstein/self._units.PlanckMass}, log_rhorad_E={self._smallest_RHS_values.log_rhorad_Einstein:.5g}, log_fm={self._smallest_RHS_values.log_fm:.5g}, log_T_J={self._smallest_RHS_values.log_T_Jordan:.5g}"
            )

    def notify_hard_reflection(self, N: float):
        self._hard_reflection_events.append(N)
        self._new_hard_reflection_events.append(N)

    @property
    def number_hard_reflections(self):
        return len(self._hard_reflection_events)

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

        self._largest_RHS_values = StateVector(
            phi_Einstein=(
                RHS.phi_Einstein
                if self._largest_RHS_values.phi_Einstein is None
                else max(self._largest_RHS_values.phi_Einstein, RHS.phi_Einstein)
            ),
            pi_Einstein=(
                RHS.pi_Einstein
                if self._largest_RHS_values.pi_Einstein is None
                else max(self._largest_RHS_values.pi_Einstein, RHS.pi_Einstein)
            ),
            log_rhorad_Einstein=(
                RHS.log_rhorad_Einstein
                if self._largest_RHS_values.log_rhorad_Einstein is None
                else max(
                    self._largest_RHS_values.log_rhorad_Einstein,
                    RHS.log_rhorad_Einstein,
                )
            ),
            log_fm=(
                RHS.log_fm
                if self._largest_RHS_values.log_fm is None
                else max(self._largest_RHS_values.log_fm, RHS.log_fm)
            ),
            log_T_Jordan=(
                RHS.log_T_Jordan
                if self._largest_RHS_values.log_T_Jordan is None
                else max(self._largest_RHS_values.log_T_Jordan, RHS.log_T_Jordan)
            ),
        )

        self._smallest_RHS_values = StateVector(
            phi_Einstein=(
                RHS.phi_Einstein
                if self._smallest_RHS_values.phi_Einstein is None
                else min(self._smallest_RHS_values.phi_Einstein, RHS.phi_Einstein)
            ),
            pi_Einstein=(
                RHS.pi_Einstein
                if self._smallest_RHS_values.pi_Einstein is None
                else min(self._smallest_RHS_values.pi_Einstein, RHS.pi_Einstein)
            ),
            log_rhorad_Einstein=(
                RHS.log_rhorad_Einstein
                if self._smallest_RHS_values.log_rhorad_Einstein is None
                else min(
                    self._smallest_RHS_values.log_rhorad_Einstein,
                    RHS.log_rhorad_Einstein,
                )
            ),
            log_fm=(
                RHS.log_fm
                if self._smallest_RHS_values.log_fm is None
                else min(self._smallest_RHS_values.log_fm, RHS.log_fm)
            ),
            log_T_Jordan=(
                RHS.log_T_Jordan
                if self._smallest_RHS_values.log_T_Jordan is None
                else min(self._smallest_RHS_values.log_T_Jordan, RHS.log_T_Jordan)
            ),
        )

        self._total_RHS_values = StateVector(
            phi_Einstein=(self._total_RHS_values.phi_Einstein + RHS.phi_Einstein),
            pi_Einstein=(self._total_RHS_values.pi_Einstein + RHS.pi_Einstein),
            log_rhorad_Einstein=(
                self._total_RHS_values.log_rhorad_Einstein + RHS.log_rhorad_Einstein
            ),
            log_fm=(self._total_RHS_values.log_fm + RHS.log_fm),
            log_T_Jordan=(self._total_RHS_values.log_T_Jordan + RHS.log_T_Jordan),
        )
