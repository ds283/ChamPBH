import time
from math import log
from typing import Optional

from CosmologyConcepts import TemperatureLike, GetTemperature
from Quadrature.supervisors.base import IntegrationSupervisor, DEFAULT_UPDATE_INTERVAL
from Units.base import UnitsLike
from utilities import format_time


class ScalarFieldIntegrationSupervisor(IntegrationSupervisor):
    def __init__(
        self,
        units: UnitsLike,
        T_init: TemperatureLike,
        T_stop: TemperatureLike,
        label: str,
        notify_interval: int = DEFAULT_UPDATE_INTERVAL,
    ):
        super().__init__(notify_interval)

        self._units = units

        self._label: str = label

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
            f"|    current T_Jordan = {T_GeV:.5g} GeV or {T_Kelvin:.5g} K | target T_Jordan = {self._T_stop_GeV:.5g} GeV or {self._T_stop_Kelvin:.5g} K"
        )
        print(
            f"|    current log(T_J/GeV) = {log_T_GeV:.5g}, init log(T_J/GeV) = {self._log_T_init_GeV:.5g}, final log(T_J/GeV) = {self._log_T_stop_GeV:.5g}, {1.0-percent_remain:.3%} complete measured in T_J"
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
                f"|    log_T_GeV advance since last update: Delta(log(TJ/GeV)) = {log_T_GeV_delta:.5g}"
            )
        print(
            f"|    {self.RHS_evaluations} RHS evaluations, mean {self.mean_RHS_time:.5g}s per evaluation, min RHS time = {self.min_RHS_time:.5g}s, max RHS time = {self.max_RHS_time:.5g}s"
        )
        print(f"|    {msg}")

        self._new_hard_reflection_events = []
        self._last_log_T_GeV = log_T_GeV

    def notify_hard_reflection(self, N: float):
        self._hard_reflection_events.append(N)
        self._new_hard_reflection_events.append(N)

    @property
    def number_hard_reflections(self):
        return len(self._hard_reflection_events)
