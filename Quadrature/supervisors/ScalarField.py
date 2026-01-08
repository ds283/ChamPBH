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

        self._log_T_init_GeV = log(self._T_init / units.GeV)
        self._log_T_stop_GeV = log(self._T_stop / units.GeV)
        self._log_T_GeV_range = self._log_T_init_GeV - self._log_T_stop_GeV

        self._last_log_T_GeV: Optional[float] = None

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

        T_GeV = T_Jordan / self._GeV
        T_Kelvin = T_Jordan / self._Kelvin

        log_T_GeV = log(T_GeV)
        log_T_GeV_remain = log_T_GeV - self._log_T_stop_GeV
        percent_complete = log_T_GeV_remain / self._log_T_GeV_range

        print(
            f"** {self._label} - STATUS UPDATE #{update_number}: integration has been running for {format_time(since_start)} ({format_time(since_last_notify)} since last notification)"
        )
        print(
            f"|    current T_Jordan = {T_GeV:.5g} GeV or {T_Kelvin:.5g} K | init log(T_J/GeV) = {self._log_T_init_GeV:.5g}, final log(T_J/GeV) = {self._log_T_stop_GeV:.5g}, {percent_complete:.3%} complete"
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

        self._last_log_T_GeV = log_T_GeV
