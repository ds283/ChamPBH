"""Prompt 06 scratch probe: worst derivative disagreements on the 60-point grid,
computed exactly as test_temperature_law cases 3 and 4 compute them."""

import numpy as np
from CosmologyModels.tests.eos_reference import (
    production_eos,
    derivative_test_grid_GeV,
    central_dG_s_dlogT,
)
from CosmologyModels.GenericEOS.SaikawaShirai_EOS_spline import SaikawaShirai_EOS_spline
from CosmologyModels.GenericEOS.SaikawaShirai_EOS_jax_autodiff import (
    SaikawaShirai_EOS_jax_autodiff,
)
from Units import GeV_units

u = GeV_units()
eos = production_eos(u)
spline = SaikawaShirai_EOS_spline(u)
jax = SaikawaShirai_EOS_jax_autodiff(u)
grid = derivative_test_grid_GeV()
c_err, j_err, dmax = [], [], 0.0
for T in grid:
    Tu = T * u.GeV
    d = float(eos.dG_s_dlogT(Tu))
    g = float(eos.G_s(Tu))
    c_err.append(abs(d - central_dG_s_dlogT(eos, T)) / g)
    ds = float(spline.dG_s_dlogT(Tu))
    j_err.append(abs(ds - float(jax.dG_s_dlogT(Tu))) / float(spline.G_s(Tu)))
    dmax = max(dmax, abs(d / g))
c_err, j_err = np.array(c_err), np.array(j_err)
print(f"grid points {len(grid)}; max |d ln g_s/d ln T| = {dmax:.4g}")
print(
    f"central: worst {c_err.max():.3e} at {grid[c_err.argmax()]:.4g} GeV; over 1e-6: {(c_err>1e-6).sum()}"
)
print(
    f"jax:     worst {j_err.max():.3e} at {grid[j_err.argmax()]:.4g} GeV; over 1e-6: {(j_err>1e-6).sum()}"
)
