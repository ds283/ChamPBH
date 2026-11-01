# diagnostic — the export and dissipation at large M

**Made 2026-11-01, after the run**, at the user's request, by
[`../run_diagnostic_large_M.sh`](../run_diagnostic_large_M.sh) at `2c29cd3` on a clean tree, on
ChamPBH's cluster (started and stopped by the Claude Code session), 22:33–22:43 UTC. It wrote
nothing to the run's two stores.

1. **The export.** `plot_by_beta.py` on the main store again, into
   `var/science-2026.8.0/diagnostic/out/main-export/`, at `3cb4374`'s `CSV_COLUMNS`: the run's 27
   columns, identical cell for cell to the run's `histories.csv`, and eight more (injections,
   injected fraction, parking and release T_J in GeV, the failing margin shares). Read-only,
   unregistered.
2. **Dissipation at large M.** α = 0.08 at M = 10⁻⁹, 10⁻⁶, 10⁻³ and 0.5 M_P, β = 1.2, 2.0, 2.9:
   12 histories, all validated, with BBN, in `var/datastores/science-2026.8.0-large-M-dissipation.db`.
   Registered as `science-2026.8.0-X-X-large-M-dissipation-20261101T223930`, done, store digest
   `f3c792c2aff28c2f…` (`../registry/`). Then `plot_ScalarHistory.py` on that store, into
   `diagnostic/out/large-M-dissipation/`. The run computed large M in natural mode only, by the
   user's ruling for science output; these histories are a diagnostic of the reflection figure.

`probe-out/` holds the probe that preceded it: the three M = 0.5 histories through
`tools/history_and_bbn.py`, three at a time under a 900 s timeout, by `probe_M05.py` (a scratch
script, kept here as run), at `26c376a`. They took 29–60 s and completed. The code warns there that
the test-field ratio R ≈ 10⁻⁶¹ at the first impact, so the reflections are expected to be nearly
elastic, and that the relativistic margins fail. `logs/` holds the diagnostic's logs, unchanged.

## What it found

**At fixed α the dark radiation does not depend on M**, from the laboratory value to 10⁻³ M_P:
ρ_χ/ρ_R at the end of the history agrees to 5×10⁻⁴ relative across 4×10⁻³¹, 10⁻¹⁵, 10⁻¹², 10⁻⁹,
10⁻⁶ and 10⁻³ M_P at each β (7.17×10⁻³, 5.69×10⁻³ and 4.43×10⁻³ at β = 1.2, 2.0, 2.9), and at
M = 0.5 it is 1–3% lower. The field's mean kinetic fraction in the BBN window agrees likewise, except
at M = 0.5. In this code α is an input, so the dark radiation a history deposits is set by α, not
by M.

At M = 0.5 the yields with dissipation are those of natural mode there: Y_p = 0.144, 0.133, 0.121
(−42%, −46%, −51%) at β = 1.2, 2.0, 2.9 (probe), against −40%, −46%, −51% in natural mode (the
run). At large M the yields are set by something other than the reflections.

**Parking and exclusion at the laboratory M**, from the export (histories with yields, α > 0):
no history that parks above 10 keV is excluded by the run's tests (0 of 41 in β = 1.06–1.50, 0 of
408 in 1.50–3.00); of those parking below 1 keV, 81% are excluded in 1.06–1.50 and 17% in
1.50–3.00. The parking temperature rises with α (Spearman 0.76 in the window; median 3.7 meV at
α = 0.001 to 12 eV at 0.15), but in 1.06–1.50 almost every α parks after BBN.
