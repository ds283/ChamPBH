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

## Added 2026-11-02: the large-M proxy, against the estimate of 2026-10-30

On 2026-10-30, in the inelastic-reflection planning (before prompts 04–07b), the user was told that a
history at M ≈ 10⁻⁶ M_P used as a proxy for laboratory M would overstate the total drain by about +33%
at β = 2 and +62% at β = 3 (α = 0.1), from the bounces after the first impact, and would park at the
wrong temperature (84 against 243 keV at β = 2; 23 MeV against 0.52 MeV at β = 3). That estimate came
from single untuned probe histories on the code at `1e31aac`
(`prompts/inelastic-reflection/source/planning-probes-2026-10-29b/`), whose laboratory-like values came
from a probe's stand-in for the unresolved-turnaround rule, because every true-M history then failed at
the bottom of its first cascade. **The code as run here does not reproduce it.**

[`compare_proxy.py`](compare_proxy.py) (read-only, sqlite `mode=ro` on the shards; output in
[`compare_proxy.out`](compare_proxy.out)) sets the α = 0.08 histories of the run's main store
(M ≤ 10⁻¹²) beside those of the diagnostic store (10⁻⁹ to 0.5), at β = 1.2, 2.0, 2.9:

| β | injections | drain Σ | ρ_χ/ρ_R today | parking T_J |
|---|---|---|---|---|
| 1.2 | 56 | 0.012197 | 7.173×10⁻³ | 3.37 eV |
| 2.0 | 55 | 0.010185 | 5.687×10⁻³ | 85.35 keV |
| 2.9 | 49 | 0.008220 | 4.430×10⁻³ | 604.8 keV |

- **10⁻³³ to 10⁻⁶ M_P agree.** The drain agrees to 2×10⁻⁴ relative and ρ_χ/ρ_R today to 3×10⁻⁴. The
  parking temperature agrees to 10⁻³, except at β = 1.2, M = 10⁻¹³ and 10⁻¹², which park 0.8% and 7%
  later, with 55 and 52 injections against 56. At 10⁻³ the drain still agrees to 3×10⁻⁴ and the parking
  temperature is 0.6–20% lower; at 0.5 the drain is 0.7–2.7% lower, and only β = 2.9 parks.
- **The agreement is not by construction.** At laboratory M every candidate is an unresolved turnaround
  (drained with w_gate = 1); at 10⁻⁶ 45–55 of them are resolved turnarounds, drained through the gate,
  and at 10⁻³ all are. Two treatments of the wall give the same drain, so the gate is ≈ 1 at these
  bounces.
- **The probe's two branches are gone.** At β = 2 every M parks at 85 keV, the probe's proxy value; at
  β = 2.9 every M parks at 0.60 MeV, near the probe's laboratory value at β = 3.
- **Not established.** Only α = 0.08 at three β was compared (the 2026-10-30 estimate expected the error
  to grow at small α); which change since `1e31aac` closed the gap (tuning, prompt 04's wall, prompt 05's
  rule, the parking cut) was not traced. None of this bears on production: 𝓡 at the first impact is
  10⁶⁰ at laboratory M and 10⁻³⁶ at 10⁻⁶, so what separates a large-M history from a laboratory one is
  the α each should carry, not the dynamics at a given α.
