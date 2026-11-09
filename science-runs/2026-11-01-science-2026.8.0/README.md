# 2026-11-01 — the `2026.8.0` science run

The run that replaces [`2026.6.0`](../2026-10-03-science-2026.6.0/README.md), made after
`jordan-normalization` and `inelastic-reflection` landed on `main`. Its plan is [`PLAN.md`](PLAN.md):
§0 and §8 hold the user's rulings and the grid that ran, and §7 the probe that set it.

**Status: current.** Every block completed and both stores are whole. 189 of the main store's 3846
histories failed; 41 of those are in the surfing region and are not yet diagnosed (below, "Failed
histories").

*Added 2026-11-02, by `reflection-restart` prompt 04:* **this store is superseded by the rebuild of record**, which the
user makes after `one-loop-drain` 08, under its one-loop drain law at `VERSION_LABEL` 2026.10.0, with the fixed α kept
as a selectable law; no store of record is built at 2026.9.0. The rebuild's tree holds `reflection-restart`'s fix of
the 42 `log_m_eff_squared` failures once that branch is merged into `one-loop-drain`. Until then this store and its
figures stand as made; the failures of the surfing region are diagnosed below.

*Added 2026-11-09:* the rebuild of record is [`2026-11-08-science-2026.10.0/`](../2026-11-08-science-2026.10.0/README.md),
built at `VERSION_LABEL` 2026.10.0 on `5837a47`. Its Figure 6 replaces this run's as the figure of record.

**Figures made from it** (each folder holds its script, its inputs and a note):
- [`fig6/`](fig6/NOTES.md): Figure 6 at the laboratory mass, with the band over α and a ΔN_eff
  panel (2026-11-01), and a parking-temperature row (2026-11-02);
- [`reflections/`](reflections/NOTES.md): the reflection structure against M (2026-11-01), restated
  2026-11-02 with the large-M dark radiation and 𝓡 at the first impact against M.

Their inputs since 2026-11-02 are the read-only exports of
[`run_figure_exports.sh`](run_figure_exports.sh).

**After the run**, [`diagnostic/`](diagnostic/NOTES.md) (2026-11-01, `run_diagnostic_large_M.sh`):
`histories.csv` again with the dissipation columns, and 12 dissipation-mode histories at large M in a
store of their own, `var/datastores/science-2026.8.0-large-M-dissipation.db`; the run's stores are
unchanged.

The comparison Figure 6 (larger M) is not yet made.

## The run

| | |
|---|---|
| When | 2026-11-01, 11:20:45 – 18:24 UTC (A to D, ended by D's crash); resumed 20:25:08 – 20:50:57 UTC (D, R, inventories, plots) |
| Trees | `75208cd` for A, A2, B, C and D's first attempt; `eecaf2e` for D's resume and R. Each block's manifest records its tree, with `git_dirty` false |
| Labels | `VERSION_LABEL` `2026.8.0`, `FINGERPRINT_FORMAT` 6, `PRYM_VERSION` `bf24c3d+ri02+sr01+bt02` (small network) |
| Packages | datastorekit 0.4.0, raypoolkit 0.3.0, runregistrykit 0.2.0, ray 2.59.0, SQLAlchemy 2.0.55, numpy 2.3.5, scipy 1.17.0; Python 3.13.16 |
| Host | `mps026088.phys.susx.ac.uk` (arm64, macOS, 10 cores), ChamPBH's isolated Ray cluster at 127.0.0.1:6380 |
| Main store | `var/datastores/science-2026.8.0.db`, 16 shards: 3846 histories (A + A2 + B + C + D), all validated; 3718 adiabatic and BBN records |
| Reflection store | `var/datastores/science-2026.8.0-reflections.db`, 16 shards: 42 histories (R), all validated, with their adiabatic and BBN records |
| Script | [`run_science_2026.8.0.sh`](run_science_2026.8.0.sh) |
| Plots | `var/science-2026.8.0/out/` (below, "Where the output is") |

Every history starts at φ* = 5 M_P with `--bbn-wall-clock-limit 600`. The blocks, from PLAN.md §8:

| Block | M / M_P | β grid | α | Histories |
|---|---|---|---|---|
| A | 4×10⁻³¹ | dense, 186 values, 0.5–20 | 0.01–0.15 (7) | 1302 |
| A2 | 4×10⁻³¹ | 1.06–1.50 dense, sparse above: 106 values | 0.001–0.007 (6) | 636 |
| B | 10⁻³⁰, 10⁻³³ | sparse, 30 values | 0.01–0.15 (7) | 420 |
| C | 10⁻¹⁵, 10⁻¹³, 10⁻¹² | coarse, 62 values | 0.01–0.15 (7) | 1302 |
| D | 10⁻³, 0.1, 0.5 | coarse, 62 values | 0 (natural mode) | 186 |
| R | 4×10⁻³¹, 10⁻¹⁵, 10⁻¹² | 1.2, 2.0, 2.9 | 0.001, 0.01, 0.08, 0.15 | 36 |
| R | 10⁻³, 0.5 | 1.2, 2.0, 2.9 | 0 | 6 |

## The registered runs

Each block ran through `RunDrivers/registered_run.py` as `--campaign science-2026.8.0 --prompt
<block>`. Their `manifest.json` and `status.json` are copied unchanged into
[`registry/runs/`](registry/runs/), and the two stores' fingerprint sidecars, as they stood after
the run, into [`registry/stores/`](registry/stores/). The store digest is the one `status.json`
records when the run ended.

| Run | Start → end (UTC) | Wall | State | Tree | Store digest at the end |
|---|---|---|---|---|---|
| `science-2026.8.0-A-A-lab-main-20261101T112047` | 11:20:47 → 12:31:42 | 1 h 11 min | done | `75208cd` | `5a0480a677d47d2e…` |
| `science-2026.8.0-A2-A2-lab-low-alpha-20261101T123215` | 12:32:15 → 16:05:08 | 3 h 33 min | done | `75208cd` | `f53f9fe5cfe33a35…` |
| `science-2026.8.0-B-B-lab-neighbourhood-20261101T160543` | 16:05:43 → 16:30:26 | 25 min | done | `75208cd` | `75f807833632b57c…` |
| `science-2026.8.0-C-C-Mcrit-20261101T163105` | 16:31:05 → 18:06:25 | 1 h 35 min | done | `75208cd` | `480fd52cc2fee56c…` |
| `science-2026.8.0-D-D-large-M-natural-20261101T180712` | 18:07:12 → 18:23:32 | 16 min | **failed** | `75208cd` | `a22e7456513b02c7…` |
| `science-2026.8.0-D-D-large-M-natural-20261101T202510` | 20:25:10 → 20:34:13 | 9 min | done | `eecaf2e` | `e8d9b41d4f93409f…` (main store, final) |
| `science-2026.8.0-R-R-reflections-dissipation-20261101T203500` | 20:35:00 → 20:41:56 | 7 min | done | `eecaf2e` | `eb769f9897fbc3f0…` |
| `science-2026.8.0-R-R-reflections-natural-20261101T204221` | 20:42:21 → 20:43:28 | 1 min | done | `eecaf2e` | `4d3d05a6a9f0ff48…` (reflection store, final) |

The manifests' `script_sha256` is that of `main.py`, which is the same in both trees
(`7ec58822…`): the change between them is outside `main.py`, so `git_head` is what tells them apart.

## D's crash and the resume

D's first attempt ended at 18:24 on one history, M = 0.5 M_P, β = 20, α = 0. Its tuning diverged
after a secant step (φ_ref from 0.088 to 1.04 M_P, then guard steps to 1.63 M_P), and on the next
step `effective_minimum` overflowed evaluating V_eff′ at 20 φ₀. The tuner turned only a
`ValueError` into a failed history, so the `OverflowError` ended the driver, and `set -e` the
script, before R, the inventories and the plots. 182 of D's 186 histories were stored by then; none
of D's adiabatic or BBN records were.

At the user's word, two commits followed on `main`:
- `745c8ee` makes an overflow in `effective_minimum` the `ValueError` a bracket without a sign change
  raises, so such a history is stored as failed ("the effective minimum was not found"). Only the
  path that raised changed; no stored value could differ.
- `eecaf2e` adds `START_AT=<block>` to the run script, and appends to each block's log rather than
  overwriting it.

The user resumed with `START_AT=D`. D integrated six histories, all at M = 0.5 (its log names no
other): the two left unvalidated by the crash, β = 5 and 6, since lookups read validated rows only,
and the four missing, β = 8, 10, 15 and 20. β = 20 reproduced the same tuning path and was stored as failed; β = 8, 10 and 15
failed with "the normalization did not converge". The resume's blocks ran on `eecaf2e`; A to C were
not re-run.

## Timings against the plan

PLAN.md §8 estimated 2.5–6.5 h. The run took 7 h 3 min to D's crash and 26 min for the resume.

| Block | Plan | Took |
|---|---|---|
| A | 25–65 min | 1 h 11 min |
| A2 | 20–130 min | 3 h 33 min |
| B | 8–22 min | 25 min |
| C | 25–65 min | 1 h 35 min |
| D | 1–1.8 h | 16 min + 9 min |
| R | minutes | 8 min |
| Inventories and plots | — | 7 min |

The dissipation-mode estimates were read from the probe's per-history times, taken three histories
at a time. At ten at a time a history took about 1.8 times as long: in A2, from the tuning lines the
log prints (a subset of histories), about 170, 230, 340 and 485 s at α = 0.003, 0.002, 0.0015 and
0.001 (at most 746 s), against the probe's 90 and 265 s at α = 0.003 and 0.001. A2's estimate also
took the probe's β = 1.2 as its dearest point, whereas across the whole window 1.06–1.50 the cost
is set by α and hardly by β, and 87 of A2's 106 β lie in it. And each α finishes its histories
before its BBN stage starts, so the slowest few run with most cores idle. D's estimate came from
`2026.6.0`'s natural-mode times; why D was so much quicker is not established.

## Failed histories

From `histories.csv`, by block, split at the analytic QCD surfing threshold β = 1.1074. The
categories are the first words of each `failure_reasons`.

| Block | β ≤ 1.1074 | of which failed | β > 1.1074 | of which failed |
|---|---|---|---|---|
| A | 161 | 21: 10 PRyMordial, 8 effective minimum, 3 classical turning point | 1141 | 0 |
| A2 | 114 | 0 | 522 | 3 classical turning point (β = 1.1775–1.1825, α = 0.001) |
| B | 70 | 42: 23 PRyMordial, 16 effective minimum, 3 classical turning point | 350 | 0 |
| C | 147 | 77: 25 effective minimum, 21 `integrate_scalar_history`, 20 PRyMordial, 8 `log_m_eff_squared`, 3 normalization | 1155 | 34 `log_m_eff_squared` |
| D | 21 | 8 PRyMordial | 165 | 4: 3 normalization (M = 0.5, β = 8, 10, 15), 1 effective minimum (M = 0.5, β = 20) |

- **PRyMordial** failures are all at β ≤ 0.9: the history's temperature at the start of BBN is
  below PRyMordial's spline floor (`T_in_MeV ≈ 0.000196 < T_BBN_spline_min = 0.0002`), as in
  `2026.6.0` at M = 10⁻³ and in the probe (PLAN.md §7.4). The field is not at the wall before BBN
  there.
- **"The effective minimum was not found"** below the threshold, at β = 0.5–0.9, is the failure the
  probe met at β = 0.6, α = 0.01 (PLAN.md §7.4), which the user left undiagnosed as being in the
  non-surfing region.
- **`log_m_eff_squared`** (m_eff² ≤ 0, with φ_E < 0 in every case): the 34 above the threshold are
  scattered over β = 1.11–2.6, at M = 10⁻¹⁵, 10⁻¹³ and 10⁻¹², across most α. They fall in the M_crit
  comparison, densest in its transition window, and are **undiagnosed**. So are the three classical-turning-point failures in A2 and the three
  normalization failures in D.
- *Added 2026-11-02:* all 42 `log_m_eff_squared` failures (the 34 above and C's 8 below the threshold) are
  diagnosed in [`.documents/post-reflection-step-2026-11-02/`](../../.documents/post-reflection-step-2026-11-02/README.md).
  After a late floor reflection the outward step can go uncapped, and Radau accepts a wrong step whose
  interpolant crosses φ = 0, where the gate evaluates m_eff². Three reproduced; the issue is
  `[run-2026.8.0-an-uncapped-outward-step-after-a-reflection-crosses-the-wall]` (`inelastic-reflection` board §3).
- *Added 2026-11-02, by `reflection-restart` prompt 04:* **the 42 recomputed with the fix** (F3 and the guard,
  `reflection-restart` 03, `b81eab8`, `VERSION_LABEL` 2026.9.0), tuned as the run made them, as a measurement only:
  **41 succeed**; none fails in `log_m_eff_squared`, and the fix's guard never fired. The one failure is 4389 (β = 1.13,
  M = 10⁻¹², α = 0.01): after its seed integration, "the effective minimum was not found", above the surfing threshold,
  opened as `[04-a-recomputed-history-does-not-find-the-effective-minimum]` (`reflection-restart` board §3). None is
  stored: this store is not changed ([verification §3.1](../../.documents/reflection-restart-verification.md)).
- *Added 2026-11-02, by `reflection-restart` prompt 04:* **the nine classical-turning-point failures are diagnosed**
  (A 3, A2 3, B 3: all "not monotone", in the tuning's seed integration). The wall search cannot find a root of
  V − V_floor = E that lies below 17 quanta of ln V's rounding (`WALL_ROUNDING_QUANTA` = 16), and raises; their roots
  lie at 0.11–16.54 quanta. It is met at the first step start of an infall, near the apex of a plateau turn, where E is
  as small as the step grid makes it. The limit predates `reflection-restart`; that campaign's fix moves which histories
  meet it (2197 and 2188, successes here, fail on the fixed tree). Narrowed as
  `[00-fixed-alpha-laboratory-M-histories-fail-in-classical-turning-point]`, its fix proposed for `one-loop-drain` 01
  ([verification §3.4–§3.6](../../.documents/reflection-restart-verification.md)).

## Where the output is

Under `var/science-2026.8.0/out/`, which git ignores; nothing there is copied here.

- **`main/QCD_Cosmology/`**, from `plot_by_beta.py` on the main store at every α:
  - `histories.csv`: one row per (M, β, α) in the product of the store's values, with the failure
    reasons; rows the store does not hold say "no ScalarHistory row in the store";
  - `alpha=<α>/plots/M=<M>_n=1/`: `abundance_shifts`, `BBN`, `fixed_T`, `max_Q`, `T_deliver` and
    `timings`, each as PDF and PNG; `alpha=<α>/plots/M_convergence` across M; and
    `alpha=<α>/csv/M=<M>_n=1/data.csv`;
  - `plots/M=<M>_n=1/Delta_N_eff` across α.
- **`reflections/QCD_Cosmology/alpha=<α>/`**, from `plot_ScalarHistory.py` on the reflection store:
  `plots/beta=<β>/M=<M>_n=1/` holds `full_history`, `BBN_era`, `BBN_era_NP`, `energy`, `thermo`,
  `Hubble`, `ODE_terms` and `dark_radiation`, and `csv/` the `fields.csv` of each of the 42 histories.

M is in eV in the folder names: 9.7413×10⁻⁴ eV is 4×10⁻³¹ M_P, 2.4353×10⁻⁶ and 2.4353×10⁻³ eV are
10⁻³³ and 10⁻³⁰ M_P, 2.4353×10¹², 10¹⁴ and 10¹⁵ eV are 10⁻¹⁵, 10⁻¹³ and 10⁻¹² M_P, and
2.4353×10²⁴, 10²⁶ and 1.2177×10²⁷ eV are 10⁻³, 0.1 and 0.5 M_P.

## Where each file came from, and how that is known

- **`PLAN.md`, `probe.py`, `summarise_probe.py` and `run_science_2026.8.0.sh`** were written by a
  Claude Code session on 2026-11-01 at the user's request, and committed in `89a2cd5` (the plan and
  the probe) and `75208cd` (the script). The rulings in PLAN.md §0 and §8 are the user's, in their
  words in that session. Until this record was made the folder was `science-runs/plan-science-2026.8.0/`,
  which the docstrings of `probe.py` and `summarise_probe.py` still name.
- **`run_science_2026.8.0.sh` was edited once during the run**, in `eecaf2e` (`START_AT` and the
  appended logs), between D's crash and the resume. The text that ran A to D's first attempt is
  `75208cd`'s, SHA-256 `953e28c2d885b6ee467b13deaf6fc7b2ce9a2d3cde0fa663ec8d09029382a5c5`; the text
  kept here, which ran the resume, is `ccc97dc19d498dfa71572e74953c68acc82862af0f1df825690aeef57fe32ddc`.
- **`probe-out/`** holds the probe's outputs, run by that session at the user's request on
  2026-11-01 into `var/science-2026.8.0/probe/`, and `summary.txt` from `summarise_probe.py`
  (PLAN.md §2, §7).
- **The run was launched by the user**, both times with `nohup` from the repository root, on the
  cluster that session started (`ray_cluster.sh start`).
- **`logs/`** is copied unchanged from `var/science-2026.8.0/logs/` after the run: each block's
  output as the script `tee`'d it, the two inventories and the two plot runs.
  `D-large-M-natural.log` holds both attempts, the first above the resume. The script's own output,
  `var/science-2026.8.0/run.out`, repeats those logs line for line and is not copied. Each run's own
  copy of its output is also kept in `var/runs/<run>/stdout.log`.
- **`registry/`** is copied unchanged from `var/runs/<run>/` and from the stores' sidecars in
  `var/datastores/`, after the run.
