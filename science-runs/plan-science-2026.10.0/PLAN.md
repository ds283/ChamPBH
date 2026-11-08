# Plan — the `2026.10.0` science run

**Drafted 2026-11-07**, on `main` at `8b05d30`, after `one-loop-drain` closed and was merged. It replaces the
`2026-11-01` run of `2026.8.0` ([`2026-11-01-science-2026.8.0/`](../2026-11-01-science-2026.8.0/README.md)), whose
stores this tree cannot read (format 6 against 7; review-remediation verification §4.20 point 1). Its grid is
2026.8.0's (that plan's §8), changed where the one-loop drain law changes what is worth computing. This folder holds
the plan and its probe (`probe.py`, §2). When the run starts, its files move into the run's own folder,
`YYYY-MM-DD-science-2026.10.0/`. Until then it is a plan: nothing has been launched.

The tree: `VERSION_LABEL` `2026.10.0` (`config/version.py:88`), `FINGERPRINT_FORMAT` 7 (`config/fingerprint.py:95`),
`PRYM_VERSION` `bf24c3d+ri02+sr01+bt02` (`ComputeTargets/BBNData.py:47`).

## 0. Rulings and proposals

### 0.1 What the one-loop law changes (02d's measurements, `one-loop-drain` verification §2d)

- **Laboratory M is no longer independent of M.** The law's drained fraction depends on M through ℓ and L, so
  ΔN_eff today at β = 2, A = 1 is 0.0375, 0.0446 and 0.0459 at 10⁻³³, 4×10⁻³¹ and 10⁻³⁰ M_P, and at β = 1.2, A = 10
  it is 0.463 and 0.544 at 10⁻³³ and 10⁻³⁰ (`measure-02d/out/observables.txt`). Under the fixed law the same M
  agreed to about 10⁻³ (2026.8.0 PLAN §7.2). Block B now measures a trend.
- **At and above M_crit the law drains essentially nothing.** In 02d's group C (10⁻¹² M_P) α_eff at the first impact is
  3×10⁻⁷ to 2×10⁻⁵⁷, so every A gives the same trajectory; C-3873 (β = 1.11) takes about an hour of tuning per A.
- **Between laboratory M and M_crit** (5×10⁻²⁰ to 10⁻¹⁶ M_P at β = 2) ΔN_eff today is the largest measured, 0.13–0.14
  at A = 1, and a history costs 600–1100 s where the cascade runs a classical tail (verification §2d.10).
- **Every `main.py` run builds at least one fixed-law row.** `--alpha-values` takes one or more values and defaults
  to 0 (`config/argument_parser.py:239`), and `main.py` builds one pipeline per α row and then one per A row
  (`main.py:1006`). A block given only `--A-values` also builds natural mode over its whole (M, β) grid.

### 0.2 Ruled by the user, 2026-11-07, in their words

- **M and A as proposed.** "Proposed `M` and proposed `A` look fine. Keep C at `A=1` only. As far as I know there is
  no physical lower limit on `M`." The proposal was: A at 4×10⁻³¹ on the dense β grid at every A; B at 10⁻³⁰, 10⁻³²,
  10⁻³³ and one lower M on the sparse β grid at every A, the lowest M tried in a probe first; C on the sparse β grid
  at 10⁻¹⁵ and 10⁻¹²; D unchanged; A2 dropped; the ladder between laboratory M and M_crit out of the main store, with
  a few β = 2 histories in the reflection store. A takes U14's values plus 0.2 and 3.
- **The fixed-law rows.** "We can have `alpha = 0.01` and `alpha = 0.1` on the dense grid, but there is no need for
  natural mode anywhere near laboratory `M`. Keep this just for the large `M` block."
- **B, C and R.** "For now, run B, C, and R as you propose. Later, we should add the option not to specify an
  `alpha`." The proposal was that B, C and R also carry α = 0.01 and 0.1, so that none builds natural mode. The option
  to give no α is a later change of code, not part of this run.

### 0.3 Proposed, not yet ruled

- **P1, the probe** of §2, before the run script is written.
- **P2, the lowest M of block B** is chosen by the user from the probe's set L (10⁻³⁶ and 10⁻⁴⁰ M_P). The probe sets
  no threshold; it reports what builds, its cost and its observables.
- **P3, the reflection store** of §1.3.
- **P4, as in 2026.8.0:** no Datastore tags for purpose (2026.8.0 PLAN §4); one main store and one reflection store;
  every `main.py` block registered as `--campaign science-2026.10.0 --prompt <block>`; the run script committed in the
  run's folder before launch and never edited afterwards (2026.8.0 PLAN §3.1).

## 1. What is sampled

Every history has φ\* = 5 M_P, n = 1, T\* = 2×10⁴ GeV, tuning tolerance 10⁻⁵, `--tuning-max-integrations` 30 and
atol = rtol = 10⁻⁸ (the defaults), 16 shards and `--bbn-wall-clock-limit 600`, as in 2026.8.0. The one-loop rows are
under the code's current model, `one-loop-measured-v1`.

### 1.1 The grids

**A**, 10 values: 0.05, 0.1, 0.2, 0.5, 1, 1.5, 2, 3, 5, 10 (U14's eight, `one-loop-drain` README §8.6, plus 0.2 and 3,
which close the factor-5 gap at 0.1–0.5 and the factor-2.5 gap at 2–5; Y_p and D/H are not monotonic in A in the
transition window, verification §2d.7).

**α (fixed law)**: 0.01 and 0.1 on every block near laboratory M or M_crit; 0 (natural mode) on D only.

**β**, generated as `i/d` from integers exactly as 2026.8.0's script does (`run_science_2026.8.0.sh`, function `G`):

- **Dense, 186 values** (2026.8.0 PLAN §8.2): NS (0.5, 0.75, 0.9, 1.0); W (1.0600–1.2000 by 0.0025, 1.20–1.50 by 0.01);
  1.50–3.00 by 0.02; 2.980–3.040 by 0.005; the tail 3.25, 3.5, 4, 5, 6, 8, 10, 15, 20.
- **Sparse, 30 values**: NS; 1.10–3.00 by 0.10; 1.15, 1.25; 3.5, 5, 10, 20.
- **Coarse, 62 values**: NS; 1.10–3.00 by 0.05; 1.08–1.20 by 0.01; the tail.

### 1.2 The main store, `var/datastores/science-2026.10.0.db`

| Block | M / M_P | β | α | A | Histories |
|---|---|---|---|---|---|
| `A-lab` | 4e-31 | dense (186) | 0.01, 0.1 | all 10 | 2232 |
| `B-lab-M-trend` | 1e-30, 1e-32, 1e-33, M_low (§2) | sparse (30) | 0.01, 0.1 | all 10 | 1440 |
| `C-Mcrit` | 1e-15, 1e-12 | sparse (30) | 0.01, 0.1 | 1 | 180 |
| `D-large-M-natural` | 1e-3, 0.1, 0.5 | coarse (62) | 0 | — | 186 |

That is 4038 histories, 2436 of them one-loop. Against 2026.8.0:

- **A** keeps its β grid; its seven fixed α become two, and it gains ten A.
- **A2 is dropped.** It sampled small fixed α; the A grid takes its place, and its β are all in A's.
- **B** goes from two M to four and from a check of independence to a trend.
- **C** loses 10⁻¹³ and moves to the sparse grid, at one A, since every A gives the same history there.
- **D** is unchanged.

### 1.3 The reflection store, `var/datastores/science-2026.10.0-reflections.db` (P3)

A few histories drawn whole by `plot_ScalarHistory.py` and, for the one-loop rows, `tools/plot_quanta_history.py`.
Each line is one `main.py` run, since its rows differ.

| Block | M / M_P | β | α | A | Histories |
|---|---|---|---|---|---|
| `R-lab` | 4e-31 | 1.2, 2, 2.9 | 0.01, 0.1 | 0.05, 1, 10 | 15 |
| `R-ladder` | 1e-16 | 2 | 0.01, 0.1 | 0.1, 1, 10 | 5 |
| `R-Mcrit` | 1e-15, 1e-12 | 1.2, 2, 2.9 | 0.01, 0.1 | 1 | 18 |
| `R-natural` | 1e-3, 0.5 | 1.2, 2, 2.9 | 0 | — | 6 |

That is 44 histories. 10⁻¹⁶ M_P at β = 2 is 02d's L-M1e-16: in the gap from N ≈ 20.7 and at R ≤ 1 from N ≈ 21.7 at
A = 1, so it shows all three regimes.

## 2. The probe, before the run script is written

### 2.1 Why

- **M below 10⁻³³** has never been run under either law, and R grows as M⁻⁴.
- **The fixed law** at M below 10⁻³³, and at 10⁻¹⁶ (`R-ladder`), has not been run.
- **The one-loop law at 10⁻¹⁵** has not been run (02d's nearest are 5×10⁻¹⁶ and 4×10⁻¹⁵), and C's sparse grid holds
  β = 1.10, next to C-3873's 1.11 at 10⁻¹².

### 2.2 The points

`tools/history_and_bbn.py BETA M --alpha X [--drain-law one_loop]`: tuned, φ\* = 5, with no Ray and no store; each
point its own process from the repository root, three at a time, through [`probe.py`](probe.py), stopped after
1800 s. Output: `var/science-2026.10.0/probe/<slug>.txt`, headed by the command, `git rev-parse HEAD`, the count of
`git status --porcelain` lines and `uptime`.

| Set | M / M_P | β | law, value | Points | Compared with |
|---|---|---|---|---|---|
| C, M_crit | 1e-15, 1e-12 | 1.1, 2 (1e-15); 1.1 (1e-12) | one-loop, A = 1 | 3 | 02d's C-3873 (1e-12, 1.11) |
| G, ladder | 1e-16 | 2 | fixed, α = 0.01, 0.1 | 2 | 02d's L-M1e-16 under the one-loop law |
| L, lowest M | 1e-36, 1e-40 | 1.2, 2, 10 | one-loop, A = 0.05, 1, 10 | 18 | 02d's B (1e-33, 1e-30) and R-865, R-611 (4e-31) |
| F, fixed at lowest M | 1e-36, 1e-40 | 1.2, 2 | fixed, α = 0.01, 0.1 | 8 | 2026.8.0's B (1e-33) |

31 points, the dearest first. A point that times out is a result (its cost) and is not retried.

### 2.3 What it reports, and what it decides

For each point: the wall time, the tuning's integrations, the accepted steps and reflections; the dissipation line
(the candidates by regime under the one-loop law, parking, ΔN_eff today and at 1 MeV); Y_p and D/H. Set L is laid
beside 02d's values at 4×10⁻³¹, 10⁻³⁰ and 10⁻³³ to show the M trend. The probe sets no threshold. From it the user rules:

1. **M_low** for block B (P2), and whether B should hold a further M between 10⁻³³ and M_low.
2. **Whether C and `R-Mcrit` run as planned**, if set C shows a tuning of C-3873's cost on the sparse grid.

The results are added to this file as a new section (§7); §1 is not rewritten.

## 3. How the run is launched and recorded

As 2026.8.0 PLAN §3, with these differences:

- **The run script** is `run_science_2026.10.0.sh`, modelled on 2026.8.0's: the same `run`, `sidecar`, `G` and
  `START_AT` machinery, `RUN=science-2026.10.0`, blocks `A B C D R`. A block passes `--alpha-values` and, where it has
  one-loop rows, `--A-values` after the wrapper's bare `--` (verification §4.20 point 2's form). It is written after
  the probe, committed in the run's folder, and never edited afterwards.
- **The wrapper's scope** records `A <values>` after the α part where `--A-values` is given
  (`RunDrivers/registered_run.py:704`).
- **The heartbeat.** A pipeline whose last histories are of C-3873's kind can be silent for longer than the registry's
  900 s window, and reads `stale` while it works. Read it with `./venv/bin/python -m runregistrykit list
  --stale-after 4000`, or by its pid (`CLAUDE.md`, long-running jobs, rule 2).
- **Where it runs**: as 2026.8.0, ChamPBH's isolated cluster on this machine (`tools/ray_cluster.sh start
  "science-2026.10.0"`, `--ray-address 127.0.0.1:6380`), launched detached by the user.

## 4. After the build

From the repository root, with the cluster up for the plot scripts (verification §4.20 point 2):

- `main.py --inventory` on each store (no Ray).
- `plot_by_beta.py` on the main store with `--alpha-values 0 0.01 0.1 --A-values 0.05 0.1 0.2 0.5 1 1.5 2 3 5 10`,
  and `plot_ScalarHistory.py` on the reflection store with the same options. A store built with `--A-values` and
  plotted without it exports the fixed rows only.
- `tools/quanta_measures.py <ROOT>` at the default associated scale (1), then with `--assoc-scale 0` and `10`.
- `tools/plot_quanta_grid.py <ROOT>`, and `tools/plot_quanta_history.py` on the reflection store's exports.

histories.csv writes a marker row for each (row, M, β) of the stored grid with no history, which is most of the
product here (D's M have no A row; C's have one A); `tools/quanta_measures.py` counts these as failed histories
(verification §4.20 point 6). They are not failures of the build.

## 5. The cost — a projection, not a measurement

| Block | Fixed rows | One-loop rows | Basis |
|---|---|---|---|
| A | about 20 min | 0.9–1.7 h | fixed: 2026.8.0's A, 71 min at 7 α; one-loop: §4.20 point 3's 0.74–1.36 h at 8 A, × 10/8 |
| B | about 25 min | about 0.7 h | fixed: 2026.8.0's B, 25 min for 2 M at 7 α; one-loop: 0.29 h for 2 M at 8 A, × 2 × 10/8 |
| C | about 10 min | 0.1 h, or ≥ 1 h | fixed: 2026.8.0's C, 95 min for 3 M × 62 β at 7 α; one-loop: 1 h if C-3873's kind recurs |
| D | — | — | 2026.8.0: 25 min |
| R | minutes | 0.3–0.5 h | `R-ladder`'s histories at 600–1100 s each |

**About 3–5 h**, and up to about 8 h if histories run 1.8 times as long at ten at a time, as 2026.8.0 found against its
probe (its README, "Timings against the plan"). The weakest parts are B's M_low and C, which the probe measures.

## 6. What stays open

- **`[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]`** (`.documents/OPEN_ISSUES.md`): β = 1.0 at
  laboratory M fails at A = 0.1 and 0.05, and may at 0.2; the other NS β at small A are not measured. A failure is
  stored and counted, and the rest is computed.
- **`[00-later-bounces-act-on-quanta-already-present]`** and **`[00-the-parking-leftover-has-no-spectrum]`**: as the
  handover states them.
- **The β = 0.6, α = 0.01 failure** of 2026.8.0 PLAN §7.4: left for later by the user (2026-11-01).
- **A run with no fixed-law row**: the later change of code the user asked for (§0.2), so that a one-loop block need
  name no α.

## 7. The probe's results (2026-11-07)

Run by the user on 2026-11-07 from `main` at `8b05d30`, with this folder untracked (`git status --porcelain` one line):
`probe.py`, three at a time, ending `22:54:35` UTC. The outputs are `var/science-2026.10.0/probe/<slug>.txt`, copied with the log into `probe-out/`; the
summary is `var/science-2026.10.0/probe/summary.txt`, from [`summarise_probe.py`](summarise_probe.py), which lays each
point beside 02d's observables (`.documents/one-loop-drain/measure-02d/out/observables.txt`) and the 2026.8.0 export
(`var/science-2026.8.0/out/main/QCD_Cosmology/histories.csv`). **29 of 31 points succeed; 2 time out.** These are
measurements; the decisions of §2.3 are the user's.

### 7.1 L — the one-loop law below 10⁻³³

All 18 succeed, each in one tuning integration (one in two), 14–24 s a point, except β = 10 at A = 0.05: 98 and 102 s
(780–797 reflections, parking at N ≈ 64). Every drain is one-loop, except β = 1.2, A = 0.05 at 10⁻⁴⁰ (79 one-loop and
10 gap candidates); ln R at the first impact is 190–255. Nothing in cost or regime distinguishes 10⁻³⁶ or 10⁻⁴⁰
from laboratory M.

**ΔN_eff today keeps falling as M falls**, with no sign of levelling off by 10⁻⁴⁰:

| β, A | 10⁻³⁰ | 4×10⁻³¹ | 10⁻³³ | 10⁻³⁶ | 10⁻⁴⁰ | 10⁻³⁰ → 10⁻⁴⁰ |
|---|---|---|---|---|---|---|
| 1.2, 1 | 0.0579 | — | 0.0483 | 0.0425 | 0.0388 | −33% |
| 1.2, 10 | 0.544 | — | 0.463 | 0.401 | 0.354 | −35% |
| 2, 0.05 | 0.00241 | 0.00234 | 0.00202 | 0.00176 | 0.00158 | −35% |
| 2, 1 | 0.0459 | 0.0446 | 0.0375 | 0.0336 | 0.0308 | −33% |
| 2, 10 | 0.1458 | 0.1456 | 0.1444 | 0.1438 | 0.1436 | −1.5% |
| 10, 1 | 0.00363 | 0.00357 | 0.00325 | 0.00297 | 0.00270 | −26% |
| 10, 10 | 0.00533 | 0.00536 | 0.00552 | 0.00567 | 0.00580 | +9% |

(10⁻³⁶ and 10⁻⁴⁰ from the probe, the rest from 02d.) The fall per decade slows a little: at β = 2, A = 1, −10.5% over
the three decades 10⁻³³ → 10⁻³⁶ and −8.3% over the four 10⁻³⁶ → 10⁻⁴⁰. Two cells go the other way: β = 2, A = 10 is
nearly flat, since its drains are capped, and β = 10, A = 10 rises by 9%. Y_p and D/H move with ΔN_eff and stay close
to 02d's at β ≥ 2.

### 7.2 F and G — the fixed law does not depend on M

At 10⁻³⁶ and 10⁻⁴⁰ (set F) every fixed-law history has the same accepted steps, reflections, parking N and ΔN_eff
(to every printed digit) as the 2026.8.0 store's at 10⁻³³ and 4×10⁻³¹, at both β and both α. Y_p and D/H differ in
the fifth or sixth figure. At 10⁻¹⁶, β = 2 (set G) the fixed law gives the same history again: α = 0.01 has 16 773 steps,
85 reflections and ΔN_eff today 0.00548245, as at 10⁻⁴⁰. The one-loop law there gives 0.0111 (A = 0.05) to 0.147
(A = 10) (02d's L-M1e-16). So the fixed rows at B's and `R-ladder`'s M are the laboratory-M fixed-law histories again.

### 7.3 C — the one-loop law at M_crit

- **10⁻¹⁵, β = 2, A = 1** succeeds in 18 s (one integration): 1 gap and 27 classical candidates, 99.1% of K left at
  parking, ΔN_eff today 6.8×10⁻⁶, as 02d found at 4×10⁻¹⁵.
- **β = 1.1 times out at both M** after 1800 s, still tuning. At 10⁻¹² it had run 13 integrations of 127–135 s; at
  10⁻¹⁵, 8 of 211–217 s. After the seed, the Hubble residual |H_J(T_CMB)/H₀ − 1| stayed between 4×10⁻⁵ and 3×10⁻⁴ at
  10⁻¹² and between 7×10⁻⁵ and 9×10⁻⁴ at 10⁻¹⁵, against the tolerance 10⁻⁵, while the φ residual reached 5×10⁻⁹ and
  3×10⁻⁸. This is C-3873's kind (02d: β = 1.11 at 10⁻¹², 28 integrations at A = 1, timeouts at the other A). The
  2026.8.0 store holds C-3873 as a fixed-law success (02d's reference for it, verification §2d.9).
- **What it implies for block C**, not measured further. The cap of 30 integrations bounds a history at about
  30 × 135 s ≈ 68 min at 10⁻¹² and 30 × 217 s ≈ 109 min at 10⁻¹⁵, and up to 1.8 times that at ten at a time. A history
  that reaches the cap is stored as a failure. C's A = 1 pipeline lasts as long as its slowest history. Which of the
  sparse grid's other β (1.15, 1.2, …) behave this way is not measured; `R-Mcrit`'s β = 1.2 at A = 1 is one of them.
- **Why the one-loop tuning does not settle here, where the fixed-law tuning does**, is not diagnosed.

## 8. The plan revised after the probe (2026-11-08)

This section supersedes, by statement, §0.3's P1 and P2, §1.2's block B and its count, §3's block order and §5's
totals. Everything else above stands.

### 8.1 Ruled by the user, 2026-11-08, after §7

In their words:

- **B's M.** "in addition to your suggested values, let's also have `M=1E-42` to push it just a littel farther." The
  suggested values were 10⁻³⁰, 10⁻³², 10⁻³³, 10⁻³⁶ and 10⁻⁴⁰, so B has six M. 10⁻⁴² was not probed; it lies two decades
  below 10⁻⁴⁰, which built as laboratory M does (§7.1), and ln R at its first impact is about 18 above 10⁻⁴⁰'s (R ∝ M⁻⁴).
- **Block C.** "Yes, run it as planned, but it's OK to do it last." C keeps §1.2's grid and runs after every other
  block. A history of C-3873's kind that reaches the 30-integration cap is stored as a failure (§7.3).

### 8.2 The blocks, in order

| Order | Block | Store | M / M_P | β | α | A | Histories |
|---|---|---|---|---|---|---|---|
| 1 | `A-lab` | main | 4e-31 | dense (186) | 0.01, 0.1 | all 10 | 2232 |
| 2 | `B-lab-M-trend` | main | 1e-30, 1e-32, 1e-33, 1e-36, 1e-40, 1e-42 | sparse (30) | 0.01, 0.1 | all 10 | 2160 |
| 3 | `D-large-M-natural` | main | 1e-3, 0.1, 0.5 | coarse (62) | 0 | — | 186 |
| 4 | `R-lab` | reflections | 4e-31 | 1.2, 2, 2.9 | 0.01, 0.1 | 0.05, 1, 10 | 15 |
| 4 | `R-ladder` | reflections | 1e-16 | 2 | 0.01, 0.1 | 0.1, 1, 10 | 5 |
| 4 | `R-natural` | reflections | 1e-3, 0.5 | 1.2, 2, 2.9 | 0 | — | 6 |
| 5 | `C-Mcrit` | main | 1e-15, 1e-12 | sparse (30) | 0.01, 0.1 | 1 | 180 |
| 5 | `R-Mcrit` | reflections | 1e-15, 1e-12 | 1.2, 2, 2.9 | 0.01, 0.1 | 1 | 18 |

The main store holds 4758 histories (3720 one-loop), the reflection store 44. `R-Mcrit` runs in slot 5 beside C,
since its β = 1.2 at A = 1 may be of C-3873's kind too (§7.3); it is registered as tier R.

### 8.3 The cost — a projection, not a measurement

Read as §5, with B at six M (three times §5's two-M figures for the one-loop rows, and 6/2 × 2/7 of 2026.8.0's B for
the fixed rows):

| Slot | Estimate |
|---|---|
| A | 1.2–2.0 h |
| B | about 1.4 h |
| D | about 25 min |
| R (without `R-Mcrit`) | 0.3–0.5 h |
| C and `R-Mcrit` | 0.3 h, and up to about 2 h a pipeline, or 3.3 h at 1.8 times, where a β ≈ 1.1 history runs to the cap |

**About 3.3–4.3 h before slot 5, and slot 5 up to about 3.5 h more.** The post-build steps of §4 follow.
