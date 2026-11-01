# Plan — the `2026.8.0` science run

**Drafted 2026-11-01**, on `main` at `d992d42`, after `jordan-normalization` and `inelastic-reflection`
landed. It replaces the `2026-10-03` run of `2026.6.0` ([`2026-10-03-science-2026.6.0/`](../2026-10-03-science-2026.6.0/README.md)),
whose stores the tree cannot read. This folder holds the plan and its probe (`probe.py`, §2). When
the run starts, the folder's files move into the run's own folder, `YYYY-MM-DD-science-2026.8.0/`.
Until then it is a plan: no part of the run has been launched.

## 0. Rulings and proposals

**Ruled by the user, 2026-11-01**, in their words:

- **Large M is natural mode only.** "At large `M` we only want `alpha=0`."
- **The small-α probe** (§2): "agree, please fold this into the plan."
- **`[00-xav-sigma-is-non-monotone-at-25-35-keV]`** (`.documents/OPEN_ISSUES.md`): "leave this. For now
  I'm running the science with the thermodynamics implementation I have been provided with." The run
  uses `Xav_EOS_spline` as it stands, and the issue stays open.
- **`[11-ray-deduplicates-the-dissipation-warnings-of-parallel-histories]`**: "leave this until it is
  proved to be a problem. I don't want lots of log noise anyway. We can inspect the Datastore." The
  warnings are read from the stored columns (𝓡 at the first impact, the margins and their failing
  shares), not from the logs.
- **φ\***: "for now, start everything at `phi* = 5 Mp`." Smaller φ\* may be filled in later. In the
  non-surfing region (β < 1.107) the results depend on the initial conditions.

**Proposed, not yet ruled:**

- **P1, no version bump.** `VERSION_LABEL` has been `2026.8.0` and `FINGERPRINT_FORMAT` 6 since
  `inelastic-reflection` 09 (`3984ec5`). The code commits since then change no stored value:
  `8c12afa` (drivers and readers), `4fefdf8` (a comment and a printed line) and `7a24826` (a probe).
  The run is a 2026.8.0 run, with `PRYM_VERSION` `bf24c3d+ri02+sr01+bt02`.
- **P2, the tiers, the β grids and the α grid** of §1.
- **P3, M_crit read as the briefing notes' M_crit**, where 𝓡 = 1: median 1.2×10⁻¹⁵ M_P (about 3 TeV),
  maximum 8.2×10⁻¹³ M_P (`production_energy_ratio.tex`, `adiabatic_transgression.tex`).
- **P4, no Datastore tags for purpose** (§4).
- **P5, one main store**, plus a small separate store for the reflection-structure figure (§1.2, §5).
- **P6, how the blocks are named and registered** (§3).

**Left to the user after the probe:** the α floor, and whether α = 0.003 and 0.001 run at all (§2.3).

## 1. What is sampled

Every history has φ\* = 5 M_P, n = 1, T\* = 2×10⁴ GeV, tuning tolerance 10⁻⁵, and atol = rtol = 10⁻⁸ (the
defaults). There are 16 shards and `--bbn-wall-clock-limit 600`, as in 2026.6.0.

### 1.1 The main store, `var/datastores/science-2026.8.0.db`

| Tier | M / M_P | β grid | α | Mode | Purpose |
|---|---|---|---|---|---|
| **A** | 4×10⁻³¹ (1 meV) | dense, 194 values | the main grid | dissipation | Figure 6 |
| **A′** | 4×10⁻³¹ | sparse, 32 values | 0.003, 0.001, as the probe allows (§2.3) | dissipation | does α = 0.01 stand for the small-α limit? |
| **B** | 10⁻³⁰, 10⁻³¹, 10⁻³², 10⁻³³ | coarse, 70 values | the main grid | dissipation | M around and below the laboratory bound; expected independent of M |
| **C** | 10⁻¹⁵, 10⁻¹³, 10⁻¹² | coarse | the main grid | dissipation | around M_crit (P3); the relativistic margins are expected to fail toward 10⁻¹² |
| **D** | 10⁻³, 0.1, 0.5 | coarse | 0 only (ruled) | natural | large M; 10⁻³ links to the 2026.6.0 figure |

**The main α grid:** 0.15, 0.10, 0.08, 0.05, 0.03, 0.02, 0.01. The value 0.08 is the one-loop estimate at
laboratory M (`inelastic-reflection` README §8.0), and Figure 6 draws a line at it. The values below
0.03 have not been measured. The probe (§2) measures them.

**β grids.** Each β is generated as `i/d` from integers, as in 2026.6.0's `B()`, so a given β is the same
double in every block.

- **Dense (A), 194 distinct values:**
  - 0.50–1.05, step 0.05: the non-surfing region;
  - 1.0600–1.2000, step 0.0025: the transition through the QCD threshold, analytic β = 1.1074 (first
    captured history in (1.10, 1.15] at M = 10⁻³);
  - 1.20–1.50, step 0.01: the 2026.6.0 figure's exclusion window;
  - 1.50–3.00, step 0.02;
  - **2.980–3.040, step 0.005**: places the electroweak surfing threshold at laboratory M. It is
    3.020 at M = 10⁻³ (`surfing-temperature` 04, with no capture in 2.999–3.019). The cut on β will be
    imposed here;
  - a sparse convergence tail: 3.25, 3.5, 4, 5, 6, 8, 10, 15, 20. These values only show the yields
    becoming independent of β. They lie beyond the cut, so they are not resolved finely.
- **Coarse (B, C, D), 70 values:** 0.50–3.00 in steps of 0.05; 1.08–1.20 in steps of 0.01; the same tail.
- **Sparse (A′), 32 values:** 0.50–3.00 in steps of 0.10; 1.10, 1.15, 1.25; and 3.5, 5, 10, 20.

The tail β ≳ 6.5 starts super-Planckian at φ\* = 5 (at M = 10⁻³; board §5.9 of `jordan-normalization`). It is
warned of and computed, by the standing ruling.

### 1.2 The reflection-structure store, `var/datastores/science-2026.8.0-reflections.db`

`plot_ScalarHistory.py` has no selection by β or M: it draws every stored history. On the main store
that would be thousands of figure sets. The reflection-structure figure (§5.3) therefore reads a small
store of its own:

- β ∈ {1.2, 2, 2.9} × M ∈ {4×10⁻³¹, 10⁻³³, 10⁻¹⁵, 10⁻¹²} × α ∈ {0.01, 0.08, 0.15}: 36 histories, in
  dissipation mode;
- β ∈ {1.2, 2, 2.9} × M ∈ {10⁻³, 0.5} at α = 0: 6 histories, in natural mode.

These histories repeat histories of the main store, which costs minutes. It needs no change of code.

### 1.3 Size and cost

All costs are read off measured per-history times, not measured as a build. They come from
`inelastic-reflection` verification §1.2 and §2.5, and §4.16 point 3 of the review-remediation
verification document. The machine has 10 CPUs.

| Tier | Histories | Estimate |
|---|---|---|
| A | 194 × 7 = 1358 | dissipation at laboratory M, 11–31 s each including BBN: 25–70 min |
| A′ | 32 × 2 = 64 | set by the probe |
| B | 4 × 70 × 7 = 1960 | 35–100 min |
| C | 3 × 70 × 7 = 1470 | 25–75 min (10⁻¹² took the same steps as 4×10⁻³¹) |
| D | 3 × 70 = 210 | natural mode, 4 integrations at 10⁻³ and 9–12 at 0.1 and 0.5: about 1.2–2.1 h |
| Reflections | 42 | minutes |
| **Total** | **about 5100** | **about 3–6 h**, before A′ |

If that is too long, tiers B and C can drop to α ∈ {0.01, 0.03, 0.08, 0.15}: 2240 histories fewer.

## 2. The probe, before anything is launched

### 2.1 Why

Three parts of §1 are unmeasured:

- **Small α.** The measured span is α ∈ [0.03, 0.9]. At small α each reflection drains less, so the
  cascade to parking may be much longer, or may not park before BBN. Without parking, "α = 1 %
  stands for the small-α limit" need not hold. Natural mode cannot be computed at laboratory M, so the
  check has to be a sequence in α, not a comparison with α = 0.
- **M below 4×10⁻³¹.** Tier B goes to 10⁻³³. The drain is converged in M for M ≤ 3×10⁻¹¹, but nothing
  below 4×10⁻³¹ has been run.
- **The ends of the β range at laboratory M.** No history at laboratory M has β below 1.2 or above 5.

### 2.2 The points

The probe uses `tools/history_and_bbn.py BETA M --alpha A`: tuned, φ\* = 5, with no Ray and no store. Each point
runs as its own process from the repository root, three at a time, through [`probe.py`](probe.py). A point
is stopped after 1800 s. Its output goes to `var/science-2026.8.0/probe/<slug>.txt`, with a header recording
`git rev-parse HEAD`, `uptime` and the command.

| Set | M / M_P | β | α | Points | Compared with |
|---|---|---|---|---|---|
| S, small α | 4×10⁻³¹ | 1.2, 2, 3 | 0.01, 0.003, 0.001 | 9 | `inelastic-reflection` 11's α = 0.03 and 0.1 at the same (M, β), `var/inelastic-reflection-11/grid/` |
| L, lower M | 10⁻³², 10⁻³³ | 1.2, 2, 3 | 0.1 | 6 | 11's 4×10⁻³¹, α = 0.1 |
| N, non-surfing | 4×10⁻³¹ | 0.6, 1.0 | 0.01, 0.1 | 4 | each other |
| T, tail | 4×10⁻³¹ | 10, 20 | 0.01, 0.1 | 4 | 11's β = 5 |

That is 23 points: about 5–15 minutes, apart from whatever small α costs. A point that times out is a
result, the cost of its α, and is not retried.

### 2.3 What it reports, and what it decides

For each point the probe reports:

- the wall time, the accepted steps, the integrations and the reflections;
- the `dissipation` line: the injections, the parking and release, ΔN_eff today and at 1 MeV, and the
  margins;
- Yp and D/H.

Against the comparison points it gives the change in Yp and D/H relative to the Standard Model, and how
they step from α = 0.03 to 0.01, 0.003 and 0.001.

The probe records measurements; it sets no threshold. From them the user rules:

1. **The α floor.** If α = 0.003 and 0.001 are affordable, they run as tier A′. If one is not, the floor
   is the smallest α that is.
2. **Whether α = 0.01 can be labelled the small-α limit** on Figure 6, or whether the lower edge of the
   band has to be stated as depending on α.
3. **Whether tier B goes to 10⁻³³**, if set L shows anything other than independence of M.

The probe's results are added to this file as a new section (§7); §1 is not rewritten.

## 3. How the run is launched and recorded

### 3.1 The record

- **The run script goes into the run's folder and is committed before launch.** `science-runs/YYYY-MM-DD-science-2026.8.0/run_science_2026.8.0.sh`
  is committed, so each manifest's `git_dirty` is false and the committed text is the text that ran.
  It is never edited afterwards; a change is a new script. In 2026.6.0 the tree had to be inferred and
  the scripts were edited after the run. Committing first avoids both.
- **Every `main.py` block is registered** through `RunDrivers/registered_run.py`, with
  `--campaign science-2026.8.0 --prompt <tier>` (A, A2, B, C, D, R) and `--register` the job name. A
  run's directory is then `var/runs/science-2026.8.0-<tier>-<job>-<time>/`. The manifest records:
  - `git_head` and `git_dirty`;
  - the SHA-256 of `main.py`;
  - every package with its pin and commit;
  - host, argv, purpose and scope (β, M, n, φ\*, tolerance, α).
- **The fingerprint sidecar** is created by a guarded `store create` after tier A's first block
  (§4.15 point 4 of the review-remediation verification). Every later block's manifest then carries
  `results_store_id`. The reflection store gets its own sidecar the same way.
- **Unregistered:** `main.py --inventory` (the wrapper refuses it) and the plot runs. Their output is
  `tee`'d into the run folder's `logs/`.
- **After the run**, each block's `manifest.json` and `status.json` are copied into the folder's
  `registry/`. `var/` is ignored by git and local to the machine. The folder's README follows
  `science-runs/README.md`: the tree, `VERSION_LABEL`, `PRYM_VERSION`, the stores and their
  fingerprints, whether the results stand, and who made each file.

### 3.2 The blocks, in order

The cheapest and most important come first. Every block can be restarted: `main.py` looks up each
history before computing it. The wrapped command is that of §4.18 point 2 of the review-remediation
verification.

| Block | Store | M | β | α |
|---|---|---|---|---|
| `A-lab-dense` | main | 4e-31 | dense | 0.15 0.1 0.08 0.05 0.03 0.02 0.01 |
| `A2-lab-small-alpha` | main | 4e-31 | sparse | per §2.3 |
| `B-lab-neighbourhood` | main | 1e-30 1e-31 1e-32 1e-33 | coarse | the main grid |
| `C-Mcrit` | main | 1e-15 1e-13 1e-12 | coarse | the main grid |
| `D-large-M-natural` | main | 1e-3 0.1 0.5 | coarse | 0 |
| `R-reflections-dissipation` | reflections | 4e-31 1e-33 1e-15 1e-12 | 1.2 2 2.9 | 0.01 0.08 0.15 |
| `R-reflections-natural` | reflections | 1e-3 0.5 | 1.2 2 2.9 | 0 |

Then: `main.py --inventory` on each store, and the plot runs of §5.

**Where it runs.** `CLAUDE.md` makes production runs the user's, on another machine. The script is
written to run from the repository root of either machine. On this machine it would use ChamPBH's
isolated cluster (`ray_cluster.sh start`, `--ray-address 127.0.0.1:6380`), launched as
`nohup bash <script> > <log> 2>&1 &`.

## 4. Selecting results for the figures: no tags

`store_tag` is part of a history's lookup key: the `ScalarHistory` query joins on every tag it is given
(`ObjectFactories/ScalarHistory.py:496-508`). A purpose tag would cause two problems:

- A history built under it would be invisible to any block that did not pass the same tag, so
  overlapping grids would be computed twice.
- The plot scripts have no tag option, so they could not read it without a change of code.

Instead:

- **The run script is the definition of the grid.** Each block has its own job name, and each manifest
  has a purpose line.
- **The figure scripts select rows from `histories.csv`.** One `plot_by_beta.py` run per store writes it
  at `<output>/QCD_Cosmology/histories.csv`, one row per history across every α, with the columns
  `alpha`, `M_Mp`, `beta` and `phi_init_Mp`. The scripts select by those columns. A tier is a set of
  (M, α) pairs, which the script lists.
- **One main store.** `plot_by_beta.py` takes the product of every stored M and β, and skips any model,
  or (model, α), with no history, printing a line for each. Because the grid is not a full product,
  its log is long, but nothing is wrong. It is given every α:
  `--alpha-values 0 0.001 0.003 0.01 0.02 0.03 0.05 0.08 0.1 0.15`.

## 5. The figures

Each figure gets a folder of its own in the run folder, holding its script, copies of the
`histories.csv` (or `fields.csv`) it read, and a note. The script records the git SHA and the store
fingerprint it read.

### 5.1 Figure 6 (laboratory M)

Tier A: ΔYp and ΔD/H, relative to the Standard Model on the same path, against β. The panels are laid
out as in the 2026.6.0 draft: a main range and the tail.

- **α as a band.** The band runs from the **minimum to the maximum over the α grid** at each β, not
  between the two end values of α. ΔN_eff is measured to rise strictly with α. For Yp and D/H that is
  not established, since the dark radiation adds to H while the drain removes the field's kinetic
  energy.
- A line at α = 0.08 runs inside the band. Tier A′'s small-α values are drawn as thin markers.
- **A ΔN_eff panel** (today and at 1 MeV) is proposed. It is a constraint of its own, from the CMB.
- The electroweak cut is marked where tier A's bracket places the threshold.

The fixes to the 2026.6.0 draft listed in its `fig6_draft/NOTES.md` carry over:

- the Yp test is one-sided;
- the "no BBN result" label is wrong;
- the blue band should end at the measured QCD threshold, not the analytic one;
- the figure should be self-contained.

### 5.2 The comparison Figure 6 (larger M)

This figure uses the same axes, with one band per M tier: A against B (M independence), C (around
M_crit), and D as α = 0 lines at 10⁻³, 0.1 and 0.5. Where a history's relativistic margins fail, it is
marked from the stored columns.

### 5.3 The reflection structure

The figure is drawn from the reflection store's `fields.csv`, written by `plot_ScalarHistory.py` with
`--alpha-values 0 0.01 0.08 0.15`:

- **ρ_χ/ρ_R against T_J**, at fixed (β, α), with one curve per M. It is a staircase: each step is an
  injection, and the curve goes flat at parking. The parking and release are marked.
- **Natural mode has no ρ_χ.** For α = 0 the trace is the kinetic fraction, from `pi_Einstein_Mp` and
  `H_Jordan_Mp` in `fields.csv`, or φ − φ_min.
- **A companion panel** shows the reflection count and the parking temperature against M, from the
  history rows.

The positions of the steps are resolved only to the axis's 250 samples per decade. That is a known
systematic of the stored yields (`bbn-ratio-spline-2026-11-01`).

## 6. What stays open

- `[00-xav-sigma-is-non-monotone-at-25-35-keV]`: open, by the ruling above; the user is asking the
  collaborators.
- `[11-ray-deduplicates-the-dissipation-warnings-of-parallel-histories]`: open, by the ruling above.
- The yields at M ≥ 10⁻³ are not in the hard-wall limit (`inelastic-reflection` README §8.6). That
  bears on reading tier D, and it is the user's to place.

## 7. The probe's results (2026-11-01)

Run at the user's request on 2026-11-01, 10:56–11:02 UTC, from `d992d42` with this folder uncommitted
(`git status --porcelain`: 2 lines). It ran `probe.py` three at a time, at a one-minute load of 6–11 from macOS
background services. Each point's output, with its header and footer, is copied unchanged into `probe-out/`;
`probe-out/summary.txt` is `summarise_probe.py`'s output. The Standard-Model baseline through the same
path (small network, `bf24c3d+ri02+sr01+bt02`) is Yp = 0.24688021, D/H = 2.4582879 × 10⁻⁵. ΔYp and ΔD/H below are
relative to it. "Ref" rows are `inelastic-reflection` 11's points (`var/inelastic-reflection-11/grid/`,
tree `6d1a77c`). Its history code is the code here: no commit since changes a stored value (P1).

21 of 23 points ended with exit 0. The two failures are both at β = 0.6 (§7.4). No point came near the 1800 s cap.

### 7.1 S — small α at M = 4×10⁻³¹ M_P

| β | α | wall | steps | reflections | parks at T_J | ΔN_eff today | ΔYp | ΔD/H |
|---|---|---|---|---|---|---|---|---|
| 1.2 | 0.1 (ref) | 12.1 s | 2 196 | 7 | 11.6 keV | 0.0657 | +1.056 % | +1.475 % |
| 1.2 | 0.03 (ref) | 26.2 s | 30 564 | 176 | 0.57 eV | 0.0215 | +2.827 % | +2.464 % |
| 1.2 | 0.01 | 12.0 s | 1 791 | 5 | 1.51 keV | 0.00676 | +3.702 % | +2.702 % |
| 1.2 | 0.003 | 90.3 s | 161 874 | 1 034 | 0.13 eV | 0.00220 | +3.905 % | +4.909 % |
| 1.2 | 0.001 | 265.0 s | 515 943 | 3 549 | 0.014 eV | 0.00094 | +3.945 % | +5.506 % |
| 2 | 0.1 (ref) | 18.2 s | 9 929 | 49 | 83.6 keV | 0.0524 | +0.086 % | +0.580 % |
| 2 | 0.03 (ref) | 14.1 s | 5 179 | 22 | 25.4 keV | 0.0169 | −0.209 % | +0.608 % |
| 2 | 0.01 | 24.1 s | 16 389 | 85 | 65.4 keV | 0.00548 | +0.026 % | +0.063 % |
| 2 | 0.003 | 18.0 s | 7 594 | 35 | 7.13 keV | 0.00168 | +0.014 % | +0.243 % |
| 2 | 0.001 | 18.1 s | 6 672 | 30 | 4.46 keV | 0.00056 | −0.073 % | +0.360 % |
| 3 | 0.1 (ref) | 17.2 s | 11 558 | 59 | 23.3 MeV | 0.0316 | +0.171 % | +0.414 % |
| 3 | 0.03 (ref) | 25.3 s | 22 146 | 118 | 90.0 keV | 0.0123 | +0.020 % | +0.129 % |
| 3 | 0.01 | 24.1 s | 19 350 | 100 | 55.1 keV | 0.00434 | +0.022 % | +0.024 % |
| 3 | 0.003 | 32.1 s | 30 474 | 166 | 71.0 keV | 0.00131 | +0.033 % | +0.023 % |
| 3 | 0.001 | 26.1 s | 16 618 | 84 | 32.5 keV | 0.00045 | −0.007 % | −0.005 % |

Every history parked, and none was released. Every margin passed (failing shares 0). Each history converged on its
seed in one integration.

What this shows:

- **Small α is affordable.** The dearest point, β = 1.2 at α = 0.001, took 265 s and 516 k steps. Every other point
  took at most 90 s.
- **ΔN_eff/α is nearly constant at β = 2 and 3** (0.548–0.563 and 0.434–0.446 from α = 0.01 to 0.001). It is less
  so at β = 1.2 (0.676, 0.732, 0.936).
- **The bounce sequence is not monotone in α.** At β = 1.2 the reflection count goes 7, 176, 5, 1 034, 3 549 from
  α = 0.1 down to 0.001. Parking goes 11.6 keV, 0.57 eV, 1.51 keV, 0.13 eV, 0.014 eV. β = 2 and 3 show the same kind
  of scatter in the parking temperature. The branch of the bounce sequence a history takes changes with α.
- **β = 1.2 (near the transition).** ΔYp approaches a limit: +3.70, +3.91 and +3.95 % at α = 0.01, 0.003 and 0.001.
  ΔD/H does not: +2.70 % at α = 0.01 against +4.91 and +5.51 % at 0.003 and 0.001. The α = 0.01 history is the one
  that parked at 1.5 keV after 5 reflections. So at this β, α = 0.01 does not stand for the small-α limit of D/H.
- **β = 2 and 3.** Every shift is below 0.61 % and changes sign with α. The differences between α values are
  partly of the size of the yields' known systematic from the positions of the steps: up to 4.8 × 10⁻⁴ relative in
  Yp and 6.2 × 10⁻⁴ in D/H (`bbn-ratio-spline-2026-11-01`).

### 7.2 L — lower M at α = 0.1

At β = 1.2, 2 and 3 the histories at M = 10⁻³² and 10⁻³³ M_P are, to every printed digit, those at 4×10⁻³¹:

- the steps, the reflections and the injections;
- the injected fraction, the parking N and temperature, ΔN_eff;
- the margins and the wall times.

The yields differ by at most 0.0047 percentage points in ΔD/H and 0.0035 in ΔYp, a few × 10⁻⁵ relative. That is
the size of PRyMordial's scatter.

### 7.3 T — the tail at M = 4×10⁻³¹

| β | α | wall | integrations | parks at T_J | ΔN_eff today | ΔYp | ΔD/H |
|---|---|---|---|---|---|---|---|
| 5 (ref) | 0.1 | 17.2 s | 1 | 616 MeV | 0.00627 | +0.037 % | +0.075 % |
| 5 (ref) | 0.03 | 31.3 s | 1 | 197 MeV | 0.00269 | +0.016 % | +0.021 % |
| 10 | 0.1 | 26.1 s | 2 | 4.0 GeV | 0.00449 | +0.026 % | +0.051 % |
| 10 | 0.01 | 62.2 s | 2 | 207 MeV | 0.00102 | +0.006 % | +0.005 % |
| 20 | 0.1 | 20.0 s | 1 | 55 GeV | 0.00218 | +0.013 % | +0.017 % |
| 20 | 0.01 | 56.2 s | 1 | 776 MeV | 0.00076 | +0.005 % | −0.003 % |

Above the electroweak threshold the field parks long before BBN, and the yields fall towards the Standard Model's.
This is the convergence the tail was meant to show.

### 7.4 N — the non-surfing region at M = 4×10⁻³¹

- **β = 1.0** ran at both α. ρ_NP/ρ_R at 1 MeV is large, nothing is injected before 1 MeV (ΔN_eff at 1 MeV = 0),
  and the field parks at 45 meV (α = 0.1) and 0.64 meV (α = 0.01). The yields are ΔYp = +75.99 % and
  ΔD/H = +323.34 %, the same at both α to 3 × 10⁻⁴ percentage points: the drain happens after BBN. ΔN_eff today is 0.334 and
  0.331. The relativistic margins fail on part of the drain:
  - birth: 0.070 (α = 0.1) and 0.71 (α = 0.01);
  - reflection: 0.42 and 0.86.
- **β = 0.6, α = 0.1.** The history was computed: 3 integrations, 133 reflections, an injected fraction of 1.059, and
  ΔN_eff today 7.89. The margins fail on most of the drain (birth 0.64, reflection 0.89). BBN failed on the spline's
  floor ("T_in_MeV=0.00019856 MeV is smaller than T_BBN_spline_min=0.0002 MeV"), as the β = 0.50–0.90 histories at
  M = 10⁻³ did in 2026.6.0. ρ_NP/ρ_R is 68 at 1 MeV: the field is not at the wall before BBN.
- **β = 0.6, α = 0.01.** The history failed in its first tuning integration, after 4.3 s: "the effective minimum was
  not found: effective_minimum (ExponentialModel(M=0.00097413eV,n=1,beta=0.6)): V_eff' does not change sign on the
  bracket [8.07036e-06, 0.00322814]". It printed the two margin warnings first, at N ≈ 43.07. In a build this is
  stored as a failed history. It is a new failure mode, not seen in `inelastic-reflection` 11, and it is **not
  diagnosed here**.

### 7.5 What the probe bears on

These are measurements. The decisions of §2.3 are the user's.

1. **The α floor.** α = 0.003 and 0.001 cost at most 90 s and 265 s a history here. Tier A′ as planned (32 β × 2 α)
   is affordable on these figures.
2. **Whether α = 0.01 can be labelled the small-α limit.** At β = 1.2 it cannot for D/H (§7.1). Because the
   bounce sequence is not monotone in α, the band of Figure 6 is an envelope over a sampled α grid, and it can
   change when a value is added to the grid. The α grid could also be made denser near the transition.
3. **Tier B.** Below 4×10⁻³¹ the histories are the same to every printed digit (§7.2). Tier B's 1960 histories
   at four M values would largely recompute tier A's histories, to show what §7.2 already shows. It could be
   reduced, for example to 10⁻³⁰ and 10⁻³³ on the sparse β grid.
4. **The non-surfing region at laboratory M.** At β ≤ 1.0 the field is not at the wall before BBN, and the yields
   are far outside the observations at any α (β = 1.0) or have no BBN result (β = 0.6). In this region the tuning can
   fail (β = 0.6, α = 0.01), and much of the drain is not relativistic. It may be enough to sample this region more
   sparsely than planned.
5. **β = 1.0's ΔN_eff today of 0.33** comes from a drain after BBN. It is a CMB-side quantity that Figure 6's
   proposed ΔN_eff panel would show.

## 8. The plan revised after the probe (2026-11-01)

This section supersedes, by statement, §1.1's tiers A, A′ and B, its β grids, §1.2's M and α, §1.3's costs,
§3.2's block list and §4's list of α. Everything else above stands.

### 8.1 Ruled by the user, 2026-11-01, after §7

In their words:

- **The low-α grid.** "We want a denser `alpha` grid at low `alpha` near the transition, then. It may be that
  there isn't really a well defined floor."
- **The floor.** "I suggest we don't go below 0.001 unless you have an objection." There is none: steps at
  β = 1.2 grow roughly as 1/α (162 k at 0.003, 516 k at 0.001), so 10⁻⁴ would cost about 5 M steps a history,
  for a yield that §7.1 does not show settling.
- **Tier B.** "Shrink Tier B to two values of `M` on the sparse `beta` grid." The two values are 10⁻³⁰ and 10⁻³³ M_P,
  the example of §7.5 item 3: one just above the laboratory bound and one well below it.
- **The non-surfing region.** "In the region we know to be non-surfing we don't need a lot of samples, just a few
  to show what happens." This means β ∈ {0.5, 0.75, 0.9, 1.0} on every grid. The transition grid starts at 1.06, below
  the analytic QCD threshold, 1.1074.
- **P1–P6.** "P1-P6 are OK as stated." These are:
  - no version bump: the run is `2026.8.0`;
  - the tiers as revised here;
  - M_crit read as the briefing notes' value;
  - no Datastore tags for purpose;
  - one main store plus the reflection store;
  - `--campaign science-2026.8.0 --prompt <tier>`.

**Not yet ruled:** the β = 0.6, α = 0.01 tuning failure of §7.4, "the effective minimum was not found". It is
undiagnosed. With the non-surfing β above, a build may store a few failures of this kind, and it computes the
rest.

**Ruled later the same day:** "Leave the `beta = 0.6`, `alpha = 0.01` issue for now. It is in the
non-surfing region. We can diagnose it later if it is needed."

### 8.2 The grids

**α**, 13 values in all:

- **main:** 0.15, 0.10, 0.08, 0.05, 0.03, 0.02, 0.01;
- **low:** 0.007, 0.005, 0.003, 0.002, 0.0015, 0.001, steps of about 1.4–1.7.

**β.** Every value is generated as `i/d` from integers, as before.

- **NS (non-surfing), 4 values:** 0.5, 0.75, 0.9, 1.0.
- **W (the transition window), 87 values:** 1.0600–1.2000 in steps of 0.0025, then 1.20–1.50 in steps of 0.01.
- **Dense (tier A), 186 values:**
  - NS, then W;
  - 1.50–3.00 in steps of 0.02;
  - 2.980–3.040 in steps of 0.005, which places the electroweak threshold;
  - the tail 3.25, 3.5, 4, 5, 6, 8, 10, 15, 20.
- **Sparse, 30 values:** NS; 1.10–3.00 in steps of 0.10; 1.15, 1.25; and 3.5, 5, 10, 20. **Sparse-out** is its 19
  values above W: 1.6–3.0 in steps of 0.1, and 3.5, 5, 10, 20.
- **Coarse, 62 values:** NS; 1.10–3.00 in steps of 0.05; 1.08–1.20 in steps of 0.01; and the tail.

### 8.3 The blocks

| Block | Store | M / M_P | β | α | Histories |
|---|---|---|---|---|---|
| `A-lab-main` | main | 4e-31 | dense (186) | main (7) | 1302 |
| `A2-lab-low-alpha` | main | 4e-31 | W + sparse-out (106) | low (6) | 636 |
| `B-lab-neighbourhood` | main | 1e-30, 1e-33 | sparse (30) | main (7) | 420 |
| `C-Mcrit` | main | 1e-15, 1e-13, 1e-12 | coarse (62) | main (7) | 1302 |
| `D-large-M-natural` | main | 1e-3, 0.1, 0.5 | coarse (62) | 0 | 186 |
| `R-reflections-dissipation` | reflections | 4e-31, 1e-15, 1e-12 | 1.2, 2, 2.9 | 0.001, 0.01, 0.08, 0.15 | 36 |
| `R-reflections-natural` | reflections | 1e-3, 0.5 | 1.2, 2, 2.9 | 0 | 6 |

That is 3846 histories in the main store and 42 in the reflection store. The reflection store has changed in two ways:

- **M = 10⁻³³ is dropped**, since §7.2 found it identical to 4×10⁻³¹. M = 4×10⁻³¹ stands for both.
- **α = 0.001 is added**, where the structure is richest: 3549 reflections at β = 1.2.

**The cost.** As before, this is read from the probe's and earlier per-history times, not measured as a build, on
10 CPUs:

| Block | Estimate | Basis |
|---|---|---|
| A | 25–65 min | 11–31 s a history |
| A2 | 20–130 min | the probe's 18–265 s a history; the dearest are near β = 1.2 at the smallest α |
| B | 8–22 min | |
| C | 25–65 min | |
| D | 1–1.8 h | natural mode, as §1.3 |
| R | minutes | |
| **Total** | **about 2.5–6.5 h** | |

**Plots.** `plot_by_beta.py` is given every α:
`--alpha-values 0 0.001 0.0015 0.002 0.003 0.005 0.007 0.01 0.02 0.03 0.05 0.08 0.1 0.15`.

**Figure 6's band.**

- Inside W, the band is the envelope over all 13 α at each β.
- Outside W, the low α are sampled only on sparse-out. There, the band is the envelope over the main α, and the
  low-α values are drawn as markers. A marker outside the band then shows where the low α would widen it.
