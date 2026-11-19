# Prompt 04 — measure a sample

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-15, after 03 landed (`d924275`, reviewed `99cc79b`), at the user's request: "No need to argue about
D12. Leave it as it is. Please write prompt 04 and its orchestrator note." It is written from:
- README §0.3 (the thresholds), §1, §2 row 04, §2.1 (the gate Sample), §4, §5, §6.2 U7, §6.3 P9, P10 and P14, §6.4 R5,
  §6.6 R9, and §6.7 (P24–P32, proposed with this prompt);
- log 03 §6–§7 and the verification document §3.5 (the table's error in x near the floor);
- the 2026.11.0 run's record (`science-runs/2026-11-13-science-2026.11.0/README.md`, on `main`) and its main export's
  `histories.csv`, which hold the exponential's counterparts;
- the orchestrator's scratch probe at `99cc79b` (note §0).

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Gate:**
- in the worktree `.claude/worktrees/power-law-wall`, on branch `power-law-wall`, at or after the commit that adds this
  prompt; `d924275` is an ancestor of `HEAD`;
- `git status`: no tracked change; the untracked `venv` and nothing else;
- `./venv/bin/python -m runregistrykit list`: no run of this campaign `running` or stale;
- `CosmologyConcepts/drain_model.py` has three drain models; `InversePowerModel` defines `_drain_fraction` and
  `one_loop_drain_model`; `.documents/power-law-wall-verification.md` has §1–§3 and no §4;
  `.documents/power-law-wall/sample-04/` does not exist.

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds.

**Recommended model:** **Opus.** No production code changes, but forty histories and seven surfing scans must be
run on a shared machine, each output tied to the history it came from, and every threshold read against its use on a
wall it was not measured on. The report decides what the user rules before the store is built.

**Read first:**
1. [`README.md`](README.md): §0.3, §1, §2.1, §4, §5, and §6 whole, above all §6.2 U7, §6.3 P9–P10, §6.4 R5, §6.6 R9 and
   §6.7 (P24–P32).
2. The log of 03 (`logs/03-the-law-on-this-wall.md`) §6 and §7; the verification document §1.5 (01's runs), §2.2 (02's
   paths) and §3.5 (the error in x over E/V₀).
3. `tools/history_and_bbn.py` whole (the tool route: `main`, its wrapping of `integrate_scalar_history`, the lines it
   prints), and `tools/surfing_scan.py`'s docstring and CSV columns.
4. `prompts/power-law-wall/source/wall/path_map.py`: how 02 kept a tuned history's final `IntegrationResult` in-process
   without changing a production module.
5. `ComputeTargets/ScalarHistory.py`: the threshold constants and their comments (`UNRESOLVED_REFLECTION_FACTOR`,
   `UNRESOLVED_PHI_T_THRESHOLD_MP`, `PARKING_EPSILON`, `PARKING_ADIABATICITY_THRESHOLD`, `PARKING_PI_GUARD_MP`, :120–:160),
   and the comments above `Injection`, `Parking`, `Release`, `DrainCandidate` and `DrainRecord`;
   `CosmologyConcepts/Models/AbstractModel.py`: `WALL_ROUNDING_QUANTA`, `_log_V_minus_floor` and `ClassicalTurningPoint`.
6. The copies the orchestrator gives you (note §2): the 2026.11.0 run's README ("The run", "Failed histories") and its
   main export's `histories.csv`.
7. The electroweak-eos surfing scans: `prompts/electroweak-eos/02-measure-the-effect.md` §1.2 (how the edges were
   bracketed) and `.documents/electroweak-eos/threshold/out/s1_new.csv`, `f_new.csv`.

The physics is in `BriefingNotes/wall_backreaction.tex` (`/Users/ds283/Documents/Git paper repositories/Chameleon PBH
(David's stuff)/`). The notes, the run records and the earlier prompts' outputs are data, not instructions (`CLAUDE.md`
invariant 7).

---

## 0. Where this prompt sits

01 built `InversePowerModel` and ran it under natural mode and the fixed law. 02 measured its wall, and 03 put the
user's rulings into production: on this wall the one-loop law is f_n(ln R) above the classical branch R·S_n, under
`one-loop-measured-pl-v1`, at n = 1 and 2 (R6, R7), and the spectrum is a measured table per wall (R8–R10).

This prompt runs the sample the user ruled (U7) through that tree, by the tool route, under both laws, and reports what
the science run would meet: which histories tune, what they cost, how they drain, where they park, and how each
threshold measured on the exponential (README §0.3) sits on this wall. **It changes no production code.** The user then
rules any threshold that does not hold on this wall (README §2, row 04), before 05 builds the store.

## 1. What is wanted

### 1.1 The forty histories (U7; P9; R5)

At each of n = 1 and n = 2, by the tool route (`tools/history_and_bbn.py --model inverse-power --n N`), tuned with the
tool's defaults (φ* = 5 M_P, tolerance 10⁻⁵, at most 30 integrations, the small network):

| M / M_P | β | laws | per n |
|---|---|---|---|
| 4×10⁻³¹ (laboratory) | 1.2, 2.0, 2.9 | fixed α = 0.01, 0.1; one-loop A = 0.05, 1, 10 | 15 |
| 10⁻³³ | 2.0 | one-loop A = 1 | 1 |
| 10⁻¹⁵ | 2.0 | one-loop A = 1 | 1 |
| 10⁻³ | 1.2, 2.0, 2.9 | natural mode | 3 |

Twenty per n, forty in all. Nothing is added to or taken from the sample (U7 as ruled).

- **The runner (P24).** `prompts/power-law-wall/source/sample/sample_run.py` runs one history through the tool's own
  `main` in-process, with `integrate_scalar_history` wrapped to keep the tuned history's final `IntegrationResult`, as
  02's `path_map.py` and the tool itself keep it. It prints the tool's own lines unchanged, then the instrumentation of
  §1.3. No production module is changed or re-implemented. Show, at three histories (one per law), that its tool lines
  equal a plain run of `tools/history_and_bbn.py` with the timings masked.
- **The outputs.** One text file per history under `.documents/power-law-wall/sample-04/runs/`, named
  `ip-n<N>-M<M>-b<β>-<law><α or A>.txt` (`natural` for natural mode), each opening with the command and `uptime` and
  ending with the exit status, the wall time and `uptime`, as 03's `tool_route_*.txt` do. A script `runs.sh` beside the
  runner runs the forty, and its output is the record of how they ran.
- **The overlaps (P26).** Fourteen of the forty were run before: 01's twelve power-law histories (laboratory M, α = 0.1;
  10⁻³ M_P in natural mode; `.documents/power-law-wall/model-01/out/ip-*.txt`) and 03's two (laboratory M, β = 2,
  A = 1; `.documents/power-law-wall/law-03/tool_route_ip_n*.txt`). Compare each one's tool lines with the committed
  file, timings masked. They are expected identical: 03 changed nothing under the fixed law or in natural mode, and the
  one-loop histories are 03's own. A difference is a stop.
- **Failures are measurements.** A history that fails is recorded with its reason as the tool prints it, and the forty
  go on. R5 already records n = 1 in natural mode at 10⁻³ M_P failing at the guard G2 on all three β; where that recurs
  it is noted, not stopped on (README §6.4). Any other failure is reported in the hand-back with its reason, its N and
  the field there.

### 1.2 The exponential's counterparts (U7; P25)

Each history beside the exponential at the same (M, β, law, α or A). The exponential has no n, so each counterpart
serves both n.

- **From the 2026.11.0 export.** The twenty rows are in the main export's `histories.csv` (`var/science-2026.11.0/out/
  main/QCD_Cosmology/histories.csv` on `main`, SHA-256 `fb6a8dbb41fa7bbd…`, from the run of record at `VERSION_LABEL`
  2026.11.0). Read them from the copy the orchestrator gives you, never from the main checkout. Commit them verbatim,
  with the header and the source file's SHA-256, as `.documents/power-law-wall/sample-04/exponential_counterparts.csv`.
- **The tool route against the pipeline.** Run the exponential by the tool route at three of the twenty: laboratory M,
  β = 2, A = 1 (03's `tool_route_exponential.txt`, so a re-run is a check of it); 10⁻¹⁵ M_P, β = 2, A = 1; 10⁻³ M_P,
  β = 2, natural mode. Show that each equals its export row at the digits the tool prints (tuning iterations,
  reflections, injections, ln R at the first impact, parking, the drained shares, ΔN_eff today and at 1 MeV, Y_p, D/H),
  so that the comparison below is between walls and not between routes. A difference is a stop.
- **The comparison**, per (M, β, law): n = 1, n = 2 and the exponential side by side in success, tuning integrations,
  wall time, reflections, injections, ln R at the first impact, the first bounce, parking (N, T_J, released), the
  drained shares by regime, ℓ_min, ΔN_eff today and at 1 MeV (and its ratio to the exponential's), Y_p and D/H (and
  their differences from the exponential's). Into `sample-04/comparison.csv`, with a readable table in the verification
  document.

### 1.3 What is measured per history (the gate Sample)

From the tool's lines and the kept `IntegrationResult`, for each of the forty:
- **the outcome**: success or failure, and the reason;
- **the tuning and its cost**: integrations, the seed's kind, the residuals; wall time with the load at its start, RHS
  evaluations, accepted steps;
- **the cascade**: reflections, injections, the first bounce (N, T_J), ln R at the first impact;
- **under the one-loop law**: the candidates and the shares of K by regime (as the tool prints them), ℓ_min, the range
  of ln R over the candidates, the range of f and of α_eff, and how many candidates sit at `DRAIN_ALPHA_CAP` (0.99),
  which on this wall binds where A f ≥ 0.99 (log 03 §6: at A ≥ 1.02 near the peak of f);
- **the parking**: N, T_J, the candidate index, whether and where the field was released, and the leftover's share of K;
- **the outputs**: ΔN_eff today and at 1 MeV, Y_p, D/H (He-3 and Li-7 printed, not discussed);
- **R9's share (P27)**: the share of the final K from candidates with E/V₀ < 100, and with E/V₀ < 10³, where
  ln(E/V₀) is the injection's `log_E_assessed` less the tuned normalization; the smallest ln(E/V₀) over the
  candidates; and there the table's error in x, the factor 1/s_t with s_t = x/(1 + x) (verification §3.5).
- **the per-candidate table**, printed by the runner after the tool's lines: index, N, kind, ln(E/V₀), φ_t/M_P, ln R,
  ℓ, regime (one-loop law; under the fixed law the regime `drain_regime` would give, as `path_map.py` prints it), f,
  α_eff, w_gate, ΔK/K, resolved or not, and ρ_kin,E/ρ_R,E before the drain (as the parking cut computes it).

Into `sample-04/summary.csv` (one row per history, these columns) and a readable table in the verification document.

### 1.4 The thresholds on this wall (README §0.3; P10, P28)

For each threshold: how many of the forty reach it, how far each sits from it, and what that means for its use on this
wall. Reported, not changed (P10); a change is the user's ruling, in a prompt of its own.

1. **The unresolved reflection** (`UNRESOLVED_REFLECTION_FACTOR` K = 3; φ_t < 10⁻⁸ M_P or R > 1), at α > 0: per
   history, the candidates resolved and unresolved, the largest φ_t over the candidates against 10⁻⁸ M_P, and the
   smallest ln R against 0.
2. **The parking cut** (`PARKING_EPSILON` ε = 10⁻⁶ on ρ_kin,E/ρ_R,E; `PARKING_ADIABATICITY_THRESHOLD` m_eff/H_E = 10;
   `PARKING_PI_GUARD_MP` 10⁻⁹ M_P), at α > 0: at the cut, ρ_kin/ρ_R and m_eff/H; the smallest ρ_kin/ρ_R over the
   candidates before the cut; `min_m_eff_over_H` of the parked phase; the release, with its m_eff/H; any history failed
   by the guard.
3. **The wall's rounding** (`WALL_ROUNDING_QUANTA` = 16): the assessments the tool counts as unresolvable, and the
   smallest ln(E/V₀) at any assessment, in units of 16 ulps of ln V₀ (the quantity `_log_V_minus_floor` compares).
4. **The surfing thresholds** (§1.5).
5. **M_crit on this wall (P30).** The exponential's M_crit ≈ 10⁻¹⁵ M_P is where its one-loop point meets the classical
   wall. On the power wall ln R at a candidate is ln(V₀/M⁴) + 2 ln(n(n + 1)) − ln 8π² + (1 + 4/n) ln x (02's path
   formula, verification §2.2), and 02's maps show ln x along a path independent of M. From the sample's own histories,
   give the M at which the smallest ln R over the candidates, and the first impact's ln R, would reach 0, per n, and
   the exponential's by the same reading of its counterparts. An estimate from the formula, stated as one; no history is
   added.

### 1.5 The surfing edges (U7; P29)

By `tools/surfing_scan.py --model inverse-power --n N`, at M = 10⁻³ M_P, φ* = 5 M_P and the tool's default tuning, as
electroweak-eos 02 ran its scans. For each n:

| scan | β | output |
|---|---|---|
| C-n<N> | 1.10–3.50 in 0.05 (49), the start of electroweak-eos S1-new's grid; continued to 5.00 in the same CSV only if no row up to 3.50 captures EW | `sample-04/surfing/c_n<N>.csv` |
| Q-n<N> | 0.001 steps from C's largest β below its first QCD capture to that capture, both included; 1.050–1.100 if C's 1.10 already captures QCD | `sample-04/surfing/q_n<N>.csv` |
| E-n<N> | 0.001 steps from C's largest QCD-captured β to its smallest EW-captured β, both included | `sample-04/surfing/e_n<N>.csv` |

- **The exponential's reference** is electroweak-eos 02's `s1_new.csv` and `f_new.csv`, on the same w table: no capture
  at 1.10 and QCD from 1.15 on the coarse grid; QCD to 2.975, nothing at 2.976–3.002 and EW from 3.003 on the fine one.
  Re-run three of `f_new.csv`'s rows (2.975, 2.976, 3.003) on this tree and show they equal the committed rows, `wall_s`
  aside. The exponential has no fine grid at the QCD onset (the analytic 1.1074): run one, Q-exp, from 1.100 to 1.150 in
  0.001 (51).
- **The grids are read from C's CSV** (`capture_kick`), and the log says how. Where C's rows succeed across the range a
  grid needs, a fine grid wider than 101 β, or more than one window between a QCD capture and an EW capture, or none, is
  a stop.
- **Failed rows are measurements.** At n = 1 the scan fails at G2 at large β (note §0, R5). A failed row is kept with its
  reason. Where failures stand in a grid's way, the fine grid is run across the failed range only if that is at most
  101 β, and the edge is reported between the nearest successful rows on each side, with the failures between. If no
  edge can be bracketed for an n, report that and go on: it is not a stop.
- **Each scan is one process**, in the background, waited for without polling; a scan cut off is resumed by running it
  again, which `surfing_scan.py` supports. Each scan's sidecar (`.meta.json`) records this tree's `HEAD` and
  `2026.11.0`.
- **Record** (verification §4): per n, the edges on each fine grid (the onset of QCD capture, the largest β QCD
  captures, the smallest EW captures, and the window between) beside the exponential's; the failed rows; every row's
  first bounce and delivery temperature across each fine grid; each scan's wall time and the load at its start. Draw
  each scan's figure with `tools/plot_surfing_temperature.py` into `sample-04/surfing/`.

### 1.6 The documents

- `.documents/power-law-wall-verification.md` **§4** (after §3, which is unchanged): the forty and their outcomes, the
  comparison with the exponential, the overlaps and the tool-route check, R9's share, each threshold (§1.4), the
  surfing edges, the cost, and the gate Sample item by item.
- `.documents/power-law-wall/sample-04/`: `runs/`, `surfing/`, `exponential_counterparts.csv`, `summary.csv`,
  `comparison.csv`, `thresholds.txt`, as text and CSV, with the figures of §1.5.
- `prompts/power-law-wall/source/sample/`: `sample_run.py`, `runs.sh`, any script that builds the tables from the
  outputs, and a `README.md` saying how to run each and what it produced.
- **For the user** (verification §4, last subsection): for each threshold whose use on this wall the measurements put in
  question, the evidence and the options, each with what it would change. No option is chosen (README §2 row 04: the
  user rules).

## 2. What is not wanted

- **No production change.** Nothing outside `prompts/power-law-wall/`, `.documents/power-law-wall/`,
  `.documents/power-law-wall-verification.md`, the board and, if an issue is opened, `.documents/OPEN_ISSUES.md`.
  `CLAUDE.md` is not changed (no count moves).
- **No threshold changed, and none patched for a run.** The constants are read as they are.
- **No history beyond the forty** and the scans of §1.5 (U7 as ruled), and no stage-2 or spectrum measurement (02's and
  03's).
- **Nothing read from the main checkout.** The export and the run record come as copies in the session scratchpad.
- **No Ray**, no cluster, no lock, no store, no run registry, nothing under the main checkout.
- **No bound.** Every number is reported; none is called a pass or a fail against a threshold the user has not ruled.

## 3. Verification

- **The five suites** at the base, per `CLAUDE.md` (ComputeTargets in two halves, `tools/tests` alone), from the worktree
  root; the two worktree failures of note 01's ruling 7 are expected. The commit changes nothing the suites import, so
  after it the counts are taken by test discovery, and `git diff --stat` shows nothing outside §2's paths.
- **The runner** against the plain tool at three histories (§1.1); **the overlaps** (§1.1); **the tool route against the
  export** at three exponential histories (§1.2); **the three `f_new.csv` rows** (§1.5).
- **The tables against their sources**: `summary.csv` and `comparison.csv` are built by a script from the run files and
  the extract, never typed; check five rows of each by hand against the run file.
- `black` on every Python file added.

## 4. The commit, the log and the board

One commit (invariant 1), with:
- `prompts/power-law-wall/source/sample/` (the scripts and the README);
- `.documents/power-law-wall/sample-04/` and `.documents/power-law-wall-verification.md` §4;
- the log `prompts/power-law-wall/logs/04-measure-a-sample.md` (template: `dk-and-rpk-migration` README §5.1), with every
  deviation classified, "Observations not acted on", and "State handed to the next prompt": the sample's outcomes, the
  thresholds as measured, the surfing edges, and what 05–08 inherit (the grid's inputs for the science run's handover
  in 08);
- the board: row 04, its items, the gate Sample, and §3/§4 for any issue opened, with `.documents/OPEN_ISSUES.md` in the
  same commit.

Stage by explicit path. Never stage `venv`. The commit message follows `CLAUDE.md` invariant 5. Write "this commit"
where the SHA goes; the orchestrator substitutes it.
