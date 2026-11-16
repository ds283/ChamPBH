# Log 04 — measure a sample

**Subject:** U7's forty power-law histories by the tool route under both drain laws, beside the exponential's 2026.11.0
counterparts; the thresholds of README §0.3 read off them; R9's share; an M_crit estimate; the surfing edges at
M = 10⁻³ M_P (P24–P32). No production change.
**Commit:** `05f6920`, on branch `power-law-wall`, on top of `7d7c578`
**Date:** 2026-11-15 to 16, by Claude Opus 5.5 (one agent, in the worktree `.claude/worktrees/power-law-wall`)
**Result:** **COMPLETE.** The gate Sample holds (§4). No stop was reached. The scans put the surfing thresholds at n = 1
in question; the evidence and options go to the user (verification §4.9), none chosen.
- **The forty:** 37 succeeded; the three failures are R5's (n = 1, natural mode, 10⁻³ M_P, G2). Every α > 0 history
  tuned in one integration from the tracking seed, every one of its 1632 candidates is an unresolved one-loop
  reflection, and every one parks at its last candidate, none released.
- **The checks:** the runner equals the plain tool at three histories, the fourteen overlaps equal 01's and 03's files,
  and the exponential's tool route equals its export row at the tool's digits at three histories (ruling 3: all
  identical).
- **Beside the exponential:** under the fixed law at laboratory M both n equal it (ΔN_eff ratio 1); under the one-loop
  law ΔN_eff at 1 MeV is 0.67–0.99 (n = 1) and 0.78–0.99 (n = 2) of the exponential's; at 10⁻¹⁵ M_P 6.8 × 10⁵ and 1.0 ×
  10⁶ times it.
- **R9's share:** 0 at every history (no candidate with E/V₀ < 10³); the smallest ln(E/V₀) is 11.46.
- **The thresholds:** the unresolved reflection, the parking cut and the wall's rounding are never near their edges
  (verification §4.5). The first impact is one-loop at every M ≤ M_P on both n.
- **The surfing edges:** n = 2 as the exponential (QCD onset (1.116, 1.117] on both, largest QCD capture 2.975 and EW
  from (3.002, 3.003] on both); n = 1 no EW edge, 74 of 79 coarse rows failing (73 at G2, one tuning).
- **Issues:** none opened, none closed.
- **Suites:** 26, 732 (219 + 513), 163, 302, 76 at the base, with only the two worktree failures of note 01's ruling 7;
  the same counts after, by discovery (P32).

## 1. What shipped

- **`prompts/power-law-wall/source/sample/`** (new): `sample_run.py` (the runner, P24), `runs.sh` (the forty),
  `tool_route.sh` (the exponential's three), `scans.sh` (one surfing scan per call), `tables.py` (the extract, the
  comparisons, the tables, the markdown of verification §4), `edges.py` (the grids and `surfing/edges.txt`),
  `README.md`.
- **`.documents/power-law-wall/sample-04/`** (new): `runs/` (forty files and `runs.log`), `tool-route/` (three files and
  `export_check.txt`), `surfing/` (the CSVs of C-n1, C-n2, Q-exp, Q-n2, E-n2 and the f_new check, their `.meta.json`,
  each scan's `.console.txt`, `edges.txt`, and the figures `surfing_temperature_*.pdf`/`.png`),
  `exponential_counterparts.csv`, `summary.csv`, `comparison.csv`, `thresholds.txt`, `checks.txt`.
- **`.documents/power-law-wall-verification.md` §4** (after §3, which is unchanged).
- **The board:** row 04, the gate Sample, the baselines, the status line and its history. No §3/§4 entry moved, so
  `.documents/OPEN_ISSUES.md` is unchanged.
- **Not committed:** the base suites' outputs, the runner check's six outputs (`…/scratchpad/agent04/runner-check/`), in
  the agent's scratch directory. `CLAUDE.md` is unchanged.

## 2. Deviations from the prompt

### 2.1 The orchestrator's rulings (note 04 §0)

1. **The worktree and the main checkout.** All work in the worktree with `./venv/bin/python` and `PYTHONPATH=.`; the
   main checkout neither read nor touched (the export and the run record were the given copies); `venv` never staged;
   `/tmp/ray-cpbh` never named; nothing in `/tmp` but the session scratchpad. *STRUCTURALLY REQUIRED.*
2. **The sample is U7's.** The forty and the scans of prompt §1.5; nothing added or dropped. No explanatory run was
   needed. *STRUCTURALLY REQUIRED.*
3. **The overlaps and the tool route.** Compared with `wall=`, `PRyM_time=` and the `uptime` lines masked: the fourteen
   overlaps identical, the tool route equal to the export at the printed digits (§3.2). *IMPLEMENTATION CHOICE, ruled.*
4. **Failures are measurements.** R5's three recur and are noted; the scans' failures (G2 at 73 rows of C-n1; a tuning
   that did not converge at C-n1 β 3.20; a φ ≤ 0 step underflow at Q-n2 β 1.149 and E-n2 β 2.991) are recorded with
   their reasons, N and field, and named first in the hand-back. *IMPLEMENTATION CHOICE, ruled.*
5. **The surfing scans.** The grids read from the coarse scans by `edges.py grids` (§2.2 D3, D9); at most three scans at
   once, none while the forty ran (the forty ended 19:47:08, the first scans started 19:47:20). *IMPLEMENTATION CHOICE,
   ruled.*
6. **The machine.** The forty one at a time; `uptime` at the start and end of every run, scan and suite, here and in the
   files. *IMPLEMENTATION CHOICE, ruled.*
7. **The two worktree failures** (`test_registered_run…this_repositorys_head`, `test_registry_config…
   test_roots_and_distributions`) failed at the base, and nothing else. *IMPLEMENTATION CHOICE, ruled.*
8. **A finding about production** is recorded (§6), not fixed. No issue opened. *IMPLEMENTATION CHOICE, ruled.*
9. **No option chosen.** Verification §4.9 states the evidence and the options; none is recommended. *STRUCTURALLY
   REQUIRED.*
10. **No file but the prompt's §2 paths.** The commit holds `prompts/power-law-wall/source/sample/`, the log, the board,
    `.documents/power-law-wall/sample-04/` and the verification document. *STRUCTURALLY REQUIRED.*

### 2.2 Others

- **D1 — the runner check's first launch did not run.** I first launched the six check runs from a zsh loop that did not
  split its argument list (zsh does not word-split `$spec`), so each run was refused by argparse (exit 2, 1–2 s) and
  left six files named with spaces in the scratch directory. I re-ran the six from a bash script; `checks.txt` §1
  records those, the only ones compared. Nothing of the first launch is committed. *UNINTENDED DRIFT.*
- **D2 — the runner also wraps the tool's `tune_history` name.** P24 names the wrap of `integrate_scalar_history` (for
  the `IntegrationResult`); the instrumentation also needs the tuned payload (the normalization, the ledger's K, the
  drain record), which the tool's `main` does not return. `sample_run.py` replaces the tool module's `tune_history`
  binding by a wrapper that keeps what it returns, for the duration of the call, and restores it. Neither wrap changes
  an argument or a result; the tool lines are identical to the plain tool's (§3.2). *IMPLEMENTATION CHOICE.*
- **D3 — Q-n1 was not run.** C-n1's first QCD capture is at 1.30; the rows below it at 1.15, 1.20 and 1.25 failed (G2)
  and 1.10 succeeded uncaptured. I read prompt §1.5's failure rule as extending the grid to the nearest successful row
  below, 1.100–1.300, 201 β, which is more than 101, so the fine grid is not run and the onset is reported in (1.10,
  1.30] with the three failures between. The other reading takes "C's largest β below its first QCD capture" literally,
  1.25 (a failed row), and gives 1.250–1.300 (51 β), which would narrow the onset only if rows there succeed. I took the
  first; the second is an option for the user (verification §4.9). *IMPLEMENTATION CHOICE.*
- **D4 — f is printed only under the one-loop law.** Prompt §1.3's per-candidate table lists f; under the fixed law no
  fraction is used, so the column is `-` there (the regime `drain_regime` would give is printed, as `path_map.py`
  prints it). *IMPLEMENTATION CHOICE.*
- **D5 — R9's share counts the candidates' ΔK only.** The parking leftover is not a candidate and has no generation's
  ln E; it is the column `K_share_parking`. *IMPLEMENTATION CHOICE.*
- **D6 — "the smallest ρ_kin/ρ_R over the candidates before the cut"** is taken over the candidates the cut's ratio test
  examined, indices 1 to the cut's less one (index 0 is never tested, `parking_ratio_test`). *IMPLEMENTATION CHOICE.*
- **D7 — more files than prompt §1.6 lists**, all under `.documents/power-law-wall/sample-04/`, a path §2 allows:
  `tool-route/` (the three exponential runs and `export_check.txt`), `checks.txt` (every comparison's output), the
  scans' `.console.txt` files, and `surfing/edges.txt`. Ruling 10 holds. *IMPLEMENTATION CHOICE.*
- **D8 — the extract opens with one comment line** naming the source, its size and SHA-256, before the source's header
  and the twenty rows verbatim, field for field; the source's CRLF line ends are written as LF (the repository's
  `core.autocrlf` is `input`, which would commit them so in any case); `tables.py` skips lines beginning `#`.
  *IMPLEMENTATION CHOICE.*
- **D9 — Q-n2 was read from C-n2 while C-n2 ran.** C-n2's rows at 1.10 (uncaptured) and 1.15 (QCD) were written by
  20:42, when `edges.py grids` gave Q 1.100–1.150; Q-n2 started then, with C-n2 still running. The grid is the same at
  C-n2's end (21:00). E-n2 was read after C-n2 ended. *IMPLEMENTATION CHOICE.*
- **D10 — the waits.** Each long job (the base suites, the forty, each scan) ran in the background, and its end was
  awaited by one background loop on its output's last line, which notified me once; I did not poll. *IMPLEMENTATION
  CHOICE.*
- **D11 — C-n1's continuation** to 5.00 ran as `scans.sh c_n1_ext` into the same CSV (`surfing_scan.py` resumes and
  skips the β it holds), with its own console file `c_n1_ext.console.txt`; the sidecar has a second entry for it.
  *IMPLEMENTATION CHOICE.*
- **D12 — the exponential's wall time and first-bounce N** are not in the export, so `comparison.csv` leaves those
  columns blank for the exponential; its first bounce's T_J is the export's `T_deliver_GeV`, equal to the tool's at the
  three tool-route histories (§3.2). *STRUCTURALLY REQUIRED.*

## 3. Verification performed

### 3.1 The suites (P32)

At the base `7d7c578`, serially, from the worktree root, before anything else ran:

| suite | tests | result | time | start (one-minute load) |
|---|---|---|---|---|
| CosmologyModels | 26 | OK | 66 s | 19:08 (4.15) |
| ObjectFactories | 163 | OK | 131 s | 19:09 (6.31) |
| RunDrivers | 76 | 1 failure: `test_the_script_is_main_py_with_its_hash_and_this_repositorys_head` | 25 s | 19:11 (3.32) |
| ComputeTargets `test_[a-f]` | 219 | OK | 147 s | 19:12 (3.95) |
| ComputeTargets `test_[g-z]` | 513 | OK | 540 s | 19:14 (2.97) |
| tools | 302 | 1 failure: `test_roots_and_distributions` | 296 s | 19:23 (8.29) |

The two failures are note 01's ruling 7's worktree failures. The commit changes nothing the suites import (only
`prompts/` and `.documents/`), so the counts after are by test discovery at the commit's tree: 26, 219 + 513, 163, 302
and 76 (2026-11-16 00:48).

### 3.2 The runner, the overlaps, the tool route (prompt §1.1, §1.2; ruling 3)

- **The runner against the plain tool**, 19:29–19:31 (load 2.8–5.9), in scratch: n = 1, laboratory M, β 1.2, α 0.01;
  n = 2, laboratory M, β 2.9, A = 10; n = 2, 10⁻³ M_P, β 1.2, natural. Identical (12, 13 and 15 tool lines).
- **The overlaps:** all fourteen identical to 01's and 03's committed files (`checks.txt` §2).
- **The tool route against the export:** at laboratory M, β 2, A 1; 10⁻¹⁵ M_P, β 2, A 1; 10⁻³ M_P, β 2, natural, every
  printed quantity equal to its export column at the tool's digits, the first bounce's T_J included
  (`tool-route/export_check.txt`); the laboratory one identical to 03's `tool_route_exponential.txt`.
- **The three `f_new.csv` rows** (2.975, 2.976, 3.003) re-run on this tree equal the committed rows in every column but
  `wall_s` (`checks.txt` §5).

### 3.3 The tables against their sources (prompt §3)

`summary.csv`, `comparison.csv` and `thresholds.txt` are written by `tables.py tables` from the run files and the
extract; the tables of verification §4.1–§4.2 by `tables.py markdown` from those. Five rows of each checked by hand:
- `summary.csv` against the run files: `ip-n1-M4e-31-b1.2-oneloop0.05` (RHS 218094, steps 30773, 170 reflections and
  candidates, bounce N 17.667851520 at 231.124418 MeV, K shares 0.996585 / 0.00341543, ℓ_min 3.61497, parking N
  37.519156875 at candidate 169, Y_p 0.2564811213, D/H 2.576543225, log_x_min 11.464016); `ip-n2-M1e-15-b2.0-oneloop1`;
  `ip-n1-M4e-31-b2.9-fixed0.1`; `ip-n2-M1e-3-b2.9-natural`; `ip-n1-M1e-3-b2.0-natural` (the failure, its full reason).
  The first check found the reason cut at its first " N=" (the key=value reader) and the run's wall time carrying the
  `uptime` text; both were fixed in `tables.py` before the tables were written for the commit.
- `comparison.csv` against the run files and the extract: the rows (10⁻¹⁵ M_P, β 2, A 1) — injections 38 / 23 / 27, Y_p
  − exp 0.000696 for n = 2 (0.247330736 − 0.24663470109898286), ratio 1.02783 × 10⁶ (0.0595076 / 5.7896504170221005e−8);
  (laboratory M, β 1.2, A 0.05); (laboratory M, β 2, α 0.1); (laboratory M, β 2.9, A 10); (10⁻³ M_P, β 2, natural).

### 3.4 The thresholds, the share, the edges

As verification §4.4–§4.6 record them, from `thresholds.txt` and `surfing/edges.txt`.

### 3.5 `black`

`black --check` on the three Python files added (`sample_run.py`, `tables.py`, `edges.py`): clean.

## 4. The gate Sample, item by item

1. **Every sampled history measured under both laws, re-tuned:** U7's forty, as ruled, by the tool route, tuned with the
   tool's defaults (verification §4.1), each beside its exponential counterpart (§4.2), the route checked against the
   export (§4.3).
2. **Its failures stated:** R5's three in the forty, with reason, N and field; the scans' failures with theirs (§4.6).
3. **Every threshold of README §0.3 reported against its use on this wall** (§4.4–§4.6), nothing changed (P10), and the
   options for the user stated without a choice (§4.9).

## 5. The deliberate-breakage record

Prompt 04 adds no test and changes no production code. The comparisons were shown to report a difference, in scratch:
`tables.py compare` of `runs/ip-n2-M4e-31-b2.0-oneloop1.txt` against a copy with one digit of its `bbn` line's Y_p
changed (0.2472528311 → …312) printed DIFFERENT and the line; `tables.py export-check` of the laboratory exponential's
tool-route output with ΔN_eff at 1 MeV changed (0.041888 → 0.041889) printed DIFFERENT for that quantity and "1
DIFFERENT".

## 6. Observations not acted on

- **The surfing tool's T_max and n_crossings on a reflected first bounce.** On the power wall at 10⁻³ M_P the first
  bounce is a floor reflection (`deliver_reflected` true at every β), where the exponential's is a resolved turning
  point. Where the exponential's T_max is its delivery, the power wall's T_max is blank and `n_crossings` one fewer
  (10 of 49 coarse rows at n = 2, 33 of Q-n2's 50, 74 of E-n2's; 3 of C-n1's 5): the turning point of T_J at the
  reflection is not counted. The capture is classified on T_capture, which agrees with the exponential's to 7.3 × 10⁻⁹,
  so no edge moves; but `plot_surfing_temperature.py` draws no "largest" mark there, and an analysis reading T_max or
  `n_crossings` from a power-law scan would read them differently from the exponential's.
- **Two failure kinds in the scans besides G2.** C-n1 β 3.20: the tuning did not converge in 30 integrations (H/H0 − 1 =
  +4.7 × 10⁻⁴). Q-n2 β 1.149 and E-n2 β 2.991: R4's φ ≤ 0 `ComputationFailureError` on repeated trial states until the
  step fell below 10⁻¹³ e-folds, at N = 27.743, φ_E = 1.32 × 10⁻¹³ M_P, π_E = −8.7 × 10⁻⁴ M_P (tuning integration 0) and
  N = 36.255, φ_E = 4.72 × 10⁻¹⁴ M_P, π_E = −4.5 × 10⁻⁴ M_P (tuning integration 2). Both are natural mode at 10⁻³ M_P,
  the regime R5 ruled "less important"; neither was investigated (U7: no history beyond the forty and the scans).
- **Ten of C-n1's G2 failures came after the seed's integration** (tuning integrations 1, 2 or 5), so a tuning step can
  move a history into G2 that its seed did not reach; the other 63 failed in the seed's.
- **n = 2 costs more to scan than the exponential:** 1.3–11 times the exponential's wall time per β on C-n2 (29–208 s
  against 19–74 s), from 2–520 reflections after the delivery against none.
- **The one-loop law on this wall never reaches its cap or its classical branch in the sample.** ln R ≥ 39.2 at every
  candidate, so f ≤ 0.50 (the falling branch) and A f ≤ 0.81 < `DRAIN_ALPHA_CAP`; no candidate is gap or classical. The
  classical branch R·S_n (R6) and the rising branch of f_n are exercised by no history here; the M_crit estimate puts
  the late cascade's classical turn at M ≳ 10⁻²⁷ M_P to above M_P depending on n, β and A (verification §4.5 item 5).
- **Natural mode at 10⁻³ M_P, n = 2, moves BBN** by +0.0053 to +0.0065 in Y_p and +0.140 to +0.164 (×10⁻⁵) in D/H
  against the exponential, with 4–59 reflections where the exponential has none (01 recorded the same).
- **The planner's M_crit figures and R9 expectation** are reproduced where they overlap, except that the one-loop
  histories at A = 0.05 and 1, β 1.2 reach ln(E/V₀) = 11.5–24.3, below the planner's 33.30 from the fixed-law maps; the
  share below 10³ is still 0.

## 7. State handed to the next prompt

- **The sample's outcomes** (`.documents/power-law-wall/sample-04/summary.csv`, `comparison.csv`): 37 of 40 succeed; the
  three failures are R5's (n = 1, natural mode, 10⁻³ M_P, G2). Every α > 0 history tunes in one integration from the
  tracking seed (11–32 s with BBN); natural mode at n = 2 in four (48–97 s). All 1632 candidates are unresolved one-loop
  reflections; every α > 0 history parks at its last candidate and none is released. Under the fixed law at laboratory M
  both n equal the exponential; under the one-loop law ΔN_eff at 1 MeV is 0.67–0.99 (n = 1) and 0.78–0.99 (n = 2) of the
  exponential's at laboratory M, 0.75 | 0.84 at 10⁻³³ M_P, and 6.8 × 10⁵ | 1.0 × 10⁶ times it at 10⁻¹⁵ M_P.
- **The thresholds as measured** (`thresholds.txt`): the unresolved reflection (φ_t ≤ 3.3 × 10⁻²⁹ M_P, ln R ≥ 39.2), the
  parking cut (ratio 3.2–9.9 × 10⁻⁷ and m_eff/H ≥ 2.2 × 10¹⁰ at the cut, ≥ 1.06 × 10⁴ parked, no release, no guard) and
  the wall's rounding (no unresolvable assessment) are never near their edges; R9's share is 0 (ln(E/V₀) ≥ 11.46). Only
  the surfing thresholds at n = 1 are in question (verification §4.9), for the user to rule before 05.
- **The surfing edges at 10⁻³ M_P**: the exponential's QCD onset (1.116, 1.117] (new, Q-exp) and EW (3.002, 3.003]
  (electroweak-eos 02, re-checked at three rows); n = 2: QCD onset (1.116, 1.117], largest QCD capture 2.975, EW from
  (3.002, 3.003], the window 2.976–3.002 (one failed row, 2.991), as the exponential's; n = 1: onset in (1.10, 1.30], no
  EW edge (74 of 79 coarse rows fail).
- **M_crit:** the first impact is one-loop at every M ≤ M_P on both n (estimates 1.4 × 10³ M_P and above); the late
  cascade's classical turn is estimated per history in `thresholds.txt` §5.
- **05 inherits** nothing new in code. The sample's tool-route outputs are the "by value" reference that 07's rehearsal
  is compared against (README §2 row 07: "against 04"): `runs/*.txt` hold each history's printed values, and the store
  keys of every history (M, n, β, the law and α or A, `one-loop-measured-pl-v1`).
- **08 inherits the grid's inputs for the science run's handover:** the per-history cost (11–32 s at α > 0, up to 97 s
  natural mode at large M), the n = 1 natural-mode failures at large M (R5, and in the scans G2 at 73 of 79 β, a tuning
  non-convergence and, at n = 2, two φ ≤ 0 step underflows), the M_crit estimates (which M the classical branch would
  first enter), the surfing edges, and the T_max/`n_crossings` difference of the surfing scans on a reflected first
  bounce (§6).
