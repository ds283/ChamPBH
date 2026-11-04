# Log 02b — measure the law over a wider A

**Subject:** Measure the one-loop drain law at six more values of A
**Commit:** `cc1f449`, on branch `one-loop-drain`, on top of `62d3f2f`
**Date:** 2026-11-04, by Claude Opus 5.5
**Result:** **COMPLETE.** The Wide gate holds (`.documents/one-loop-drain-verification.md` §2b.12). U14 is open for the
user, with 02's and 02b's measurements at nine A (verification §2b.11).
- **The reproduction check** holds. 02's `P-2197-A1-zero` and `R-865-A0.1-zero`, re-run by 02b's driver, are bitwise
  equal to 02's lines and partial files. So 02's lines at A = 1, 0.1 and 0.01 are reused.
- **The sample.** 02's thirty histories at A ∈ {10, 5, 2, 1.5, 0.5, 0.05}, under both gap prescriptions: 360 tasks. Of
  these, 171 were run (167 succeeded, 4 failed, none timed out), 159 were derived by S1 and 30 by S2. The spot checks of
  both shortcuts and the tool check are bitwise.
- **The law over nine A.**
  - Every tuning at A = 10, 5, 2, 1.5 and 0.5 succeeds, under both prescriptions.
  - At laboratory M every P and W history stays one-loop and parks, from A = 10 down to A = 0.05. A = 0.05 builds there
    (24–26 s a tuning), where A = 0.01 does not.
  - The four failures are at A = 0.05: R-632 (β = 1.0), by "the effective minimum was not found", and the ladder's
    5 × 10⁻²⁰ M_P, by the step budget, each under both prescriptions. These are 02's two kinds, at the two histories
    whose A = 0.1 tunings failed in 02.
- **The first impact.** At A = 10 it drains 84–99% of the condensate's kinetic energy at laboratory M in the
  transition window, and the field parks after 2–5 more one-loop drains, at N = 18.8–24.7. At A = 5 it drains 42–59%,
  and the field parks after 5–10 more. The cap acts at 1–3 drains per P/W history at A = 10, and at the first impact for
  P-2188 (A = 10) and R-632 (A ≥ 5). 2197's ΔN_eff today is 0.564 at A = 10 and 0.277 at A = 5.
- **Cost:** 43 min 12 s from the first launch to the last end, at most six workers; ruling 5's projection was 0.06 h.
- **Issues:** none opened. `[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]` is extended by a dated
  paragraph (ruling 9). Index: 31 open, unchanged.
- **Suites** before anything else: 26, 566 (163 + 403), 130, 156, 68, all passing. After: the same counts, all passing.

The orchestrator's note for this prompt (§0, rulings 1–10 and the conventions) governed where it differs from the
prompt, with note 02's rulings 6 and 9 where it names them. Each ruling is classified in §2.

## 1. What shipped

No code outside `.documents/one-loop-drain/measure-02b/` changed (prompt §1.5), and nothing under
`.documents/one-loop-drain/measure/` changed.

- **`.documents/one-loop-drain/measure-02b/`**, each script with its docstring and its command:
  - `wide.py`: 02b's paths and A values, 02's `common` imported, the merge of 02's and 02b's lines, and the comparisons
    across the two directories (ruling 4);
  - `tasks_wide.py`: 02's `tasks.py` with nine A, written as `var/one-loop-drain/02b/tasks.json` (540 tasks, 02's 180
    among them, equal to 02's);
  - `run_wide.py`: the driver, 02's `run_sample.py` imported, with its paths moved and ruling 6's additions;
  - `decide_wide.py`: ruling 1's order, S2 generalised, S1, ruling 5's workers and projection, and the derived lines;
  - `reproduce.py`: ruling 4's reproduction check;
  - `spot_wide.py` and `tool_wide.py`: the spot checks (prompt §1.2) and the tool check (ruling 8);
  - `analyse_wide.py`: the Wide gate's tables (§2.1–§2.7), by 02's `analyse.py` imported, and the first impact and the
    cap.
- **`measure-02b/out/`** (text only): `tasks.txt`, `decisions.txt`, `derive.txt`, `reproduce.txt`, `spot_plan.txt`,
  `spot_check.txt`, `tool_check.txt`, `sample.txt`, `regimes.txt`, `first.txt`, `spread.txt`, `fixed.txt`, `cost.txt`,
  `u14.txt`. The section numbers printed in the headers of `regimes.txt`, `spread.txt`, `fixed.txt`, `cost.txt` and
  `u14.txt` are 02's (they come from 02's `analyse.py`); the verification document maps them.
- **`.documents/one-loop-drain-verification.md` §2b**, added after §2, which is unchanged.
- **The board**: row 02b, two items, the Wide gate, U14 "open for the user, with 02's and 02b's measurements", the
  dated paragraph on `[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]`, §5, and **Last updated**.
  `.documents/OPEN_ISSUES.md` is not changed (ruling 9; D5).

Not committed: everything under `var/one-loop-drain/02b/`, namely:
- `sample.jsonl` (360 lines), `repro.jsonl` and `spot.jsonl`;
- `tasks.json`;
- `tasks/`, with each task's stdout, stderr, partial file and result;
- `batches/`, with each batch's ids, `batch.pid`, `batch.log` and `batch.end`.

**The scripts: each import, override and copy (ruling 3).**

| 02b script | 02 module imported | overrides of its module globals (each read at call time) | copies |
|---|---|---|---|
| `wide.py` | `common` | none; `run_lines` and `all_lines` are called with 02b's path explicitly, since their default argument is bound to 02's `SAMPLE` at import | none |
| `tasks_wide.py` | `tasks` | `A_VALUES` → the nine A | none |
| `run_wide.py` | `run_sample` | `OUT`, `TASKS_JSON`, `TASK_DIR`, `BATCH_DIR` → `var/one-loop-drain/02b/…`; `SELF` → `run_wide.py`, so that 02's `batch()` launches each child through 02b's driver; `read_partial` → 02's, wrapped to drop `ells` as well | `child()`, below |
| `decide_wide.py` | (through `wide`) | none | none: its own subcommands, written on 02's `decide.py` |
| `reproduce.py`, `spot_wide.py` | (through `wide`) | none | none |
| `tool_wide.py` | `tool_check` | `CHECKS` → `("P-2197-A10-zero",)`; `TASK_DIR`, `SPOT` → 02b's; `run_lines` → 02b's | none |
| `analyse_wide.py` | `analyse` | before each subcommand: `all_lines` → the merged 540 lines (sample, regimes, fixed, u14) or 02b's 360 (spread, cost); `A_VALUES` → the nine A or 02b's six; `OUT` → 02's directory (fixed, u14: `fixed_law.json`) or 02b's (cost: its batches) | none |

`run_wide.py` also wraps two of 02's functions, without copying them (ruling 6's additions):
- `reduce_result` calls 02's, then adds `ells` (each candidate's `DrainCandidate.ell`, in order) to the partial line,
  and two counts to the partial line and the task's line, `cap_one_loop_by_ell` and `cap_gap_edge`;
- `accepted_values` calls 02's, then adds `first_impact`, from the payload.

**The one copy**, `run_wide.child`, against 02's `run_sample.child` (`inspect.getsource` of each, after `black`):

```diff
-            rec.pop("regimes"), rec.pop("injections")
+            rec.pop("regimes"), rec.pop("injections"), rec.pop("ells")
 ...
-        rec.pop("regimes"), rec.pop("injections")
+        rec.pop("regimes"), rec.pop("injections"), rec.pop("ells")
```

Nothing else differs. The names the copy calls (`ROOT`, `TASK_DIR`, `load_tasks`, `reduce_result`, `accepted_values`)
resolve in `run_wide.py`'s module, to 02b's paths and the wrappers above. The reproduction check is the test that the
copy changed nothing it should not.

**Where A is read (prompt §1.2, re-checked on this tree).** In `ComputeTargets/ScalarHistory.py`,
`integrate_scalar_history` reads `alpha`:
- in `check_drain_law` (:1933);
- as `DrainRecord.A` (:1948);
- in the tests `alpha > 0.0` or `alpha == 0.0` (:1961, :2066, :2353, :2375, :2433, :2448, :2474, :2945, :3131);
- in the fixed law's `alpha_effective` (:2130, :2134), which the one-loop branch (:2190–:2195) does not call;
- in `drain_alpha_effective` (:2195), under the one-loop law.

`integrate_history` reads it in `check_drain_law` (:4430), passes it to the integration (:4525), and tests
`alpha > 0.0` (:4647). `tune_history` reads it in `check_drain_law` (:5087) and passes it on (:5126). `check_drain_law`
under the one-loop law (:1662) asks only that A be finite and > 0. Those are the sites note 02 and note 02b found, so S2
holds at any A > 0 and nothing is a stop. The tool reads `args.alpha` only to parse it, to pass it to the tuning
(:571, :588) and to print it.

## 2. Deviations from the prompt

**The orchestrator's rulings**, each as its note classes it:
1. **The order of §1.4, made definite** — *IMPLEMENTATION CHOICE*, ruled; followed. Step 2 was one batch for `zero` at
   A = 10, 5, 2, 1.5 and 0.5 (125 tasks; my reading of S2 added none to the orchestrator's five histories), then S1 and
   one batch of 16 `edge` runs. Step 3 started with P's two alone at A = 0.05 on two workers, then the projection, then
   the 23 `zero` tasks and the 5 `edge` runs.
2. **Detached, not registered** — *STRUCTURALLY REQUIRED*. Each batch was launched with `nohup` (and `disown`), its pid
   in `batches/<name>/batch.pid`, with `batch.log` and `batch.end`. Each was checked once that it had started, and waited
   on by a command that ended with it.
3. **The scripts** — *IMPLEMENTATION CHOICE*, ruled; followed. The imports, overrides and the one copy are in §1, with
   the copy's diff. Every child was launched through `run_wide.py`. Nothing was written under `var/one-loop-drain/02/`:
   it holds the same files as before, and 02's `measure/` has no new file. `wide.py` sets `sys.dont_write_bytecode`
   before it imports 02's modules, so that not even a `__pycache__` is written there.
4. **The reproduction check, made exact** — as the prompt intends; followed. It holds (§4; verification §2b.2). The
   comparison is run under both of note 02's shortcut modes, and the differences of either are reported, so that
   neither `gap_prescription` nor `A` is excused.
5. **Workers and the projection** — *IMPLEMENTATION CHOICE*, ruled; followed. Step 2 at six workers. The projection
   after P's two: 49 × 26.1 s / 6 = 213 s = 0.06 h, with min(6, ⌊8 GiB / 0.254 GiB⌋) = 6 workers, so it did not stop.
   Later batches: the largest peak RSS reached 1.326 GiB in step 3b, and ⌊8 / 1.326⌋ = 6, so step 3c had six workers.
   The spot batch ran five tasks on five.
6. **The first impact and the cap, made exact** — *IMPLEMENTATION CHOICE*, ruled; followed. The parking leftover is not
   among the injections: it is the `Parking` record's ΔK, a ledger entry of its own, while the injections are parallel
   to the candidates (`assert len(cands) == len(inj)` holds in every run). So it is counted apart, as its share of K.
   The two cap counts agree on all 167 successful 02b runs, 43 of them with a capped one-loop drain.
7. **"Bitwise"** — as note 02; followed.
8. **The tool check** — as note 02; followed. It is bitwise after masking (§4).
9. **The failures** — as the prompt intends; followed. The four failures at A = 0.05 are of 02's two kinds and extend the
   issue as a dated paragraph. None is of another kind, and none points at the drain law's code, the wall search or the
   inside-wall guard.
10. **No file but these** — *STRUCTURALLY REQUIRED*; followed (§4's staged list).

**The agent's own deviations:**
- **D1 — four batches ran on the scripts before `black`.** The repro, step2a, step2b and step3a batches ran on the
  scripts as first written. `black` was then run (16:13 UTC, while step 3a's two children, already loaded, were
  finishing), and the rest ran on the formatted text. Black changed only layout: the AST of every one of the eight files
  is equal before and after (checked with `ast.dump`). *IMPLEMENTATION CHOICE*.
- **D2 — the first chain of the suites after the batches ran nothing.** A zsh loop did not split its words, and each
  `unittest` call failed on a path, in under a second. It was re-run as a bash script, and those are the numbers in §4.
  *UNINTENDED DRIFT*, corrected.
- **D3 — two header lines of `analyse_wide.py` were corrected after the first run**, and `first.txt` and `u14.txt`
  re-made. They described an "edge … where it differs" filter and a "flags after the history" column that the code did
  not have. The numbers were unchanged. *UNINTENDED DRIFT*, corrected.
- **D4 — the spot batch also carried the tool check**, as 02's step 4 did, at five workers for five tasks.
  *IMPLEMENTATION CHOICE*.
- **D5 — `.documents/OPEN_ISSUES.md` is not changed.** Ruling 9 says the issue's row changes only if its hook no longer
  holds. The hook, "fail tuned at A = 0.01 … β = 1.0 at A = 0.1 too", still holds, so the count, the row and the date
  stay. *IMPLEMENTATION CHOICE*, under ruling 9.
- **D6 — a derived line also carries `values_from_sample`** ("02" or "02b"), so that the analysis finds the partial
  file of an S2 source in 02's directory. *IMPLEMENTATION CHOICE*.
- **D7 — `tool_check.txt`'s closing line says "on both tasks"**, from 02's script, where there is one. It was left as
  02's text rather than copying the script to change one string. *IMPLEMENTATION CHOICE*.
- **D8 — `out/spot_plan.txt`** records the spot batch's ids as planned. It is an output beyond §1's list.
  *IMPLEMENTATION CHOICE*.

## 3. The run

The batches, from `batches/*/batch.log` (verification §2b.4); times UTC, 2026-11-04:

| batch | tasks | workers | wall | loads start → end | largest task RSS |
|---|---|---|---|---|---|
| repro | 2 | 2 | 22.1 s | 6.58 8.73 12.98 → 6.73 8.61 12.81 | 248 MiB |
| step2a | 125 | 6 | 325.2 s | 6.83 8.44 12.62 → 12.19 12.07 13.31 | 260 MiB |
| step2b | 16 | 6 | 72.3 s | 11.17 11.85 13.20 → 27.16 15.57 14.44 | 257 MiB |
| step3a | 2 | 2 | 26.1 s | 33.05 17.35 15.09 → 51.69 23.38 17.35 | 246 MiB |
| step3b | 23 | 6 | 996.5 s | 44.77 23.65 17.59 → 5.75 13.67 18.49 | 1354 MiB |
| step3c | 5 | 6 | 942.3 s | 5.22 13.16 18.23 → 9.50 19.63 26.96 | 1358 MiB |
| spot | 5 | 5 | 86.3 s | 9.25 18.78 26.44 → 14.34 20.28 26.41 | 280 MiB |

- **Outcomes.** 171 run, of which 167 succeeded and 4 failed (`R-632-A0.05-zero` and `-edge`: the effective minimum;
  `L-M5e-20-A0.05-zero` and `-edge`: the step budget). None timed out. 189 were derived, 159 by S1 and 30 by S2.
- **Stop conditions.** None met: the suites stand; the reproduction check, the spot checks and the tool check are
  bitwise; A is read nowhere §1.2 does not list; the projection was 0.06 h at six workers; no failure is the wall search
  or the inside-wall guard, or points at the drain law's code.
- **Workers and memory.** At most 7 computing processes of this prompt ran at once, a batch parent and six children,
  beside the idle shells that waited on them. The peak RSS of a task was 1358 MiB.
- **Nothing outlives the batches.** Every `batch.pid` fails `kill -0`, every batch has its `batch.end`, and no
  `run_wide.py`, `run_sample.py` or `history_and_bbn.py` process is alive. `runregistrykit list` is unchanged at 23 runs,
  all finished.

## 4. Verification performed

- **The Gate**, again at `62d3f2f` before anything ran: it held (verification §2b.1).
- **The suites before** (2026-11-04, local time = UTC; serially, the `test_[g-z]` half and `tools/tests` in the
  background):

  | suite | count | time | uptime at start |
  |---|---|---|---|
  | CosmologyModels | 26 OK | 71.7 s | 15:47, load 10.71 18.97 22.10 |
  | ObjectFactories | 130 OK | 83.8 s | (same chain) |
  | RunDrivers | 68 OK | 27.3 s | (same chain; at its end, 15:50, 7.04 13.22 19.02) |
  | ComputeTargets `test_[a-f]` | 163 OK | 141.4 s | 15:50, 6.78 12.96 18.87 |
  | ComputeTargets `test_[g-z]` | 403 OK | 477.4 s | 15:53, 17.12 14.84 18.56 |
  | tools | 156 OK | 160.6 s | 16:01, 7.49 10.21 14.38 |

- **The suites after** (one serial background chain, 16:48–17:05, after every batch had ended):

  | suite | count | time | load at start |
  |---|---|---|---|
  | CosmologyModels | 26 OK | 70.7 s | 16:48, 22.79 20.89 26.24 |
  | ComputeTargets `test_[a-f]` | 163 OK | 142.8 s | 16:50, 52.30 31.83 30.06 |
  | ComputeTargets `test_[g-z]` | 403 OK | 483.5 s | 16:52, 14.03 22.94 26.68 |
  | ObjectFactories | 130 OK | 84.0 s | 17:00, 31.45 17.85 21.07 |
  | tools | 156 OK | 163.7 s | 17:02, 26.58 19.67 21.46 |
  | RunDrivers | 68 OK | 24.0 s | 17:04, 24.47 22.86 22.53 (at its end 21.95 22.36 22.36) |

  The counts equal those before: 26, 566 (163 + 403), 130, 156, 68.
- **The reproduction check** (`out/reproduce.txt`): both tasks bitwise equal to 02's, in the line and the partial file
  (10 and 78 candidates). Their stdout is 13 of 13 lines equal after masking.
- **The spot checks** (`out/spot_check.txt`): S1 on `P-2197` at A = 10 and A = 0.05, and S2 on `C-3016` at A = 10 and
  `C-3233` at A = 0.05. All four are bitwise equal, including the full regime sequences and the injections' triples
  (12, 268, 660 and 1806 entries).
- **The tool check** (`out/tool_check.txt`): `P-2197-A10-zero`, 13 of 13 lines byte-equal after masking, and 22 of 22
  values.
- **Ruling 6's cross-checks** (`out/first.txt`, last section): the cap counted by ℓ and by α_eff agree in all 167
  successful 02b runs, and the line's `first_impact` (from the payload) equals the partial file's in all 167.
- **`black`** was run on the eight scripts; the AST of each is unchanged by it (D1).
- **`git diff --cached --name-status`** before the commit:

  ```
  M	.documents/one-loop-drain-verification.md
  A	.documents/one-loop-drain/measure-02b/analyse_wide.py
  A	.documents/one-loop-drain/measure-02b/decide_wide.py
  A	.documents/one-loop-drain/measure-02b/out/cost.txt
  A	.documents/one-loop-drain/measure-02b/out/decisions.txt
  A	.documents/one-loop-drain/measure-02b/out/derive.txt
  A	.documents/one-loop-drain/measure-02b/out/first.txt
  A	.documents/one-loop-drain/measure-02b/out/fixed.txt
  A	.documents/one-loop-drain/measure-02b/out/regimes.txt
  A	.documents/one-loop-drain/measure-02b/out/reproduce.txt
  A	.documents/one-loop-drain/measure-02b/out/sample.txt
  A	.documents/one-loop-drain/measure-02b/out/spot_check.txt
  A	.documents/one-loop-drain/measure-02b/out/spot_plan.txt
  A	.documents/one-loop-drain/measure-02b/out/spread.txt
  A	.documents/one-loop-drain/measure-02b/out/tasks.txt
  A	.documents/one-loop-drain/measure-02b/out/tool_check.txt
  A	.documents/one-loop-drain/measure-02b/out/u14.txt
  A	.documents/one-loop-drain/measure-02b/reproduce.py
  A	.documents/one-loop-drain/measure-02b/run_wide.py
  A	.documents/one-loop-drain/measure-02b/spot_wide.py
  A	.documents/one-loop-drain/measure-02b/tasks_wide.py
  A	.documents/one-loop-drain/measure-02b/tool_wide.py
  A	.documents/one-loop-drain/measure-02b/wide.py
  M	prompts/one-loop-drain/IMPLEMENTATION_STATE.md
  A	prompts/one-loop-drain/logs/02b-measure-wide-A.md
  ```

## 5. The deliberate-breakage record

None: this prompt changes no code. The checks that could fail did run on real data: the reproduction check (two tasks),
the spot checks (four), the tool check, ruling 6's two cross-checks, and the store check in `tasks.py`, which exits 1 on
a mismatch.

The cross-directory comparison of `wide.py` was exercised, by hand, on pairs known to differ:
- the reproduced `P-2197-A1-zero` against 02's `P-2197-A0.1-zero` reports 54 differences in the line and 11 in the
  partial file, the first being N_final and accepted_steps;
- the same against 02's spot run `P-2197-A1-edge`, compared with nothing excused, reports the one difference
  `partial[0].drain.gap_prescription: 'zero' != 'edge'`.

## 6. Observations not acted on

- **Ruling 5's projection was low by a factor of about nine.** It costed every remaining A = 0.05 task at P's 26 s, but
  the ladder's 5 × 10⁻²⁰, 8 × 10⁻¹⁸ and 10⁻¹⁶ M_P took 711–942 s each, in their long classical tails. Steps 3b and 3c
  took 0.54 h of batch wall time against the projected 0.06 h, still under the 3 h limit.
- **ΔN_eff at 1 MeV is close to proportional to A at laboratory M** (2197: 0.0535–0.0547 × A over A = 0.05–10), while
  ΔN_eff today falls more slowly than A below 0.5, because more of the drain comes after 1 MeV.
- **Y_p and D/H in the transition window are not monotonic in A.** For W-2192, Y_p is 0.25405, 0.24976, 0.25889, 0.26258,
  0.26723 from A = 10 down to 1. Not diagnosed.
- **β = 10 has its largest ΔN_eff today at A = 5** (0.0060–0.0061), not at A = 10 (0.0053–0.0055).
- **Where the cap binds at the first impact, the result is independent of A.** R-632 is the same history at A = 5 and 10,
  and L-M5e-20 and L-M8e-18 at A ≥ 5 give the same ΔN_eff today, 0.147450948991, to 12 digits.
- **Under `edge`, A ≥ 1.4667 caps every gap drain at 0.99** (A·c₁/ℓ_edge ≥ α_cap).
- **R-632 at A = 0.5 enters the gap at its last candidate only** (index 20, ln R = 6.30, K share 5 × 10⁻⁷ under `edge`);
  the two prescriptions agree there to the digits printed.
- **`L-M8e-18-A0.05-edge` declined one unresolved reflection** in its accepted integration, and `R-632-A0.05-edge`
  (failed) met 7 unresolvable assessments. In 02, unresolvable assessments arose in failed integrations only, and
  declined reflections in an accepted integration only for 3873.
- **Loads of 30–79 from other work** on the shared machine came during steps 2b to 3c and the suites after; the wall
  times above were taken under them.

## 7. State handed to the next prompt

For 03 (the store):
- **The regimes that arise over nine A:** one-loop, gap and classical. No candidate was unassessed (0 of 50 935 at 02b's
  A), and no one-loop or gap candidate had w_gate < 1 (0 of 6358 and 0 of 158).
- **A range of A that 02 did not meet:** A up to 10 is accepted by `check_drain_law` and tunes in one integration. The
  cap acts at one-loop drains from A = 0.5 at R-632 and on the ladder (5 × 10⁻²⁰ and 8 × 10⁻¹⁸ M_P), and from A = 5 at
  laboratory M in the transition window, so 03's key must carry A as a float with no upper bound. Under `edge`, A ≥ 1.4667 caps every gap drain.
- **A record field 02 did not need:** none. The cap is derivable from `DrainCandidate.ell` and A, and the first impact
  from the candidates and injections in order, which 01's record holds. The parking leftover is the `Parking` record's
  ΔK, outside the injections, so a store that carries the injections and the parking record carries both.
- **Histories at A = 10 park early**: N = 18.8–24.7 for P and W, 18.6–19.7 in block B at β = 1.2. A store of them holds
  few bounce rows: 3–9 candidates per history at A = 10 across P, W, R and B.
- **U14** is open for the user, with 02's and 02b's measurements at nine A (verification §2b.11).
- **`[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]`** is open (board §3), extended to A = 0.05: R-632
  and L-M5e-20 fail there, under both prescriptions; every P and W history builds.
