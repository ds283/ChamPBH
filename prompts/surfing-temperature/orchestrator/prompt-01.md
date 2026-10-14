# Orchestrator — prompt 01, reachability roots

Read [`../README.md`](../README.md) first: §0.1, §0.3 (with the U5 addendum), §2.2 (01's
charter), §4, §5 and §6.1 (U5). This is the campaign's first note. Its conventions are those of
`read-only-plots`' notes ([`../../read-only-plots/orchestrator/prompt-01.md`](../../read-only-plots/orchestrator/prompt-01.md)
§0, "Conventions"), restated here where 01 needs them.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`01-reachability-roots.md`](../01-reachability-roots.md), as amended for U5 in
`09b41ce`
**Board items:** R1–R3 · **Gate:** "Roots" · **Closes:** nothing · **Opens:** only what the work
finds · **Model:** Sonnet, as the prompt recommends · **No network.**

**Gate:**
- The prompts are written (`a8823a9`) and U5 is recorded (`09b41ce`). `HEAD` is the commit that adds
  this note, or a later one that touches no code ("code" is anything outside `prompts/`; nothing
  outside `prompts/` has changed since `f41b104`).
- One prompt at a time in this checkout. 02 is not dispatched until 01 is reviewed.

## 0. What makes this prompt unusual

**It is a pure function over the EOS, and the EOS has a fourth kick.** The orchestrator's probe at
`a8823a9` found it while writing this note, and the user ruled it a fourth kick, `mu-pi` (U5). The
prompt was amended before dispatch. The amendments:
- **four labels**, `EW`, `QCD`, `mu-pi`, `ee`, and a stop for any other count;
- **both records of a merged interval carry the interval's roots**;
- **a merge test at β = 2.1 and 2.2**;
- **three troughs**;
- 2.1 and 2.2 added to the measurement table.

The risk is not the code but two numerical points:
- **The merge is inside the campaign's range.** The QCD–`mu-pi` trough has Σ_eff = 0.07182, which
  exceeds c = 1/(3β²) for every β > 2.154. So more than half the scan's β see a merged QCD interval,
  and how the merged record is shaped is what 02 and 03 read. The amended R2 says it: both records
  carry the interval's own `T_hot` and `T_cold` and slopes, and `merged` names the other kick.
- **Σ near the table's ends.** `Xav_EOS_spline.w` returns exactly 1/3 at and beyond the table's
  ends, so Σ = 0 there and the spline is not consulted. Inside the bottom decade (10–30 keV) Σ
  ripples about zero, between −3e-6 and +2e-5. Those ripples are five local maxima below the floor,
  which the log must still list (R1). No ripple comes near c, which is at least 0.0133 in the
  campaign's range, so none gives a root.

**The peak temperatures, ruled here so that §4 of the prompt does not fire for nothing.** README
§0.3's 53.25 GeV for the EW peak is a grid argmax (`test_kicking_function.py:86` gives 53.2214 at
1000 per decade). The EW maximum is flat, and the orchestrator's bounded refinement puts it at
53.262 GeV. §4's "differs from README §0.3 by more than its last digit" binds **Σ_peak**, which is
well conditioned. For `T_peak` the test is 1e-3 relative against the orchestrator's refined values
below, and a difference beyond that is recorded and reported, not a stop. The agent's own refined
values govern, and its log records them.

**What moves on purpose:**
- `extract_common.py`: `find_kicks` and `reachability_roots` (and any constants and imports they
  need), beside `kick_threshold_curve` (`:554-572`), which is unchanged.
- New: `ComputeTargets/tests/test_reachability_roots.py`. It sorts into the `test_[g-z]*.py` half.
- The log, `prompts/surfing-temperature/logs/01-reachability-roots.md` (`logs/` does not exist yet),
  and the board.
- No issue is expected. The index (`.documents/OPEN_ISSUES.md`) is untouched unless one opens. That
  `test_three_peaks` does not see the shoulder is not an issue: it pins three windowed peaks, and
  pins them correctly.

**The facts, checked by the orchestrator on 2026-10-14 at `a8823a9` / `09b41ce`.** Pass them on.
- **The tree.** Branch `surfing-temperature`, `HEAD` `09b41ce` before this note; nothing staged. The
  untracked set is the prompt's gate list: 17 `pilot-2026.6.0*.db`, `pilot-main.log`,
  `pilot-plot.log`, `pilot-out/`, `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh`,
  `prompts/jordan-normalization/`.
- **`extract_common.py`**: 879 lines, black-clean, blob `7a20c34b` at `09b41ce`. It imports `numpy`,
  `matplotlib` and five functions from `math`. It imports nothing from `scipy` today, so a `scipy.optimize` import
  is new. Its importers are `plot_by_beta.py`, `plot_ScalarModel.py`, `test_extraction.py` and
  `test_reflection_reporting.py`, so an import added at module level is paid by both plot scripts.
  That is fine, and the log says which import was added.
- **The production cosmology** (`QCD_Cosmology(0, Planck_units(), Planck2018())`) builds in about
  1 s on this machine, at load about 140.
- **The table's ends**: `_T_min = 1e-05` GeV, `_T_max = 25118.864315095823` GeV
  (`Xav_EOS_spline._T_min`/`_T_max`; the CSV's first and last rows).
- **The orchestrator's probe** (a 20 000-point grid uniform in ln T over the table, local extrema
  refined by `minimize_scalar(method="bounded", xatol=1e-12)` in ln T; scratch only). Its results
  are measurements, not specifications, and the agent's own governs.
  - The nine local maxima of Σ:

    | T | Σ | Kick |
    |---|---|---|
    | 53.2623 GeV | 0.0374358 | EW |
    | 0.181893 GeV | 0.314532 | QCD |
    | 43.1559 MeV | 0.0853651 | `mu-pi` |
    | 0.160450 MeV | 0.100732 | ee |
    | 27.76, 21.67, 17.06, 13.52, 10.49 keV | 2.1e-5, 1.7e-6, 1.1e-7, 8.0e-9, 2.5e-9 | below the floor |

    The count is the same on 2000, 20 000 and 200 000 points.
  - The troughs between the four kicks:
    - EW–QCD: 9.02648 GeV, Σ = 0.00474113, merging at β = 8.395;
    - QCD–`mu-pi`: 72.9881 MeV, Σ = 0.0744962, merging at β = 2.15434;
    - `mu-pi`–ee: 7.54403 MeV, Σ = 0.000259515, merging at β = 35.84.

    Below the floor there are five ripple minima at 12–30 keV, four of them with Σ < 0.
  - The thresholds √((2 + Σ_peak)/(6 Σ_peak)): EW 3.01177, QCD 1.10745, `mu-pi` 2.01779,
    ee 1.86436.
  - The roots, in GeV, hot then cold, as T decreases:

    | β | Roots |
    |---|---|
    | 1.2 | 0.22918, 0.15328 |
    | 2.0 | 0.7416, 0.096529; 2.2912e-4, 1.1825e-4 |
    | 2.1 | 0.85259, 0.086344; 0.058696, 0.034502; 2.5812e-4, 1.0907e-4 |
    | 2.2 | 0.97291, 0.031096; 2.8544e-4, 1.0259e-4 |
    | 3.1 | 68.498, 41.095; 2.1112, 0.021073; 4.9198e-4, 7.8677e-5 |
    | 5.0 | 175.99, 18.431; 4.2609, 0.015989; 8.8436e-4, 6.3413e-5 |

    At β = 2.2 the QCD interval runs across the trough, from 0.973 GeV down to 31.1 MeV.
- **`test_kicking_function.py`'s `test_three_peaks`** looks for each peak by argmax in fixed windows:
  [10 keV, 5 MeV], [50 MeV, 1 GeV] and [20 GeV, 1 TeV]. The shoulder at 43 MeV lies outside all
  three, which is why no earlier campaign saw it. That test is not touched (README §5 rule 13).
- **The stub idiom.** `TestKickThresholdCurve` (`test_extraction.py:112-152`) uses
  `SimpleNamespace(w=…)` and calls with dimensionful T. A stub for 01 needs `w` and `dw_dlogT`. Its
  two Gaussians must be far enough apart that the other's tail is below 1e-9 relative at each root,
  or the "closed form on each flank" in R3.1 is not exact to the tolerance.
- **The suites at `09b41ce`** are given in the dispatch (§1 step 2).

**What the review exists to establish.**
- **(E1) The kicks.** `find_kicks` on the production EOS returns four kicks, labelled and ordered as
  U5 says. Their Σ_peak equal README §0.3's to its digits and their `T_peak` the probe's to 1e-3.
  The windows' edges are the refined troughs and the table's ends. Every local maximum is in the
  log, above and below the floor.
- **(E2) The roots.** On the stubs: analytic to 1e-9, the slope signs right, None below threshold,
  and the merged stub giving one interval with no root at the trough. On the production EOS: the
  probe's roots reproduced; β_th(root) = β to 1e-9; and the merge appearing between β = 2.1 and 2.2,
  with both records carrying the interval's roots.
- **(E3) The interface.** The field names are R2's, which 02 and 03 read. `kicks` is passed in. The
  records are plain. `kick_threshold_curve` is unchanged.
- **(E4) The pins.** Each of the four breakages fails a named new test.
- **(E5) The records.** The log, the board, the counts.

**Conventions.** All of these bind the agent, and the brief says so.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-only`
  before committing. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its
  follow-up.
- **Suites.** `CLAUDE.md`'s commands, ComputeTargets in its two halves, each in the foreground,
  serially. Each run's output goes to a file in the agent's scratch directory, the verdict is grepped
  from it, and the load (`uptime`) is recorded beside it. The machine is heavily loaded (load
  80–145 at writing), so the halves may take much longer than 200 s together. A failure is re-run
  alone before it is called real.
- **Breakage diffs** are recorded so that `git apply` replays them from a clean tree at the agent's
  commit. Check each with `git apply --check` and `-R --check`, and replay in `bash`, not zsh.
- **Scratch.** The agent's own subdirectory of the session scratchpad, never `/tmp`. Python's
  `tempfile` in tests is fine. No scratch file goes under `ComputeTargets/` or `.documents/`.
- **The venv.** Nothing installed or uninstalled.
- **Stores and Ray.** None opened, none started. 01 runs no history and no PRyMordial.
- **Stop conditions are real.** A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Review against the agent's commit.

## 1. Before you dispatch

1. **The tree.** Branch and `HEAD`; `git status --short` shows only §0's untracked set; nothing
   staged; `git log --oneline a8823a9..HEAD` touches only `prompts/`.
2. **Baselines.** The four roots at `HEAD`, serially, with the load. Give the agent the numbers.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Sonnet**. Give it:
- the prompt;
- the campaign README and this board;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad.
  The agent does not read `orchestrator/`, and does not read prompts 02–05.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/01-reachability-roots.md`, in `dk-and-rpk-migration`
  README §5.1's form.
- **The files it may add or change are the prompt's §5 list, and nothing else.**
- **U5 is ruled.** Four kicks is the expected result, not a stop; any other count is.
- **It starts no Ray, opens no store, runs no history and installs nothing.**
- **Stop and ask** on any of the prompt's §4 conditions, as narrowed for `T_peak` in §0. An existing
  test that fails is current behaviour moving, never a reason to edit it.
- **It replies with:**
  - the log's path and the message as committed;
  - the four kicks with their windows, and every local maximum found;
  - the troughs;
  - the β table of §2.3;
  - the new tests by class, with counts and wall time;
  - the four suites;
  - every deviation with its tag;
  - "State handed to the next prompt", verbatim.

## 3. The review — ten checks

1. **Scope.** `git diff <agent>~1 <agent> --stat` touches only `extract_common.py`, the new test
   module, the board and the log. `git status` shows §0's untracked set and nothing new.
2. **E3, by `ast` against the pre-image** (`09b41ce:extract_common.py`). The module body differs by
   added imports and constants (if any) and the two new `FunctionDef`s, placed after
   `kick_threshold_curve`. Every other top-level statement dumps identically.
3. **E1, by reading.** `find_kicks` in full: the grid, the refinement, the floor, the labels and the
   window edges. Check how a maximum at a grid end is handled, how a ripple below the floor between
   two kicks is handled (it must not split a window), and whether the outermost edges are T_lo and
   T_hi.
4. **E2, by reading.** `reachability_roots` in full:
   - the hot and cold classification by the direction of Σ_eff − c as T decreases;
   - `brentq`'s bracket and xtol;
   - the pairing into intervals;
   - the assignment to kicks by `T_peak` containment;
   - the merged record, with both kicks carrying the interval's roots;
   - the `multiple_crossings` rule;
   - the slope formula −3 (dw/d ln T)/Σ, and its fallback;
   - what happens at a root that is exactly a grid point, and in an interval left open at a table end.
5. **E1/E2, a probe** from the scratchpad, independent of the agent's tests. On the production EOS,
   `find_kicks` with the defaults is compared with §0's table: Σ_peak to 1e-5 and `T_peak` to 1e-3.
   `reachability_roots` at β = 1.2, 2.0, 2.1, 2.2, 3.1 and 5.0 is compared with §0's roots table to
   its digits, with the `merged` fields read at 2.1 and 2.2. At β = 1.10 every root is None. At
   β = 1.1075 only QCD has roots, a narrow pair either side of 0.1819 GeV. At each root,
   `kick_threshold_curve` gives β to 1e-9.
6. **E2, edge cases by probe.** β exactly at a threshold, to the printed digit and a hair either
   side: no exception, and None or a degenerate pair consistently. Non-default `n_grid`, such as 500
   and 5000: the same kicks and the same roots, to the xtol.
7. **E4.** In `bash`, replay each breakage diff with `git apply`, run the named test, see it fail for
   the stated reason, revert with `git apply -R`, and check `git status` is clean. Then read the
   tests: does a test pin each of these?
   - the hot root above `T_peak` and the cold one below;
   - the slope signs;
   - the merged record's shape at 2.2 on the production EOS (not only on the stub);
   - the four labels.
8. **E5, the records.** The board: row 01 ✅ with "this commit" and the log; R1–R3 ✅; the gate
   "Roots" ✅; `### 5.2 After prompt 01 (<date>)`. §3 and the index untouched, or, if one opened,
   the index row, count and date in the same commit. The log in §5.1's form, every deviation
   classified; the table of local maxima, the troughs and §2.3's β table present.
9. **The suites** at the agent's commit, serially: 18, 61 + (91 + new), 69, 34. `black --check` on
   the two touched Python files.
10. **The interface for 02 and 03.** The log's "State handed to the next prompt" names the
    functions, their signatures, the record's fields, the kick labels and how a merged interval
    reads. Check them against prompts 02 §T3 and 03 §F1–F2 (read here by the orchestrator; the
    agent did not). Anything that would force 02 or 03 to read a field that is not there is recorded
    for their notes.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 01* paragraph in §5.2. Fix
  small residue in a follow-up commit of its own:
  - "this commit" → the SHA (board and log);
  - a list of the orchestrator notes on the board, with this one marked "used for 01";
  - the status line: 1 of 5 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the message as committed;
  - the four kicks and their thresholds;
  - the merge and where it begins;
  - the tests added and the breakages;
  - the four suites' counts;
  - anything 02's or 03's note must carry.
- **Then 02.** Its note is written after this review.
