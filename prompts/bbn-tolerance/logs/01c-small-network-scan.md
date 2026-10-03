# Log 01c — Measure the small network

**Prompt:** prompts/bbn-tolerance/01c-small-network-scan.md
**Commit:** the commit that adds this file ("Measure PRyMordial's small network for bbn-tolerance"); its SHA is in `git log`
**Model:** Claude Opus 5.5
**Date:** 2026-10-03
**Result:** COMPLETE WITH DEVIATIONS. P11 selects **the small network at low-T `rtol` 1e-6, `atol`
1e-11**, which meets all four criteria. The reproduction of log 01's S3 is bitwise. There were no
solver failures in 340 small-network solves. One flagged judgement needs the orchestrator's
review: deviation 2. The breadth sample, drawn by the stated rule, includes the store's one
output-check row (β = 0.95, M = 10⁻³; README §0.5 excludes it). It comes back as an output-check
FAILURE (Yp = 0.51205, as stored), even though every `solve_ivp` succeeded. I have not counted it
as a failed solve.

All runs are on `893a5b1`, the branch head when the prompt started; nothing landed on the branch
while it ran. The tool's CSV `commit` column reads `893a5b1`. Nothing in `tools/`, `PRyM/` or any
production file changed. The store was opened only through the tool's `connect_ro`.

## What shipped

No code. **`VERSION_LABEL`** stays `"2026.6.0"` and **`PRYM_VERSION`** stays `"bf24c3d+ri02+sr01"`.

**`prompts/bbn-tolerance/logs/01c-probes/` (new).**

- **Scripts.**
  - `breadth.py`: enumerates T2's breadth sample through `connect_ro`.
  - `run_jobs.py`: the parallel driver, adapted from `01-probes/run_jobs.py` and copied rather
    than edited in place. Its blocks are `T1-repro`, `T1-rest` and `T2 --t2-rtol X`.
  - `compare_s3.py`: T1 against log 01's S3.
  - `summarize.py`: P11 criteria 1–3, P12 and T2.
  - `cost.py`: adapted from `01-probes/cost.py`. It takes the network in the setting, as
    `NETWORK:RTOL`.
- **Data.**
  - `breadth.csv`: 95 histories.
  - `scan.csv`: 340 rows (T1 245, T2 95), with the columns of log 01's `scan.csv`. The block is in
    the `tag` column (deviation 1).
  - `cost.csv`: 36 rows.
- **Printed outputs.** `compare_s3.txt`, `scan_summary.txt` (from `summarize.py --detail`),
  `cost_output.txt`, `pinned_values.txt`.

`run_jobs.py` also wrote each invocation's stdout to `out/`. That directory was deleted, because
the CSV holds every value it printed.

## Deviations from the prompt

### 1. The `block` column is the tool's `tag` column — IMPLEMENTATION CHOICE

The prompt asks for "the columns of log 01's `scan.csv` and a `block` column". The tool writes
`CSV_FIELDS`, and it may not change (P10). Its `--tag` column is where log 01 put S1/S2/S3. So
`run_jobs.py` passes `--tag T1` or `--tag T2`, and `tag` is the block column. The alternative was
to post-process the CSV and add a `block` column copying `tag`. That would duplicate a column and
make the file differ from the tool's own format. The summary scripts read `tag`.

### 2. The breadth sample's output-check row is not counted as a failed solve — IMPLEMENTATION CHOICE (flagged for review)

- **What the sample drew.** The rule "every 10th φ\* = 5 history in (M, β) order" draws
  β = 0.95, M = 10⁻³. That is the store's one output-check row: stored `failure = 1`, reason
  `PRyMordial output: Yp_BBN=0.5120499968 is outside (0, 0.5)`. README §0.5 says the campaign
  "does not touch … the 1 output-check row".
- **What the small network gave.** At `rtol` 1e-6 the tool returns FAILURE with
  `PRyMordial output: Yp_BBN=0.5120491178 is outside (0, 0.5)`. **Every `solve_ivp` call
  succeeded.** `failure_stage` and `t_reached` are empty, because the tool records them only for
  a call with `success = False`. The output check, `_check_abundances`, fails the result: Yp is
  above 0.5 for this β < 1 history. The full network gave the same Yp in production: the
  networks differ by 1.7×10⁻⁶ relative.
- **How I counted it.** P11 (1) asks for "no failed solve", and the prompt's §6 stops on "any
  small-network solve fails". Both read most naturally as the solver failing, which is what U3
  is about: "computing Yp and D/H reliably". This row is a physical result that ChamPBH's own
  check classifies, on a row the campaign excludes. So it is reported in full, but **not**
  counted against criterion 1 or as the §6 stop.
- **The other reading.** If the orchestrator or the user reads §6 literally, this is a stop. The
  grid is finished, as §6 asks, so nothing is missing for that ruling.
- **Rejected alternative.** Dropping the row from the sample would have changed the stated rule
  after the draw.

### 3. How the breadth sample is ordered and indexed — IMPLEMENTATION CHOICE

- **The φ\* = 5 part.** The 684 histories are sorted by (M / M_P, β), and indices 0, 10, …, 680
  are taken. That gives 69, the "68 or 69" of README §2 (c′).
- **The φ\* ≠ 5 part.** All 26 histories are taken, sorted by (φ\*, M, β): 13 at φ\* = 1 and 13
  at φ\* = 2.
- **Uniqueness.** `breadth.py` checks that `find_model`'s matching rule singles out each of the
  710 histories: β and φ\* to 1e-9, M to 1e-3.
- **Overlap with the roster.** By coincidence the sample holds four roster failures (β = 1.6 and
  2.4 at M = 10⁻⁵; β = 1.345 and 2.89 at M = 10⁻³) and one control (β = 2, M = 10⁻⁵). Their T2
  rows are identical in every field to T1's `prod` rows at 1e-6. This is a free check that the
  sample's runs are the same solves as the scan's.

### 4. T2's setting chosen from criteria 1–3 alone — IMPLEMENTATION CHOICE

The prompt allows a provisional timing "if one is needed to separate two candidates". After T1,
only 1e-6 and 1e-8 met criteria 1–3, and P11 takes the larger `rtol`. So T2 ran at **1e-6** with
no provisional timing. The serial cost measured afterwards confirms that 1e-6 meets criterion 4.

### 5. The P7 runs shared the machine with T2, and log 01's P7 scripts were run unmodified — IMPLEMENTATION CHOICE

- **Shared machine.** Five P7 solves (passenger (c), and network (b) twice) and the two
  `7b518c9` reference runs ran while T2 ran 8 at a time. There were two P7 processes, so at most
  10 solves at once, as U1 allows. They measure outcomes only. Every repeated outcome in this
  log is bitwise stable under load: T1 against log 01, and the cost run against T1.
- **Unmodified scripts.** `01-probes/pinned_values_now.py` and `01-probes/pinned_reference_7b518c9.py`
  were run from where they are, with `--rtol 1e-6`. The prompt asks for a copy only of a script
  that is adapted, and these were not.

### 6. `test_network_flag (b)`'s shift measured two ways — IMPLEMENTATION CHOICE

P13 has not settled whether the full network's low-T call is patched too. So the ⁷Li/H shift is
given two ways:

- with both networks at 1e-6, which is the script's override of both low-T calls;
- with the small network at 1e-6 and the full network at the default, which is production
  today.

Both pass.

### 7. The machine was not fully idle during the cost run — UNINTENDED DRIFT (environmental; kept)

- **Before the run.** During T1 and T2, other processes pushed the 1-minute load average to about
  290; the nine scan solves alone account for about 9. These were Time Machine (`backupd-helper`),
  `contactsd`, `accountsd`, Spotlight and an idle Ray cluster, none of them this prompt's. I
  waited until the 1-minute load fell below 10 before starting `cost.py`.
- **During the run.** The 1-minute load was 4.2–9.4 (11.2 at the start; the 15-minute average
  was still falling from 129). Nothing of this prompt's ran alongside it.
- **The repeats agree.** Each setting's three repeats agree to within 2.3 %, except small 1e-6
  SM, at 9.15, 8.79 and 8.73 s (4.8 %; its first repeat ran at load 8.1). The full network's
  default control was 8.86, 8.86 and 9.06 s.
- **The decision.** The median is robust to this, so the measurement was kept, as log 01 kept
  its own.

## Verification performed

**Suites.** Run from the repository root with `PYTHONPATH=. ./venv/bin/python -m unittest
discover -s <pkg>/tests -t .`.

| package | before (`893a5b1`) | after (`893a5b1` + this prompt's untracked probes) |
|---|---|---|
| CosmologyModels | 18, OK | 18, OK |
| ComputeTargets | 106, OK | 106, OK |
| Datastore | 31, OK | 31, OK |

These are unchanged, as §6.1c requires; no code changed. `black --check` is clean on every probe
script.

**The store stayed read-only.** `/usr/bin/stat -f "%m %z"` on all 17 files of
`~/ChamPBH-stores/science-2026.6.0*` was identical before the suites and the first solve and
after the cost run. No `-wal` or `-shm` file appeared.

### §6.1c row by row

**1. T1 `prod` on S3's four inputs against log 01's S3: 20 of 20 identical.**

- **How it was run.** `run_jobs.py T1-repro` was run first, 9 at a time, and then compared by
  `compare_s3.py`; the output is in `compare_s3.txt`.
- **What it showed.** Status, stage, `t reached`, `t target`, Yp, D/H, ³He/H and ⁷Li/H are
  identical **as the CSV strings were written** (`repr`, all 17 significant digits) for the SM
  baseline, β = 1.6 at M = 10⁻³, β = 1.6 at M = 10⁻⁵ and β = 2.4 at M = 10⁻⁵, at all five
  `rtol` values.
- **Examples.** The SM at the default gives Yp 0.2468818825691892 and D/H 2.4579769989623. The
  control at 1e-6 gives D/H 2.4609141011473543.
- **Conclusion.** Nothing has drifted since `ad2cafb`. The §6 stop is not met.

**2. Every cell filled.**

- **T1.** 245 solves: 5 settings × (16 histories × 3 variants + the SM baseline). The first
  block was `T1-repro`; `T1-rest` ran 65 invocations, 9 at a time, in 1047 s.
- **T2.** 95 solves at 1e-6, run as `run_jobs.py T2 --t2-rtol 1e-6`, 8 at a time, in 318 s.
- **Where the values are.** Every row records status, stage, `t reached / t target`, Yp, D/H,
  ³He/H, ⁷Li/H and the wall time, which is under load and not used for cost. Per-history values
  are in `scan_summary.txt`.

**3. Failures** (`summarize.py`):

| block, setting | solves | failed solves (solver) | other FAILURE outcomes |
|---|---|---|---|
| T1, default (no `rtol` passed) | 49 | **0** | 0 |
| T1, 1e-4 | 49 | **0** | 0 |
| T1, 1e-5 | 49 | **0** | 0 |
| T1, 1e-6 | 49 | **0** | 0 |
| T1, 1e-8 | 49 | **0** | 0 |
| T2, 1e-6 | 95 | **0** | 1: β = 0.95, M = 10⁻³, `prod`. Output check `Yp_BBN=0.5120491178 is outside (0, 0.5)`; no `solve_ivp` failed, so there is no stage or `t reached`. It is the store's output-check row (deviation 2) |

**On the 11 full-network failures, the small network completes every solve.** That is 165
solves: 5 `rtol` × 3 variants, including the default tolerance. These are the solves at which the
full network fails in `low-T nuclear network (full)`. Over the breadth sample, the small network
completes all 69 φ\* = 5 and all 26 φ\* ≠ 5 histories apart from the output-check row. The
completed Yp lie in [0.245935, 0.434493] and the D/H in [2.44718, 10.407]. No failure was
instrumented, because none occurred in the solver.

**4. P11's four criteria at every T1 setting.** The witnesses are `summarize.py` (criteria 1–3)
and `cost.py` (criterion 4). A spread is (max − min)/median over `prod`, `pert12` and `pert9`.
Criterion 2's second clause compares with the same history's spread at 1e-8. Criterion 3 compares
each input (the SM, and each history's `prod`) with the same input at 1e-8. Criterion 4 is the
ratio of serial medians against the full network at the default (row 5).

| low-T `rtol` | 1. failed solves (T1; T2) | 2. D/H spread: max (history); misses | 3. vs 1e-8: max D/H; max Yp; inputs missing | 4. cost: SM; control | P11 |
|---|---|---|---|---|---|
| default (1e-3) | 0; — | 1.91e-3 (β 1.2, M 1e-3); **16 of 16 miss** | **1.77e-3**; **3.2e-5**; **17 of 17** | 0.67; 0.71 | fails 2, 3 |
| 1e-4 | 0; — | 6.21e-4 (β 1.7, M 0.03); **9 miss** | **5.4e-4**; 3.8e-6; **15 of 17** | 0.75; 0.78 | fails 2, 3 |
| 1e-5 | 0; — | 1.14e-4 (β 2.12, M 1e-5; 1e-8: 4.27e-5); **1 miss** | **1.57e-4**; 6.0e-7; **13 of 17** | 0.94; 0.93 | fails 2, 3 |
| **1e-6** | **0; 0** | 1.51e-4 (β 2.4, M 1e-5), **1.46× its 1e-8 spread** of 1.03e-4; **0 miss** | 5.5e-5 (β 1.6, M 1e-5); 2.1e-7; **0** | **1.15; 1.13** | **meets all four** |
| 1e-8 | 0; — | 1.03e-4 (β 2.4, M 1e-5); 0 miss | 0 (the reference) | 1.56; 1.46 | meets all four |

**Criterion 2 at 1e-6.**

- Fifteen of the sixteen histories are below 1e-4, with a median of 4.4e-5.
- β = 2.4, M = 10⁻⁵ is at 1.505e-4, which is 1.459× its own 1.031e-4 at 1e-8. It passes on the
  second clause, **with a margin of 3 %** under 1.5×.
- That history is above 1e-4 even at 1e-8. Its floor does not come from the low-T stage: it does
  not fall from 1e-6 to 1e-8. Log 01 found a floor of this kind on β = 2, M = 10⁻⁵ in the full
  network and traced it to the thermodynamic stage.
- **The Yp spread does not move with the low-T `rtol` either**: max 4.3e-5 and median 1.5e-5 at
  1e-5, 1e-6 and 1e-8, all on β = 2.4, M = 10⁻⁵. This repeats log 01's Yp floor, which is set by
  the other stages (P9).

**Criterion 3 at 1e-6.**

- **D/H.** The median over the 17 inputs is 2.6e-5, and the maximum is 5.5e-5, on β = 1.6,
  M = 10⁻⁵. The SM baseline moves by 2.6e-5: D/H 2.458287893 at 1e-6 against 2.458223906 at 1e-8.
- **Yp.** At most 2.1e-7.
- **At 1e-5.** D/H is 0.9–1.6e-4 off on every input (median 1.09e-4), which misses 1e-4 on 13 of
  17. That is why 1e-5 fails.

**The default's own error on the small network.** Against 1e-8, D/H is off by up to 1.77e-3, with
a median of 7.6e-4 over the 17 inputs. The default `rtol`'s scatter, 6.5e-4 to 1.9e-3, is the same
size as the full network's in log 01.

**5. Cost** (`cost.py full:default small:default small:1e-4 small:1e-5 small:1e-6 small:1e-8`,
`cost_output.txt`, `cost.csv`).

- **How it was run.** Alone, serially, after T1 and T2 and the P7 runs had all finished. There
  was one process, with one discarded warm-up per network, then three repeats round-robin over
  the settings.
- **The load.** Deviation 7: 1-minute load 4.2–9.4.
- **Medians, in seconds:**

| setting | SM median | ratio | control (β 1.6, M 1e-3, prod) median | ratio |
|---|---|---|---|---|
| **full, default** (production today) | 7.61 | 1.00 | 8.86 | 1.00 |
| small, default | 5.09 | 0.67 | 6.30 | 0.71 |
| small, 1e-4 | 5.74 | 0.75 | 6.91 | 0.78 |
| small, 1e-5 | 7.12 | 0.94 | 8.28 | 0.93 |
| **small, 1e-6** | **8.79** | **1.15** | **10.05** | **1.13** |
| small, 1e-8 | 11.85 | 1.56 | 12.95 | 1.46 |

- **Repeats.** Every repeat gave identical abundances, and every one is identical to the T1 row
  for the same input and setting. The full network's default control reproduced the stored Yp
  0.24689483901116643 bitwise in this process.
- **Against criterion 4.** Every small-network setting is within 1.6× today's production cost;
  criterion 4 allows 3×. As the user expected, the small network at 1e-6 costs about the same as
  the full network at the default. Log 01 measured the full network at 1e-6 as 26.26 s (SM) and
  27.73 s (control), 3.7–4.1× its default, in another session. Taken across the two sessions,
  that is 2.8–3.0× this session's small network at 1e-6.

**6. The offset from the full network (P12).** The witness is `summarize.py`: (small − full)/full
per input, for the SM and each history's `prod`, with full-network values from log 01's S1 rows
(`01-probes/scan.csv`). This is a measurement, not a bound and not an issue.

| low-T `rtol` (both networks) | inputs | Yp: median; max \|.\| (input); range | D/H: median; max \|.\| (input); range |
|---|---|---|---|
| 1e-5 | 17 | −1.44e-5; 3.17e-5 (β 2.09, M 1e-5); [−3.17e-5, +2.11e-5] | −3.92e-4; 4.35e-4 (β 2.09, M 1e-5); [−4.35e-4, −3.23e-4] |
| 1e-6 | 16 (full failed β 1.1, M 0.03) | −1.40e-5; 3.21e-5 (β 2.09, M 1e-5); [−3.21e-5, +2.09e-5] | −2.85e-4; 3.58e-4 (β 1.6, M 1e-5); [−3.58e-4, −2.52e-4] |
| 1e-8 | 16 (full failed β 1.2, M 1e-3) | −1.48e-5; 3.19e-5 (β 2.09, M 1e-5); [−3.19e-5, +2.10e-5] | −2.77e-4; 3.06e-4 (β 2.09, M 1e-5); [−3.06e-4, −2.35e-4] |

- **D/H.** The small network gives D/H **2.4–3.6×10⁻⁴ lower** than the full network on every
  input at converged settings, and the shift is systematic: never positive. The SM shift at 1e-8
  is −2.82e-4.
- **Yp.** The offset is −3.2e-5 to +2.1e-5, the same size as Yp's own variant spread.
- Per-input values are in `scan_summary.txt`.

**7. The pinned small-network values (P7) at 1e-6** (`pinned_values.txt`).

- **Provenance.** The provenance of `CONST_HONLY_SMALL_*` is the "honly" constant on `7b518c9`.
  That tree was checked out with `git worktree add --detach` in the scratchpad and removed
  afterwards; `git worktree list` shows only the main tree.
- **Checked first.** With no override, it reproduced both pins to every printed digit: Yp
  0.2536690816 and D/H 2.6481673.

| value (test, bound) | pinned | the test's quantity now, at 1e-6 | against the pin | re-derived on `7b518c9` at 1e-6 | against the re-derived pin |
|---|---|---|---|---|---|
| `CONST_HONLY_SMALL_YP` (passenger (c), 1e-6) | 0.2536690816 | 0.253669508 | 1.68e-6, **would fail** | 0.253669508 | 0 to printed digits; passes |
| `CONST_HONLY_SMALL_D_OVER_H_E5` (passenger (c), 1e-6) | 2.6481673 | 2.649288446 | 4.23e-4, **would fail** | 2.649288446 | 0; passes |
| `test_network_flag (b)` ⁷Li/H shift, both networks at 1e-6 (≥ 5e-3) | — | small 5.186233632, full 5.133189648: **1.033e-2** | passes | — | — |
| the same, small at 1e-6 against full at the default (≥ 5e-3) | — | small 5.186233632, full 5.137924042: **9.40e-3** | passes | — | — |

So the small-network constants re-pin from their provenance, with the bound unchanged, and the
shift stays about 2× above its bound either way. The network (b) run at 1e-6 also shows what
happens if the full call is patched too. Its `CONST_HONLY_FULL_D_OVER_H_E5` would move 4.58e-4
against a 1e-5 bound. That is a P7 matter for the rewritten prompt 02 and is not measured further
here (§5).

### The recommendation: the small network, low-T `rtol` 1e-6, `atol` 1e-11

By P11, the largest `rtol` meeting all four criteria is **1e-6** (table in row 4). In summary:

1. **Reliability.** No solver failure in T1 (49 solves) or T2 (95 solves). On the 11 histories
   the full network fails, the small network completes at every setting, the default included.
2. **Scatter.** The D/H spread is below 1e-4 on 15 of 16 histories. The 16th, β = 2.4,
   M = 10⁻⁵, is at 1.46× its 1e-8 spread, so its floor is not the low-T stage's.
3. **Convergence.** Within 5.5e-5 in D/H and 2.1e-7 in Yp of 1e-8, on all 17 inputs.
4. **Cost.** 1.13–1.15× the full network at the default.

The next setting up, 1e-5, misses criteria 2 and 3. 1e-8 also meets all four, but P11 takes the
largest `rtol`; it costs 1.46–1.56×.

## Observations not acted on

1. **The output-check row is in the breadth sample** (deviation 2). It is β = 0.95, M = 10⁻³:
   Yp 0.51205 on both networks. README §0.5 leaves its classification to the analysis. This is
   not an issue: the check is behaving as designed.
2. **β = 2.4, M = 10⁻⁵ carries a D/H spread floor of about 1.0–1.5×10⁻⁴** on the small network at
   every `rtol` ≤ 1e-6. It is like log 01's β = 2, M = 10⁻⁵ floor on the full network, which the
   thermodynamic stage sets. On the small network β = 2, M = 10⁻⁵ is at 3.9e-5 at 1e-6. This was
   not traced to a stage here: §5 rules out further stage studies. It is a measurement (P9) for
   prompt 03, and it is why criterion 2 passes at 1e-6 by only 3 %.
3. **`test_bbn_callbacks (i)`'s `README_BASELINE`** is the full network's SM at the default. If
   P13 moves the SM baseline to the small network at 1e-6, the comparison against its 1e-4 bound
   moves as follows (from T1's SM row):

   | quantity | value | against `README_BASELINE` | at the 1e-4 bound |
   |---|---|---|---|
   | Yp | 0.2468802117 | 3.97e-5 | passes |
   | D/H | 2.458287893 | 1.63e-3 | **fails** |
   | ³He/H | 1.041932695 | 6.5e-5 | passes |
   | ⁷Li/H | 5.486373007 | 1.17e-2 | **fails** |

   This is handed on as part of P13's ruling and not opened as an issue, as log 01 did for the
   full network.
4. **The machine's background load** during the scan reached a 1-minute average of about 290,
   from Time Machine, `contactsd`, `accountsd`, Spotlight and a Ray cluster idle since the night
   before. None of these were this prompt's. Not acted on (deviation 7).

No issue is opened by this prompt.

## State handed to the next prompt

The next step is **the user's ruling** on P11's setting and on P13. Prompt 02 is rewritten after
it.

- **The recommended setting.** The small network at low-T `rtol` **1e-6**, `atol` 1e-11 (as
  now). It meets all four of P11's criteria (table above).
- **Prompt 02's "reproduce to every printed digit" target** at that setting is `01c-probes/scan.csv`
  with `lowT_rtol == "1e-06"`: 49 rows with `tag == "T1"` (the roster, three variants, and the
  SM) and 95 with `tag == "T2"` (the breadth sample, `prod`). For example:
  - SM (small): Yp 0.24688021169088586, D/H 2.4582878928660548, ³He/H 1.0419326951489363,
    ⁷Li/H 5.48637300688257.
  - Control β = 1.6, M = 10⁻³, `prod`: Yp 0.24688971029506177, D/H 2.4609141011473543.
- **The cost target.** The small network at 1e-6 has serial medians of 8.79 s (SM) and 10.05 s
  (control). The full network at the default, on the same session, has 7.61 s and 8.86 s.
- **The P7 re-pins at 1e-6.**
  - `CONST_HONLY_SMALL_YP = 0.253669508` and `CONST_HONLY_SMALL_D_OVER_H_E5 = 2.649288446`.
    These were re-derived on `7b518c9` and equal the current tree's value under the override.
  - `test_network_flag (b)`'s ⁷Li/H shift is 9.40e-3 with full at the default, and 1.033e-2 with
    both networks at 1e-6.
  - `test_bbn_from_store.LOWT_SMALL_RTOL_AS_PASSED` must become 1e-6 if the small call is
    patched (log 01, deviation 7).
- **For P13.**
  - The small network's offset from the full one is D/H −2.4 to −3.6×10⁻⁴, systematic, and Yp
    within ±3.2×10⁻⁵ (row 6).
  - `README_BASELINE` would fail at the small network's 1e-6 baseline in D/H (1.63e-3) and ⁷Li/H
    (1.17e-2) (Observations 3).
  - If the full call were also patched to 1e-6, `CONST_HONLY_FULL_D_OVER_H_E5` moves 4.58e-4 (row
    7). Log 01's provisional full-network setting was 1e-5, where it moves 4.46e-4.
- **The residual floors at 1e-6, small network (P9, for prompt 03).**
  - D/H spread: median 4.4e-5; 1.5e-4 on β = 2.4, M = 10⁻⁵, whose spread at 1e-8 is 1.03e-4.
  - Yp spread: median 1.5e-5, max 4.3e-5, unchanged by the low-T `rtol`.
  - Convergence error against 1e-8: D/H ≤ 5.5e-5, Yp ≤ 2.1e-7.

**Reproduction commands.** Run from the root; the scripts are in
`prompts/bbn-tolerance/logs/01c-probes/`.

```bash
./venv/bin/python prompts/bbn-tolerance/logs/01c-probes/breadth.py                 # breadth.csv
./venv/bin/python prompts/bbn-tolerance/logs/01c-probes/run_jobs.py T1-repro       # 9 at a time
./venv/bin/python prompts/bbn-tolerance/logs/01c-probes/compare_s3.py
./venv/bin/python prompts/bbn-tolerance/logs/01c-probes/run_jobs.py T1-rest
./venv/bin/python prompts/bbn-tolerance/logs/01c-probes/run_jobs.py T2 --t2-rtol 1e-6 --jobs 8
./venv/bin/python prompts/bbn-tolerance/logs/01c-probes/summarize.py --detail
./venv/bin/python prompts/bbn-tolerance/logs/01c-probes/cost.py full:default small:default \
    small:1e-4 small:1e-5 small:1e-6 small:1e-8                                    # alone
./venv/bin/python prompts/bbn-tolerance/logs/01-probes/pinned_values_now.py passenger-c --rtol 1e-6
./venv/bin/python prompts/bbn-tolerance/logs/01-probes/pinned_values_now.py network-b [--rtol 1e-6]
# in a 7b518c9 worktree, with PYTHONPATH=. and this repository's venv:
#   pinned_reference_7b518c9.py const-honly-small [--rtol 1e-6]
```
