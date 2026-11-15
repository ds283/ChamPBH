# Orchestrator — prompt 03, the law on this wall

**Not yet used.** Written 2026-11-15 with the prompt. U8 and U9 were ruled by the user the same day, both as proposed
(README §6.6, R9 and R10), so it may be dispatched at the user's word.

Read [`../README.md`](../README.md) first: §2, §4, §5, and §6 as ruled (R1–R8, U1–U7) and as proposed (§6.6). For this
prompt the user's words are: "Write prompt 03 and its orchestrator note" (2026-11-15, after R6–R8). Then read the prompt.
This note binds 03 only. The conventions of earlier notes bind here as well: a tolerance is a number with its reason,
timing is taken under load, and the suites run serially.

**You do not write code.** You may:
- run the suites and the scripts the log names;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`03-the-law-on-this-wall.md`](../03-the-law-on-this-wall.md) · **Gate:** "Law" · **Closes:** nothing ·
**Opens:** only what the work finds · **Model:** Opus · **Network:** none. Nothing is pushed.

**When:** written 2026-11-15 at the user's request. Whether it is dispatched from this context or a fresh one is the
user's to say. Either way, the dispatching context re-checks the Gate (§1), and confirms that U8 and U9 are ruled in the
user's own words in README §6.6; a proposal left standing is not a ruling.

**Gate**, as measured by the planner on 2026-11-15 at 09:27 local, before this note and the prompt were committed:
- **The worktree.** `.claude/worktrees/power-law-wall` is on `power-law-wall` at `434c747` (R8 recorded), on top of
  `66aeb62` (R7), `9613c92` (R6), `ef063d3` (review of 02) and `693784b` (02). `git status --porcelain` shows `?? venv`,
  plus this note and the prompt before their commit.
- **The main checkout** is on `main` at `9df9e13`. Nothing of this campaign has touched it.
- **The registry.** `runregistrykit list`: 62 runs, all finished, none `running` or stale, none of this campaign.
- **The code.** `InversePowerModel` has no `_drain_L`; `DRAIN_MODELS` is (`fixed`, `one-loop-measured-v1`);
  `SPECTRUM_MODEL` is `U4-U17-v1`; the verification document has §1 and §2.
- **Load and disk.** Load averages 2.44 / 4.52 / 5.57; 125 GB free.
- **The suites.** Not re-run by the planner. At `693784b` 02's agent measured 26, 705 (219 + 486), 163, 302, 76, with only
  the two worktree failures of note 01's ruling 7. Only text has changed since.

## 0. What the orchestrator found after 02

Scratch scripts of 2026-11-15, in this session's scratchpad, not committed:
`/private/tmp/claude-35086/-Users-ds283--claude-spares-spare-B0YsIU/c84ffa39-1dde-424f-9107-8e7c7c1c1751/scratchpad/review02/`
(`laws_plot.py` and its output `laws_fit.txt`, `laws_n1.png`, `laws_n2.png`; `kappa_probe.py`, `energy_split.py`,
`lowk_probe.py`, `lowk_probe2.py`). Each was run from `prompts/power-law-wall/source/wall/`, importing 02's modules
unchanged. That directory belongs to this session and may not survive it. The numbers below are the record.

- **The f_n(ln R) fit the user was shown before ruling R7.** From 02's `stage2_scan.txt`, the `curve-*` rows with the
  field back out and regime not classical; unweighted least squares in f (`scipy.optimize.curve_fit`):
  - rising branch y = G (ln R − R₀)^P on 0.3 < ln R < 20; falling branch y = A/(ln R − c) on ln R > 28; switched where
    the two y cross; f = 1 − e^{−y}.

  | n | G | R₀ | P | A | c | switch ln R_× | worst, all (points) | rms | worst, mapped | peak f |
  |---|---|---|---|---|---|---|---|---|---|---|
  | 1 | 0.002868 | −1.2741 | 2.2213 | 27.9671 | 14.9835 | 23.089 | 0.054 (62) | 0.019 | 0.004 (12) | 0.968 |
  | 2 | 0.002859 | −1.1090 | 2.4561 | 19.6100 | 10.9039 | 16.710 | 0.056 (60) | 0.022 | 0.003 (15) | 0.966 |

  - Worst by curve and production regime, n = 1: laboratory M one-loop 0.009, gap 0.054; 10⁻³³ M_P one-loop 0.004,
    plateau 0.004, gap 0.043; 10⁻¹⁵ M_P one-loop 0.005, plateau 0.027, gap 0.051. n = 2: 0.031, 0.034 (plateau), 0.048;
    0.002, 0.002, 0.038; 0.022, 0.032, 0.056.
  - Against the not-back-out points (law − measured): n = 1 from −0.018 to +0.011; n = 2 from −0.049 to +0.026.
  - Beside it, the candidate laws of 02 at the same points (worst): (i) L_E up to 0.81 | 0.90, (ii) L = n 0.45 | 0.57,
    (iv) and (v) 0.45 | 0.57, all on the 10⁻³³ M_P curve, where the floor lowers production's ℓ and labels bounces at
    ln R ≈ 25–50 "gap".
  - **The step at ln R = 0** between the branches: f_n(0⁺) ≈ 0.005 (n = 1) against R·S₁ = 0.0103. The classical branch
    itself is 2.1 times the measurement at ln R = −0.5 (02's `fit_law.txt`, f/(R S_n) = 0.473).
- **Where the spectrum's energy sits** (`kappa_probe.py`, `energy_split.py`; |β|² on 500 points of x ∈ [3×10⁻³, 14]):

  | wall | x_p | energy x³\|β\|² above x_p | energy quartiles in x | share above x = 3 | local −d ln\|β\|²/dx at x = 1, 2, 8 |
  |---|---|---|---|---|---|
  | power, n = 1 | 0.466 | 99.5% | 1.07, 1.55, 2.14 | 6.8% | 1.37, 1.88, 2.17 |
  | power, n = 2 | 0.507 | 98.9% | 0.95, 1.33, 1.84 | 2.5% | 1.63, 2.36, 2.82 |
  | exponential, L = 90 | 0.646 | 95.6% | 0.86, 1.14, 1.47 | 0.2% | 1.92, 3.66, 4.30 |

  Under `U4-U17-v1` on the exponential, J = M₂/M₃ is 1.04 against 0.84 measured, M₁/M₃ 1.26 against 0.83, and M₄/M₃
  1.13 against 1.38: the single exponential above x_p falls too fast between x_p and x ≈ 3, where the energy is.
- **The power at low k** (`lowk_probe.py`, `lowk_probe2.py`; d ln|β|²/d ln k against k/ω_t; EPS 10⁻⁵ and 10⁻⁶ agree):

  | k/ω_t | 10⁻² | 10⁻³ | 3×10⁻⁴ | 10⁻⁴ |
  |---|---|---|---|---|
  | exponential, L = 90 | 1.998 | 2.000 | 2.006 | 2.058 |
  | power, n = 2 | 1.88 | 1.989 | 2.002 | 2.048 |
  | power, n = 1 | 1.45 | 1.66 | 1.72 | 1.75 |

  - Below about 10⁻⁴ ω_t (10⁻³ on the exponential) the local power wanders (−3 to +2.9): the solver's precision floor.
    So R8 puts the table's lower edge at 10⁻³.
  - n = 2's shortfall 2 − p is linear in k (about 11.5 k); n = 1's falls as 1/ln(1/k): (2 − p) ln(1/k) = 2.27–2.53 over
    10⁻⁴–10⁻², as k² ln²(1/k) gives. The user ruled the logarithm neglected (R8).
  - 02's slopes "below the peak" (1.34, 1.76) were fitted on k/k_ex ∈ [0.02, 0.1], short of the asymptotic region.
- **The power wall's shape against E/V₀** (02's `spectrum.txt`, not re-run): in k/ω_t the bounce is the same at every
  E/V₀ (n = 1: |β|²_max 7.6799, 7.6796 and 7.6797 ×10⁻², and k_peak/ω_t 0.1646, at x = 10⁴⁰, 6.87 and 0.93), because V₀
  is a constant and enters no derivative of V. In x = k/k_ex it moves, since k_ex/ω_t = (s_t/2)√(n/(n + 1)) with
  s_t = x/(1 + x): x_p is 0.466, 0.533 and 0.966 at those three points (n = 1), and 0.507, 0.549 and 1.695 at x = 10⁴⁰,
  11.9 and 0.43 (n = 2; k_peak/ω_t 0.2068 at all three). That is U8's question. The
  first, energetic bounces sit at s_t = 1 (x ≈ 10⁴⁶ at laboratory M); the floor matters only in the late cascade.

**Rulings.** Each is recorded in the log, with its class.
1. **The worktree and the main checkout.** As note 01's ruling 1: the agent works only in the worktree, from its root,
   with `./venv/bin/python` and `PYTHONPATH=.`. It never stages `venv`, never touches the main checkout, and never names
   `/tmp/ray-cpbh`. *STRUCTURALLY REQUIRED.*
2. **The fit.** The agent reproduces §0's fit, or explains where its own windows or weights differ and why; its
   residuals are reported beside §0's. A fit whose worst residual over all points exceeds §0's by more than 0.01 is
   reported to the orchestrator before the constants go into production, since R7 was ruled on §0's accuracy.
   *IMPLEMENTATION CHOICE, ruled here.*
3. **One set of literals.** Production's constants and `fit_fn.fraction_power`'s are the same decimal literals, so that
   the gate's comparison is bitwise, as crossover's reference and `drain_fraction` are. *IMPLEMENTATION CHOICE, ruled
   here.*
4. **The hook's shape** (name, signature, where the model's one-loop drain model is declared) is the agent's choice,
   within P15–P16: no generic default, loud refusal, nothing wall-specific in `ScalarHistory.py`. It is stated in the log
   with the reason. *IMPLEMENTATION CHOICE, ruled here.*
5. **The spectrum's tolerances.** Interpolation within 10⁻⁴ relative at the midpoints; every kernel within 10⁻⁷ relative of
   its quadrature reference; the tail's energy share at most 10⁻³. A tolerance that cannot be met is reported with what
   was met, and is a stop if it is the kernels' (the post-processing's accuracy was a ruled requirement of one-loop-drain
   05). *IMPLEMENTATION CHOICE, ruled here.*
6. **The tests that move** (P22). An assertion in the three quanta test modules may be re-pinned only because the
   spectrum moved, with its old and new value in the log. Any edit to a test that pins an exponential history, a golden or
   a fingerprint is a stop. *STRUCTURALLY REQUIRED* (README §5 as amended by R8).
7. **The two worktree failures** of note 01's §0.1 are expected on the base and after. Any other failure is a stop.
   *IMPLEMENTATION CHOICE, ruled here.*
8. **A finding about production** outside this prompt's scope is recorded and reported, not fixed. The agent may open a
   §3 issue for it, with `.documents/OPEN_ISSUES.md` in the same commit. *IMPLEMENTATION CHOICE, ruled here.*
9. **The machine.** At most six workers for the spectrum tables, and the three tool-route histories one at a time. The
   load is recorded beside every timing. *IMPLEMENTATION CHOICE, ruled here.*
10. **No file but the prompt's §4 list.** *STRUCTURALLY REQUIRED.*

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before committing.
  Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit", and the orchestrator substitutes it.
- **Order.**
  1. The Gate, and the five suites on the base: serially, ComputeTargets in its two halves, the `test_[g-z]` half and
     `tools/tests` in the background, waiting for each notification.
  2. The fit and its reference (prompt §1.1).
  3. The drain models, the hooks, the check and the loop (§1.2), with their tests; the exponential's goldens and tool
     tests at once, before anything else is built on the change.
  4. The spectrum tables and their measurements (§1.3), then the module, then the readers, then the tests.
  5. The gate Law (§1.4), the suites again, the documents, and the commit.
- **Scratch.** Probes and suite outputs go in the agent's own subdirectory of the session scratchpad. Nothing goes in
  `/tmp`.
- **No Ray.** Nothing starts a cluster, and no `ray` command is run. No store is written.
- **Stop conditions are real:**
  - a suite count that falls, or a test that passed on the base and fails after;
  - an exponential history, golden or fingerprint that moves (ruling 6);
  - a production change outside the prompt's §4 list becoming necessary;
  - the kernels' 10⁻⁷ not reachable (ruling 5);
  - the fit worse than §0's by more than 0.01 (ruling 2), which goes to the user before the constants are written.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

1. **U8 and U9 ruled** in README §6.6, in the user's words. *(Done 2026-11-15: R9 and R10, both as proposed; the prompt
   needs no amendment.)*
2. **The Gate above**, on the commit that adds this note, or a later one.
3. **`runregistrykit list`**, and the load average.

## 2. Dispatch

Start one fresh-context subagent, on **Opus**, in the worktree, in the background. Give it:
- the prompt, the README and the board;
- `HEAD`, and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad;
- the scratch scripts of §0 and `laws_fit.txt`, copied beside it, as data (they are not to be committed).

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the files of ruling 10. Its log is `logs/03-the-law-on-this-wall.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **Stop and ask** on any of the conventions' stop conditions.
- **At the end it replies with:**
  - the log's path and the message as committed; `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the fit: its constants and errors, its switch, and its residuals beside §0's;
  - the hooks as built, and the check's messages;
  - the spectrum: each wall's table (edges, size, x_p, κ_hi, the tail's share), the low-k power and precision floor, the
    exponential's shape over L, the power wall's over E/V₀, and old against new on the exponential;
  - the gate Law item by item; the three tool-route histories;
  - the tests that moved, with old and new values; the breakage record;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — six checks

1. **Scope.**
   - `git show --stat <agent>` lists only ruling 10's files, and `venv` is not in it.
   - `git diff <base> <agent>` touches no driver, plot script, `config/`, `ObjectFactories/`, `extract_common.py`,
     `plot_inputs.py` or store tool, and nothing under `science-runs/` or crossover.
   - The commit's parent is this note's commit, or a later one that is not the agent's.
   - The main checkout is untouched. Nothing is pushed.
2. **The exponential does not move.**
   - `git diff <base> <agent> -- ComputeTargets/ScalarHistory.py` changes no constant, no S-table literal and nothing of
     `drain_fraction`'s body.
   - Re-run myself, serially: `test_golden_history`, `test_golden_reflecting_history`, `test_tuning`,
     `test_store_fingerprint_golden`, `test_fingerprint_format_guard`, `test_history_and_bbn`,
     `test_history_and_bbn_drain_law`. None is edited in the commit.
   - The exponential's tool-route history under the one-loop law against the base's, timings masked.
3. **The law.**
   - With a scratch script that does not import `fit_fn.py`: evaluate the committed constants' f_n(ln R) and R·S_n at
     every candidate of three of 02's map files, and compare with production's α_eff at A = 1 through
     `InversePowerModel`.
   - Recompute the fit's worst residual and rms per n from `stage2_scan.txt` with the committed constants, and set them
     beside §0's.
   - Check the refusal at n = 3, and that the fixed law runs at n = 3.
4. **The spectrum.**
   - Re-run `spectrum_table.py` at one wall and match three table entries.
   - Independently, with scipy's `quad` on the committed shape: I, J and G(1) at c_a = 1 for one power-law wall, to 10⁻⁷.
   - Check the matching at both edges and the associated piece's coefficient against U9 as ruled.
   - Check the readers pass the exponential's wall and name 06.
5. **The tool-route histories.** Re-run the n = 2 one, and match its printed output to the log's, timings masked.
6. **The records.**
   - The log is in §5.1's form, with every deviation classed and every moved test listed.
   - The verification document has §3; `law-03/` exists; `source/wall/README.md` is appended.
   - The board has row 03, its items and the gate Law.
   - `CLAUDE.md`'s counts match the suites' after.
   - Any issue opened is on §3 and in `.documents/OPEN_ISSUES.md`.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 03 landed, the gate Law held, and this note marked "used for 03";
- an *Orchestrator review of prompt 03* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

**Report to the user:**
- the agent's commit and the review commit;
- the gate Law;
- the fit's constants and residuals against §0's (the accuracy R7 was ruled on);
- the spectrum: each wall's table, the exponential's old against new numbers (what the relativistic-quanta analysis
  will move by), and the power wall's error in x over E/V₀ (U8's measurement);
- what 04 inherits (the law and its drain model), and what 05 and 06 must wire (the drain model's selection by model;
  the spectrum's wall from the export's model column);
- that 04 is written only when the user asks for it.
