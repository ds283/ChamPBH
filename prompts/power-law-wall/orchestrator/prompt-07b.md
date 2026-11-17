# Orchestrator — prompt 07b, re-measure everything that k_ex defines

**Written** 2026-11-17 with the prompt, on the user's rulings R13–R17 (README §6.12) and their words for the prompts:
"The prompts can go into `power-law-wall`." Not dispatched. P73–P90 stand unless the user rules otherwise; U11–U13 are
the user's, and none is needed before 07b. 07b is dispatched at the user's word.

Read [`../README.md`](../README.md) first: §6.12 whole, then §2.1 (the gate Remeasure), §5, §6.5 and §6.6. Then read the
prompt. This note binds 07b only, and governs where it differs from the prompt. The conventions of earlier notes bind
here as well: a tolerance is a number with its reason, timing is taken under load, and the suites run serially.

**You do not write code.** You may:
- run the suites and the scripts the log names;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`07b-remeasure-k-ex.md`](../07b-remeasure-k-ex.md) · **Gate:** "Remeasure" · **Closes:** nothing ·
**Opens:** only what the work finds · **Model:** Opus · **Network:** one fetch at dispatch, by the orchestrator: the
PDF of arXiv:1310.5149 (§1). Nothing is pushed.

**Gate**, as measured by the planner on 2026-11-17 at 10:38 local, before this note was committed:
- **The worktree.** `.claude/worktrees/power-law-wall` was on `power-law-wall` at `c798d02` (review of 07), on top of
  `bbd99f4` (07). `git status --porcelain` showed `?? venv`, plus the planning files before their commit.
- **The main checkout** is on `main` at `9df9e13`, clean.
- **The registry.** `runregistrykit list`: 78 runs, all finished; none `running` or stale.
- **Prompt files.** `source/kex/` and `.documents/power-law-wall/kex-07b/` do not exist; the verification document's
  last section is §6 (07's). Crossover's directory was last changed at `18add05`.
- **Load.** 32.1 / 37.1 / 25.1 at 10:38: another session's work. 07b's stage-2 runs must be timed with the load beside
  them (ruling 3).

## 0. What the planner found before writing 07b

### 0.1 The paper

Erickcek, Barnaby, Burrage and Huang, arXiv:1310.5149, §V B, read by the planner on 2026-11-16 from the arXiv PDF. In the
planner's words and notation:
- The field slows near its turnaround as φ̄ − φ̄_ta ≈ −½V_D′(φ̄_ta)t², with V_D the potential that dissipation adds
  (their Eq. (62)).
- They take Δt as the time over which V″ changes by its own size, |V″(φ̄) − V″(φ̄_ta)| = V″(φ̄_ta), doubled for the way
  in and the way out, which gives t = [2V″/(V_D′V‴)]^{1/2} (Eq. (63)) and k_ex = (Δt)⁻¹ = ½[V_D′V‴/(2V″)]^{1/2} at φ̄_ta,
  with V_D(φ̄_ta) = φ̇²_M/2 (Eq. (64)).
- On V = M_v⁴ exp((M_s/φ)ⁿ) they find k_ex = (n/2)(M_s/φ̄_ta)^{n+1}V_D^{1/2}/M_s × [1 + O((φ̄_ta/M_s)ⁿ)] (Eq. (66)). On
  V = M_v⁴[1 + (M_s/φ)ⁿ] they find k_ex = (n + 2)|φ̇_M|/(2√2 φ̄_ta) (Eq. (71)).
- A footnote records that their earlier work took the time for V″ to change by half its size, which gave a k_ex √2
  smaller, and that they changed to the full change because it fitted their numerical results better.

**The planner's check.** With the dressing V_D ∝ (V″)², as `one_loop_turnaround` takes it (m⁴ℓ/32π², m² = V″, ℓ held
fixed), V_D′/V_D = 2V‴/V″, so at the turnaround V_D′ = 2E V‴/V″, and Eq. (64) becomes ½E^{1/2}|V‴/V″|: the ruling's
form. On the power wall |V‴/V″| = (n + 2)/φ exactly and E^{1/2} = |φ̇_M|/√2, which is Eq. (71). On the exponential the
leading term of |V‴/V″| is n(M/φ)ⁿ/φ = −d ln V/dφ, which is Eq. (66). What the reduction leaves out is ℓ's own variation
along the approach (a relative correction of order 1/ℓ in V_D′); 07b states it (prompt §1.1).

### 0.2 The size of the change

New over old, |V‴/V″|/(−d ln V/dφ), at u = (M/φ)ⁿ, from closed forms checked by the planner in scratch:

| wall | n | u = 10 | u = 40 | u = 1000 |
|---|---|---|---|---|
| exponential | 1 | 1.3833 (Δ ln 0.324) | 1.0988 (0.094) | 1.0040 (0.004) |
| exponential | 2 | 1.0299 (0.029) | 1.0019 (0.002) | 1.0000 |
| power | 1 | 3.3000 (1.194) | 3.0750 (1.123) | 3.0030 (1.100) |
| power | 2 | 2.0200 (0.703) | 2.0013 (0.694) | 2.0000 (0.693) |

On the power wall the ratio is exactly (n + 2)/n · (1 + u)/u. At the spectrum's references (walls.py's units, the
planner's scratch evaluation through `WallN.V2`, `V3` and `PowerWall`'s closed form):

| reference | k_ex/ω_t, old | k_ex/ω_t, new |
|---|---|---|
| `WallN(90, 1)` | 0.49453535504684026 | 0.5163952511757224 |
| `WallN(40, 1)`, `WallN(134, 1)` | — | 0.5361641472211286, 0.5110706662156198 |
| `PowerWall`, n = 1 | 0.3535533905932738 | 1.0606601717798212 = 3/(2√2) |
| `PowerWall`, n = 2 | 0.408248290463863 | 0.8164965809277261 = 2/√6 |

On the power wall the new k_ex/ω_t = (n + 2)/(2√(n(n + 1))) holds at every E/V₀, since ω_t² = n(n + 1)E/φ_t² there, so
the table's error near the floor (R9) should vanish. x_* = 0.0029/0.5163952511757224 = 0.005616 (P82).

### 0.3 Crossover's stage-2 rows, re-chained

A scratch probe, 2026-11-17 at about 10:20, run from crossover's directory with `python -B` (no bytecode written there):
`/private/tmp/claude-35086/-Users-ds283-Documents-Code-ChamPBH/7e4b78a0-ac84-413d-a96c-a4e36fdf2abb/scratchpad/kex/probe.py`
and `probe2.py`, not committed. That directory may not survive the session; the numbers below are the record. It reads
`fit_law.scan_rows()` (`stage2_scan.txt` and `stage2_mscan.txt`, 107 rows; not `stage2_n2.txt`), builds `Wall(L)` per row,
and runs a copy of `born.chain` with the rate either −d ln V/dφ or |V3/V2|.
- **The old form reproduces the record.** ℓ equal to the recorded ℓ to 5×10⁻⁴ (the printed digits) at all 46 rows with
  a chain; crossover's fit reproduced to its last digit: one-loop b₀ = 0.9691, c = 0.4099 (max 0.074, rms 0.021, 27
  points); gap 0.0198, 1.5601, 1.5472, 0.841 (max 0.062, rms 0.024, 34 points).
- **ℓ_new − ℓ_old** runs from 0.035 (L = 132) through 0.24 (L = 52) and 0.82 (L = 14.7) to 1.0–2.3 at L ≲ 10.
- **20 rows change regime**, all from gap (no chain) to a chain:

  | set | L | ln R | ℓ_new | f | came back |
  |---|---|---|---|---|---|
  | 2197 | 3.62 | 13.791 | 1.364 | 0.826 | yes |
  | 2197 | 3.05 | 12.001 | 1.109 | 0.773 | yes |
  | 2197 | 2.32 | 9.362 | 0.758 | 0.522 | yes |
  | 2197 | 1.79 | 7.124 | 0.470 | 0.282 | yes |
  | grid | 1.30 | 5, 10, 15, 20, 25 | 0.143, 0.789, 1.266, 1.713, 2.150 | 0.159, 0.528, 0.786, 0.816, 0.329 | yes |
  | grid | 2.00 | 10, 15, 20, 25 | 0.819, 1.328, 1.785, 2.224 | 0.566, 0.807, 0.962, 0.608 | yes, yes, no, yes |
  | grid | 3.00 | 10, 15, 20, 25 | 0.864, 1.432, 1.918, 2.367 | 0.644, 0.839, 0.975, 0.820 | yes, yes, no, yes |
  | grid | 4.00, 6.00 | 10 | 0.907, 0.987 | 0.709, 0.780 | yes |
  | L-M8e-18 | 93.82 | 6.667 | 0.358 | 0.726 | no |

- **v1's form in the new ℓ**, refitted on crossover's on-curve rows (its own exclusion: no `curve` sets, no `grid` row
  above ln R = 15): one-loop, A = 2.69 held, b₀ = 0.3977, c = 0.507, max 0.190, rms 0.079 (25 points), the worst at the
  smallest ℓ_new (L = 8, ln R = 10, ℓ_new 1.056: f 0.809, fit 0.999) and at ℓ_new 2.2–2.7 on the laboratory curve (f
  0.81–0.92, fit 0.72–0.78). On all rows with a chain, A free: 3.167, 0.516, 0.336, max 0.378. The rows below ℓ_new = 1
  that came back have f = 0.16–0.78 (eight on-curve), not v1's plateau 0.9. The gap, on its 21 remaining on-curve rows:
  0.023, 1.797, 1.380, 1.050, max 0.047, rms 0.014.

So on the exponential the change is not a relabelling: near the floor, where the laboratory cascade ends, the regimes
move and v1's form misses. That is what 07b measures and the user rules (U13).

### 0.4 The inventory, as far as the planner took it (prompt §1.9 completes it)

From `git grep` at `c798d02` of `k_ex`, `kex`, `lambda_t`, `lambda_ta`, `_d_logV_dphi`, `ell`, the law's constants and the
tables' fields, outside the records:
- `CosmologyConcepts/Models/AbstractModel.py`: `ClassicalTurningPoint` and `OneLoopTurnaround` (:65–:90, the λ fields and
  their comments); `classical_turning_point` (:735–:812; k_ex at :795–:809, docstring :750–:752 citing eq:kex-over-wt);
  `one_loop_turnaround` (:815–:905; λ_ta at :873, k_ex at :879, docstring :821–:825); `ONE_LOOP_ELL_START` (:114); the
  hooks (:506–:512; no third derivative).
- `CosmologyConcepts/Models/ExponentialModel.py` (:123, :139) and `InversePowerModel.py` (:253, :257): the hooks; each
  model's `one_loop_drain_model`.
- `CosmologyConcepts/drain_model.py`: the drain-model names.
- `ComputeTargets/ScalarHistory.py`: `MARGIN_THRESHOLD` (:172); the law's constants (:230–:239); `drain_fraction`;
  `drain_regime` (the chain's existence and ℓ > 0); `PeakEstimate` (:4128), `_assessed_peak` and `peak_estimate`
  (:4260–:4335); the generations' `log_q` and birth margin (:4459–:4460); the records (:4628–:4642, :4852, :4966–:4995);
  comments at :633 and :834.
- `ComputeTargets/quanta_spectrum.py`: the docstring's x (:23, :63–:66), x_* (:160–:175), the tables' header
  (:210–:225) and `k_ex_over_omega_t` with the ln|β|² tuples (:700–:725), `SPECTRUM_MODEL`.
- `ObjectFactories/ScalarHistory.py`: the stored `ell`, `log_k_ex_Mp`, `log_q_Mp`, `birth_margin` (:181–:191,
  :1424–:1438).
- `tools/quanta_measures.py` (`k_ex_over_H`, the phase slip from the stored `log_k_ex_Mp`) and
  `tools/plot_quanta_history.py` (panel 6): readers of the stored value.
- The drain-model name at `main.py`, `plot_inputs.py`, the plot scripts and `config/argument_parser.py`, through each
  model's `one_loop_drain_model` since 06; `config/version.py`.
- Tests that pin λ, k_ex or ℓ: `test_drain_law`, `test_inverse_power_model`, `test_measured_drain_law`,
  `test_unresolvable_turning_points`, `test_unresolved_turnarounds`, `test_wall_model`, `test_margins`,
  `test_power_wall_law`, `test_drain_store_records`, `test_drain_store`, `test_export_04`, `tools/tests`'s
  `quanta_exports`, `test_plot_quanta_history`, `test_quanta_figures`, `test_quanta_measures`; the tables:
  `test_quanta_spectrum`; the law's name or constants: about twenty more (`git grep -l 'one-loop-measured-v1\|DRAIN_ONE_LOOP_'`).
  The goldens (`ComputeTargets/tests/data/golden_history_*.json`) are fixed-law or natural-mode histories as far as the
  planner knows; 07b says which hold k_ex columns.

**Rulings.** Each is recorded in the log, with its class.
1. **The worktree and the main checkout.** As note 01's ruling 1: the agent works only in the worktree, from its root,
   with `./venv/bin/python` and `PYTHONPATH=.`. It never stages `venv`, never reads or touches the main checkout, and
   never names `/tmp/ray-cpbh`. *STRUCTURALLY REQUIRED.*
2. **Records stay records.** Crossover's directory and `source/wall/` are read and copied, never written; scripts in them
   are run with `python -B` or from a copy, so that no `__pycache__` appears there. *STRUCTURALLY REQUIRED* (prompt §2).
3. **The machine.** Stage-2 points run at most four at once, and one at a time while the one-minute load is above 24;
   the load is recorded beside every timing. A point not done in 30 minutes is stopped and recorded with its state, not a
   stop. *IMPLEMENTATION CHOICE, ruled here.*
4. **The grid's reference** (prompt §1.4): a re-run point off its recorded fraction by more than stage 2's stated
   precision is a stop. *IMPLEMENTATION CHOICE, ruled here.*
5. **The two worktree failures** of note 01's §0.1 are expected in the base suites. Any other failure there is a stop.
   *IMPLEMENTATION CHOICE, ruled here.*
6. **A finding about production** is recorded and reported, not fixed. The agent may open a §3 issue for it, with
   `.documents/OPEN_ISSUES.md` in the same commit. *IMPLEMENTATION CHOICE, ruled here.*
7. **No option chosen.** For U13 the hand-back states each candidate's evidence and recommends none as the agent's
   ruling. *STRUCTURALLY REQUIRED* (README §6.12).
8. **No file but the prompt's §4 paths.** *STRUCTURALLY REQUIRED.*

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before committing.
  Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit", and the orchestrator substitutes it.
- **Order.**
  1. The Gate, and the five suites on the base: serially, ComputeTargets in its two halves, the `test_[g-z]` half and
     `tools/tests` in the background, waiting for each notification.
  2. The copies and the chain (§1.2); the definition and the paper (§1.1).
  3. The re-chaining (§1.3), the power wall's law (§1.6), the tables and x_* (§1.7): all cheap.
  4. The grid's reference (§1.4, ruling 4), then the new stage-2 points, in the background, waited for without polling.
  5. The fits (§1.5), the paths (§1.8), the inventory (§1.9), the documents, discovery counts, and the commit.
- **Scratch.** Probes and suite outputs go in the agent's own subdirectory of the session scratchpad. Nothing goes in
  `/tmp`.
- **No Ray.** Nothing starts a cluster, and no `ray` command is run. No store is written, and nothing is registered.
- **Stop conditions are real:**
  - a test failing on the base other than the two known;
  - the old-form chain not reproducing a recorded ℓ to its printed digits, or the fitting code not reproducing
    crossover's constants;
  - ruling 4;
  - a production change becoming necessary.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

1. **The Gate above**, on the commit that adds this note, or a later one.
2. **`runregistrykit list`** and the load average. A cluster or lock of another session is left alone; 07b needs
   neither.
3. **The paper.** Fetch the PDF of arXiv:1310.5149 into a dispatch directory of the session scratchpad and extract its
   text there (`pdftotext -layout`), as data for the agent. Check that §V B and Eqs. (62)–(72) are in it. It is not
   committed.

## 2. Dispatch

Start one fresh-context subagent, on **Opus**, in the worktree, in the background. Give it:
- the prompt, the README and the board;
- `HEAD`, and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad;
- the paper's text from §1 item 3, as data.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the files of ruling 8. Its log is `logs/07b-remeasure-k-ex.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **Stop and ask** on any of the conventions' stop conditions.
- **At the end it replies with:**
  - the log's path and the message as committed; `git show --stat` of its commit;
  - the suites at the base, with counts, times and `uptime`, and the discovery counts after;
  - the definition's check against Eqs. (64), (66) and (71), and against the production hooks;
  - the re-chaining: the reproduction of the record, the rows that move per scan, ℓ_new − ℓ_old against L and x;
  - the grid's reference (three points), and the new points with their validation;
  - each candidate law at n = 1 and 2, with its parameters and residuals beside v1's;
  - the power wall's law, unchanged or not;
  - the tables: k_ex/ω_t, X_LO, X_HI, x_p, κ_hi, the cross-check in k/ω_t, R9's error on the power wall, and x_*;
  - the paths: per history, the regime moves and the drained-share change;
  - the inventory's length, and anything in it the planner's list (§0.4) missed;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — six checks

1. **Scope.**
   - `git show --stat <agent>` lists only ruling 8's paths, and `venv` is not in it.
   - `git diff <base> <agent>` outside `prompts/power-law-wall/`, `.documents/power-law-wall/`,
     `.documents/power-law-wall-verification.md` and `.documents/OPEN_ISSUES.md` is empty.
   - Crossover's directory and `source/wall/` are unchanged and hold no `__pycache__`.
   - The commit's parent is this note's commit, or a later one that is not the agent's.
   - The main checkout is untouched. Nothing is pushed.
2. **The re-chaining.** Re-run the agent's chain on crossover's two scans myself and compare its row table, field by
   field; the 20 moves of §0.3 must be among its moves. Re-run its old-form fit and match crossover's constants.
3. **One new stage-2 point and one grid-reference point**, re-run myself, serially, with the load, against the committed
   rows at their printed digits.
4. **The fits.** Re-run the fitting script and `cmp` its output with the committed file. For one candidate, recompute the
   largest residual from the rows by a scratch script.
5. **The tables and x_*.** Re-run one table (the exponential's) and `cmp` it with the committed literals; check one power
   table's k_ex/ω_t against the closed form, and x_*'s arithmetic.
6. **The inventory and the records.**
   - A fresh `git grep` of `k_ex\|kex\|lambda_t\|lambda_ta\|_d_logV_dphi\|\bell\b\|DRAIN_ONE_LOOP_\|one-loop-measured`
     outside the records: every production and test hit is in the inventory.
   - The log is in §5.1's form, every deviation classed; verification §7 exists; `kex-07b/` and `source/kex/` exist,
     with a README.
   - The board has row 07b, its items and the gate Remeasure. Any issue opened is on §3 and in
     `.documents/OPEN_ISSUES.md`.
   - `CLAUDE.md` is unchanged, and discovery counts match the base's.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 07b landed, the gate Remeasure held, and this note marked "used for 07b";
- an *Orchestrator review of prompt 07b* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

**Report to the user:**
- the agent's commit and the review commit;
- the gate Remeasure;
- the candidate laws for `one-loop-exponential-v2` at n = 1 and 2 against v1, and where the laboratory cascade's drain
  moves (U13 is the user's);
- the tables and x_* in the new x, and R9 on the power wall;
- anything in the inventory that bears on U11 or U12;
- that 07c is written only when the user has ruled U11–U13 and asks for it.
