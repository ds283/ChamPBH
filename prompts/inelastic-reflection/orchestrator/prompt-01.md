# Orchestrator — prompt 01, the dark-radiation component

Read [`../README.md`](../README.md) first: §0.2, §0.4, §1, §2.2 (01's charter), §2.3 (the gate
"DR"), §4, §5 and §6.2 P1, P2, P11 and P12. Then the prompt and the board. This is the campaign's
first note; it binds 01 only.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`01-dark-radiation-component.md`](../01-dark-radiation-component.md)
· **Gate:** "DR" · **Closes:** nothing · **Opens:** only what the work finds · **Model:** Opus, as
the prompt recommends · **Network:** none. Nothing is pushed.

**Gate**, checked at `c887f4e` on 2026-10-28 at 08:42 GMT, before this note was committed:
- `inelastic-reflection` checked out in the main checkout, at `c887f4e` (prompt 01 written), after
  the prompt's own gate `327f78b`. `git status --porcelain --untracked-files=all` empty;
  `git worktree list` shows the main checkout alone. `HEAD` is the commit that adds this note,
  which touches only `orchestrator/`.
- `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing. No Ray is started by this prompt.
- `./venv/bin/python -m runregistrykit list`: 11 runs, all finished (`done` or `killed`), none
  `running` or stale.
- The suites were not re-run for this note. The agent measures the baselines (prompt §2.1) against
  README §0.4: 26 / 269 (77 + 192) / 103 / 115 / 64.
- The load averages were 21.6 / 33.7 / 22.3. Other workloads share the machine.

## 0. What makes this prompt unusual

**It changes the expressions every history is integrated with, under a bitwise regression, and
nothing that reaches the store.** The orchestrator read at `c887f4e` every file the prompt names.
Its line numbers are right for this tree. What it found, where the prompt is silent, loose or
wrong:

- **The initial ρ_χ and the sampling axis.** `truncate_to_history` keeps a point of the axis up to
  10⁻¹⁰ beyond `final_N`, so a sample can be taken at `N_forward` in [−10⁻¹⁰, 0)
  (`ComputeTargets/ScalarHistory.py:1118-1125`, `:1296-1298`). With the harness's ρ_χ held as an
  injection at N_start = 0 under the rule N_j ≤ N, that sample would see K = 0 while every other
  sees ρ_χ. Ruling 1.
- **The V = 0 branch needs ln(ρ_R/V) ≥ 746, not > 745.** `exp(-745.0)` and `exp(-745.13)` are
  5×10⁻³²⁴, a positive subnormal, so `data.V_over_3H2Mp2 > 0.0` holds and `HubblePolicy` takes the
  log branch. `exp(-745.14)` is 0.0. Measured in the venv at `c887f4e`. Ruling 2.
- **The ledger must be in the policy from the start.** 02 adds to the ledger while Radau runs, so
  the `ODEPolicy` that integrates must hold the ledger object even when it is empty. `integrate_history`
  already uses one `policy` for the step loop, the guards, the sampling loop and `fixed_T_values`
  (`:1217`, `:1300`, `:1350`). Ruling 3.
- **The payload has no dense output.** `integrate_history` returns samples, not the
  `IntegrationResult`, so §1.5 item 4's "on the dense output" cannot be done through it.
  `test_fixed_T_values.py:124` and `test_kinematic_cap_loop.py:113` build an `ODEPolicy` and drive
  `integrate_scalar_history` directly. Ruling 4.
- **Ḣ/H² is not in `ODEPolicyData`.** Only `friction_term` = −π(Ḣ/H² + 3) is, and it is zero at
  π = 0. Ruling 4.
- **The scripts under `.documents/` do not run today.** `.documents/integrator-audit-2026-09-30/harness.py:60`
  and `.documents/peakewsurf-field-equations/scripts/pipe_runs.py:146` call `ODEPolicy` with four
  positional arguments (potential, coupling), the signature before `jordan-normalization` 04. The
  constructor takes three. So the prompt's "keep working with no change" is moot. Ruling 5.
- **The mocks of `integrate_history`.** `test_tuning.py:395` patches `integrate_history` to return
  `{"sample": []}` and patches `_tuning_state` with it, so `_tuning_state` never reads a payload
  without the new key. `test_tuning.py:456` asserts it is not called. `tools/tests/test_history_and_bbn.py:161`
  wraps the tool's own call and reads positional arguments. Nothing on the way to the store
  iterates over the payload's keys: `ScalarHistory.store` (`:2311-2385`), `build_extra_data`
  (`:1884`) and `golden_history.record_of` read named keys, and `test_tuned_history_round_trip.py`
  reads `TUNING_OUTPUT_KEYS` only. **Read, not tested.** The agent confirms it by reading (prompt
  §1.4).
- **`AdiabaticHistory`** calls `Hdot_over_H2_plus_3` (`ComputeTargets/AdiabaticHistory.py:219`,
  `:236`) and `test_adiabatic_mass.py:344` calls it. Both use positional arguments up to `fm` and
  get `r_chi = 0.0` by default. `AdiabaticHistory`'s own E (`:199`) is 05's.
- **What the tuned test with an initial ρ_χ needs. These are the orchestrator's estimates, read
  and not measured.**
  - ρ_χ a⁴ is constant and ρ_R a⁴ ∝ g_ρ g_s^{−4/3}, so r_χ(T_CMB)/r₀ ≈ 106.75^{−1/3}/(3.36 · 3.91^{−4/3}) ≈ 0.39
    from T* = 2×10⁴ GeV. The photons are heated; the quanta are not.
  - Leaving ρ_χ out of V_target misplaces H_J²/H₀² by r_χ(today) Ω_R, with Ω_R ≈ 9×10⁻⁵. So
    |H_J/H₀ − 1| ≈ 4.5×10⁻⁵ r_χ(today), against the tolerance 10⁻⁵.
  - So r₀ of order 1 is needed for the breakage to fail the tuning. The agent measures; this only
    tells it where to look.
  - The tuned golden point is β = 3, M = 0.01 M_P, five integrations, about 25 s unloaded
    (`golden_tuned_history.py`). A run to the 30-integration cap takes six times as long.
- **The load.** The machine is at a load of 20–35. `ComputeTargets/tests` takes about 320 s in
  one run unloaded, and either half can run past the Bash cap of 600 s under this load. Timings
  taken now compare only with timings taken beside them.

**Rulings.** Each binds the agent and is recorded in the log with its class.

1. **The initial ρ_χ is a constant K₀, present at every N.** It is the history's initial
   condition, not an injection. `DarkRadiationLedger(K_initial: float = 0.0)`, and
   K(N) = K₀ + Σ_{N_j ≤ N} ΔK_j, for N below N_start as well. `add(N, delta_K)` is as the prompt
   says, ordered against the injections only. The ledger exposes K₀ and the injections read-only,
   so that 02 can check Σ ΔK_j against its drains. `integrate_history` builds it with
   K₀ = r₀ ρ_R,E,init e^{4 N_start}. "Empty" means K₀ = 0 and no injections. The prompt's tests of
   §1.1 apply to the injections. Add one test: K₀ alone is the same at N_start − 10⁻⁹ as at N_start.
   *IMPLEMENTATION CHOICE*, ruled here.
2. **The V = 0 test state has ln(ρ_R/V) ≥ 746**, and the test asserts
   `data.V_over_3H2Mp2 == 0.0` at it, so that the branch is shown to be taken. *STRUCTURALLY
   REQUIRED*.
3. **`integrate_history` always builds a ledger and always passes it to the one `ODEPolicy`**,
   empty when r₀ = 0.0. The payload of every successful integration carries `"dark_radiation"`.
   A failure payload carries only what `_failure_payload` gives, as today. `_tuning_state` reads it
   as `data["dark_radiation"]`. `ODEPolicy` takes the old path when its ledger is `None` or empty
   at N. "Empty at N" is a test the ledger answers without an exp or a log. No second
   `ODEPolicy` is built in `integrate_history`. *STRUCTURALLY REQUIRED* (02).
4. **§1.5 item 4 splits.**
   - The constraint check reads `integrate_history`'s samples and its payload's ledger. A sample
     carries φ, ln ρ_R,E, ln f_m and H_E, and V comes from the realized model.
   - The H check on the dense output may drive `integrate_scalar_history` directly, as
     `test_fixed_T_values.py` does. Its ledger is built as `integrate_history` builds it, and the
     test says so.
   - Ḣ/H² comes from `policy.V_policy.Hdot_over_H2_plus_3(..., r_chi=data.rho_chi_over_rho_rad)`
     at the interpolated state.

   *IMPLEMENTATION CHOICE*.
5. **The `.documents/` scripts are not touched** (`CLAUDE.md` invariant 6). The log records, under
   "Observations not acted on", that they already fail to construct `ODEPolicy`. No issue is
   opened: they are dated records.
6. **`ODEPolicyData`'s new field is appended last.** The one construction (`:347`) uses keywords.
   *IMPLEMENTATION CHOICE*.
7. **`tune_history` passes `rho_chi_ratio_init` to `integrate_history` as a keyword.**
   `compute_scalar_history` is unchanged, byte for byte. An existing test that fails because of the
   keyword is a stop, not a reason to edit it.
8. **"To rounding" is a number.** Each such assertion states its tolerance as a relative bound and
   says why that bound is rounding for the expression: how many operations, and which
   cancellation. No assertion uses `assertAlmostEqual`'s default places.
9. **The form of E** is the agent's choice (prompt §1.2). The log says how each candidate behaves
   when G − V/3H²M_P² is a cancelled difference and when r_χ ≫ 1 + f_m. Note that
   (G − U) − Y subtracts two terms whose ratio tends to 1 as r_χ grows.
10. **Timing under load.** Prompt §2.4's cost at K = 0 is measured as the golden tests' wall time
    at `HEAD` and with the change, back to back, three times each. The log reports the minima
    and the load. A difference inside the spread is "nothing measurable"; say so with the numbers.
11. **Suites.** Run `CLAUDE.md`'s commands serially, with ComputeTargets in its two halves. Send
    output to a file in the agent's own scratch directory, grep the verdict from it, and record
    `uptime` beside each. A run that may pass 600 s goes in the background
    (`run_in_background`), and the agent waits for its notification without polling. A failure is
    re-run alone before it is called real.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status`
  before committing. Count, then paste. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The five suites, and the two golden tests alone with their times, before any edit.
  2. The ledger and its tests.
  3. The expressions and the policies, the golden tests after each file.
  4. The harness, the payload and `_tuning_state`.
  5. The tests of §1.5, then the breakages, then the suites and `black --check`.
  6. The records.
- **Breakage diffs** are checked with `git apply --check` and `-R --check`, and replayed in `bash`,
  not zsh. None is committed.
- **Scratch.** The agent uses its own subdirectory of the session scratchpad, never `/tmp`. No
  scratch file goes under the repository. Any worktree is removed afterwards (`git worktree list`
  shows only the main checkout).
- **Stores and Ray.** No store is opened. No Ray is started. Nothing is written to `var/`. No
  golden JSON is regenerated.
- **Stop conditions are real.** They are the prompt's §4, plus an existing test failing for a
  reason this note does not name. A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `c887f4e`, before this note was committed:
1. **The Gate above**, and the reading of §0.
2. **The expressions, derived by the orchestrator.** This is not given to the agent, which derives
   its own (prompt §1.2). With U = V/3H²M_P², Y = ρ_χ/3H²M_P², E = ρ_R(1 + f_m)/3H²M_P² and
   p_R = ρ_R(1 − Σ)/3, the constraint gives E + Y = G − U. Then:
   - (ρ_R(1 + f_m) + p_R)/2M_P²H² = (E/2)(4 − R);
   - π²/2M_P² = 3(1 − G);
   - (4/3)ρ_χ/2M_P²H² = 2Y.

   So Ḣ/H² + 3 = 3G − (G − U − Y)(4 − R)/2 − 2Y = G(1 + R/2) + (U/2)(4 − R) − YR/2. This is the
   planner's G·A₁ + C·A₂ − Y·R/2.
   - At R = 1, π = 0, V = 0 it gives Ḣ/H² = −(3/2)(ρ_m + (4/3)ρ_χ)/(ρ_m + ρ_χ).
   - Y = r_χ(G − U)/(1 + f_m + r_χ), and T = 1/(1 + (ρ_R/V)(1 + f_m + r_χ)).
   - V_target = A₀⁴(3M_P²H₀²G/(A₀²b′²) − ρ_R,J(1 + f_m + r_χ)), from H_J = H_E b′/A and
     ρ_R,E = A⁴ρ_R,J.

   All of these agree with prompt §1.2.
3. **The suites' baselines** are the agent's (Gate).

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout. Give it:
- the prompt, the README and the board;
- `HEAD` and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the prompt's §5 files. Its log is
  `logs/01-dark-radiation-component.md`, in `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **No Ray, no store, no golden JSON written.**
- **Stop and ask** on any of the prompt's §4 conditions or §0's.
- **It replies with:**
  - the log's path and the message as committed;
  - `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the golden tests' times before and after (ruling 10);
  - its derivations, in brief, and whether each agrees with the prompt;
  - the form of E it chose, and why;
  - the new tests by name, each with what it asserts, its tolerance and its cost;
  - for item 4: where Y·R/2 peaks and how large it is;
  - for item 5: r₀, the iterations, the residuals, and how far the V_target breakage falls short;
  - each breakage, with the tests it failed and how;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — ten checks

1. **Scope.** `git show --stat <agent>` lists exactly the prompt's §5 files, and its parent is this
   note's commit. `git status --porcelain --untracked-files=all` is empty, and nothing is pushed.
   `git diff <dispatch> <agent> -- ComputeTargets/AdiabaticHistory.py ObjectFactories config main.py tools RunDrivers ComputeTargets/tests/data ComputeTargets/tests/golden_history.py ComputeTargets/tests/golden_tuned_history.py`
   is empty. `compute_scalar_history` is unchanged.
2. **The golden histories.** `test_golden_history` and the tuned golden test pass alone at
   `<agent>`. The two JSONs are byte-identical to `<dispatch>`'s.
3. **The expressions.** Read every changed line of `PotentialDerivativePolicy.py`, `ODEPolicy`,
   `HubblePolicy` and `_tuning_state` against §1's derivation. At r_χ = 0 each branch performs
   exactly `<dispatch>`'s operations, in the same order. No path adds 0.0.
4. **The ledger.** Rulings 1 and 3: K₀ at every N, `add`'s order, the emptiness test with no
   exp or log, and the payload key on every success. It pickles. The policy reads ρ_χ and forms
   r_χ from the state's ln ρ_R,E, so Radau's Jacobian sees the dependence.
5. **The breakages.** Replay each diff with `git apply` in `bash`. Each fails the test the log
   names, and is reverted.
6. **The numerics of the tests.** Rulings 2 and 8: the V = 0 state really is at U = 0.0, and each
   tolerance is justified. The size of Y·R/2 in item 4 is well above rtol. A probe of my own, in
   the session scratchpad and not committed: one constructed state at r_χ = 1, its constraint and
   Ḣ/H² from the definitions.
7. **The tuning with ρ_χ.** It converges at the log's r₀, inside the tolerance. The log's account of
   the V_target breakage is consistent with §0's estimate, or explains the difference.
8. **Nothing reaches the store.** Reread `ScalarHistory.store` and `build_extra_data` at `<agent>`.
   No new column, key or fingerprint. The ObjectFactories suite count is unchanged.
9. **The records.** The log is in §5.1's form, with the derivations, every deviation classed and
   §0's rulings among them. The two `.documents/` notes are dated and additive only. The board
   (row 01, gate DR, §5.1) and `.documents/OPEN_ISSUES.md`, if anything was opened, agree with each
   other.
10. **The suites** at `<agent>`, serially: the counts the log gives, each at least the baselines,
    with ComputeTargets up by the new tests only. `black --check` on the changed `.py` files.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, in the board, the log and `.documents/OPEN_ISSUES.md` wherever it
  appears;
- the board's row 01 landed, the gate DR held, and a list of orchestrator notes with this one
  marked "used for 01";
- under `### 5.1`, an *Orchestrator review of prompt 01* paragraph holding what each check found.

Residue the review finds is fixed in that commit or a follow-up of its own. Anything that is not
residue is opened on the board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the gate DR;
- the counts;
- anything opened;
- the ledger's interface as handed to 02;
- that 02 is written next, from 01's "State handed to the next prompt" (README §2.1).
