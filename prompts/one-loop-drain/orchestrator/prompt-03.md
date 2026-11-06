# Orchestrator — prompt 03, the store

Read [`../README.md`](../README.md) first: §5, and §8 whole, in particular §8.3, §8.6, §8.10 (decision 1) and §8.11
(P27–P32). Then the prompt. This note binds 03 only, and governs where it differs from the prompt (prompt,
"Written"). Note 01's conventions bind as well: a tolerance is a number with its reason, timing is taken under load,
and the suites run serially.

**The prompt:** [`03-the-store.md`](../03-the-store.md) · **Gate:** "Store" · **Closes:**
`[00-weighted-margins-hide-the-least-relativistic-generations]`, `[00-fixed-alpha-drain-overstates-late-reflections]` ·
**Opens:** only what the work finds · **Model:** Opus, as the prompt recommends · **Network:** none. Nothing is
pushed.

**When:** at once, at the user's request of 2026-11-06 ("Please write the orchestration note and dispatch it, and
afterwards carry out a review of the implementation agent's work"), in a fresh session.

**Gate**, checked at `2a6825b` on 2026-11-06 at 00:29 local time, before this note was committed:
- `one-loop-drain` is checked out in the main checkout at `2a6825b`, the commit that adds prompt 03.
  `git status --porcelain` is empty. `98b1601` and `f830085` are ancestors of `HEAD`.
- `git diff f830085 HEAD --stat` touches only the README, the board and the prompt, so the code and the tests are
  those 02d's review passed.
- `./venv/bin/python -m runregistrykit list`: 23 runs, all finished; none running, stale or unknown.
- `var/one-loop-drain/02d/sample.jsonl` and its `tasks/` (977 files) exist. `var/one-loop-drain/03/` and
  `.documents/one-loop-drain/store-03/` do not. The verification document ends at §2d.12.
- `.documents/OPEN_ISSUES.md`: 31 open, last updated 2026-11-05; both issues 03 closes are listed.
- The suites were not re-run for this note. 02d's review left them at 26, 594 (162 + 432), 130, 156 and 68, all
  passing, and nothing under any `tests/` has changed since. The agent measures them again first (prompt §4).
- Load averages 5.97 / 6.20 / 5.92 at 00:29. The machine has 10 cores and **16 GB of memory**, and it is shared.

## 0. What the orchestrator found

For this note the orchestrator read:
- `CosmologyConcepts/alpha_value.py`, `DimensionlessQuantity.py`; `ObjectFactories/DimensionlessQuantity.py` whole;
- `ComputeTargets/ScalarHistory.py`: the law's constants (:150–:280), the records (:430–:746), `check_drain_law`,
  `check_drain_law_model`, `drain_law_inputs` (:1736–:1825), `DissipationValues` (:3824), the margins' records and
  core (:3932–:4360), `history_margins` (:4360–:4500), `integrate_history`'s payload (:4800–:4905), `tune_history`'s
  success branch (:5345), `compute_scalar_history`, `ScalarHistory.compute` and `store`;
- `ComputeTargets/dark_radiation.py` (the ledger's API), `ComputeTargets/__init__.py`;
- `ObjectFactories/ScalarHistory.py`'s conversions (:56–:118) and the outline of its factories;
- `ObjectFactories/tests/test_dissipation_store.py`'s harness (how its payloads are made) and `dissipation_histories.py`;
- `datastorekit.store_inventory`'s `InventorySpec` and `canonical` at the pinned tag;
- `tools/history_and_bbn.py`'s `drain_values`; the users of `alpha_value` in `main.py`, `plot_inputs.py` and `tools/`;
- the head of 02d's `sample.jsonl` and the layout of its `tasks/`.

Here is what it found where the prompt is silent, loose or wrong.

- **The sample masses and §2.1 conflict as written.** `test_dissipation_store` and `test_tuned_history_round_trip`
  make their payloads with `integrate_history` alone (`dissipation_histories.payload`, and the tuned golden's one
  integration), with stand-in tuning keys, and store them. Prompt §1.5 has `store()` refuse a payload with samples but
  no masses. If only `tune_history` computes the masses (P31), both tests' payloads are refused, and §2.1 says they
  pass unchanged but for the literals. So `integrate_history` must compute the masses when it is called alone, and
  `tune_history` must not compute them on each of its integrations (ruling 2).
- **Other tests build payloads by hand** with `"rho_chi_over_rho_rad"` and samples: `test_store_fingerprint_golden`,
  `test_adiabatic_dark_radiation`, `test_alpha_figures` (ObjectFactories) and `tools/tests/test_compare_stores.py`.
  These need the mass keys added. That is a change the new column forces (prompt §1.8, "Existing tests"), and each
  one is named in the log.
- **The guard and a payload without `"drain"`.** Every payload `integrate_history` returns carries `"drain"`; a
  hand-built test payload may not. A guard that refuses a missing record would break tests §2.1 protects (ruling 3).
- **The import cycle is real.** `ComputeTargets/__init__` imports `ScalarHistory`, which imports `CosmologyConcepts`,
  whose `__init__` imports `alpha_value`. If `alpha_value` imports the constants or `check_drain_law` from
  `ComputeTargets`, the import fails (ruling 1).
- **`history_margins` gathers and then calls the pure core.** The core (`relativistic_margins`) never raises
  `ComputationFailureError`; only the gathering's model calls do (`field_log_m` on an unassessed candidate or an event,
  `tracker_log_m` on a late sample or today). Where a failure falls decides how much of the events was computed, which
  the prompt leaves to the agent (ruling 5).
- **"K just before the candidate" is ambiguous in the ledger.** `DarkRadiationLedger` holds only entries with
  ΔK > 0, plus the parking leftover, which is entered at the cut candidate's own N. `K(N)` is right-continuous
  (`bisect_right`), so `K(N)` at a candidate's N already includes its own entry, and at the cut candidate it includes
  the leftover too. The ledger keeps each entry's cumulative K, computed by its own addition, but exposes it only
  through `K(N)` (ruling 6).
- **The row's ln T_J.** A `"turning_point"` candidate has a wall-side `TurningPoint` at N_turn, matched by exact N
  equality, as `history_margins`' `drained_turns` matches it. A reflection (`"floor"`, `"unresolved"`) has none.
  Every event is a `TurningPoint` (ruling 7).
- **The shares at K_final = 0.** `drain_values` (the tool's `drain` line, which 02d's numbers come from) gives
  `None` for every share where K(N_final) = 0. `relativistic_margins` gives 0.0 there. A one-loop history that drains
  nothing (02's 3016 and 3233) has K_final = 0 (ruling 8).
- **DatastoreKit allows a string leaf.** `canonical` keeps a `str` as it is, and `BBNData`'s spec already has one
  (`PRyM_version`). So `alpha_value`'s spec can carry `drain_model`, and nothing in the package blocks the key.
- **The tools that join `alpha_value` by raw SQL** (`tools/bbn_from_store.py:222`, `tools/outward_step_scan.py:848`)
  and `tools/inventory_report.py`, which names the table, do not see `drain_model`. Before 04 no driver can write a
  one-loop row, so nothing they read changes. On a store with both models they would not tell them apart. That is 04's
  work, not 03's (ruling 11).
- **§2.4's "once, untuned, at its accepted normalization"** needs `phi_ref` as well. 02d's line keeps both under
  `tuning` (JSON floats, which round-trip exactly). The accepted integration's candidates are the last line of the
  task's `tasks/<id>.partial.jsonl` (ruling 10).
- **C-3873 timed out at seven of eight A in 02d** (an hour each). It cannot serve as §2.4's C history.

**Rulings.** Each is recorded in the log, with its class.
1. **The key's home.**
   - `DRAIN_LAW_FIXED`, `DRAIN_LAW_ONE_LOOP`, `DRAIN_LAWS`, `DRAIN_MODEL_FIXED`, `DRAIN_MODEL_ONE_LOOP`, the
     model → law mapping, and `check_drain_law` move to one leaf module. That module imports nothing from
     `CosmologyConcepts` or `ComputeTargets` (for example `CosmologyConcepts/drain_model.py`; the agent names it).
   - `ComputeTargets/ScalarHistory.py` imports them from there, so every existing `SM.<name>` still resolves to the
     same object. The fitted constants, the S table and the law's long comment stay where they are, with a pointer.
     `check_drain_law`'s messages are unchanged.
   - `alpha_value(store_id, value, drain_model=DRAIN_MODEL_FIXED)`, so existing constructions are unchanged. It
     checks the pair through `check_drain_law(law_of(drain_model), value, …)`, and has a `drain_law` property.
   - The factory checks the pair before it queries or inserts. It matches `value` as today (relative, or absolute at
     0) **and** `drain_model` exactly. `read_table` returns both.

   *STRUCTURALLY REQUIRED* (§0, fourth point); the module's name is an IMPLEMENTATION CHOICE.
2. **The sample masses: one function, two callers.**
   - A function computes both tuples from (realized model, cosmology, samples).
     - The field's mass is `field_log_m` on the sample's own stored state, (φ, π, ln ρ_R,E, ln f_m, ln T_J).
     - The tracker's is `tracker_log_m` from the sample's stored φ, ln ρ_R,E, ln f_m and Σ, as `history_margins`
       calls it.
     - Each is None where the call raises `ComputationFailureError`. Any other exception propagates.
   - `integrate_history` calls it by default. A keyword, which only `tune_history` passes, turns that off.
   - `tune_history` turns it off on every integration, and calls the function once on the converged payload, with the
     converged `rm`. A test shows that the two give bitwise the same tuples for the same integration.

   *STRUCTURALLY REQUIRED* (§0, first point): it keeps both §2.1 and P31.
3. **The guard (P28), made exact.** It runs on a success payload only, before anything is stored or set on the object.
   - A `"drain"` record whose `model` differs from the key's `drain_model` is refused.
   - A missing or None `"drain"` is accepted under `fixed` and refused under the one-loop model.

   *IMPLEMENTATION CHOICE*, ruled here (§0, third point).
4. **The bounce key.**
   - `history_margins` is split. A gatherer returns the core's inputs: the `MarginCandidate`s, the `MarginEvent`s,
     N_late, the late samples, today, K_final and K_unassessed. `history_margins` calls the gatherer and the core and
     returns what it returns today.
   - `integrate_history` calls the gatherer and the core itself, and keeps the inputs for the builder. The core runs
     once, and no model call is added.
   - The builder is pure. Every value from the dense output or the ledger (ruling 6, ruling 7) is computed before it
     is called and handed to it.
   - The payload key is present under every law. It is None under the fixed law and at α = 0, and on a failure it is
     absent, as every new key is.

   *IMPLEMENTATION CHOICE*, ruled here.
5. **Where the margins fail.** The history's margin warning stands, as today.
   - No event row is written. Candidate rows are written in full from the `Injection`, the `DrainCandidate`, the
     ledger and the reused `PeakEstimate`.
   - NULL on the candidate rows: the Generation columns, and `log_m_Mp` of an unassessed candidate.
   - NULL on the history: every `all_*` column. `bounce_rows` counts the rows written.

   This gives one rule wherever in the gathering the failure falls. §2.4 counts margin failures.
   *IMPLEMENTATION CHOICE*, ruled here (§0, fifth point).
6. **K before the candidate.**
   - `K_before` is the ledger's cumulative K after the entries made before the candidate. It is read by position, by
     walking the ledger's entries beside the candidates in index order, with the leftover after the cut candidate. It
     is never read by `K(N)` at a candidate's N, and never re-summed.
   - A read-only accessor on `DarkRadiationLedger` that returns its cumulative K per entry, with a test, is allowed.
     `ComputeTargets/dark_radiation.py` then joins the commit.
   - The tests check, at every candidate with ΔK > 0, that `K_before` is the previous entry's cumulative K (or
     `K_initial` at the first), and that the ledger's own K after the entry is `K_before + ΔK` bitwise.

   *As the prompt intends; made exact* (§0, sixth point).
7. **The row's ln T_J.**
   - A `"turning_point"` candidate and every event take the `TurningPoint`'s own `log_T_Jordan`.
   - A reflection candidate takes the dense output's, `solution(N_turn)`'s fifth component.
   - The log gives the count of each, on §2.4's histories.

   *As the prompt says; made exact* (§0, seventh point).
8. **The shares.**
   - Where K_final = 0, the five shares are NULL.
   - Otherwise each share is computed as `drain_values` computes it (the injections' ΔK by regime, in index order, over
     K(N_final); the parking leftover's ΔK over the same) and equals the tool's bitwise.
   - The sum to 1 is tested where K_final > 0.
   - §2.4 compares the stored shares with each history's 02d `drain_values`.

   *IMPLEMENTATION CHOICE*, ruled here (§0, eighth point).
9. **The units.**
   - One helper per suffix, used by the store, the read-back and the tests:
     - a log column: ln x − ln u forward, + ln u back, with ln u = `log(units.PlanckMass)`, `4*log(units.PlanckMass)`
       or `log(units.GeV)`;
     - a linear column (`delta_K_Mp4`, `K_before_Mp4`): x / `units.PlanckMass**4` forward, × back.
   - A nonzero value that converts to 0.0 or to ±inf, in a test or in §2.4, is a stop.

   *As the prompt says; made exact.*
10. **§2.4, made exact.**
    - **The six.** C-3873 and any task whose 02d wall exceeded 1800 s are excluded.
    - **Each run.**
      - It is realized from the line's `tuning` (`normalization`, `phi_ref`) exactly as `tune_history` realizes an
        integration, and integrated with `tune_history`'s T_stop, z grid and tolerances.
      - Its candidates are compared with the **last** line of `var/one-loop-drain/02d/tasks/<id>.partial.jsonl` by
        `repr`: regime, α_eff and ln R, and the whole `DrainCandidate`.
      - The temporary stores go under `var/one-loop-drain/03/stores/`, through `StoreFixtures.standin_pool`. They are
        never opened through Ray, and they are never registered.
    - **Launch.** Detached as note 02d's ruling 2: `nohup`, pid recorded, a log and an end marker under
      `var/one-loop-drain/03/`, one check that it started, and a background wait, not polling. At most six at once.
    - **Reported.** Each part's wall time, and the 1-, 5- and 15-minute loads at the start and end.
    - **The stop.** The median over the six of the ratio (added time / that history's integration wall) exceeds 0.25.
      With it come the breakdown and the loads.

    *As the prompt says; made exact* (§0, tenth and eleventh points).
11. **Nothing in `tools/`, `main.py` or the plot scripts changes** but what a test of §1.8 forces. The raw-SQL readers
    of `alpha_value` go into "Observations not acted on", for 04. *As the prompt says.*
12. **Files.**
    - The commit holds prompt §5's files. Also, where used: ruling 1's module, `ComputeTargets/dark_radiation.py`
      (ruling 6), and the test helpers and test modules that a forced change touches, each named in the log.
    - Nothing under `var/` is committed. Nothing under `.documents/one-loop-drain/` but `store-03/` changes.
    - *STRUCTURALLY REQUIRED*; followed.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before
  committing. Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The Gate, and the five suites, before anything else. They run serially: ComputeTargets in its two halves, with
     the `test_[g-z]` half and `tools/tests` in the background, waiting for each notification.
  2. The key (ruling 1), the masses (ruling 2), the guard, the bounce key and the history record, with their
     `ComputeTargets` tests.
  3. The table, the factories, the configuration and the labels, with their `ObjectFactories` tests.
  4. §2.1's key-by-key comparison on a golden history, against the parent commit's payload in a scratch worktree of
     the parent (removed afterwards).
  5. §2.4.
  6. `black`; the suites again; the documents; the records; the commit.
- **No value of the integration moves.** A golden that fails, or a pre-existing payload key that differs under the
  fixed law, is a stop. It is not "fixed" by re-pinning.
- **Scratch.** Outputs go under `var/one-loop-drain/03/`. Probes and suite outputs go in the agent's own
  subdirectory of the session scratchpad. Nothing goes in `/tmp`. Nothing is written under `var/one-loop-drain/02*/`.
- **No Ray.** Nothing starts a cluster, and no `ray` command is run. No store is opened but the tests' temporary ones
  and ruling 10's.
- **Stop conditions are real:**
  - the prompt's: §1.3's two equality checks (ln R, the estimate), what 05 needs, the inventory, DatastoreKit, §2.4's
    median;
  - ruling 9's conversion check;
  - a golden or a fixed-law key that moves;
  - a suite count that falls.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `2a6825b`, before this note was committed:
1. **The Gate above.**
2. **The reading of §0.**

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout, in the background. Give it:
- the prompt, the README, the board and this note;
- `HEAD`, and the Gate as measured.

The agent reads this note and no other file of `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with ruling 12's files. Its log is `logs/03-the-store.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **It starts nothing that outlives its turn** but §2.4's runs, and every one of them must have ended before it
  commits.
- **Stop and ask** on any of the conventions' stop conditions.
- **It replies with:**
  - the log's path, the message as committed, and `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the key: where the constants went, and the refusals;
  - the masses (ruling 2), the guard (ruling 3), and the split of `history_margins` (ruling 4);
  - the bounce columns and the history columns as built, with units;
  - the Store gate, item by item (prompt §2.1–§2.4), with §2.4's table;
  - the digests that moved, and why;
  - every existing test changed, and why;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — eight checks

1. **Scope.**
   - `git show --stat <agent>` lists only ruling 12's files. Its parent is this note's commit, or a later commit that
     is not the agent's.
   - `git status --porcelain --untracked-files=all` is empty, and nothing is pushed.
   - Read the whole diff of `ComputeTargets/ScalarHistory.py`. No line of the step loop, the law, the classification,
     the gate, the margins' core or the tuning's iteration changes; the split of `history_margins` moves code without
     changing it.
   - `config/fingerprint.py`'s formats 1–6 and the old golden files are unchanged.
2. **Nothing left running.** No pid under `var/one-loop-drain/03/` answers `kill -0`, every end marker exists, and
   `runregistrykit list` is unchanged.
3. **The suites, re-run by me**, serially as the conventions say: the counts the log states, all passing, none fallen.
4. **The fixed law, independently.**
   - In a scratch worktree at the agent's parent, integrate one golden history under the fixed law at α > 0 and at
     α = 0, and the same at the agent's commit.
   - Compare the payloads key by key with a script of my own. Only the mass keys and the bounce key (None) are new; every
     other key is equal by `repr`.
5. **A one-loop round trip, independently.**
   - In a scratch script, store P-2197 at A = 1 (seconds) on a temporary store, and read it back with and without
     `_do_not_populate`. Check:
     - the bounce rows' count against the candidates plus the events, and their order;
     - `log_R` against `DrainCandidate.log_R` and `PeakEstimate.log_R`, bitwise;
     - the `K_before` chain against the ledger;
     - the shares against `drain_values`, bitwise, and their sum;
     - the all-generation margins against the core with `MARGIN_WEIGHT_SHARE` patched to 0;
     - the masses' NULL counts.
6. **The key.**
   - Two rows for one value under the two models.
   - A lookup with no model finds the fixed row.
   - Refusals of A = 0 under the one-loop model and of an unknown model, before any row.
   - A payload whose model differs is refused by `store()`.
7. **§2.4's numbers.** Recompute one history's ratio and the median from the files the verification section names,
   and check one history's candidates against its 02d partial file.
8. **The records.**
   - The log is in §5.1's form, with every deviation classed and §0's rulings among them.
   - The verification document's §1–§2d are unchanged, and §3 follows prompt §2.
   - The board: row 03, its items, the Store gate, the two issues in §4, and **Last updated**.
   - `.documents/OPEN_ISSUES.md`: the two rows gone, 29 open, the date.
   - `CLAUDE.md`'s counts are those measured, and its "Redshift" sentence names the drain model.
   - `architecture-summary.md` and `numerical-strategies.md` have their dated notes.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 03 landed, the Store gate held, and this note added to the list of orchestrator notes, marked
  "used for 03";
- an *Orchestrator review of prompt 03* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

Residue is fixed in that commit or a follow-up of its own. Anything that is not residue is opened on the board's §3
and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the Store gate: the key, the table and its row counts, the fixed law unchanged, the labels, and the cost;
- the two issues closed, and anything opened;
- that 04 (the drivers and exports) is written next.
