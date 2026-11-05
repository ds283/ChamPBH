# Orchestrator — prompt 02c, the measured law

Read [`../README.md`](../README.md) first: §5, and §8 whole, in particular §8.10 (the measured law, its constants and
sources, decisions 1–3, U15 as ruled, the Measured gate). Then the prompt. This note binds 02c only, and governs where
it differs from the prompt (prompt, "Written"). Note 01's conventions bind as well: a tolerance is a number with its
reason, timing is taken under load, and the suites run serially.

**The prompt:** [`02c-the-measured-law.md`](../02c-the-measured-law.md) · **Gate:** "Measured" · **Closes:** nothing ·
**Opens:** what the work finds, and ruling 9's issue · **Model:** Opus, as the prompt recommends · **Network:** none.
Nothing is pushed.

**When:** at once, at the user's request of 2026-11-05 ("Please write the orchestration note and dispatch it, and
afterwards carry out a review of the implementation agent's work"), in a fresh session.

**Gate**, checked at `1b16c62` on 2026-11-05 at 02:00 local time, before this note was committed:
- `one-loop-drain` is checked out in the main checkout at `1b16c62`, after `38e3ee7`, the commit that adds prompt 02c.
  `git status --porcelain` is empty.
- **U15 is ruled:** option (b), in the user's words, "OK, let's take option (b) for now" (README §8.10).
- `git diff 1edb56c HEAD --` outside `prompts/` and `.documents/` is empty, and inside `.documents/` it touches only
  `OPEN_ISSUES.md`. So the code and the tests are those that 02b's review (`1edb56c`) left.
- `./venv/bin/python -m runregistrykit list`: 23 runs, all finished. The rehearsal lock is free.
- The inputs of prompt §2.3 exist under `var/one-loop-drain/02b/tasks/`, each with a tuning record:
  `P-2197-A0.05-zero`, `W-2187-A0.05-zero`, `R-632-A0.5-zero` and `L-M5e-20-A0.5-zero` (`.result.json`).
- The suites were not re-run for this note. 02b's review left them at 26, 566 (163 + 403), 130, 156 and 68, all
  passing, and nothing under any `tests/` has changed since. The agent measures them again first (prompt §4).
- Load averages 26.00 / 27.43 / 31.33 at 02:00, from no single process of this project. The machine has 10 cores and
  **16 GB of memory**, and it is shared.

## 0. What the orchestrator found

For this note the orchestrator read:
- `source/crossover/cascade_map.py` and `measured_law_map.py` whole; `out/stage1a_S_of_L.txt`; the headers and the
  regimes, ℓ and L of the four maps of prompt §2.3; crossover's README, "The adopted law at small A";
- `ComputeTargets/ScalarHistory.py`: the law's constants (:150–:195), `DrainCandidate` and `DrainRecord` (:618–:658),
  `check_drain_law`, `drain_regime`, `drain_alpha_effective` (:1647–:1735), the record's construction (:1930–:1950),
  `candidate` (:2160–:2215) and `_assessed_peak` (:3913);
- `CosmologyConcepts/Models/AbstractModel.py`: `ClassicalTurningPoint`, `classical_turning_point`,
  `RealizedModel.log_V_floor`, `_turning_point_unresolvable`; `ExponentialModel._log_V_floor` and `_log_V`;
- `tools/history_and_bbn.py`'s gap-prescription handling, and the list of test classes in `test_drain_law.py`;
- every Python file of the tree that names a removed symbol.

Here is what it found where the prompt is silent, loose or wrong.

- **An assessment always carries a turning point.** `drain_regime` returns `unassessed` exactly where the assessment
  is None. Otherwise `_assessed_peak` reads `ct.log_R` from the assessment's `ClassicalTurningPoint`, which is never
  None there. So every `one_loop`, `gap` and `classical` candidate has ln R and an ln E.
  - The reference's branch "turning point None, `one_loop`, ℓ < 1 → 0.9" is never reached in production.
  - Neither is "turning point None, `one_loop`, ℓ ≥ 1 → 0".
  - L under (b) needs ln E and ln V_floor only, and both exist at every assessed candidate.
- **ln V_floor has a public route.** `RealizedModel.log_V_floor` (:418) returns `ExponentialModel._log_V_floor`, which
  is the normalization. No model file needs to change.
- **L is safe from underflow and overflow in practice.** An assessed candidate has a resolvable turning point. Then
  ln E is at least the least finite ln(V − V_floor), about ln V_floor − 28 at |ln V_floor| ≈ 277
  (`_turning_point_unresolvable`). So x = ln E − ln V_floor > −28, and L > 10⁻¹², and ln L is finite. The maps reach
  x ≈ 106. But `log1p(exp(x))` overflows above x ≈ 709 (ruling 2).
- **At n = 1, L = ln(1 + E/V₀) is M/φ_t to the root's accuracy.** V − V_floor = V₀(e^{M/φ} − 1), so
  V(φ_t) − V_floor = E gives M/φ_t = ln(1 + E/V₀) exactly. φ_t is a brentq root in ln φ at rtol 4ε, xtol 10⁻¹⁴
  (`WALL_ROOT_RTOL`, `WALL_ROOT_XTOL`). So the two forms of L differ by about 10⁻¹² relative, not bitwise.
  - `fraction_adopted` reads L as `M_MP / turning_point.phi_t`.
  - The maps print L as M/φ_t too, computed by `cascade_map.py` itself, so that column does not test the production
    L.
- **The reference's S(L) is not the ruled S(L) above L = 134.** `measured_law_map.S_of` is `np.interp`, which holds
  the end value on both sides. README §8.10 and the prompt continue it as S(134)·(134/L)² above 134.
  - Below 0.3 the two agree (both hold S(0.3)).
  - None of the four maps of §2.3 has a candidate with L > 134. Their L runs over 2.6–106, and the classical
    candidates (the ladder's only) over 54.5–68.8.
  - So §2.3 cannot see the difference. §2.2 and §1.6's reference test must be told where it lies (ruling 3).
- **The table's values are printed to five figures.** `S_of` interpolates `np.log` of those printed floats against
  `np.log` of L's printed floats, with `np.interp`. A module table of the same decimal literals, interpolated by the
  same expression, is bitwise equal to it (ruling 3).
- **The gap fraction is negative for L < e^{−1/q} ≈ 0.304.** With q = 0.841, 1 + q ln L < 0 there, so
  f = 1 − exp(−g (ln R − r₀)^p (1 + q ln L)) < 0 for ln R > r₀. `fraction_adopted` does the same. Then α_eff < 0. The
  caller's `alpha_eff > 0.0` test skips the drain, but the record carries the negative value.
  - Whether a gap candidate reaches L < 0.3 is not known. R > 1 at small L needs V₀/M⁴ large. The B group of the
    2026.8.0 run (M = 10⁻³³ M_P) is the likeliest place.
  - Stage 2's gap points lie at larger L.
  - What f should be there is a physics choice, and so the user's (ruling 9).
- **The four maps' inputs.**
  - P-2197 and W-2187 at A = 0.05 are 02b's own tasks.
  - R-632 at A = 0.5 is 02b's `R-632-A0.5-zero` as it stands: its `argv` already has A = 0.5, with its own tuning.
  - Only the ladder needs a copy: `L-M5e-20-A0.5-zero` with `--alpha` set to 0.2 in `argv`.
  - Crossover's headers carry an edited id ("… (A=0.2, tuning of A0.5)"). Prompt §2.3 does not compare the id.
- **The removed symbols are named elsewhere too.** Besides `.documents/one-loop-drain/measure/` and `measure-02b/`,
  01's `.documents/one-loop-drain/law/` names them (`probe_table.py`, `one_loop_run.py`). None of the three folders
  has a README.
  - In `source/crossover/`, only `cascade_map.py` names `_GAP_PRESCRIPTION`.
  - `measured_law_map.py` names only `DRAIN_ALPHA_CAP` and the two patched functions.
- **The classical regime now drains.** Under the old law a classical candidate drained exactly 0, and took the
  α_eff = 0 branch. Under the adopted law it drains A·R·S(L)·w_gate, which can be 10⁻⁴⁰ and still > 0. It then takes
  `drain` and a restart.
  - The ladder at A = 0.2 has 83 such candidates. R-632 at A = 0.1 had 3269 in crossover.
  - It is the law as ruled (decision 3), and crossover's maps were made with it. Its cost is 02d's to measure; the
    handover names it (ruling 10).

**Rulings.** Each is recorded in the log, with its class.
1. **The signature.** The agent chooses it, as the prompt says.
   - Whatever it chooses, ln R and L reach `drain_alpha_effective` as numbers computed once per candidate in
     `candidate`. The same numbers go into `DrainCandidate.log_R` and `DrainCandidate.L`, so that the record and the
     drain cannot disagree.
   - At an unassessed candidate both are None, and the function returns 0.0 without reading them.

   *IMPLEMENTATION CHOICE*, bounded here.
2. **L, computed stably.** L = ln(1 + e^x), x = ln E − ln V_floor, by a form that cannot overflow for any finite x.
   Two examples are x + log1p(exp(−x)) for x > 0 with log1p(exp(x)) otherwise, or `np.logaddexp(0.0, x)`.
   - It is a pure function, tested at x = −30, 0, 30 and 800, and at the maps' x against `log1p(exp(x))`.
   - The prompt's "through `log1p` of an exponential, not by subtracting" stands. Only the overflow is added.

   *STRUCTURALLY REQUIRED* (§0, third point).
3. **The reference, made exact.**
   - **The test copies the reference.** `fraction_adopted` and `S_of`, with the table loading, are copied verbatim
     into the test module, naming their file and the commit `1b16c62`. They are not imported from `prompts/`.
     Importing `measured_law_map` inserts into `sys.path`, imports `cascade_map` and the tool, and keeps `M_MP` as a
     module global. A test of the production code should not depend on a campaign's record folder.
   - **How the comparison runs.** It calls the production law with L = M_MP/φ_t, computed by the same expression the
     reference uses, and the reference with a synthetic `ClassicalTurningPoint` carrying that φ_t and ln R.
     - At L ≤ 134 the two are equal bitwise, at every grid point.
     - At L > 134 in the classical regime, the reference's S is replaced in the test by the ruled continuation. The
       S(L) test checks the continuation alone.
     - The log states this as the one place the production law departs from `fraction_adopted`, and why: README
       §8.10 rules it.
   - **The S table** holds the 21 values of `stage1a_S_of_L.txt`'s columns L and S(L) as the same decimal literals,
     and interpolates by `np.interp` on their `np.log`, as `S_of` does. If the agent interpolates otherwise, the
     10⁻¹⁴ bound of prompt §2.2 applies, and the log names it.
   - **The two forms of L** are tested apart (U15, §1.6). The n = 1 agreement is to 10⁻¹⁰ relative: 100 times the
     about 10⁻¹² that §0's fourth point estimates, which leaves margin for |ln φ| up to about 80 and L up to about
     130.

   *IMPLEMENTATION CHOICE*, ruled here (§0, fourth to sixth points).
4. **§2.3, made exact.**
   - **Inputs.** P-2197 and W-2187 use 02b's `…-A0.05-zero.result.json` and R-632 uses `R-632-A0.5-zero.result.json`,
     each unchanged and read in place. The ladder uses a copy of `L-M5e-20-A0.5-zero.result.json`, with `argv`'s
     `--alpha` value set to `"0.2"` and nothing else changed. The copy is written under
     `var/one-loop-drain/02c/inputs/` by a script in `law-02c/`.
   - **The comparison.**
     - Every candidate row is compared as a string with crossover's row of the same index.
     - From the `# <id>:` header line, `parking_N`, `N_final` and `candidates` are compared as strings.
     - Not compared: the first line of crossover's file (the adopted-law banner, which `measured_law_map.py` prints
       and unpatched `cascade_map.py` does not), the `@@` line, and the header's id and `gap=` fields.
   - **The control.** After the code change, `measured_law_map.py --law=adopted`, with the §1.5 adaptation, is run
     on P-2197 at A = 0.05 on the new tree. Its whole output must equal crossover's file byte for byte. This checks
     that the adapted patch applies the same law, and that nothing else in the integration has moved.
   - **On a difference** in an unpatched map, run the control on that history too, before reporting.
     - If the control equals crossover's file and the unpatched map does not, the production law differs from the
       reference. Name the first differing candidate, its inputs, and both α_eff to full precision.
     - The prompt's single exception is a difference traced to the 10⁻¹² between the two forms of L, carried by the
       integration. It is stated and the work continues only if both hold:
       - (i) the first differing row differs only in the last printed digit of one column;
       - (ii) the production and reference α_eff at the first candidate whose inputs agree differ by no more than
         10⁻¹⁰ relative.

       Anything else is a stop.

   *As the prompt intends; made exact.*
5. **The detached runs.** The five map runs (four unpatched, one control) go in one detached batch, at most five at
   once.
   - `nohup`, pid recorded; `batch.log` and `batch.end` under `var/one-loop-drain/02c/batches/<name>/`; one check that
     it started; and a background wait, not polling.
   - Not registered: they are diagnostic integrations of minutes, as 02's and 02b's batches were.
   - §2.4's tool run may run in the foreground if it takes under ten minutes. Otherwise it goes in the batch.

   *STRUCTURALLY REQUIRED* (precedent: notes 02 and 02b, ruling 2).
6. **The dated notes of §1.2.**
   - A new `README.md` in each of `.documents/one-loop-drain/law/`, `measure/` and `measure-02b/`, holding only the
     dated note. It says that the scripts name `_GAP_PRESCRIPTION`, `DRAIN_C1` or `--gap-prescription`, removed by
     02c, and run only on a tree before this commit, which it names as "this commit". The scripts stay
     byte-identical.
   - `law/` is added to the prompt's two folders for the same reason.

   *IMPLEMENTATION CHOICE* (the README in place of a docstring, so that no script changes) and *STRUCTURALLY
   REQUIRED* (`law/`).
7. **The record's fields.**
   - `DrainCandidate` appends `L` and `log_R` after today's six fields, in that order.
   - `DrainRecord` puts `model` where `gap_prescription` stood, so that the record keeps six fields.
   - Every test that builds either positionally is updated. The record crosses Ray and pickles as before: plain
     values only.

   *IMPLEMENTATION CHOICE*, ruled here.
8. **The suites and the counts.**
   - Before and after, serially: CosmologyModels; ComputeTargets in two halves, the `test_[g-z]` half in the
     background; ObjectFactories; `tools/tests` alone, in the background; RunDrivers.
   - At this load the `test_[g-z]` half may pass Bash's ten minutes, so it never runs in the foreground.
   - The tests deleted under prompt §1.6 are named one by one in the log, with their suite. Every other fall is a
     stop.

   *As the prompt says; made exact.*
9. **The gap below L ≈ 0.304.** The agent implements f as README §8.10 writes it, with no clamp, as
   `fraction_adopted` does.
   - **The test.** One test pins the boundary L = e^{−1/q}: f = 0 at it to rounding, and f < 0 just below it, with ln R
     > r₀. Its docstring names this note's issue.
   - **The issue.** The agent opens it on the board's §3 and in `.documents/OPEN_ISSUES.md`:
     `[02c-the-gap-fraction-is-negative-below-L-0.304]`. It holds the measurement, and the observation that the
     caller skips the drain while the record carries α_eff < 0. Its next step: the user rules what f is there (for
     example 0, or the value at L = e^{−1/q}) before 02d.
   - **The count.** The agent counts the gap candidates with L < 0.304 in §2.3's and §2.4's runs. A count above
     zero is reported, not a stop: the reference does the same, and the reproduction is unaffected.
   - If the user rules on it before the agent commits, the orchestrator forwards the ruling in the user's words with
     `SendMessage`.

   *As the prompt intends* (the law as ruled); the issue is the orchestrator's (§0, seventh point).
10. **The handover to 02d** ("State handed to the next prompt") names, besides the prompt's list:
    - that classical candidates now drain, with the counts and wall times of §2.3's runs against crossover's where
      crossover recorded them;
    - ruling 9's issue;
    - that 02's and 02b's scripts no longer run on this tree.

    *As the prompt intends; made exact.*
11. **No file but the prompt §5 list, plus:**
    - the three READMEs of ruling 6;
    - `source/crossover/cascade_map.py` and `measured_law_map.py`, changed as prompt §1.5 allows and no further;
    - the board and `.documents/OPEN_ISSUES.md` for ruling 9's issue.

    No model file changes (§0, second point). Nothing under `var/` is committed. `law-02c/out/` holds text only.
    *STRUCTURALLY REQUIRED*; followed.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before
  committing. Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The Gate, and the five suites before anything else (ruling 8).
  2. The code, the tests, and the crossover scripts' change.
  3. The suites again, with the goldens of prompt §2.1 confirmed among them.
  4. The batch of ruling 5, then the comparisons of ruling 4, then §2.4's tool run.
  5. The documents, the records, and the commit.
- **Load.** Record the 1-, 5- and 15-minute loads at each suite's and the batch's start and end, beside its wall
  time.
- **Scratch.** Outputs go under `var/one-loop-drain/02c/`. Probes and suite outputs go in the agent's own
  subdirectory of the session scratchpad. Nothing goes in `/tmp`, and nothing is written under
  `var/one-loop-drain/02/` or `02b/`.
- **No Ray.** Nothing starts a cluster, and no `ray` command is run. No store is written but a test's temporary one.
- **Stop conditions are real:**
  - the prompt's Gate, and the falls of prompt §1.6;
  - a golden that does not pass bitwise;
  - a failure of prompt §2.2;
  - ruling 4's control or comparison, under its exception only.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `1b16c62`, before this note was committed:
1. **The Gate above.**
2. **The reading of §0.**

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout, in the background. Give it:
- the prompt, the README, the board, and this note's path;
- `HEAD`, and the Gate as measured.

The agent reads this note but no other file of `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the files of prompt §5 and ruling 11. Its log is
  `logs/02c-the-measured-law.md`, in `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **It starts nothing that outlives its turn** but the batch of ruling 5, which must have ended before it commits.
- **Stop and ask** on any of the conventions' stop conditions.
- **It replies with:**
  - the log's path, the message as committed, and `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`, and each deleted test named;
  - the signature chosen, and the form of L;
  - the reference test's grid, and where it is bitwise and where it is not;
  - the four maps and the control: each equal or not, with the first difference if any, and wall times;
  - the tool run, with its `history` and `drain` lines verbatim;
  - ruling 9's count, and the issue as opened;
  - the changes to `cascade_map.py` and `measured_law_map.py` as a diff;
  - the Measured gate, item by item (prompt §2.1–§2.4);
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — seven checks

1. **Scope.**
   - `git show --stat <agent>` lists only ruling 11's files. Its parent is this note's commit, or a later commit that
     is not the agent's.
   - `git status --porcelain --untracked-files=all` is empty, and nothing is pushed.
   - In `source/crossover/`, only the two scripts change, and only as prompt §1.5 allows.
   - `.documents/one-loop-drain/law/`, `measure/` and `measure-02b/` change only by their new READMEs.
   - Nothing under `config/`, `ObjectFactories/`, `main.py`, the plot scripts, `PRyM/` or `thirdparty/` changes, and
     neither do `VERSION_LABEL`, `FINGERPRINT_FORMAT` or the stepper label.
2. **Nothing left running.** No pid under `var/one-loop-drain/02c/` answers `kill -0`, `batch.end` exists, and
   `runregistrykit list` is unchanged.
3. **The law, read.** Read the new code against README §8.10 and `fraction_adopted` term by term: the constants, each
   regime's expression and order of operations, the S table's 21 literals against the file, the continuation, L's
   form, and that the removed symbols are gone from the tree outside the records.
4. **The reference, independently.** In a scratch script of my own, evaluate the production law against
   `fraction_adopted` imported from `source/crossover/` (not the test's copy), at a random grid of 10⁴ points over
   every regime, with ℓ in (0, 40), L in (0.01, 300) and ln R in (−50, 30). Expect bitwise equality at L ≤ 134, and the
   continuation's departure only above it.
5. **One map, re-run.** Re-run unpatched `cascade_map.py` on one of the four, in a process of my own, and compare it
   with crossover's file and with the agent's output under ruling 4.
6. **The tool, re-run.** Re-run §2.4's command, and compare its `history` and `drain` lines with the agent's.
7. **The records.**
   - The log is in §5.1's form, with every deviation classed and §0's rulings among them.
   - The verification document's §1, §2 and §2b are unchanged, and §2c follows prompt §2's four items.
   - On the board: row 02c, its items, the Measured gate, ruling 9's issue, and **Last updated**.
     `.documents/OPEN_ISSUES.md` matches §3, with its count and date.
   - `CLAUDE.md`'s counts match the suites as I re-run them (the five, serially).

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 02c landed, the Measured gate held, and this note added to the list of orchestrator notes, marked
  "used for 02c";
- an *Orchestrator review of prompt 02c* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

Residue is fixed in that commit or a follow-up of its own. Anything that is not residue is opened on the board's §3 and
in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the Measured gate: the goldens, the reference, the four maps and the tool;
- ruling 9's issue, put to the user for a ruling in their own words;
- that 02d is written next.
