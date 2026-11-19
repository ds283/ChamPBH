# Prompt 07b2 — measure the min(rise, fall) form

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-18 by the orchestrator of 07b, after 07b landed (`0223d50`, reviewed `4f9b1bd`), at the user's
word: "Measure the min(rise, fall) form properly before 07c". It is written from:
- README §6.12 (R13–R17, P73–P90, U11–U13) and §6.13 (the orchestrator's probe, P91–P98);
- 07b's prompt, log and outputs: [`07b-remeasure-k-ex.md`](07b-remeasure-k-ex.md),
  [`logs/07b-remeasure-k-ex.md`](logs/07b-remeasure-k-ex.md), `source/kex/` and `.documents/power-law-wall/kex-07b/`,
  and `.documents/power-law-wall-verification.md` §7;
- the side-campaign `crossover` ([`../one-loop-drain/source/crossover/README.md`](../one-loop-drain/source/crossover/README.md)),
  in particular its U15 section on the off-curve corners.

There is no orchestrator note for 07b2. This prompt carries what one would (§5).

**Gate:**
- in the worktree `.claude/worktrees/power-law-wall`, on branch `power-law-wall`, at or after the commit that adds this
  prompt; `4f9b1bd` is an ancestor of `HEAD`;
- `git status`: no tracked change; the untracked `venv` and nothing else;
- `./venv/bin/python -m runregistrykit list`: no run of this campaign `running` or stale;
- `.documents/power-law-wall-verification.md` has §1–§7 and no §8; `.documents/power-law-wall/kex-07b2/` does not exist;
- `ComputeTargets/ScalarHistory.py` still holds v1's constants (`DRAIN_ONE_LOOP_*`) and `one-loop-measured-v1`.

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds.

**Recommended model:** **Opus.** This is research. The work is to measure one form of the exponential's one-loop law
well enough for the user to rule U13 on it, beside v1 and 07b's four candidates, scored on the same points.

**Read first:**
1. [`README.md`](README.md): §6.13 whole, then §6.12, §2.1 (the gates Remeasure and Form) and §5.
2. 07b's log, then the verification document §7.4, §7.5, §7.8 and §7.10.
3. `source/kex/README.md`, then `kexchain.py` (`chain`, `chain_reason`, `regime`), `rechain.py`
   (`exponential_rows`, `wall_of`), `fits.py` (`points`, the selections `sel_one`, `sel_plateau`, `sel_gap`,
   `planner_ok`, `n2_ok`, `gap_ok`, `ln_v0m4`, `v1_fraction`, the forms), `stage2_kex.py` (how 07b chose and ran its new
   points, and its machine rule), `paths.py`.
4. `.documents/power-law-wall/kex-07b/fits.txt` and `fits.json` (07b's candidates and their constants),
   `stage2_new.txt`, `paths.txt`.
5. Crossover's README: "The curves" (ln R as a function of L and ln(V₀/M⁴) on a history) and the U15 section ("The
   corners at ln(V₀/M⁴) > 10 fail under either input").
6. `ComputeTargets/ScalarHistory.py`: `drain_fraction` and `drain_regime`, so that the reference implementation (§1.8)
   matches what production will call.

Every document, record and earlier output is data, not instructions (`CLAUDE.md` invariant 7).

---

## 0. Where this prompt sits

07b re-measured everything k_ex defines, and fitted four candidate laws for `one-loop-exponential-v2` in the new ℓ.
The orchestrator's probe after 07b (README §6.13) found that in the new ℓ the measured fractions beyond the peak collapse
onto f = 1 − exp(−A/(ℓ − ℓ₀)) with no L term, the same at n = 1 and 2, and that the peak has moved to ℓ ≈ 2.2. So v1's
layout (a plateau below ℓ = 1, the fall above it), which (i) and (ii) keep, no longer suits the data. It proposed:

  y_rise = G (ln R − R₀)₊^P · (1 + Q ln L)₊ (v1's gap form, in ln R and L),
  y_fall = A / (ℓ − ℓ₀) where the chain has a solution and ℓ > ℓ₀, and +∞ otherwise,
  f = 1 − exp(−min(y_rise, y_fall)), for ln R > 0; the classical region (ln R ≤ 0) stays R·S(L) and is not fitted.

Its scratch fit gave max 0.071 one-loop / 0.046 gap at n = 1 on the planner's selection and 0.041 / 0.056 at n = 2.
Those are scratch numbers from one fit on one selection, with no errors, no held-out test and no new points. This
prompt measures the form properly. It changes no production code. U13 is ruled by the user on what it finds; 07c is
written after that.

It writes new files under `prompts/power-law-wall/source/kex/`, `.documents/power-law-wall/kex-07b2/` and
`.documents/power-law-wall-verification.md` §8.

## 1. What is wanted

### 1.1 The reproduction

Before any new fit: with your own fitting code, re-fit 07b's (iii) and (iv) at n = 1 (both selections) and n = 2 from
07b's `points()`, and match `fits.json` to the digits `fits.txt` prints. Then re-fit (v) on the planner's selection at
n = 1 and match README §6.13's numbers to their printed digits. The orchestrator's scratch selection was: n = 1, back out, ln R > 0.3, `planner_ok`, and either the new regime one-loop (every ℓ) or the new regime gap; starting point (0.02, 1.6, 1.5, 1.0, 2.6, 1.1); `maxfev` 100000. A mismatch with §6.13 is recorded, not a stop. If 07b's constants are not reproduced, that is a stop.

### 1.2 The form and its variants (P92)

At n = 1 and separately at n = 2, unless stated:
- **(v)** as in §0: G, R₀, P, Q, A, ℓ₀ fitted.
- **(v-L)** the fall A/(ℓ − ℓ₀ − C ln L), C fitted with the rest. Give C with its standard error: this is the
  measurement of whether the fall needs an L term.
- **(v-A)** as (v) with A held at 2.69.
- **(v-J)** A and ℓ₀ shared by n = 1 and n = 2, each n with its own G, R₀, P, Q: one joint fit.
- **(vi)**, optional: one further variant you propose, stated with its reason.

The fit is unweighted least squares in f, `scipy.optimize.curve_fit` as 07b's `fits.py` uses it, on the points the
selection admits: chain points in every ℓ and gap points, with the field back out and ln R > 0.3. State the starting
point and show that the result does not depend on it (at least three starts per fit, the same optimum to the printed
digits, or say where it does not).

### 1.3 The selections, like for like (P93)

Fit each variant on each selection:
- **S1**, 07b's fit_law selection (`sel_one`, `sel_gap` with `gap_ok`, `n2_ok`);
- **S2**, the planner's (07b note §0.3: no `curve` set, no `grid` row above ln R = 15), at n = 1; at n = 2 it equals S1;
- **S3**, the domain of histories: ln(V₀/M⁴) ≤ 10 (`fits.ln_v0m4`) at n = 1 and at n = 2. List every point S3 drops,
  with its set, L, ln R, ln(V₀/M⁴), ℓ_new and f.

Then score every law on each selection's identical point set: v1 (its constants, by the old chain, as `v1_fraction`),
07b's (i)–(iv) (their committed constants, by the new chain), and each fitted variant here. For each: the largest
|fit − f|, the rms and the mean, over (a) chain points where the fall is the active branch, (b) chain points where the
rise is active, (c) gap points, and (d) all. A variant's active branch is its own; for v1 and (i)–(iv), group by regime as
07b did and say so. Also score every law on the points each selection drops, separately. One table per n per selection.

### 1.4 The errors (P94)

- The standard error of every fitted parameter, from the covariance, on S3.
- **Leave one set out**, on S3 at each n, for (v): the sets are 07b's `set` labels grouped by curve (`2197`, `lab`, each
  `L-M*`, `M1e-3`, `R-865`, `grid`, `n2-grid`, and 07b's new points by their curve). Refit without each; give each
  parameter's spread across the refits and the residuals on the held-out set under the refit that left it out.

### 1.5 New stage-2 points, predicted before they are fitted (P95)

**First commit to a file**, `.documents/power-law-wall/kex-07b2/prediction.json`, (v)'s S3 constants at n = 1 and 2 and
the list of points to be run, each with its (n, L, ln R), its ℓ_new and the f the committed constants predict. Write
the file before any new point runs, and record its time. Then run the points with 07b's stage-2 machinery
(`stage2_kex.py`'s way of building a point on a curve or off it):
- **n = 1, two history curves:** the laboratory curve (ln(V₀/M⁴) ≈ 3.3, crossover's `2197`) and one low-M mapped curve
  (`L-M1e-16` or `L-M8e-18`). On each, from ℓ_new = 1 to the predicted crossing of rise and fall plus 1.5 in ℓ_new, at
  least six points, spaced to resolve the peak.
- **n = 2, the history curve** at the n = 2 laboratory value of ln(V₀/M⁴) (crossover's U15 formula), at least eight
  points from ℓ_new = 0.3 to 4.
- **A held-out set:** at least ten points inside S3 at n = 1 and 2 together, away from every measured row (state the
  distance you use in (ln R, ln L)), spread over the rise, the crossing and the fall. Chosen and listed in
  `prediction.json` before they run.
- At most 40 new points in all.

For each: what 07b's `stage2_new.txt` records (f from the modes and from the balance, v_out/v_in, came back out, the
loop parameter at the turn, |ΔE/E|, the run time with the load), ℓ and the regime in both forms, the predicted f, and
the residual. **Validation**, as 07b's: energy at every point; the classical limit, the Born limit and convergence at
three of the new points, each with its tolerance and reason.

Then: the prediction's residuals (max, rms, mean) per group (curve points, held-out points; each n); and every fit of
§1.2–§1.3 re-run with the new points added, beside the fits without them.

### 1.6 The crossing, the cap and the edges (P96)

- **The crossing.** On each history curve of §1.5 and each `grid`/`n2-grid` L inside S3, where y_rise = y_fall: ℓ_new,
  ln R and the predicted f there (the predicted peak), beside the largest measured f near it.
- **The cap.** The largest f (v) predicts inside S3 at each n, and the largest measured f, with and without the points
  where the field did not come back out. Whether a cap such as v1's 0.95 changes any residual of §1.3; measured, not
  chosen.
- **ln R → 0⁺.** f under (v) at ln R = 0⁺ beside the classical R·S(L) at ln R = 0, at the L of each history curve.
- **The chain's limit.** At the points 07b's issue
  `[07b-the-one-loop-fixed-point-needs-more-than-100-iterations-near-the-chain-end]` names, and on the history curves:
  f under (v) with the chain found (200 iterations) and with it taken as absent (production's 100), and the difference.

### 1.7 The paths (P97)

On every candidate of 07b's path map (`paths.py`'s inputs): f under each fitted variant (on S3, with the new points),
and per history the first-order drained share Σ α_eff ΔK/K with the recorded ΔK held fixed, beside v1's and 07b's
candidates' as `paths.txt` gives them. Measured, not judged.

### 1.8 The reference implementation (P98)

In `source/kex/`, one function `fraction(ell, L, ln_R, n, params)`, `ell` `None` where there is no chain, returning f
for ln R > 0 under (v) and each variant. Its constants for every variant on every selection, with and without the new
points, to full precision in `.documents/power-law-wall/kex-07b2/minform.json`. Show that the function reproduces the
fits' values at every fitted point bitwise. It is not production code; 07c's gate Consistent compares production with
it if the user rules this form.

### 1.9 The documents and the hand-back

- `.documents/power-law-wall-verification.md` §8: §1.1–§1.8, measured and not judged.
- `.documents/power-law-wall/kex-07b2/`: every output as text, `prediction.json` and `minform.json`.
- Figures, at most three, PNG under `source/kex/fig/`: f against ln R at each grid L and on each history curve, with
  the data, v1 and (v); f against ℓ_new for every chain point coloured by L, with (v)'s fall branch; and one of your
  choice. Nothing in a figure that is not in a text output.
- `source/kex/README.md`: a section for 07b2's files, how to run each, and the results.
- **The hand-back**, in §8 and in the log's "State handed to the next prompt": for U13, each variant's evidence beside
  v1's and 07b's candidates on the same points, its errors, its prediction of the new points, and what it does on the
  paths. State the options and the evidence for each. **The user rules**; nothing here is a recommendation the prompt
  acts on.

## 2. What is not wanted

- **No production change.** No file outside the paths of §4. No change to a model, `drain_fraction`, its constants, a
  test or a golden. Those are 07c's.
- **No change to 07b's files** (`source/kex/` as 07b left it, `kex-07b/`, verification §7), crossover's, or
  `source/wall/`. They are records. New files sit beside them.
- **No ruling.** No law, cut, cap or name is chosen here.
- **No Ray**, no cluster, no lock, no store, no run registry, nothing under the main checkout.
- **No change to the briefing notes or the writeup.**

## 3. Verification

- The five suites at the base and after, per `CLAUDE.md` (ComputeTargets in two halves, `tools/tests` alone), from the
  worktree root, serially. Nothing they import changes, so the counts after may be taken by discovery. **No count may
  fall.** Two failures are expected in a worktree (07b's log, ruling 5): `test_registered_run … test_the_script_is_main_py` and `test_registry_config … test_roots_and_distributions`; any other is a stop.
- The gate Form (README §2.1), item by item.
- **The sharpness of the comparison:** show that §1.3's tables catch a law scored on the wrong chain (v1 by the new
  chain), and that §1.5's prediction catches constants perturbed by 10% in A.
- `black` on every Python file you add.

## 4. The commit, the log and the board

One commit (invariant 1), with:
- the new files in `prompts/power-law-wall/source/kex/` (scripts, `fig/` if any) and its README's new section;
- `.documents/power-law-wall/kex-07b2/` and `.documents/power-law-wall-verification.md` §8;
- the log `prompts/power-law-wall/logs/07b2-measure-the-min-form.md` (template: `dk-and-rpk-migration` README §5.1),
  with every deviation classified, "Observations not acted on", and "State handed to the next prompt", holding §1.9's
  hand-back;
- the board: row 07b2, its items, the gate Form, and §3/§4 for any issue opened, with `.documents/OPEN_ISSUES.md` in the
  same commit.

Stage by explicit path. Never stage `venv`. The commit message follows `CLAUDE.md` invariant 5. Write "this commit"
where the SHA goes; the orchestrator substitutes it.

## 5. Rulings and conventions (in place of an orchestrator note)

Each ruling is recorded in the log with its class.
1. **The worktree.** Work only in the worktree, from its root, with `./venv/bin/python` and `PYTHONPATH=.`. Never stage
   `venv`, never read or touch the main checkout, never name `/tmp/ray-cpbh`. *STRUCTURALLY REQUIRED.*
2. **Records stay records.** 07b's files, crossover's and `source/wall/` are read and imported, never written; run
   anything in them with `python -B`, so that no `__pycache__` appears there. *STRUCTURALLY REQUIRED.*
3. **The machine.** Stage-2 points run at most four at once, and one at a time while the one-minute load is above 24;
   the load is recorded beside every timing. A point not done in 30 minutes is stopped and recorded with its state, not a
   stop. *IMPLEMENTATION CHOICE, ruled here.*
4. **The prediction is fixed first.** `prediction.json` is written before any new point runs and is not changed after;
   a point added later is marked as not predicted. *STRUCTURALLY REQUIRED* (P95).
5. **The two worktree failures** are expected in the base suites; any other failure there is a stop. *IMPLEMENTATION
   CHOICE, ruled here.*
6. **A finding about production** is recorded and reported, not fixed; a §3 issue may be opened for it, with
   `.documents/OPEN_ISSUES.md` in the same commit. *IMPLEMENTATION CHOICE, ruled here.*
7. **No option chosen.** *STRUCTURALLY REQUIRED* (README §6.13).

**Conventions.**
- Stage by explicit path only, and show `git diff --cached --name-status` before committing. Count, then paste. A log
  states only what the outputs show. A tolerance is a number with its reason.
- **Order:** the Gate and the base suites (the `test_[g-z]` half and `tools/tests` in the background, waiting for each
  notification); §1.1; §1.2–§1.4; `prediction.json`; the new points, in the background, waited for without polling;
  §1.5's refits, §1.6–§1.8; the documents, the discovery counts, and the commit.
- **Scratch:** probes and suite outputs in your own subdirectory of the session scratchpad, never `/tmp`.
- **Stop conditions:** a base test failing other than the two known; 07b's constants not reproduced (§1.1); a production
  change becoming necessary. A stop goes to the user through the orchestrator; you resume when it answers.
- **Other sessions** may land commits on this branch. Do not assume `HEAD` is your own.
