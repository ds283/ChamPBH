# Prompt 04 — the close

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-02, after 03 landed (`b81eab8`, reviewed `2d50b95`) and after the user assigned
`[00-fixed-alpha-laboratory-M-histories-fail-in-classical-turning-point]` to this prompt (`b306c45`). It is
written from:
- README §6.3, and §6.4 (the handover as the user restated it on 2026-11-02, after `quanta-diagnostics` was
  re-planned and renamed `one-loop-drain`);
- 03's record: the log `logs/03-the-fix.md`, §7 and §8 ("State handed to the next prompt") in particular,
  and `.documents/reflection-restart-verification.md` §2;
- the board's §3.1, both entries;
- the `one-loop-drain` drain probe, read on its own branch (§1.2): `source/drain-probe_2026-11-02.md` §5 and
  `source/probe/drain/results/{2197,2188}-base-stored-alpha.{log,json}`;
- what the planner read from `var/datastores/science-2026.8.0` (read-only) for this prompt, set out in §0.2.

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Gate:**
- on branch `reflection-restart`, at or after the commit that adds this prompt, with no tracked change in
  `git status`; `b81eab8` is an ancestor of `HEAD`;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- `var/datastores/science-2026.8.0-shard0000.db` exists, and the store's 42 `log_m_eff_squared` failures
  and nine `classical_turning_point` failures are counted as §0.2 gives them;
- `git show one-loop-drain:prompts/one-loop-drain/source/probe/drain/results/2197-base-stored-alpha.log`
  prints the failure that §0.2 quotes;
- `.documents/reflection-restart-verification.md` has §1 and §2 and no §3.

If any of these fails, stop and ask.

**Closes:** `[run-2026.8.0-an-uncapped-outward-step-after-a-reflection-crosses-the-wall]` (P10). **Narrows:**
`[00-fixed-alpha-laboratory-M-histories-fail-in-classical-turning-point]`, with its cause. It closes here
only if the cause is shown and the user rules a fix within this campaign (§1.2, verdict (i)). **Opens:** only
what the work finds. **Closes the campaign.**

**Recommended model:** **Opus**. The recomputation is mechanical. The cause of the turning-point failure has
to be established by measurement on two trees, and the record must separate what 03 changed from what was
already there.

**Read first:**
1. [`README.md`](README.md): §0.1, §4, §5, §6.3, §6.4 and §7.
2. The log of 03, §7 and §8; the verification document §2, for the form of a section and for the scanner's
   records.
3. The board's §3.1, both entries; the issue's opening entry on the `inelastic-reflection` board (§3 there,
   `[run-2026.8.0-…]`), which is the record of the opening.
4. `tools/outward_step_scan.py`:
   - `run_history`, with its modes `tuned` and `store`;
   - `select_from_store` and `BLOCKS`;
   - the batch, `--timeout`, and the record of a failure (`outcome`, `failure_reason`, `scan`).
5. `ComputeTargets/ScalarHistory.py`:
   - `_restart_first_step` and `_outward_step_flagged`;
   - where the step loop calls the wall search (`classical_turning_point`, through the helper that returns
     `(log_E, turning point)`, near :1145–:1171), and what a raise there does to the history.
6. `CosmologyConcepts/Models/AbstractModel.py`:
   - `WALL_SEARCH_HALVINGS`, `WALL_ROUNDING_QUANTA` and the comment that derives K = 16;
   - `_log_V_minus_floor`, `_search_log_field` (its halvings, and the extra halvings while the outer end is
     −∞), and `classical_turning_point`;
   - the `inelastic-reflection` board's `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]`,
     closed by that campaign's 07b (`92f41f2`), which set K.
7. On the other branch, by `git show` only: `one-loop-drain:prompts/one-loop-drain/source/drain-probe_2026-11-02.md`
   §5; `…/source/probe/drain/drain_probe.py`; `…/results/2197-base-stored-alpha.log` and `2188-…`. Never
   check out `one-loop-drain`, and never touch its worktree (`.claude/worktrees/quanta-diagnostics`).
8. The run record `science-runs/2026-11-01-science-2026.8.0/README.md` ("Failed histories") and
   `fig6b/PLAN.md` §3; `.documents/review-remediation-verification.md` §4.18, for an addendum's form.

---

## 0. Where this prompt sits

### 0.1 What is left

03 made F3 and the guard the production code at `VERSION_LABEL` 2026.9.0. Its Fix gate held: the three
reproduced failures succeed, and the replay of 01's 66 F3 tasks is bit for bit. Three things remain:
- **the 42** `log_m_eff_squared` failures of the 2026.8.0 run, recomputed with the fix as a measurement. No
  store is written (README §3, §6.3);
- **the fixed-α turning-point failures** of 2197 and 2188, assigned here by the user. On the fixed tree they
  fail at their stored normalization where the store holds them as successes. Did 03's F3 cause it?
- **the close:** the issue closed, the handover written, the campaign closed.

The handover has changed since README §6.3 was written. The rebuild of record now follows `one-loop-drain`
08, not `quanta-diagnostics` 07 (README §6.4). Nothing about the drain is this prompt's.

### 0.2 What the planner found, to be tested (read-only, 2026-11-02)

**The probe's failure.** On `2d50b95`, at the stored normalization −276.63072859401797 and the stored α =
10⁻³, the drain probe's runs of 2197 and 2188 fail after 117 s and 115 s:
- 2197 (β = 1.185): `classical_turning_point (… phi = 2.950090845531173e-07, log_E = -306.7166549210319): the
  searched quantity is not monotone along the search (a midpoint of the bracket [-42.28625968598544,
  -42.34875968598544] is not between its ends)`;
- 2188 (β = 1.06): the same, at φ = 2.647733963294608×10⁻⁷ and ln E = −306.6698578858307.

In both, the probes run −∞ … −∞, then a finite positive value, then halve towards the edge of the −∞ region.
They end on two equal finite values, 2.4206637264330197 in 2197 and 2.3738666912318536 in 2188. In the
store both are successes, with compute times of 624 s and 653 s.

**The store already holds this failure.** `var/datastores/science-2026.8.0` (built at `75208cd`, before
anything of this campaign's) has **nine** failures whose reason begins `classical_turning_point` and says
"not monotone". The run record lists them as undiagnosed ("Failed histories": A 3, A2 3, B 3). All nine have
NULL normalization, so they failed in the tuning:

| serial | M (eV) | β | α | ln E at the failure |
|---|---|---|---|---|
| 1001 | 9.741×10⁻⁴ | 0.75 | 0.03 | −304.32 |
| 766 | 9.741×10⁻⁴ | 0.9 | 0.02 | −306.30 |
| 403 | 9.741×10⁻⁴ | 0.5 | 0.1 | −308.85 |
| 1592 | 9.741×10⁻⁴ | 1.1775 | 0.001 | −308.61 |
| 2208 | 9.741×10⁻⁴ | 1.18 | 0.001 | −309.38 |
| 2199 | 9.741×10⁻⁴ | 1.1825 | 0.001 | −308.66 |
| 2735 | 2.435×10⁻⁶ | 0.75 | 0.03 | −305.92 |
| 2480 | 2.435×10⁻⁶ | 0.5 | 0.1 | −308.24 |
| 2386 | 2.435×10⁻³ | 0.5 | 0.15 | −306.82 |

All nine name the same normalization, −276.63072859401797, as 2197 and 2188 do. Three of them are 2197's
β neighbours at the same M and α.

**The planner's reading, not tested.** At laboratory M, ln V_floor is the normalization, about −276.63, and
one ulp of it is 5.68×10⁻¹⁴. The search's target is V − V_floor = E. That puts the root at ln V − ln V_floor ≈
E/V_floor = e^{ln E + 276.63}:
- ≈ 1.5 quanta in 2197 and 2188;
- 0.1–3.4 quanta in eight of the nine;
- ≈ 16.5 quanta in 1001.

07b treats every difference within K = 16 quanta as −∞. A root below about 17 quanta therefore lies inside
the −∞ region, and the search cannot find it. Its extra halvings, made while the outer end is −∞, close in on
the region's edge, where two probes round to the same quantum and the monotonicity check raises. (At 17
quanta the residual ln(V − V_floor) − ln E would be about −276.63 + ln(17 × 5.68×10⁻¹⁴) + 306.72 ≈ 2.43,
close to the repeated 2.4207 of 2197.) 07b's
derivation of K orders two finite probes 2⁻ᴴ e-folds apart, with H = `WALL_SEARCH_HALVINGS` = 4. In 2197 the
raise comes at a midpoint 2⁻⁵ e-folds from an end. If this reading holds, the limit predates this campaign.
F3, by changing the trajectory after every restart, would only change *which* histories meet a candidate
whose energy is below ln V's resolution. §1.2 tests this. It is not assumed.

## 1. What is wanted

### 1.1 The 42

Run exactly as log 03 §8 gives the route:
- from `select_from_store("var/datastores/science-2026.8.0", blocks=["C"])`, keep the specs whose stored
  `failure_reason` contains `log_m_eff_squared`. There must be 42: 31 at M = 10⁻¹², 10 at 10⁻¹⁵, 1 at 10⁻¹³.
  Another count is a stop;
- set `"mode": "tuned"`, drop `normalization`, `phi_ref_Mp` and `store`, and keep β, M, α, `phi_init_Mp`,
  `T_init_GeV`, `atol` and `rtol`;
- write the list to `var/reflection-restart/04/tasks-42.json`, and run it through the scanner's batch on this
  branch's tree, detached (`--jobs 6`, `--timeout 1800`), into `var/reflection-restart/04/the-42.jsonl`.

For each history, record `outcome`, `accepted_steps`, `restart_retakes`, the reflection count,
`scan.signature_kept` and `scan.signature_discarded`, and for a failure its `failure_reason`. Then:
- **a stop:** any `log_m_eff_squared` failure, or any kept signature step. Either would mean the fix does
  not hold;
- **recorded, not a stop:** any other failure, with its reason and its kind, matched against the run's
  failure kinds ("Failed histories"). A kind not already open as an issue is opened (§3 and the index);
- **recorded:** a history with `restart_retakes` > 0, which is the guard working, with its count.

The histories are tuned, as the run made them, since their stored normalization is NULL. Nothing here is
compared with the store but the inputs.

### 1.2 The fixed-α turning-point failures

The question the user put is this: does 03's F3 make 2197 and 2188 fail? Answer it by measurement, in four
steps, then give the verdict.

**(a) Before and after 03, at the stored normalization.** Build the two specs from
`select_from_store(…)`, in mode `store`, filtered to serials 2197 and 2188. Each carries `store` (shard,
serial), so a success is compared with the stored history. Use absolute shard paths. Run them through the
scanner on two trees:
- **`89a0760`** (`b81eab8^`), in a temporary detached worktree under the session scratchpad. Run it from
  that worktree's root, with `PYTHONPATH=.` and this checkout's `./venv/bin/python` given by absolute path.
  At that commit the scanner still has `--variant`, and a spec without one runs the production code as it
  was before 03 (01's None). The worktree is removed with `git worktree remove` before the commit;
- **this branch's tree**, whose computing code equals `b81eab8`'s.

Expected, and each a fact to record, not to assume:
- at `89a0760`, both succeed, with the store comparison bitwise. 01 found this for 54 stored successes, and
  this checks it again on two whose α is the run's smallest;
- on this tree, both fail with the probe's reason, at the same φ and ln E to every printed digit.

A different outcome on either tree is not a stop. It is recorded and changes the question. If both trees
fail, F3 is not the cause; find the first tree that fails, back to `75208cd` if need be (the store's own tree,
`science-runs/…/README.md`).

The probe's driver, `drain_probe.py`, is not run on either tree. After a success it reads `SM._PROBE_LOG`,
which exists only with its patch. The scanner gives the same integration (stored normalization, φ_ref, α,
the tool's grid). Where its records and the probe's differ, say how.

**(b) Which of 03's changes moves the trajectory.** On this tree, in a script of
`.documents/reflection-restart/close/` that patches only its own process, replace `_restart_first_step` with
a function that returns SciPy's own choice (as the review of 03 did, board check 5), and rerun both specs.
The guard is left live. If each is then bitwise equal to its `89a0760` run (outcome, samples' SHA-256,
`accepted_steps`, `RHS_evaluations`), and the guard discards nothing, F3 alone is the change. Otherwise say
what differs. The issue's impact statement says the histories at 4×10⁻³¹ have no uncapped restart, so the
guard is expected to fire in neither. The scan's `signature_discarded` says whether it did.

**(c) The failing call.** Take the failing candidate's φ and ln E from the failure, and the normalization, and:
- call the model's `classical_turning_point` with exactly those three, alone, in a script of `close/`. Show
  that it raises with the same message and probes, and how long it takes;
- compute where the root lies: ln V − ln V_floor at the target, in quanta of
  ulp(max(|ln V|, |ln V_floor|)), against `WALL_ROUNDING_QUANTA`. Do the same for the nine of §0.2, from
  their stored `failure_reason`s;
- from the `89a0760` run of each history (a success), take every assessed candidate and give the smallest
  margin in quanta that any of them reached. Do the same on this tree up to the failure. This says whether
  the history before 03 came near the limit and F3 crossed it, or whether F3 took the trajectory somewhere
  new;
- say whether §0.2's reading of 07b's derivation holds: the 2⁻⁵ midpoint, and the root inside the −∞
  region. If it does not, say what does.

**(d) The nine, on the fixed tree.** Run the nine of §0.2 tuned, as §1.1 runs the 42, in the same batch or
one of their own, with `--timeout 3600`: the three at α = 10⁻³ are the run's slowest. Record each outcome,
and for a failure its reason, φ and ln E. These failed before 03. Whether they now succeed or fail, and at
what margin, measures how F3 moves the histories that meet the limit. No outcome here is a stop.

**The verdict.** Exactly one of the following, stated with the evidence of (a)–(d):
- **(i) 03 is wrong.** F3 or the guard puts the history in a wrong state before the failing call: a kept
  signature step, a discarded step whose retake goes wrong, a restart with a first step that the code does
  not intend, or a candidate whose energy contradicts the preceding cascade. This is a defect of 03.
  **Stop and ask**, with the history, the step and the evidence. A fix is the user's to rule (a prompt 04b, or
  within this prompt after a ruling); it is not made unasked.
- **(ii) 03 moves a correct trajectory onto a limit that was already there.** The failing call raises on its
  own inputs at any tree, its root lies within the wall search's rounding width, and the store's nine show
  the same limit before 03. Then 03 is not the defect. The limit is the wall search's. Record it, narrow the
  issue (§1.3), and **propose** where its fix belongs, with reasons. Do not fix it: a change to the wall
  search or its constants is outside this campaign's scope (§2). The planner's candidate destination is
  `one-loop-drain` 01, which lands the drain law in the step loop on the merged tree and keeps the fixed α as
  a selectable law, under which these histories run. It already owns the related inside-wall stop of 3873.
  The user rules it.
- **(iii) Neither.** Stop and ask, with what was measured.

Bound the work in (c) at a few hours of agent time. Whatever is unsettled at that bound is recorded as open,
with what was measured.

### 1.3 The issues

- **`[run-2026.8.0-an-uncapped-outward-step-after-a-reflection-crosses-the-wall]` closes** on this board's §4,
  with a **Closed** line: the fix (03, `b81eab8`), the Fix gate, the 42 (§1.1), and the handover. On the
  `inelastic-reflection` board, which holds the issue's opening entry, add a dated **Closed** pointer line to
  that entry. Do not rewrite it (invariant 6). Follow the precedent of an issue assigned from one board and
  closed by another campaign (for example `[08-a-history-that-did-not-converge-is-never-retried]`, opened on
  the `jordan-normalization` board and closed by `tuning-followups` 01), and name the precedent in the log.
  Delete its row in `.documents/OPEN_ISSUES.md` §1.21, and correct the count (27 → 26 if nothing else
  changes) and **Last updated**.
- **`[00-fixed-alpha-laboratory-M-histories-fail-in-classical-turning-point]`:**
  - under verdict (ii), narrow it in place on §3.1 with a dated **Narrowed** line: the cause, the margins,
    the nine, and "**Next:** for the user; proposed: …". Its index row's hook follows, and the row stays in
    §1.21 until the user rules the destination. The orchestrator moves it at the review, or at the merge
    into `one-loop-drain`;
  - under (i), it waits on the stop.
  Its home board is on the other branch, and nothing is written there.
- **Anything else found** is opened on §3 and in the index, in the same commit, not fixed.

### 1.4 The handover

What it says, in each place below: the whole store is rebuilt by the user as the rebuild of record. That
happens after `one-loop-drain` 08, under its one-loop drain law at `VERSION_LABEL` 2026.10.0, with the fixed α
kept as a selectable law. **No store of record is built at 2026.9.0.** The 2026.8.0 store is superseded by
that rebuild, not by anything of this campaign's. The fix of 03 is in the tree the rebuild runs, once
`reflection-restart` is merged into `one-loop-drain` (that campaign's U1), which follows this prompt at the
user's word. The 42 were recomputed here as a measurement only, and are not stored. Say what the verdict of
§1.2 means for a fixed-α build at laboratory M.

Where it goes, each as a dated note, without rewriting what is there (invariant 6):
- `science-runs/2026-11-01-science-2026.8.0/README.md`: under "Failed histories", the 42 recomputed with
  the fix (outcomes) and the nine classical-turning-point failures diagnosed (§1.2's verdict, one or two
  lines). Under its status, the store superseded by the rebuild of record;
- `science-runs/2026-11-01-science-2026.8.0/fig6b/PLAN.md` §3: steps 1–2 as they now stand. The fix has
  landed and the 42 succeed or do not (§1.1). The 42 cannot be recomputed into the 2026.8.0 store, since
  lookups are version-keyed (orchestrator note 03, §0). The figure's store is therefore the rebuild of
  record's. State this as the plan's prerequisites; the figure's design is not this prompt's;
- `.documents/review-remediation-verification.md`: a handover addendum §4.19 for this campaign, in §4.18's
  form;
- `.documents/post-reflection-step-2026-11-02/README.md`: a dated pointer to the fix and the verification
  document (P9), nothing else;
- `.documents/reflection-restart-verification.md` §3: §1.1's table (42 rows), §1.2's measurements and verdict,
  and the Close gate item by item.

### 1.5 The Close gate (README §2.1)

1. **The 42 recomputed** and each outcome stated (§1.1), with no `log_m_eff_squared` failure and no kept
   signature step.
2. **The issue closed** (§1.3).
3. **The handover written** (§1.4).
4. **The fixed-α question answered** (§1.2): a verdict of (ii) recorded with its evidence, or a stop under
   (i) or (iii).

Then the campaign closes: the board's status becomes **CLOSED**, row 04 is landed, the Close gate is held,
and `prompts/INDEX.md`'s row is closed.

## 2. What is not wanted

- **No change to computing code or tests.** That covers `ComputeTargets/`, `CosmologyConcepts/`,
  `CosmologyModels/`, `ObjectFactories/`, `config/`, `tools/`, `RunDrivers/`, the drivers and the plot
  scripts, and in particular the wall search, `WALL_ROUNDING_QUANTA`, `WALL_SEARCH_HALVINGS`, F3 and the
  guard. A fix that turns out to be needed is a stop (§1.2 (i)) or a proposal (§1.2 (ii)).
- **No store is written**, not even a temporary one beyond what a test makes. The 2026.8.0 store is read
  only. No Ray, no cluster.
- **Nothing about the drain.** The one-loop law, α_eff, the gap prescription and the 3873 inside-wall stop
  are `one-loop-drain`'s (01 for 3873). The 42 run at their stored α.
- **No merge and no push.** The merge into `one-loop-drain` is the user's, after this prompt.
- Nothing on the `one-loop-drain` branch or in its worktree; no checkout of it.
- No change to `.documents/reflection-restart/measure/` or `fix/`, or to their `out/`.
- No figure.

## 3. Verification

- **The suites, once, before anything else**, per `CLAUDE.md` (ComputeTargets in two halves; tools alone),
  with counts, times and `uptime`. They must stand at 26, 514 (122 + 392), 130, 147 and 68, all passing; a
  count that differs is a stop. Since this prompt changes no code or test file (§2), they are not run again.
  The log says so, and the commit's `--name-status` shows it.
- **The runs.** Every batch is detached (`nohup`, pid recorded), checked once that it started, and waited on
  by a background command that ends with it, not by polling. Outputs go under `var/reflection-restart/04/`;
  probes and suite outputs go in the agent's own subdirectory of the session scratchpad; nothing goes in
  `/tmp`. The cost expected: the 42 take a few minutes at six workers (log 03 §8). The two histories take
  about 10 min each per tree at their stored normalization (624–653 s stored), and about 2 min each where
  they fail. The nine take up to an hour, the three at α = 10⁻³ dominating. No run outlives the prompt.
- **The worktree** of `89a0760` is listed by `git worktree list` while used and gone before the commit.
- **Reproducibility.** Every number in §3 of the verification document comes from a script in
  `.documents/reflection-restart/close/`, whose output is in `close/out/` (text only), or from a JSONL line
  under `var/reflection-restart/04/` that the document names.

## 4. The commit, the log and the board

One commit (invariant 1), with:
- `.documents/reflection-restart/close/` and its `out/`; the verification document §3, added, not over §1–§2;
- the handover notes of §1.4, in their five files;
- the `inelastic-reflection` board's Closed pointer line (§1.3);
- the log `logs/04-close.md` (template: `dk-and-rpk-migration` README §5.1), with:
  - every deviation classified;
  - "Observations not acted on";
  - "State handed to the next prompt", which here is the state handed to `one-loop-drain`. That covers what
    its 01 inherits on the merged tree: the labels, the scanner, the verdict of §1.2 and the proposed
    destination, the 3873 stop untouched, and the open issues;
- the board: row 04 landed, its items, the Close gate, §3.1/§4 as §1.3 says, the status **CLOSED**, and
  **Last updated**; `.documents/OPEN_ISSUES.md` in the same commit; `prompts/INDEX.md`'s row.

`CLAUDE.md` is not changed: the test counts do not move.

Stage by explicit path, and show `git diff --cached --name-status` before committing. The commit message
follows `CLAUDE.md` invariant 5. Write "this commit" where the SHA goes; the orchestrator substitutes it.
