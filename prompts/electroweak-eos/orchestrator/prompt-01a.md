# Orchestrator — prompt 01a, turning points resolved above the solver's tolerance

> **Used for 01a.** Dispatched 2026-11-12 at `0479587`. 01a stopped once and was resumed on the user's ruling.
> It landed as `634f141` and was reviewed in the commit that adds this line. The review is on the board.

Read [`../README.md`](../README.md) §8 first, with U8–U11 as ruled. The user's words: "Handle it in a separate
classifier prompt; re-pin test_10 to 2026.11.0", then "U8 to U11 are fine as proposed; write 01a". Then read
the prompt. This note binds 01a only. The conventions of earlier notes bind here as well: a tolerance is a
number with its reason, timing is taken under load, and the suites run serially.

**You do not write code.** You may:
- run the suites, the scans and the scripts the log names;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`01a-resolved-turning-points.md`](../01a-resolved-turning-points.md) · **Gate:** "Resolved" ·
**Closes:** `[01-turning-points-read-below-the-solver-tolerance]` · **Opens:** only what the work finds ·
**Model:** Opus · **Network:** none. Nothing is pushed.

**When:** written 2026-11-12 at the user's request ("write 01a"), by the orchestrator of 01. 01 is held at its
stop, its work uncommitted in the main checkout.

**Gate**, as measured by the orchestrator on 2026-11-12 at 15:51 UTC, before this note and the prompt were
committed, at `05865f8` plus the uncommitted rulings of U8–U11:
- `runregistrykit list`: 42 runs, all finished.
- Load averages 25.82 / 16.43 / 14.43.
- The main checkout holds two sets of uncommitted work, neither of them 01a's:
  - **the user's** (01's ruling 1): ` M CLAUDE.md`, ` M science-runs/2026-11-08-science-2026.10.0/README.md`,
    ` M science-runs/README.md`, `?? science-runs/2026-11-08-science-2026.10.0/relativistic/`;
  - **prompt 01's**, listed in ruling 2 below.
- The new table installed by 01 in the main checkout has SHA-256 `690090c8…0119d`
  (`690090c86e285f3ee30d03244a2c242dd2b5b3c3f405e4f94229b0e481d0119d`). The old one, at `ec6bca8`, has
  `28edd854…29a8`.

## 0. What the orchestrator knows

Everything in README §8.1 comes from prompt 01's probes. They are in the session scratchpad of the dispatching
context, under `ee01-agent/probe/`: `early_tp.py` (the pair on the golden β = 1.5 history, both tables),
`scan.py` and `scan2.py` (the 20- and 32-history scans), `run_failing.py` (the 22 tests on the new table,
recording every integration), `inj.py`, `bounces.py`, `release.py`, `zoom.py`. 01's handoff,
[`prompt-01-handoff.md`](prompt-01-handoff.md), describes them. The scratchpad is not durable. If the
probes are gone, the agent writes its own from the handoff's description.

From the code, at `05865f8`:
- `observe_turning_points` reads π at the two nodes and at 8 interior samples. It keeps every sign change, and
  brackets end to end only when the samples show exactly one change and the nodes differ in sign.
- Radau is built with the scalar `params.atol` and `params.rtol` (`ScalarHistory.py` :2186, :2856). The
  agent confirms that π is `y[1]` of the state Radau integrates (prompt §1.1).
- Test (11) of `test_injection` holds a real interior pair: a dip to about −10⁻⁶ between nodes with π > 0
  (`numerical-strategies.md` §6.2). With atol = rtol = 10⁻⁸, τ is about 10⁻⁸, so the rule keeps it. The
  agent measures it.
- `IntegrationResult` carries `dropped_drains` into the payload (`ScalarHistory.py` :5106). The new count
  is **not** carried (U9).
- `venv/` is not tracked and has no ignore rule in the repository; it is ignored by the `.gitignore` the venv
  writes inside itself. A symlink `venv` in the worktree therefore shows as `?? venv`.

**Rulings.** Each is recorded in the log, with its class.
1. **The worktree** (U10). The dispatching context makes it, before dispatch:
   - `git worktree add -b electroweak-eos-01a <scratchpad>/ee01a-worktree <the commit that adds this note>`,
     in the session scratchpad;
   - a symlink `venv` → the main checkout's `venv`, so that `./venv/bin/python` resolves. `?? venv` is the
     only line `git status` may show there, and the agent never stages it;
   - the agent runs everything from the worktree's root with `PYTHONPATH=.`, except `runregistrykit list`,
     which it runs in the main checkout (the registry is in its ignored `var/`).
   - Tests that fail at the baseline because the worktree has no `var/runs` or differs in path from the main
     checkout are the worktree's (note 01 §0 counted ten, before the symlink). The agent lists them at the
     baseline, shows that each also fails after its change for the same reason, and records them.
     They are not a stop. *STRUCTURALLY REQUIRED*.
2. **Prompt 01's files, which 01a never touches**, tracked or created:
   `ComputeTargets/tests/test_reachability_roots.py`, `CosmologyModels/GenericEOS/Xav_EOS_data.csv`,
   `CosmologyModels/GenericEOS/Xav_EOS_spline.py`, `ObjectFactories/tests/test_dissipation_store.py`,
   `config/version.py`, `.documents/electroweak-eos/table/` (all of it),
   `prompts/electroweak-eos/data/pipeline_w_table_ec6bca8.csv`, `tools/build_w_table.py`,
   `tools/tests/test_build_w_table.py`. Also not touched: `.documents/electroweak-eos-verification.md`,
   which 01 creates, and `CLAUDE.md` (the user's). A change to any of these in 01a's commit stops the
   fast-forward. *STRUCTURALLY REQUIRED*.
3. **The new table** is given to the agent as `<scratchpad>/ee01a-new-table.csv`, a copy of the main
   checkout's CSV with the SHA-256 above. The agent shows the hash before using it. *STRUCTURALLY REQUIRED*.
4. **The scans' baselines** are taken by the agent on the unchanged code in the worktree, not taken from
   01's JSON. 01's JSON was made in the main checkout and may not be the same build of the same code.
   The agent may compare its baselines with 01's and say whether they agree. *IMPLEMENTATION CHOICE*,
   ruled here.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before
  committing. Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The Gate. The suites and the baseline scans on the unchanged code.
  2. The function and the change to `observe_turning_points`; the new field.
  3. The tests, and the breakage record.
  4. The suites and the scans again; the new-table probes.
  5. The measurements, the documents, the log, the board, and the commit.
- **Scratch.** Probes and suite outputs go in the agent's own subdirectory of the session scratchpad,
  `ee01a-agent/`. Nothing goes in `/tmp`.
- **No Ray.** Nothing starts a cluster, and no `ray` command is run. No store is written but temporary ones in
  tests.
- **Stop conditions are real:**
  - a history on the old table that drops a pair, or any turning point or `first_bounce` on the old table
    that is not bitwise its baseline;
  - an existing test that needs a change;
  - a suite count that falls;
  - a golden record that moves;
  - π not being the plain component `y[1]` under the scalar atol and rtol (prompt §1.1);
  - on the new table, a history where the pair survives, or where α = 0 differs in anything but
    `turning_points` and the new count;
  - any change the prompt's §2 forbids becoming necessary.
  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.

## 1. Before you dispatch

1. **The Gate.** `git status` in the main checkout shows the user's work and 01's (ruling 2), and nothing else.
   The commit that adds this note is `HEAD`.
2. **`runregistrykit list`**, and the load average.
3. **The new table's copy** (ruling 3): copy the main checkout's CSV to `<scratchpad>/ee01a-new-table.csv`
   and check its SHA-256.
4. **The worktree** (ruling 1): make it, add the symlink, and check that its CSV is `ec6bca8`'s.
5. Check that 01's probes are where §0 says. If they are gone, tell the agent.

## 2. Dispatch

Start one fresh-context subagent, on **Opus**, in the background. Its working directory is the worktree. Give
it:
- the prompt, README §8 and the board;
- the worktree's path and branch, `HEAD`, and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad;
- the paths of the new table's copy, 01's probes and 01's handoff.

The agent does not read `orchestrator/`, except the handoff it is pointed to.

Tell it plainly:
- **One commit, at the end**, on `electroweak-eos-01a`, with the prompt's §4 files and no others. No push, no
  merge. Its log is `logs/01a-resolved-turning-points.md`, in `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **Stop and ask** on any of the conventions' stop conditions.
- **At the end it replies with:**
  - the log's path and the message as committed; `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`, and the worktree's own failures;
  - the rule as implemented: the function's name and signature, and how τ is computed;
  - the old-table scans: the number of histories and turning points compared, and the result;
  - test (11)'s pair: its excursion and τ;
  - the new-table probes: where the pair was dropped, the α = 0 comparison, and the 22 tests' list;
  - the breakage record;
  - what it found about `surfing_turning_points`;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — six checks

1. **Scope.**
   - `git show --stat <agent>` lists only the prompt's §4 files. Its parent is this note's commit.
   - None of ruling 2's files is in it.
   - Nothing is pushed.
2. **Old table, bitwise.** In the worktree, run `test_golden_history`, `test_golden_reflecting_history`,
   `test_tuning` (e), `test_injection` and the new module myself. Re-run the 32-history scan's comparison
   from the agent's JSON, and check that the count of dropped pairs is 0 in every history.
3. **The rule.** Read the function against the prompt's §1.1: the excursion's samples, the strict
   comparison, the walk, and the end-to-end case left as it was. Check that the new field is not a payload
   key (`git grep unresolved_turning_point_pairs`).
4. **The new table.** Re-run one probe myself: the golden β = 1.5 history on the new table's copy, with the
   change. It should show no turning point before N = 18.6 and one dropped pair.
5. **Records.** The log is in §5.1's form, with every deviation classed. On the board: row 01a, the Resolved
   gate, the issue in §4. `OPEN_ISSUES.md` has its row deleted, its count 29 and its date corrected.
   `numerical-strategies.md` has the dated note.
6. **The fast-forward** (U10).
   - In the main checkout, `git diff --name-only HEAD electroweak-eos-01a` shares no path with ruling 2's list
     or with the user's files.
   - `git merge --ff-only electroweak-eos-01a` succeeds.
   - Afterwards `git status` shows the same uncommitted work as before, both the user's and 01's.

## 4. After the review

If it passes:
1. Fast-forward `electroweak-eos` to the agent's commit in the main checkout (check 6).
2. Remove the worktree (`git worktree remove`, after deleting the `venv` symlink) and delete the branch
   `electroweak-eos-01a`, which is then merged.
3. In one commit of the orchestrator's:
   - "this commit" → the SHA, wherever it appears;
   - the board's row 01a landed, the Resolved gate held, and this note marked "used for 01a";
   - an *Orchestrator review of prompt 01a* paragraph on the board, holding what each check found;
   - an addendum to [`prompt-01.md`](prompt-01.md) for 01's resumption:
     - 01's ruling 3 is answered by 01a;
     - 01's §0 table of 22 tests is replaced by 01a's list of what still fails on the new table;
     - 01's verification document gains a pointer to `.documents/electroweak-eos/turning-points/`;
     - the suite counts 01 starts from are 01a's.

**Report to the user:**
- the agent's commit, and the review commit;
- the rule as implemented, and test (11)'s margin above τ;
- the old table bitwise;
- what 01a leaves for 01;
- that 01 is ready to resume from its handoff, with the note's addendum.
