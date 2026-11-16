# Orchestrator — prompt 05, the store

**Written** 2026-11-16 with the prompt, at the user's request: "I'm not going to chase this now. Note it and move on.
Please write 05 and its orchestration note." The first sentence is R11 (README §6.8). Not dispatched. P33–P44 (README
§6.9) are proposed with it and stand unless the user rules otherwise; none needs a ruling first. It is dispatched at the
user's word.

Read [`../README.md`](../README.md) first: §2, §4, §5, and §6 as ruled (R1–R11, U1–U9) and as proposed (§6.9, P33–P44).
Then read the prompt. This note binds 05 only. The conventions of earlier notes bind here as well: a tolerance is a
number with its reason, timing is taken under load, and the suites run serially.

**You do not write code.** You may:
- run the suites and the scripts the log names;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`05-the-store.md`](../05-the-store.md) · **Gate:** "Store" · **Closes:** nothing · **Opens:** only
what the work finds · **Model:** Opus · **Network:** none. Nothing is pushed.

**Gate**, as measured by the planner on 2026-11-16 at 01:22 local, before this note and the prompt were committed:
- **The worktree.** `.claude/worktrees/power-law-wall` is on `power-law-wall` at `1233229` (R11), on top of `5314d8a`
  (review of 04) and `05f6920` (04). `git status --porcelain` shows `?? venv`, plus this note, the prompt and README
  §6.9 before their commit.
- **The main checkout** is on `main` at `9df9e13`, clean. Nothing of this campaign has touched it.
- **The registry.** `runregistrykit list`: 62 runs, all finished, none `running` or stale, none of this campaign.
- **The rehearsal lock** is free.
- **The code.** `FINGERPRINT_FORMAT = 7`; `VERSION_LABEL = "2026.11.0"`; `ObjectFactories/InversePowerModel.py` does
  not exist; `tools/compare_stores.py` declares id 2 in `UNSTORED_TYPE_VALUES["ScalarHistory"]["model_type"]` (01's D1).
- **Load and disk.** Load averages 3.24 / 5.45 / 6.71; 119 GB free.
- **The suites.** Not re-run by the planner. At `7d7c578` 04's agent measured 26, 732 (219 + 513), 163, 302, 76, with
  only the two worktree failures of note 01's ruling 7; 04 and everything since changed nothing the suites import.

## 0. What the planner found before writing 05

Three serial probes at `1233229`, run from the worktree root on 2026-11-16 at about 01:20 (load 3.6 at the start). Each
patches the registry in-process to what 05 would commit (a copy of `sqla_ExponentialModel_factory` registered as
`InversePowerModel` immediately after `ExponentialModel`, batch size 50, sharded by `shard_key`, and type id 2 added to
`ScalarHistory`'s `model` parent) and changes nothing in the tree. The scripts are in this session's scratchpad, not
committed:
`/private/tmp/claude-35086/-Users-ds283--claude-spares-spare-klLTE2/4bb6edbc-aba8-4706-b381-9a438e8ce86c/scratchpad/plan05/`
(`probe_fp.py`, `probe_hist.py`, `probe_old.py`). That directory may not survive the session; the results below are the
record.

- **The spec digests** (`spec_digests` of `test_fingerprint_format_guard`, `probe_fp.py`): 17 pairs and 14 distinct
  digests, against format 7's 16 and 14. `InversePowerModel`'s digest (`5bb13de4…`) equals `ExponentialModel`'s, so
  the distinct count does not rise. `ScalarHistory`'s moved (its `model` parent's types). The other fifteen pairs equal
  format 7's.
- **The golden store at format 8** (`build_store` unchanged, so exponential rows only), against
  `store_fingerprint_one-loop-drain-03.json`. Exactly three differences:
  - `fingerprint_format` 7 → 8;
  - a new class `InversePowerModel` with count 0, the empty digest `e3b0c442…` and no tag sets;
  - the top-level `digest`.

  `ScalarHistory`'s class entry does not move: the spec's digest is not part of the content. This is P13's "the format
  and the new class's empty entry only", and the gate Store's "unchanged apart from the format".
- **Serials collide** (`probe_fp.py`). In one store the first `ExponentialModel` row and the first `InversePowerModel`
  row both take serial 1. A join on `model_serial` alone (`tools/bbn_from_store.py:232` and `:253`,
  `tools/outward_step_scan.py:865`) would pair a power-law history with the exponential row (P37).
- **The inventory** (`probe_hist.py`), on a store with one history of each model:
  - with the types map patched: "problems: none", with one `ExponentialModel`, one `InversePowerModel` and two
    `ScalarHistory` rows;
  - with the class registered but the map not patched: "unresolved-parent: ScalarHistory: 1 row(s) … reference a parent
    that cannot be resolved (field(s) model)", and only one history counted.
- **A format-7 store under format 8** (`probe_old.py`): the golden store built by today's registry, then read with the
  patched one:
  - `read_inventory` and the fingerprinter raise `StoreSchemaMismatch` (the shard "differs from the tables this code
    declares");
  - reopening it with the actor raises `RuntimeError: Mismatch between sharded tables supplied to the constructor and
    read from the existing ShardedPool`.

  So the tip cannot read a 2026.11.0 store, as U6 said (P41).
- **The code survey.**
  - The raw SQL joins of a model table in production code are the three above. Everything else that names
    `ExponentialModel` outside the model package is 06's (`main.py`, `plot_inputs.py`, the plot scripts' docstrings,
    `config/argument_parser.py`'s help, `pipeline_selection.py`'s docstring) or a record under `.documents/` and
    `science-runs/`.
  - `bbn_from_store` matches M in Python (`abs(M_eV/Mp_eV/M_Mp − 1) < 1e-3`) and β and φ in SQL, and does not filter n.
    `--n-values` defaults to [1], and the exponential's science stores hold n = 1 only.
  - `ComputeTargets/ScalarHistory.py`'s `ScalarHistory(DatastoreObject).__init__` takes `model` and `alpha` (an
    `alpha_value`). The guard comparing the payload's drain model with the key's is in the compute path ("the guard
    (one-loop-drain prompt 03 §1.2 …)"), after the integration.
  - Tests that pin today's label or maps: `test_dissipation_store` (10, `VERSION_LABEL`), `test_fingerprint_format_guard`,
    `test_store_fingerprint_golden`, `test_compare_stores` 13 and 13b (13b reads `type_id` by `ast` from
    `CosmologyConcepts.Models`, which exports `InversePowerModel`). `test_plot_inputs`, `test_alpha_plot_inputs`,
    `test_a_values_plot_inputs` and `test_surfing_scan` import `VERSION_LABEL` rather than pinning it, and
    `test_version_keyed_lookups` uses labels of its own (2026.3.0, 2026.4.0), so none of these moves with the label.
- **The cost.** The two real round trips cost about one tuned history each: 04 measured 11–32 s with BBN at laboratory
  M, so without BBN somewhat less. The rest is constructed rows.

**Rulings.** Each is recorded in the log, with its class.
1. **The worktree and the main checkout.** As note 01's ruling 1: the agent works only in the worktree, from its root,
   with `./venv/bin/python` and `PYTHONPATH=.`. It never stages `venv`, never reads or touches the main checkout, and
   never names `/tmp/ray-cpbh`. *STRUCTURALLY REQUIRED.*
2. **The exponential does not move.** A change to `ObjectFactories/ExponentialModel.py`, to the exponential's spec, to
   the golden store's rows, or to any expectation of the goldens (`test_golden_history`,
   `test_golden_reflecting_history`, `test_tuning`) is a stop. So is a difference in the exponential's tool route against
   04's file, timings masked. *STRUCTURALLY REQUIRED* (README §5, P2).
3. **The golden's diff.** The new golden file must differ from format 7's in exactly the three ways of §0. Any other
   difference is a stop. *IMPLEMENTATION CHOICE, ruled here.*
4. **The entry check.** If an existing caller or test constructs a `ScalarHistory` with a key the check of prompt §1.4
   refuses, stop and ask: do not change the caller. *IMPLEMENTATION CHOICE, ruled here.*
5. **Existing tests** are changed only for the label, the format, the new class and the map. Each change is listed with
   its old and new value. A change for any other reason is a stop. *IMPLEMENTATION CHOICE, ruled here.*
6. **Other joins.** Any further raw join of a model table found in production code (not `.documents/`, `science-runs/`
   or `prompts/`) is fixed as P37 says and listed. One in a file outside prompt §5's list is a stop. *IMPLEMENTATION
   CHOICE, ruled here.*
7. **The two worktree failures** of note 01's §0.1 are expected in the suites at the base and after. Any other failure is
   a stop. *IMPLEMENTATION CHOICE, ruled here.*
8. **The machine.** The suites run serially; the load is recorded beside every timing. *IMPLEMENTATION CHOICE, ruled
   here.*
9. **A finding about production** outside the prompt's scope is recorded and reported, not fixed. The agent may open a
   §3 issue for it, with `.documents/OPEN_ISSUES.md` in the same commit. *IMPLEMENTATION CHOICE, ruled here.*
10. **No file but the prompt's §5 list.** *STRUCTURALLY REQUIRED.*

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before committing.
  Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit", and the orchestrator substitutes it.
- **Order.**
  1. The Gate, and the five suites on the base: serially, ComputeTargets in its two halves, the `test_[g-z]` half and
     `tools/tests` in the background, waiting for each notification.
  2. The factory, the registry, the sharding and the map; the format, the digests and the golden, with the diff of
     prompt §1.6.
  3. The entry check; the store tools.
  4. The tests, then the exponential's tool route against 04's file.
  5. The suites after, the label paragraph, `CLAUDE.md`'s counts, the documents, and the commit.
- **Scratch.** Probes and suite outputs go in the agent's own subdirectory of the session scratchpad. Nothing goes in
  `/tmp`.
- **No Ray.** Nothing starts a cluster, and no `ray` command is run. No store but a test's temporary one is opened, and
  nothing is registered.
- **Stop conditions are real:**
  - a test failing at the base other than the two known;
  - ruling 2, 3, 4, 5 or 6;
  - DatastoreKit's interface blocking the table or the map;
  - a production change outside prompt §5's files becoming necessary.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

1. **The Gate above**, on the commit that adds this note, or a later one.
2. **`runregistrykit list`**, the rehearsal lock (`./venv/bin/python -m raypoolkit.rehearsal_lock status`), and the load
   average. A cluster or lock of another session is left alone; 05 needs neither.
3. **The probes.** If the planner's scratch directory still exists, copy its three scripts into a dispatch directory of
   the session scratchpad, read-only. Nothing is copied from the main checkout.

## 2. Dispatch

Start one fresh-context subagent, on **Opus**, in the worktree, in the background. Give it:
- the prompt, the README and the board;
- `HEAD`, and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad;
- the probe scripts of §1 item 3, if they survived, as data (they are not to be committed).

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the files of ruling 10. Its log is `logs/05-the-store.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **Stop and ask** on any of the conventions' stop conditions.
- **At the end it replies with:**
  - the log's path and the message as committed; `git show --stat` of its commit;
  - the suites at the base and after, with counts, times and `uptime`;
  - the Store gate item by item (prompt §2), each with its command and output;
  - the spec digests (pairs, distinct, which moved) and the golden's diff;
  - the entry check: where it is, what it refuses, and whether any caller met it;
  - every raw join changed, with its file and line;
  - every existing test changed, with its old and new value;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — six checks

1. **Scope.**
   - `git show --stat <agent>` lists only ruling 10's paths, and `venv` is not in it.
   - `git diff <base> <agent> -- ObjectFactories/ExponentialModel.py` is empty, and so is the diff of the golden
     store's `build_store`.
   - In `ComputeTargets/ScalarHistory.py` the diff is the entry check and its comment, nothing else.
   - The commit's parent is this note's commit, or a later one that is not the agent's.
   - The main checkout is untouched. Nothing is pushed.
2. **The exponential is unchanged.** Run, serially and timed:
   - `test_golden_history`, `test_golden_reflecting_history`, `test_tuning`, `test_tuned_history_round_trip`,
     `test_drain_store` and `test_dissipation_store`;
   - the exponential's tool route at laboratory M, β = 2, A = 1. It must equal 04's
     `sample-04/tool-route/exp-M4e-31-b2.0-oneloop1.txt` with timings masked.

   None of the three goldens' modules is edited.
3. **The fingerprint.**
   - Regenerate `spec_digests(config.datastore.factories)` and compare it with `SPEC_DIGESTS[8]`.
   - `git diff` shows formats 1–7's pins untouched.
   - Diff the two golden JSON files myself: exactly §0's three differences.
4. **The round trip.**
   - Run the new store tests.
   - In a scratch probe of my own (never committed), build a mixed store through the committed factories: the two
     models at the same (M, n, β) take the same serial, and each lookup finds only its own history.
   - Read the entry check: written through the model interface, no power-law expression.
   - Construct the four refused and the three accepted keys of prompt §1.7 myself.
5. **The store tools.**
   - `git grep` for raw joins of a model table outside `.documents/`, `science-runs/` and `prompts/`: every one filters
     `model_type`.
   - Run `bbn_from_store`'s and `outward_step_scan`'s tests, and the new mixed-store tests.
   - Check that an existing `bbn_from_store` command's header line is unchanged.
6. **The records.**
   - The log is in §5.1's form, every deviation classed, every changed test listed with its old and new value.
   - The board has row 05, its items, the gate Store and the baselines.
   - `CLAUDE.md` changes only in the counts and their date. Test discovery matches them.
   - The `config/version.py` paragraph says a 2026.11.0 store cannot be read by this tree.
   - `black --check` passes on every changed Python file.
   - Any issue opened is on §3 and in `.documents/OPEN_ISSUES.md`.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 05 landed, the gate Store held, and this note marked "used for 05";
- an *Orchestrator review of prompt 05* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

**Report to the user:**
- the agent's commit and the review commit;
- the gate Store, and the fingerprint's diff;
- the entry check, and the joins changed;
- every existing test changed;
- any observation that bears on 06 or on the science run;
- that 06 is written only when the user asks for it.
