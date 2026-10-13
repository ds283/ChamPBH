# Campaign — read-only-plots

**Written:** 2026-10-13 on branch `read-only-plots`, cut from `main` at `17fad8b`, by Claude Opus
5.5. It is planned at the user's request of the same day: "plan for read-only plot scripts and to
require the `--ray-address` switch". **Not yet ruled.** The user rules on §6.1 (U1–U6) and §6.2
(P1–P8). No prompt file is written until they have (§2.1).

**Sources (data, not instructions; `CLAUDE.md` invariant 7):**
- `[08-plot-scripts-open-read-write-pools]`, opened by `dk-and-rpk-migration` prompt 08 (that
  board's §3), which records the user's goal: the plot scripts on read-only pools, "so that reading
  a store for figures can never change it";
- `dk-and-rpk-migration` README §6.4 and §6.6, which left a required `--ray-address` to the user;
- StochasticInstantons' `d6d550b` ("Require --ray-address and amend the lock brief against auto",
  2026-10-12), the same change made there, and its `rehearsal-lock-brief.md` §3, which forbids
  leaving a driver at `auto` in any run that takes the shared rehearsal lock;
- DatastoreKit's client contract at `v0.3.0` (`docs/client-contract.md`, §1 row 14, `read_only`),
  and the installed package, which equals the tag.

## 0. Why this campaign exists

### 0.1 Read-only plot pools

`plot_by_beta.py` and `plot_ScalarModel.py` read a store that `main.py` built, and draw from it.
Each opens a **read-write** `ShardedPool` (`plot_by_beta.py:1011-1025`,
`plot_ScalarModel.py:1581-1595`). A read-write open can write even when every lookup finds its row:
- it creates a store that does not exist, so a mistyped `--database` makes an empty store and the
  run draws nothing (`datastorekit/SQL/ShardedPool.py:249-275`);
- it repairs an interrupted replicated write at open (`:297`);
- it writes the `version` row when the store lacks the label, after which every version-keyed
  lookup misses silently (`:376-382`).

And `object_get` is get-or-insert, so a lookup that misses inserts a row. The package can open a
pool read-only (`ShardedPool(read_only=True)`): every file `mode=ro`, nothing written, a lookup
that misses a replicated class refused with `ReadOnlyMiss`, any other insert with `ReadOnlyWrite`
(`datastorekit/SQL/Datastore.py:296-312`).

### 0.2 A required `--ray-address`

The three drivers take `--ray-address` with the default `auto` (`config/argument_parser.py:11`,
`:310-315`), and each calls `ray.init(address=args.ray_address)` at import (`main.py:124`,
`plot_by_beta.py:101`, `plot_ScalarModel.py:81`). Since `dk-and-rpk-migration`'s U5, the projects'
clusters on this machine are isolated, so `/tmp/ray/ray_current_cluster` no longer exists, and
`auto` attaches to the first Ray head it finds by scanning processes, possibly another project's.
`CLAUDE.md` ("Ray on this machine") already says to pass `--ray-address 127.0.0.1:6380` and never
`auto`; the parser does not enforce it. StochasticInstantons made the switch required on
2026-10-12; SecondaryGWKit assigned the same issue to its `handover` campaign.

### 0.3 Measured at `17fad8b` (2026-10-13), read-only

No store was opened, no driver run and no Ray started. A subagent read both plot scripts, their
helpers, every factory's `build`, and the package at the tag; the orchestrator checked the
load-bearing claims (`main.py:1186-1197`, `plot_by_beta.py:1100-1108`,
`datastorekit/SQL/Datastore.py:296-312`, `datastorekit/store_reader.py:196-200`).

**Every datastore call the plot scripts make.** Neither calls `object_store`, `object_validate`,
`object_get_vectorized` or `object_read_batch`. Every `RayWorkPool` they build has
`compute_handler=None`, so raypoolkit drops the store and persist handlers. `extract_common.py` makes
no datastore call.

| # | Lookup (both scripts unless noted) | Values from | `main.py` stores it | On a miss under `read_only=True` |
|---|---|---|---|---|
| 1–2 | `tolerance`, `--abs-tol`, `--rel-tol` | the options | `:1014-1015` | `ReadOnlyMiss` |
| 3 | `temperature`, `--T-init-GeV` | the option | `:921-923` | `ReadOnlyMiss` |
| 4–5 | `phi_value` (`--phi-init-Mp`), `pi_value` (0) | option, constant | `:926-933` | `ReadOnlyMiss` |
| 6 | the potential, **one lookup over every stored M × every stored Λ** (`--potential-type`; M and Λ from `read_table`) | the store | `:963-997`, over **that run's** `M_grid × Λ_grid` | `ReadOnlyMiss`, and one miss refuses the whole call |
| 7 | `ExponentialCoupling` (sharded), for every stored β | the store | `:999-1007`, for every β it stored | `ReadOnlyWrite` (an insert into a sharded class) |
| 8 | `QCD_Cosmology` (`config/model_list.py:24-27`) | constant | `:1186`, the same function | `ReadOnlyMiss` |
| 9 | `temperature`, `--T-stop-GeV` or the cosmology's `T_CMB` | option, constant | `:1190-1197`, identical code | `ReadOnlyMiss` |
| 10–16 | `ScalarModel`, `AdiabaticHistory`, `BBNData` | rows 1–9 | — | **never refused**: these three `build`s return an unavailable object on a miss and never call the inserter (`ScalarModel.py:390-406`, `AdiabaticHistory.py:184-188`, `BBNData.py:213-214`), with or without `_do_not_populate` |
| 17 | `read_table` of `beta_value`, `M_value`, `Lambda_value` | — | — | reads one shard; works read-only |

So **on a store built by `main.py` runs with the options the plot run is given, every lookup finds
its row**: the issue's guess that `T_CMB` might be missing is wrong (row 9). Two lookups can miss
on a store that was built correctly:
- **Row 6, the potentials.** The plot scripts take the product of *every* stored M with *every*
  stored Λ, but `main.py` stored the product of one run's grids. A store filled by several runs with
  different M and Λ grids lacks the cross terms. The science run's script fills one store in eight
  runs with different M (`full_run_2026.6.0.sh:28-46`), every one on the same default Λ grid (no
  Λ option is passed; `config/argument_parser.py:29-31`, `main.py:1142-1162`), so its product is
  complete. A store whose runs used different Λ grids, or different potential types, would not be. Today such a miss inserts a potential row
  for which no history can exist; read-only, it would refuse the whole plotting run.
- **Row 7, the couplings.** A `main.py` run interrupted between storing a β (`:1123`) and its
  coupling (`:1180`) leaves a β with no coupling, and so no history.

Rows 1–5, 8 and 9 miss only when the plot run is given different options from `main.py`'s, and
then every history lookup after them would miss too.

**What a read-only open refuses that today's open does not** (`ShardedPool.py`, `store_reader.py`
at `v0.3.0`): a missing store (`RuntimeError`, nothing created); a journal beside any file,
including a store `main.py` has open; a `replication_in_flight` record (`ReadOnlyWrite`; read-write
repairs it); replicated tables that differ between shards (`ReplicatedDivergence`); a store without
this `VERSION_LABEL` (`ReadOnlyMiss`, naming the labels it holds); `prune_unvalidated=True` or a
non-empty `drop_tables` (`ReadOnlyWrite`, before anything is opened). The plot scripts pass
`prune_unvalidated=False` and no `drop_tables` today, but they accept the shared parser's
`--prune-unvalidated` (default `True`) and `--drop`, and ignore both.

**The setup the two scripts share.** `plot_by_beta.py:1026-1135` and `plot_ScalarModel.py:1596-1694`
are the same code but for `plot_by_beta.py`'s Standard-Model baseline: rows 1–9 and 17, and the
two helpers `convert_to_potential` and `convert_to_coupling`. Neither script can be imported by a
test (each parses `sys.argv` and calls `ray.init` at import), so every existing test reads them by
`ast` or text, and no test exercises a plot lookup.

**Tests that read the plot scripts' text**, and would notice a change:
- `ComputeTargets/tests/test_extraction.py:339-368`: the names `plot_by_beta.py` calls, and exactly
  four `"_do_not_populate": True` literals in it;
- `ComputeTargets/tests/test_kinematic_cap_loop.py:413-424`: `"Radau+kinematic-cap-stepping0"` in
  both scripts;
- `ComputeTargets/tests/test_initial_field_option.py:79-105`: both scripts read `args.phi_init_Mp`;
- `ObjectFactories/tests/test_datastore_config.py:170-193`, `:253-293`: no `object_get_vectorized`
  in the plot scripts; exactly one `ShardedPool(` per driver, given `factories=` and
  `serial_batch_sizes=`, and no `drop_tables` in the plot scripts;
- `ObjectFactories/tests/test_package_provenance.py:317-345`: the provenance line before
  `ray.init` and before `ShardedPool(`.

**`--ray-address` today.**
- **The parser.** `config/argument_parser.py:11` defines `DEFAULT_RAY_ADDRESS = "auto"`, and every
  driver reads it through `create_argument_parser()`. A YAML config value satisfies a `configargparse`
  required argument. None of the three `*.yaml` configs sets one.
- **Who passes it already.** The rehearsal scripts pass it to every driver
  (`.documents/dk-and-rpk-migration/rehearsal/build.sh:99`, `run_readers.sh:111-122`), and so does
  `tools/tests/test_inventory_report.py:397`.
- **Who does not.** Three tests parse with `--database` alone:
  - `ComputeTargets/tests/test_extraction.py:331-336`;
  - `test_initial_field_option.py:115-118`;
  - `test_pipeline_selection.py:157-162`.

  A required switch breaks all three unless they give it.
- **Which runs start Ray.** `main.py --inventory` reads the closed store and exits before
  `ray.init` (`main.py:93-111`), so it never uses the address.
- **What Ray 2.53.0 accepts** (`ray.init`'s docstring): a GCS address such as `127.0.0.1:6380`;
  `"auto"`, which follows `ray_current_cluster` or scans; and `"local"`, which starts a private
  instance in the process.

**The suites at `17fad8b`:** CosmologyModels 18, ComputeTargets 114 (61 + 53), ObjectFactories 63,
tools 34 (`CLAUDE.md`, "Repository mechanics").

## 1. Scope

**In scope:**
- `--ray-address` required, with what it accepts (01);
- the plot scripts' shared store-reading setup moved into one importable module, with tests on a
  stand-in store (02);
- both plot scripts on read-only pools, with what each kind of miss does (02);
- a rehearsal of a real plotting run on a real store, read-only, which proves no file changes (03,
  if U5 is ruled so);
- the documents, and the close (04).

**Out of scope:**
- **What the code computes.** No stored or plotted value changes (§5 rule 9).
- **`main.py`'s own pool.** It writes by design.
- **The pre-existing lookup behaviours** the measurement found in passing, which hold in both modes:
  - with `tags=[]`, two validated rows for one model that differ only by tag raise
    `MultipleResultsFound`;
  - `AdiabaticHistory.build` and `BBNData.build` do not filter on `validated`
    (`dk-and-rpk-migration` log 09, Observation 3).

  Each is recorded, not fixed.
- **The science rebuild**, held by the user until `jordan-normalization` lands.
- **The user's run scripts**, `full_run_2026.6.0*.sh` (untracked): 04's handover lists what in
  them needs `--ray-address`, as `.documents/review-remediation-verification.md` §4.12 already does.
- **DatastoreKit, RayPoolKit, SecondaryGWKit, StochasticInstantons:** read, never edited. A package
  defect stops the prompt (`dk-and-rpk-migration` README §5 rule 11; `CLAUDE.md`, "The packages").

## 2. Prompts

| # | Prompt | Covers | Model |
|---|---|---|---|
| 01 | `01-require-the-ray-address.md` | The parser: no default; what it accepts (U1, U2); a message that names `127.0.0.1:6380` and `local`. The three tests that parse without it given a value (named, §5 rule 7). New tests of the rule. `CLAUDE.md`'s run lines | Sonnet |
| 02 | `02-read-only-plot-pools.md` | A new module, `plot_inputs.py` (P1), holding the shared setup (rows 1–9, 17) as functions of a pool. Both scripts call it and open `read_only=True`. A miss per U3 and U4; `--drop` refused (P3). Tests on stand-in stores, read-only. Closes `[08-plot-scripts-open-read-write-pools]` | Opus |
| 03 | `03-rehearse-a-read-only-plotting-run.md` | **If U5 is "rehearse".** One pilot build by `main.py` on ChamPBH's isolated cluster, then both plot scripts and `--inventory` on it. Every store file byte-identical before and after the readers; `histories.csv` equal to R0's | Opus |
| 04 | `04-close.md` | `architecture-summary.md` (the plot scripts, the parser); `CLAUDE.md`; the handover's lines for the run scripts; the index; the board closed | Sonnet |

### 2.1 When each prompt file is written

- **01–04** are written once the user has ruled on §6. 01 and 02 are independent of each other;
  03 needs both.

### 2.2 The charters

**01 — Require the Ray address** (Sonnet).
- **What it changes.**
  1. `config/argument_parser.py`: `--ray-address` has no default. With U1 as proposed, the parser
     leaves it `None`. `main.py` refuses a run that will start Ray without it, before `ray.init`,
     exit 2, after the `--inventory` branch. The plot scripts refuse it outright.
  2. With U2 as proposed, `auto` is refused by name in the same check.
  3. The message names both choices: ChamPBH's cluster (`127.0.0.1:6380`, started by
     `ray_cluster.sh`) and `local` (a private instance).
  4. The three tests of §0.3 add `--ray-address 127.0.0.1:6380` to their base arguments; nothing
     else in them changes.
  5. `CLAUDE.md`: the run lines and "Ray on this machine".
- **Acceptance.**
  1. New tests, by `ast` and by subprocess (as `test_inventory_report._main` does, at an address
     nothing listens on), with no Ray:
     - each driver refuses a run without the switch, before `ray.init`, exit 2, with the message;
     - each refuses `auto`;
     - `main.py --inventory` without the switch still reads a store (U1).
  2. A YAML value satisfies it (configargparse).
  3. The suites at §0.3's counts plus the new tests; `black` clean.
  4. Deliberate breakages: the default restored; the check moved after `ray.init`; `auto` accepted.

**02 — Read-only plot pools** (Opus).
- **What it changes.**
  1. **`plot_inputs.py`** (P1), with no argument parsing and no Ray at import. It holds the shared
     setup as functions taking `pool`, `args` and `units`: the tolerances, temperatures and field
     values; the cosmology list; the stored β, M and Λ by `read_table`; the potentials and
     couplings. Both scripts call it in place of their copies, so `plot_by_beta.py:1026-1135` and
     `plot_ScalarModel.py:1596-1694` shrink to calls. The baseline stays in `plot_by_beta.py`.
  2. **The potentials and couplings looked up one combination at a time** (P2), so that one miss
     cannot refuse the rest. A miss is handled per U3: by default the combination is skipped, with
     one warning line per skipped combination and a count.
  3. **A parameter row that misses** (rows 1–5, 8, 9) stops the run per U4, with a message naming
     the option and the value, and saying that `main.py` was not run with it.
  4. **Both pools `read_only=True`**, `prune_unvalidated=False`, no `drop_tables`. `--drop` is
     refused by the plot scripts before Ray starts (P3); `--prune-unvalidated` is documented as
     `main.py`'s.
  5. Everything else in the scripts is unchanged: the history lookups (rows 10–16), the figures,
     `histories.csv`.
- **Acceptance.**
  1. **Tests on stand-in stores**, built through the harness and opened read-only through
     `StoreFixtures/standin_pool.py` (its `open_pool` passes `read_only=` on):
     - a store `main.py`'s lookups would build: every `plot_inputs` call finds its row;
     - a store whose M × Λ product is incomplete: the missing combinations are skipped with a
       warning, the others returned (U3);
     - a missing `--T-stop-GeV` row: the run stops with the message (U4);
     - every file's SHA-256 unchanged after the calls;
     - a missing store refused, nothing created.
  2. The text tests of §0.3 still pass, or are updated only where 02 moves the text they read, each
     named with its reason (P6).
  3. `[08-plot-scripts-open-read-write-pools]` closes on the `dk-and-rpk-migration` board §4 (it is
     that board's), with the index row deleted.
  4. Deliberate breakages: `read_only=True` removed (the checksum test fails); the per-combination
     lookup reverted to one call (the incomplete-product test fails); a parameter miss swallowed.

**03 — Rehearse a read-only plotting run** (Opus; if U5 is "rehearse").
- **What it does.** Under the shared lock, on ChamPBH's isolated cluster, as
  `dk-and-rpk-migration` §4.1 but smaller:
  1. one `main.py` build of the pilot grid into a fresh store under
     `~/ScienceData/ChamPBH-rehearsals/read-only-plots/`, with `--ray-address 127.0.0.1:6380`;
  2. SHA-256 of all 17 files;
  3. `plot_by_beta.py --phi-init-Mp 5`, `plot_ScalarModel.py`, `main.py --inventory`;
  4. the SHA-256s again.
- **Acceptance.**
  1. Every file byte-identical before and after the readers.
  2. `histories.csv` equal to R0's (SHA-256 `d7c62c37…c522`), since nothing that computes has
     changed.
  3. A plot run with `--ray-address auto` refused before Ray starts.
  4. A plot run with a `--T-stop-GeV` the build did not use stops with U4's message, and changes no
     file.
- **Out.** No resume test: 02 changes no write path. The store is kept until the user removes it
  (§3).

**04 — Close** (Sonnet). `architecture-summary.md` (the plot scripts' pools, `plot_inputs.py`, the
parser); `CLAUDE.md` if 01 left anything; a dated addendum to
`.documents/review-remediation-verification.md` (§4.13) listing the run scripts' lines that now need
`--ray-address`; the board closed, the index and `prompts/INDEX.md`. Final suites.

## 3. Stores

None but 03's, and only if U5 is "rehearse": one pilot store,
`~/ScienceData/ChamPBH-rehearsals/read-only-plots/03-pilot.db` with 16 shards, outside the
repository. 01, 02 and 04 open no store but the tests' temporary ones. The science store, its
bbn-tolerance copy and the user's pilot store are not opened.

## 4. The interfaces between prompts

- **01 ships** the parser's rule and its message. 02's tests and 03's runs pass the switch.
- **02 ships** `plot_inputs.py` and the read-only pools. 03 runs the scripts as 02 left them.
- **03 ships** the rehearsal's checksums and outputs under `.documents/read-only-plots/out/`
  (small text only), and creates `.documents/read-only-plots-verification.md`.

## 5. The rules this campaign runs under

`CLAUDE.md`'s invariants 1–7 and "Repository mechanics", plus `dk-and-rpk-migration`'s rules 8
(deliberate breakage, recorded as diffs), 9 (no stored science value changes), 10 (no test needs
Ray, none opens a store outside a `tempfile` directory), 11 (a package defect is reported upstream
and never worked around here), 12 (no change in a sibling repository), 15 (no `ray stop`; a cluster
only through `ray_cluster.sh`) and 16 (the shared rehearsal lock: a held lock is a stop, never a
wait). And:
- **Existing tests are not modified**, except where a prompt names the test and the reason: 01's
  three base-argument lines, and 02's text tests (P6).
- **A rehearsal (03) is a one-off exception** to `CLAUDE.md`'s "no campaign prompt starts" a
  `main.py` run with a datastore, as `dk-and-rpk-migration`'s U2 was, and only if U5 is so ruled.

The log template is `dk-and-rpk-migration` README §5.1's.

## 6. Decisions

### 6.1 For the user to rule (U1–U6)

| | Proposed | Alternatives |
|---|---|---|
| **U1** — `--inventory` and the address | **Required whenever Ray starts.** `main.py --inventory` reads a closed store before `ray.init` and never uses an address, so it is exempt. The plot scripts and every other `main.py` run require it | Required always, as StochasticInstantons did (`configargparse` `required=True`, satisfied by a YAML value): simpler, but `--inventory` must be given an address it ignores |
| **U2** — the value `auto` | **Refused**, by name, with the message. `CLAUDE.md` and the shared lock brief already forbid it; with the clusters isolated, `auto` scans for any Ray head | Accepted when given explicitly, as StochasticInstantons' parser does (it removes the default only) |
| **U3** — a potential or coupling that is not stored (rows 6, 7) | **Skip that combination**, with one warning line each and a count. No history can exist for a combination whose potential or coupling was never stored, so nothing drawable is lost | Stop the run with a message. Or keep one vectorized lookup and let one miss refuse the run |
| **U4** — a parameter row that is not stored (rows 1–5, 8, 9) | **Stop**, before any figure, with a message naming the option and value, saying `main.py` was not run with it. Every history lookup after it would miss | Warn and draw nothing. (The user's rule "warn, don't filter" is about physical inputs to a computation, not a plot run whose options do not match its store) |
| **U5** — the proof | **Rehearse (03)**: one pilot build and the readers, with every file's checksum compared. It is the only proof that a real plotting run, under real Ray, changes nothing | Tests on stand-in stores only (02); 03 is dropped and the campaign is three prompts |
| **U6** — a store built under another `VERSION_LABEL` | **Accept the package's refusal** (`ReadOnlyMiss`, naming the labels the store holds). Today such a run writes a new version row and draws nothing | Fall back to read-write for such a store. Not proposed: it is the silent write this campaign removes |

### 6.2 Proposed by the planner (P1–P8)

Accepted as proposed unless the user rules otherwise.
- **P1 — `plot_inputs.py`**, a top-level module beside `pipeline_selection.py` and
  `extract_common.py`, which exist for the same reason: drivers that parse `sys.argv` at import
  cannot be tested.
- **P2 — one lookup per potential and coupling**, in place of the vectorized `payload_data` calls,
  so that U3 can skip one. The cost is more round-trips on a large store, through the same pool;
  02 measures it on a stand-in store and reports it.
- **P3 — the plot scripts refuse `--drop`** (exit 2, before Ray), since a read-only pool would
  refuse it later with a less useful message. `--prune-unvalidated` cannot be told from its default
  `True` (`BooleanOptionalAction`), so it is documented as `main.py`'s and not refused.
- **P4 — `shards=` is not passed** by the plot scripts, as today. A read-only open takes the count
  from the primary, and the warning that the count differs from the default (10) stays.
- **P5 — the branch** is `read-only-plots`, cut from `main` at `17fad8b`, merged into `main` at the
  close.
- **P6 — the text tests** of §0.3 are updated by 02 only where it moves the text they read, each
  named in its log. In particular:
  - `test_extraction`'s four `_do_not_populate` literals stay in `plot_by_beta.py`, since the
    history lookups do not move;
  - `test_datastore_config`'s "no `object_get_vectorized` in the plot scripts" is extended to
    `plot_inputs.py`.
- **P7 — the issue.** `[08-plot-scripts-open-read-write-pools]` is assigned to this campaign. It
  stays on the `dk-and-rpk-migration` board with an **Assigned** line. Its index row moves to a new
  §1.12 of `.documents/OPEN_ISSUES.md`, and 02 closes it.
- **P8 — models.** 01 and 04 Sonnet; 02 and 03 Opus.

## 7. What this campaign hands to the user

- The run scripts' drivers need `--ray-address 127.0.0.1:6380` (already in §4.12's list), and,
  after 02, a plot run whose options do not match the build stops rather than drawing nothing.
- 03's store, kept until the user removes it.
- The two recorded behaviours of §1 ("Out of scope"), if the user wants either opened as an issue.
