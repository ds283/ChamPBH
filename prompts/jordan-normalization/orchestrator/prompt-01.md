# Orchestrator — prompt 01, strip to the exponential model

Read [`../README.md`](../README.md) first: §0.3 item 5, §2.2 (01's charter), §4, §5, §6.0 R11 and
§6.2 (P2, P3, P13). This is the campaign's first note. Its conventions are those of `rrk-adoption`'s
notes ([`../../rrk-adoption/orchestrator/prompt-01.md`](../../rrk-adoption/orchestrator/prompt-01.md)
§0, "Conventions"), restated here where 01 needs them.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`01-strip-to-the-exponential-model.md`](../01-strip-to-the-exponential-model.md)
**Board items:** X1–X5 · **Gate:** "Strip" · **Closes:**
`[00-declare-reflects-at-origin-for-the-other-potentials]` (`integrator-remediation`) · **Opens:**
only what the work finds · **Model:** Sonnet, as the prompt recommends · **Network:** none.

**Gate:**
- The prompts are written (`c6a0e72`). `HEAD` is the commit that adds this note. The code is
  `ac187c8`'s: `git diff --stat ac187c8 HEAD -- . ':!prompts' ':!.documents'` is empty (only
  `.documents/OPEN_ISSUES.md` and `prompts/` moved since).
- The prompt's own gate (its header): no tracked change, the five suites at their counts, no
  ChamPBH Ray process.
- One prompt at a time in this checkout. 02 is not dispatched until 01 is reviewed.

## 0. What makes this prompt unusual

**It ships the campaign's regression** (README §4, P3). Every later prompt runs the golden history,
and 03 and 05 regenerate it and log the change. So its form is ruled here in full, and the
orchestrator has run it.

**The orchestrator's probe of X1, at `c6a0e72`** (code = `ac187c8`), from the session scratchpad,
with the setup the prompt gives (`compute_scalar_model._function`, no Ray; `QCD_Cosmology` on
`Planck2018` in `Planck_units`; `ExponentialPotential(M = 10⁻³ M_P, Λ = 10⁻³ eV, n = 1)`;
φ* = 5 M_P, π* = 0, T* = 2 × 10⁴ GeV, T_stop = 10⁻³ GeV; atol = rtol = 10⁻⁸; main.py's grid to
10⁸⁵), each history run twice, at a load of 12–15:

| β | samples | accepted steps | RHS | first bounce N | fixed-T values | wall |
|---|---|---|---|---|---|---|
| 1.5 | 2722 × 17 | 1231 | 9887 | 18.64397081911504 | 1 MeV set; 70 keV `None` | 0.6 s |
| 5.5 | 4894 × 17 | 26343 | 232810 | 32.10765422995129 | all four `None` | 10–12 s |

- **Deterministic.** Between the two runs of each, the SHA-256 of the samples, the SHA-256 of the
  sampled z, and every field of the payload agreed, **except `metadata`**, whose `compute_time`,
  `mean_RHS_time`, `max_RHS_time` and `min_RHS_time` are wall-clock measurements.
  `compute_steps` and `RHS_evaluations` agreed.
- **Cheap.** About 12 s for both, well inside the minute.
- `reflections` 0 for both; `largest_RHS_values`, `smallest_RHS_values`, `mean_RHS_values` are
  `None` (full statistics are not collected); `solver_label` is `'Radau+kinematic-cap-stepping0'`.
- The SHA-256 of the samples, for the agent to compare its own against before it writes the JSON:
  β = 1.5 `0d2c641c7fc774cae24f8fac18cbae473343a283378df9d44853a724c3f72857`, β = 5.5
  `29a5cd4364f1b29fc78edb865c29f605204740a381c82f8e2d854690f9019dc8`; of the z values, β = 1.5
  `3802cc774fbd2b63547c497c3ce9271ff120694b1ae3f3b640245fc06d35d570`, β = 5.5
  `ecd92935903e034055e4b125ddb3b344355f684320f2e7d86ad536582ac77f10`. A difference is not a
  stop by itself (the agent's setup may legitimately build a value another way), but it must be
  explained in the log before the JSON is committed.

**Eight statements in the prompt are loose, silent or incomplete. Ruled here:**

1. **The golden history's setup, exactly.** "As `tools/history_and_bbn.py` runs one" and "built as
   `main.py:1062-1073` builds it" leave choices open. Ruled:
   - `units = Planck_units()`, `params = Planck2018()`, `QCD_Cosmology(0, units, params)`;
     `ExponentialPotential(0, M_value(0, 1.0e-3 * units.PlanckMass), Lambda_value(0, 1.0e-3 *
     units.eV), 1, units)`; `ExponentialCoupling(0, beta_value(0, β), units)`;
     `temperature(0, 2.0e4 * units.GeV)`, `temperature(1, 1.0e-3 * units.GeV)` (as `main.py:1205`
     builds a stop from `--T-stop-GeV 1e-3`); `phi_value(0, 5.0 * units.PlanckMass)`,
     `pi_value(0, 0.0)`; `atol=1.0e-8, rtol=1.0e-8` passed explicitly.
   - **The grid** is main.py's arithmetic, unchanged, with `log10_one_plus_z_low = 0`,
     `high = 85`, `samples_per_log10_z = 250`: `num = int(round(250 * (85 - 0) + 0.5, 0))`
     (21250, since `round` takes 21250.5 to the even neighbour), `np.logspace(0, 85, num) - 1.0`,
     each value as `redshift(i, float(z))` with `i` its index. That is 21250 points, not
     250 × 85 + 1; reproduce it, do not correct it (03 rewrites the grid).
   - Both histories must succeed; a failure payload is a stop.

   Classify as `IMPLEMENTATION CHOICE`, citing this note.

2. **What is pinned, field by field.** "Every scalar the payload carries" is ruled as:
   - `samples_sha256` (`numpy.asarray(data["sample"], dtype=numpy.float64).tobytes()`, which is
     row-major in `SampleValues` order), `z_sha256` (the same of `[r.z for r in data["z_grid"]]`),
     `n_samples`, and the field names of `SampleValues` in order (so that a reordering is caught
     by name);
   - `reflections`, `accepted_steps`, `steps_rejected_by_exception`, `metadata.compute_steps`,
     `metadata.RHS_evaluations` (ints, as JSON ints);
   - `first_bounce` (`N`, `phi_Einstein`, `log_T_Jordan` as `float.hex`; `reflected` as a JSON bool;
     the whole as `null` if `None`), `fixed_T_values` (four fields, each `float.hex` or `null`);
   - `cap_fraction`, `cap_floor`, `cap_global_max_step`, `jacobian_factor_max` (`float.hex`);
     `solver_label` (a string); the three RHS-statistics blocks (`null`, asserted `None`);
   - **not** `metadata.compute_time`, `mean_RHS_time`, `max_RHS_time`, `min_RHS_time`: they are
     wall-clock times and differ between runs (the probe above). The JSON and the generator's
     docstring list these four as excluded, and why.
   - **The excerpts**: the samples at indices `0, 250, 500, …` **and the last**, each as its index,
     its z as `float.hex`, and its 17 fields by name as `float.hex`. The test compares every
     excerpt exactly too, so that a changed excerpt fails (this is what the prompt's first breakage
     relies on).
   - **Provenance**: the generating commit (`git rev-parse HEAD` at generation; state in the JSON
     and the log that the code tree is `ac187c8`'s, by the empty diff of the gate above), the
     command that regenerates it, and the platform (`platform.machine()`, Python, NumPy and SciPy
     versions).

   Classify as `IMPLEMENTATION CHOICE`, citing this note.

3. **The generator and the test.**
   - `ComputeTargets/tests/golden_history.py` holds the setup and a function that returns the
     pinned record of one history; its `__main__` writes the JSON. It is run as
     `PYTHONPATH=. ./venv/bin/python -m ComputeTargets.tests.golden_history` from the root, and
     writes `ComputeTargets/tests/data/golden_history_01.json` (a new directory; no `__init__.py`
     needed) as `json.dumps(record, indent=2, sort_keys=True) + "\n"`. Its name does not match
     `test*.py`, so discovery does not run it.
   - `test_golden_history.py` (in the `[g-z]` half) recomputes both histories, once each, and
     compares every pinned item. **On a mismatch** it reports, from the excerpts, the first
     excerpted sample and field that differ, with both hex values; if every excerpt agrees but the
     SHA does not, it says the difference lies between excerpts. It also prints the recorded and
     current platform and versions, since a pin of floating-point output holds for this venv on
     this platform. It never skips.
   - Its docstring states the cost (about 12 s at load 12–15), that it is the campaign's regression
     (P3), which prompts regenerate it (03, 05), and how.

   Classify as `IMPLEMENTATION CHOICE`, citing this note.

4. **The prompt's file list (§5) is incomplete.** X3 says "every other reference outside
   `prompts/`, `science-runs/` and `.documents/`", and that governs. `git grep` at `c6a0e72` finds
   **29 tracked files** (README §0.3 item 5 says 30). Beyond §5's list, these must change:
   - `plot_by_beta.py:91`: `potential_types` becomes `["Exponential"]`. Nothing in the file reads
     it (`grep -n potential_types plot_by_beta.py` finds only the definition); do not delete it
     here, and record that in "Observations not acted on";
   - `tools/compare_stores.py:182-186`: `POLYMORPHIC_REFERENCES`' potential map keeps only
     `1: "ExponentialPotential"`. Do **not** add the four ids to `UNSTORED_TYPE_VALUES`:
     `tools/tests/test_compare_stores.py:782-792` requires the map and the unstored set together
     to equal the ids `model_ids` defines, and after X2 it defines one;
   - `tools/inventory_report.py:86-90`: the section "Potentials and couplings" keeps
     `ExponentialPotential` and `ExponentialCoupling`;
   - `RunDrivers/tests/test_registered_run.py:323` and `:336`: `test_more_than_six_values_read_as_a_count_and_a_range`
     passes `--potential-type Recliner` through `main.py`'s parser, which will refuse it. It becomes
     `Exponential`, and the expected scope reads `potential Exponential`. What the test is for (a
     count and a range for more than six values) is unchanged;
   - `ComputeTargets/tests/test_plot_inputs.py:451-468`: `test_the_potentials` keeps the
     `Exponential` row, and gains one subtest per removed name (`InversePower`, `Starobinsky`,
     `Recliner`) asserting `plot_inputs.potentials` raises `ValueError("Unknown potential type:
     <name>")`, so that what the removed rows were for (each type dispatches to its class) becomes
     "each removed type is refused";
   - `ObjectFactories/tests/test_fingerprint_format_guard.py`: `test_the_pin_has_one_pair_per_class_with_a_spec`
     asserts `len(pinned) == 21`. It becomes 17, and the module docstring's counts follow
     (17 pairs from 13 definitions). It names no deleted class, so `grep` will not find it;
   - `CosmologyConcepts/Potentials/__init__.py` also re-exports `INVERSE_POWER_POTENTIAL`
     (its last line). Nothing outside the package imports it from there; the line goes;
   - `.documents/numerical-strategies.md` §2.2 (`:100-103`) states in the present tense that
     `ReclinerPotential.default_abs_tol` loosens tolerances. Add one dated note after that
     paragraph (the form of the dated notes in `architecture-summary.md`), saying the four were
     removed by this prompt (R11) and that the exponential potential's overrides are the only
     ones left; do not rewrite the paragraph (`CLAUDE.md` invariant 6). The passages at `:151` and
     `:350` describe the region scheme and a past addendum, already superseded; leave them.

   `ComputeTargets/tests/test_kinematic_cap_loop.py` matches the grep through
   `NonReflectingExponentialPotential` (`:93`), a stand-in subclass of `ExponentialPotential` that
   declares no wall. It is not one of the four and **does not change**.

   Classify each as `STRUCTURALLY REQUIRED`, citing this note.

5. **What the closing `grep` may still find.** The prompt says "nothing", but P2 keeps format 1's
   pin and golden file. Ruled: the check is
   `git grep -nE '\b(InversePower|Starobinsky|Recliner|Reflecting)|INVERSE_POWER|STAROBINSKY|RECLINER|REFLECTING_POTENTIAL|starobinsky\.yaml|recliner\.yaml' -- . ':!prompts' ':!science-runs' ':!.documents'`
   (the word boundary excludes `NonReflecting…`), and its only hits are:
   - `config/fingerprint.py`: `SPEC_DIGESTS[1]`'s four pairs and the format-1 comment, which stay
     as they are;
   - `ObjectFactories/tests/data/store_fingerprint_rrk-adoption-02.json`, which is never edited.

   The log pastes the command and its whole output, and says that each hit is one of these.
   Classify as `STRUCTURALLY REQUIRED`, citing this note.

6. **The golden fingerprint's store** (X4, X5). `_GoldenStoreCase.build_store` builds one
   `ScalarModel` per potential type, five, the first tagged, and three tests read that shape
   (`TestGolden`'s counts, `test_two_tag_sets_are_read`, `TestCommandLine`'s format). With one
   potential type, ruled:
   - `POTENTIALS` is replaced by **two `ExponentialPotential` rows, `n_value` 1 and 2** (format 1's
     store had an `n = 2` row on `InversePowerPotential`), each with its own `ScalarModel`, the
     first tagged, value rows as before on the first. Two tag sets are still read;
   - `LABEL` and `T0` **do not change**, so that the format-1 and format-2 golden files differ only
     by what the strip changes;
   - `GOLDEN` names the new file, `data/store_fingerprint_jordan-normalization-01.json`, written once
     from `fingerprinter.take(primary, None)` as format 1's was; the old file stays;
   - `test_the_store_reads_with_no_problem_and_every_class`: 17 classes, `ScalarModel` 2,
     `ExponentialPotential` 2; `TestCommandLine` asserts format 2;
   - the module docstring's counts and shape follow. `RunDrivers/tests/test_registered_run_fingerprint.py`
     imports `GOLDEN` and `_GoldenStoreCase`, so it follows without an edit; do not edit it.

   Classify as `IMPLEMENTATION CHOICE`, citing this note.

7. **`test_inventory_specs.py`** (X4). `EXPECTED_ORDER` loses the four; the polymorphic-map test
   keeps the exponential id; `test_seventeen_definitions_serve_twenty_one_classes` asserts 17
   classes from **13** definitions (the orchestrator counted both at `c6a0e72`, removing the four
   from `config.datastore.factories`: 21 → 17 classes, 17 → 13 factory types) and is renamed
   `test_thirteen_definitions_serve_seventeen_classes`. The registry goes from 27 classes to 23.
   Log the rename with the old name. Any other test whose literal count moves with the registry is
   changed the same way, with its line in the log. Classify as `STRUCTURALLY REQUIRED`.

8. **The records.**
   - `config/fingerprint.py`: the format comment gains the prompt's line for format 2, and the
     `SPEC_DIGESTS` comment's counts for format 2 (17 pairs, **14** distinct digests: format 1's 15
     less the one that `StarobinskyPotential`, `ReclinerPotential` and `ReflectingPotential`
     shared; `ExponentialPotential` keeps its own, and `ScalarModel`'s changes). Format 1's text
     stays. Check the 14 by `len(set(d for _, d in SPEC_DIGESTS[2]))` and put the line in the log.
   - **This board:** row 01 ✅, X1–X5, the gate "Strip" ✅, §3.1's row for the closed issue keeps
     its place with "01 — closed (this commit)", §4 gains one line pointing at
     `integrator-remediation`'s §4, and `### 5.2 After prompt 01 (<date>)`.
   - **`integrator-remediation`'s board:** the entry moves from §3 to §4 with a
     `**Resolved (<date>):**` line (moot: the four classes are deleted by `jordan-normalization`
     prompt 01, this commit), in the form of that §4's other entries.
   - **`.documents/OPEN_ISSUES.md`:** the row leaves §1.15; the header's count goes from **28** to
     **27**, and its `integrator-remediation` clause from 9 to 8; **Last updated** the date.
   - `.documents/architecture-summary.md`: the directory trees (`:33-34`, `:51-54`, `:93-96`), the
     registry table (`:427-431`), `--potential-type`'s choices (`:578`), the replicated tables
     (`:621-622`) and the list of implementations (`:963`) become true of the tree, with one dated
     note at §8.3 saying the four were removed by this prompt (R11).

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-only`
  before committing; deletions by `git rm`. Count, then paste. A log states only what the diff and
  the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its
  follow-up.
- **Order.** X1 first, on the unmodified tree: the JSON is generated, the golden test passes twice,
  and only then is anything deleted. The JSON is not regenerated afterwards.
- **Suites.** `CLAUDE.md`'s commands, serially, in the foreground, ComputeTargets in its two
  halves, output to a file in the agent's scratch directory, the verdict grepped from it, `uptime`
  beside each. A failure is re-run alone before it is called real.
- **Breakage diffs** are recorded so that `git apply` replays them from a clean tree at the agent's
  commit. Check each with `git apply --check` and `-R --check`, and replay in `bash`, not zsh. The
  first breakage multiplies a **nonzero, finite** excerpt value (an excerpt's `phi_Einstein`, say,
  not a `reflecting_term`, which may be zero) by `1 + 2⁻⁵²`, and the log shows the hex before and
  after, so that it is seen to have changed.
- **Scratch.** The agent uses its own subdirectory of the session scratchpad, never `/tmp`. No
  scratch file goes under the repository.
- **Stores and Ray.** None is opened but the tests' temporary ones. No driver is run, and no Ray
  started. `var/` exists (from `rrk-adoption`) with an empty `var/runs/`; nothing is written to it.
- **Stop conditions are real.** A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Review against the agent's commit, and check
  `git log` before measuring at `HEAD`.

## 1. Before you dispatch

1. **The tree.** Branch and `HEAD`; `git status --short` empty; the gate's diff against `ac187c8`
   empty.
2. **Ray.** `ray_cluster.sh status` and `pgrep -lf 'gcs_server|raylet|ray::'`.
3. **The registry.** `./venv/bin/python -m runregistrykit list`: nothing running or stale.
4. **Baselines.** The five suites at `HEAD`, serially, with the load. Give the agent the numbers.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Sonnet**. Give it:
- the prompt;
- the campaign README and this board;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad.
  The agent does not read `orchestrator/` or prompts 02–10.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/01-strip-to-the-exponential-model.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **The files it may add, change or delete are the prompt's §5 list and §0's ruling 4, and
  nothing else.**
- **§0's eight rulings** bind it, each classified as §0 says.
- **X1 before X2.** The JSON is generated on the unmodified tree and never regenerated.
- **It starts no Ray, runs no driver and opens no store but a test's.**
- **Stop and ask** on any of the prompt's §4 conditions. An existing test that fails for a reason
  §0 does not cover is a stop, never a reason to edit it.
- **It replies with:**
  - the log's path and the message as committed;
  - the golden JSON's pinned SHAs against §0's, its size in lines, and the golden test's time
    (both runs on the old tree, and after);
  - the closing `grep` and its output;
  - every test method removed, renamed or changed, with what it asserted;
  - each breakage with the tests it failed and how;
  - the five suites, before and after, with each count's change explained;
  - the format-2 pin's pair and distinct-digest counts;
  - every deviation with its tag;
  - "State handed to the next prompt", verbatim.

## 3. The review — ten checks

1. **Scope.** `git diff <agent>~1 <agent> --stat` lists the prompt's §5 files and §0's ruling 4
   only; ten deletions (four classes, four factories, two YAML files). `git status` clean.
2. **X1, the golden history.** Read the generator, the test and the JSON. The setup is §0's
   ruling 1, field by field. The pinned items are ruling 2's, the four wall-clock fields excluded
   and named. Run the agent's generator with its output redirected **into the scratchpad**, never
   over the committed file: once in a temporary worktree at `<agent>~1` (the generator copied in,
   since it does not exist there; the worktree removed afterwards), and once at `<agent>`. Both
   records equal the committed JSON in every pinned item, which shows it was made on the old tree
   and still holds on the new. The SHAs equal §0's or the log explains why.
3. **X1, the test.** Run `test_golden_history` alone, twice, with its time. Its docstring states
   the cost and the regeneration command; it never skips.
4. **X2–X3, the grep.** Run §0 ruling 5's command; only the two kinds of hit it names. Read each
   registry change (`config/datastore.py`, `sharding.py`, `argument_parser.py`, the spec, the
   dispatch of `main.py` and `plot_inputs.py`): one case each, an unknown type still raises.
   `EXPONENTIAL_POTENTIAL` is still 1.
5. **X4, the tests.** For each test method the log lists, `git diff` agrees with the line and
   with what it asserted; none is gone without its line. `test_kinematic_cap_loop.py` and
   `test_registered_run_fingerprint.py` unchanged.
6. **X5, the format.** `FINGERPRINT_FORMAT` 2; `SPEC_DIGESTS[1]` byte-identical to `c6a0e72`'s;
   `SPEC_DIGESTS[2]` equal to `spec_digests(factories)` recomputed by the orchestrator; 17 pairs,
   14 distinct. The format-1 golden file unchanged; the new one present, with `taken` `null`,
   format 2, and the same `LABEL`.
7. **The breakages.** In `bash`, replay each diff with `git apply`, run the named tests, see them
   fail as the log says, revert with `git apply -R`, `git status` clean. As the orchestrator's own
   probes, each reverted: one hex digit of a `samples_sha256` changed in the JSON (the test fails
   and says the difference lies between excerpts, or names one); and the generator's `rtol`
   changed from `1.0e-8` to `1.1e-8` (the test fails on the samples, naming the first excerpted
   field that moved).
8. **The prose.** `architecture-summary.md` and `numerical-strategies.md` as §0 ruling 8 and 4
   say: true of the tree, dated notes, nothing rewritten that was a record.
9. **The records.** This board, `integrator-remediation`'s §3 → §4, `.documents/OPEN_ISSUES.md`
   (row gone, 27, the integrator clause 8, the date), all in the one commit. The log in §5.1's
   form, every deviation classified, every test change listed, the breakage diffs.
10. **The suites** at the agent's commit, serially: 18; ComputeTargets 61 + 115 + the golden
    test's methods; ObjectFactories, tools and RunDrivers each at its baseline less only what X4
    removed. `black --check` on every changed or new Python file.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 01* paragraph in §5.2. Fix small
  residue in a follow-up commit of its own:
  - "this commit" → the SHA (board, log, `integrator-remediation`'s §4);
  - a list of the orchestrator notes on the board, with this one marked "used for 01";
  - the status line: 1 of 10 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the message as committed;
  - the golden history: what it pins, its cost, and that it pins floating-point output on this
    venv and platform (arm64, NumPy 2.3.5, SciPy 1.17.0), so a run elsewhere may differ in the
    last bits without a regression;
  - §0's rulings that read the prompt otherwise than it was written (the file list, the grep's
    residue, the fingerprint store's two exponential rows);
  - the five suites' counts.
- **Then 02.** Its note is written after this review.
