# Prompt 01 — strip to the exponential model

**Campaign:** [`README.md`](README.md) · **Board items:** **X1**–**X5** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-20, after the user's rulings (README §6: R11, "for now, strip everything down
to just the exponential"; P2, P3). Its orchestrator note, written before dispatch, governs where it
differs from this file.
**Gate:**
- on branch `jordan-normalization` at or after the commit that wrote this file, no tracked change in
  `git status`;
- the five suites at README §0.4's counts (18 / 176 / 78 / 84 / 61);
- no ChamPBH Ray process is running. You start none.

**Closes:** `[00-declare-reflects-at-origin-for-the-other-potentials]` (`integrator-remediation`
board; moot once the classes are gone). **Opens:** only what the work finds.
**Recommended model:** **Sonnet**. Mechanical deletion with a registry, a spec and a fingerprint to
keep consistent, and one new regression test that every later prompt leans on.

**Read first:**

1. [`README.md`](README.md): §0.3 item 5 (what the strip removes), §2.2 (01's charter), §4, §5,
   §6.0 R11, §6.2 P2, P3, P13.
2. `CLAUDE.md`: "Repository mechanics" (the packages, the tests, `black`), and invariants 1–7.
3. `config/datastore.py` (factories, `serial_batch_sizes`), `config/sharding.py`
   (`replicated_tables`), `config/fingerprint.py` (`FINGERPRINT_FORMAT`, `SPEC_DIGESTS`, and its
   comment on how a format moves), `config/argument_parser.py` (`potential_types`).
4. `ObjectFactories/ScalarModel.py`'s `inventory_spec` (the `potential` parent's `types`), and the
   golden-fingerprint test `ObjectFactories/tests/test_store_fingerprint_golden.py` with its data
   files, and `test_fingerprint_format_guard.py`.
5. `main.py`'s and `plot_inputs.py`'s dispatch on `--potential-type` (`convert_to_potential`,
   `_potential_payload`).
6. `ComputeTargets/ScalarModel.py`'s `compute_scalar_model` (its signature and what it returns), and
   `tools/history_and_bbn.py` (how a history is run without Ray: `compute_scalar_model._function`).

---

## 1. What is wanted

Only the exponential potential and the exponential coupling remain in the tree, and nothing they
compute changes by a bit. A regression test pins that, for this prompt and every later one.

### 1.1 The items

- **X1 — the golden history, first.** Before anything is deleted, write
  `ComputeTargets/tests/test_golden_history.py` and its data file
  `ComputeTargets/tests/data/golden_history_01.json`, generated on the tree you started from.
  - Two histories, each run as `tools/history_and_bbn.py` runs one, through
    `compute_scalar_model._function` with no Ray: `QCD_Cosmology` on `Planck2018` in
    `Planck_units`; `ExponentialPotential(M = 10⁻³ M_P, Λ = 1 meV, n = 1)`;
    `ExponentialCoupling(β)` for β = 1.5 and β = 5.5; φ* = 5 M_P, π* = 0, T* = 2 × 10⁴ GeV,
    T_stop = 1 MeV; atol = rtol = 10⁻⁸ (`exponential.yaml`'s); the production z grid
    (250 per decade of 1 + z, up to 10⁸⁵, built as `main.py:1062-1073` builds it, with dummy store
    ids).
  - **What is pinned**, per history: the SHA-256 of the samples as one contiguous float64 array
    (`numpy.asarray(samples, dtype=float64).tobytes()`, in `SampleValues` order), the SHA-256 of
    the sampled z values, the number of samples, the reflections count, and every scalar the
    payload carries (first bounce, fixed-temperature values, steps), as `float.hex` strings. Also,
    for diagnosis, every 250th sample in full as `float.hex`. The JSON names the commit it was
    generated at and the script, so that a later prompt can regenerate it.
  - **The test** recomputes both and compares every pinned item exactly. On a mismatch it reports
    which field of which sample differs first, from the stored excerpts.
  - A small generator, `ComputeTargets/tests/golden_history.py` (not a test module), holds the
    setup, so that the test and a regeneration use one definition. Its `__main__` writes the JSON.
  - The two histories must run in under a minute together; say how long they took.
- **X2 — the classes go.** Delete
  `CosmologyConcepts/Potentials/{InversePowerPotential,StarobinskyPotential,ReclinerPotential,ReflectingPotential}.py`
  and `ObjectFactories/{InversePowerPotential,Starobinsky,ReclinerPotential,ReflectingPotential}.py`
  (`git rm`), their exports in `CosmologyConcepts/Potentials/__init__.py`, and their ids in
  `CosmologyConcepts/Potentials/model_ids.py` (keep `EXPONENTIAL_POTENTIAL`'s value).
  `starobinsky.yaml` and `recliner.yaml` go too.
- **X3 — the registries follow.** `config/datastore.py` (imports, `factories`,
  `serial_batch_sizes`), `config/sharding.py`, `ObjectFactories/ScalarModel.py`'s spec (the
  `potential` parent's `types` keeps only `EXPONENTIAL_POTENTIAL`), `config/argument_parser.py`
  (`potential_types = ["Exponential"]`; the option itself stays until 08), `main.py`'s and
  `plot_inputs.py`'s dispatch (one case; an unknown type still raises), and every other reference
  outside `prompts/`, `science-runs/` and `.documents/` (README §0.3 item 5 counts 30 files; `grep`
  for the four class names and the four id names).
- **X4 — the tests.** Each test that builds one of the deleted classes is cut to the exponential
  case. List every test method removed or changed in the log with what it asserted; none is
  deleted without that line. The golden fingerprint's store builder loses the four potential
  types.
- **X5 — fingerprint format 2.** The registry and `ScalarModel`'s spec change, so
  `FINGERPRINT_FORMAT` becomes 2, with its history line ("Format 2: jordan-normalization prompt 01,
  the four non-exponential potentials removed"), a new `SPEC_DIGESTS[2]` generated by
  `spec_digests` (never typed), and a new golden file beside format 1's, never over it.
- **The prose this commit makes false:** `.documents/architecture-summary.md` where it lists the
  potentials; `CLAUDE.md` nowhere that I know of, but `grep` it. Campaign records and dated
  documents are not edited.

---

## 2. Verification

1. **Before any edit:** the five suites at README §0.4's counts, serially, ComputeTargets in two
   halves.
2. **X1 on the old tree:** the golden test passes, twice in a row (determinism), and its time.
3. **After:**
   - the golden test passes **unchanged** (the JSON is not regenerated after X2–X5);
   - the five suites, with each count's change explained by X1 (added) and X4 (removed);
   - `grep -rn` for `InversePower`, `Starobinsky`, `Recliner`, `Reflecting` and their ids outside
     `prompts/`, `science-runs/` and `.documents/`: nothing;
   - `black --check` on every changed or new Python file.
4. **Deliberate breakages** (README §5, rule 8: each recorded as a diff, never committed), each
   failing a named test:
   - one bit of one sample changed (multiply one stored excerpt value by `1 + 2⁻⁵²`): the golden
     test fails and names the field;
   - the format left at 1 after X3: the format guard fails;
   - a deleted potential's id left in `ScalarModel`'s spec: the guard or the golden fingerprint
     fails.

## 3. Acceptance

1. X1–X5 as §1.1 says.
2. §2's checks in the log, each with its command and output.
3. **One commit**, with the log `logs/01-strip-to-the-exponential-model.md` (template: README §5).
   The board: row 01, X1–X5, the gate "Strip", `[00-declare-reflects-at-origin-for-the-other-potentials]`
   closed on `integrator-remediation`'s board (§4) and its row deleted from
   `.documents/OPEN_ISSUES.md` §1.15 with the count corrected, and a `### 5.2 After prompt 01
   (<date>)`. Write "this commit" where the SHA goes.

## 4. Stop conditions: stop and ask

- The golden test is not deterministic on the old tree (two runs differ).
- The golden test fails after the deletion: something the exponential path computes depended on a
  deleted class.
- A test cannot be cut to the exponential case without losing what it was for (say which and why).
- The two histories take more than a minute together.

## 5. What this prompt does not do

- It does not remove `--potential-type` (08), rename anything (02), or change any computation.
- It does not touch the vendored `PRyM/`, `thirdparty/`, the packages, or any store.
- **Files it changes:** the deleted files of X2; `CosmologyConcepts/Potentials/__init__.py`,
  `model_ids.py`; `config/datastore.py`, `config/sharding.py`, `config/fingerprint.py`,
  `config/argument_parser.py`; `ObjectFactories/ScalarModel.py` (the spec only); `main.py` and
  `plot_inputs.py` (the dispatch only); the tests of X4 and the golden-fingerprint data;
  `ComputeTargets/tests/test_golden_history.py`, `golden_history.py`, `data/golden_history_01.json`
  (new); `.documents/architecture-summary.md`; the boards and the index of §3 item 3; the log.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
