# jordan-normalization campaign — implementation state

**Last updated:** 2026-10-25 · **Status: LIVE — 10 of 10 prompts written, 4 landed (01, 02, 03
reviewed; 04 landed in this commit, after one stop ruled by the orchestrator); R1–R14, U1–U5 and
P1–P20 ruled by the user 2026-10-20 (README §6). Next: the orchestrator's review of 04.**
The plan is [`README.md`](README.md), written on 2026-10-20 on branch `jordan-normalization`, cut
from `main` at `ac187c8`.

The campaign normalizes the conformal factor at the reference point today, A(φ_ref) = 1 with
φ_ref = φ_min(T_CMB), and tunes each history's potential normalization so that H_J(T_CMB) = H0:
- the model is stripped to the exponential one, and `ScalarModel` is renamed `ScalarHistory`;
- the sampling axis stores ln(1 + z), and every sample carries ln(1 + z_J);
- V(φ) and A(φ) become one `AbstractModel`, whose normalization the model interprets;
- the Planck mass comes from PRyMordial's G_N;
- a tuning layer, with a secant step, integrates each history to T_CMB until both conditions hold;
- the store keys a history on its model and initial data, and holds the tuned values.

It ends in the handover for the science rebuild, which is the user's.

**Campaign:** [`README.md`](README.md) ·
**Code:** `CosmologyConcepts/`, `ComputeTargets/`, `ObjectFactories/`, `Units/`, `config/`,
`main.py`, `plot_inputs.py`, `plot_by_beta.py`, `plot_ScalarModel.py`, `extract_common.py`,
`pipeline_selection.py`, `RunDrivers/registered_run.py`, `tools/` ·
**Verification:** `.documents/jordan-normalization-verification.md` (created by 09) ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) §1.15 (assigned) and
§1.16 (opened)

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, or an issue assigned to this campaign closes, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md)
> is updated **in the same commit**. See `CLAUDE.md`.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | Strip to the exponential model | four potentials and their configs removed; a golden short history; format 2 | Sonnet | ✅ | ✅ | `a1c88b6` | [`logs/01-strip-to-the-exponential-model.md`](logs/01-strip-to-the-exponential-model.md) |
| 02 | Rename to `ScalarHistory` | classes, tables, drop group, proxies, documents; format 3 | Sonnet | ✅ | ✅ | `abccc23` | [`logs/02-rename-to-scalarhistory.md`](logs/02-rename-to-scalarhistory.md) |
| 03 | The sampling axis | ln(1 + z_E) on the axis, exact-multiple grid, absolute lookup, ln(1 + z_J) per sample; format 4 | Opus | ✅ | ✅ | `bd9ae42` | [`logs/03-the-sampling-axis.md`](logs/03-the-sampling-axis.md) |
| 04 | The model interface | `AbstractModel`, `ExponentialModel`, `RealizedModel`; consumers on it via an adapter; φ_ref = 0 bitwise; the shift law | Opus | ✍️ | ✅ | this commit | [`logs/04-the-model-interface.md`](logs/04-the-model-interface.md) |
| 05 | The Planck mass | one reduced Planck mass from PRyMordial's G_N in three unit systems; consistency tests | Sonnet | ✍️ | ⬜ | — | — |
| 06 | The tuning layer | `integrate_history` and `tune_history`: secant, seed, residuals, failure reasons, progress lines | Opus | ✍️ | ⬜ | — | — |
| 07 | The store | `ExponentialModel` table; `ScalarHistory`'s key and tuned outputs; format 5; `VERSION_LABEL` 2026.7.0; writers and readers moved | Opus | ✍️ | ⬜ | — | — |
| 08 | The driver and the tools | `main.py`'s options; the super-Planckian warning; the wrapper's scope; the tools and the surfing scan on tuned histories | Opus | ✍️ | ⬜ | — | — |
| 09 | Rehearse a tuned build | registered pilot build on the isolated cluster, interrupted and resumed, compared bitwise (if U2) | Opus | ✍️ | ⬜ | — | — |
| 10 | Close | documents, `CLAUDE.md`, the handover for the science rebuild, indexes, board | Sonnet | ✍️ | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

**Written 2026-10-20** after the rulings (README §2.1). Each is dispatched with an orchestrator
note in `orchestrator/`, written after the previous prompt's review.

Orchestrator notes, one prompt at a time, in [`orchestrator/`](orchestrator/):
- [`orchestrator/prompt-01.md`](orchestrator/prompt-01.md) (`701ed4c`), used for 01.
- [`orchestrator/prompt-02.md`](orchestrator/prompt-02.md) (`481b65e`), used for 02. Two of its
  rulings were corrected after dispatch (§5.3).
- [`orchestrator/prompt-03.md`](orchestrator/prompt-03.md) (`9997eef`), used for 03. One gap in its
  rulings (a fixture of `test_compare_stores.py`) was ruled after the agent stopped (§5.4).

### 1.1 Items

| Prompt | Items |
|---|---|
| 01 | X1 the golden history · X2 the classes deleted · X3 the registries · X4 the tests · X5 format 2 |
| 02 | N1 the code, tables, columns and drop group · N2 format 3 · N3 the current documents · N4 the run scripts left as records |
| 03 | Z1 the concept ✅ · Z2 the factory and format 4 ✅ · Z3 the grid ✅ · Z4 the sampling ✅ · Z5 ln(1 + z_J) ✅ · Z6 the readers ✅ · Z7 the value factory's issues ✅ (all landed in `bd9ae42`) |
| 04 | M1 `AbstractModel` ✅ · M2 `RealizedModel` ✅ · M3 `ExponentialModel` ✅ · M4 the consumers ✅ · M5 the adapter ✅ · M6 the unread properties ✅ · M7 the tests ✅ (all landed in this commit) |
| 05 | K1 one constant · K2 the three unit systems · K3 the tests |
| 06 | T1 `integrate_history` · T2 the residuals · T3 the secant and its guards · T4 the seed · T5 the failures · T6 progress · T7 the outputs |
| 07 | S1 the model's table · S2 the key · S3 the outputs · S4 the compute path · S5 `main.py` · S6 the readers · S7 format 5 and 2026.7.0 · S8 the tests |
| 08 | D1 the options · D2 the warning · D3 the wrapper's scope · D4 `history_and_bbn.py` · D5 the surfing tools · D6 `histories.csv` · D7 the tests |
| 09 | H1 reference · H2 resumed · H3 the comparisons · H4 the measurements · H5 the document |
| 10 | C1 the documents · C2 `CLAUDE.md` · C3 the handover · C4 the board and indexes · C5 the final suites |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Strip | only the exponential potential and coupling remain; the golden history, generated at `ac187c8`, passes bitwise | 01 | ✅ |
| Rename | no `ScalarModel` name outside the records; the golden history bitwise | 02 | ✅ |
| Axis | the axis is ln(1 + z_E) with no overflow; ln(1 + z_J) agrees with `cosmology.z(T_J)` and with the A identity; the change in values measured | 03 | ✅ |
| Model | the golden history bitwise through `ExponentialModel` at φ_ref = 0, V₀ = Λ⁴; the shift law holds | 04 | ✅ |
| Units | one Planck mass equal to PRyMordial's; 1 K, ħc, ħ and the kilogram consistent to ≤ 10⁻⁶ in each unit system | 05 | ⬜ |
| Tuning | a tuned history converges inside the tolerance, deterministically; the secant is no worse than the plain update on README §0.3's grid | 06 | ⬜ |
| Store | a tuned history round-trips bitwise and its realized model is rebuilt equal; the golden fingerprint at format 5 | 07 | ⬜ |
| Driver | the old options are refused; the surfing scan records tuned values with its roots unchanged | 08 | ⬜ |
| R | on the isolated cluster every history converged, the stored values equal the tuning layer's, and the resumed store equals the reference | 09 | ⬜ |

## 3. Active and unresolved issues

Prompt 02 opened one and prompt 03 one. Prompt 01 closed one assigned issue, prompt 03 three and
prompt 04 one (§3.1, §4).

### `[02-the-scalar-history-parent-is-still-called-model]`

**Opened:** 2026-10-21 by prompt 02 (ruling 3 of its orchestrator note).

- **What.** The rename changed every name that says `ScalarModel`. Lowercase names that mean a scalar
  history but do not say it were left: the parent field `"model"` in the inventory specs of
  `AdiabaticHistory` and `BBNData`; `tools/inventory_report.py`'s `model_text_of`, `model_parts`,
  `model_sort` and its printed `model: {…}`; the payload key `model_proxy` and the attribute
  `_model_proxy`; locals and parameters named `model`.
- **Impact.** None to values. Once 07 gives `model` to the Lagrangian, `model` would mean two things.
- **Next step.** 07, which rewrites these specs and readers, renames them. Not assigned yet.

### `[03-scalar-history-functions-splines-z-but-evaluates-ln-one-plus-z]`

**Opened:** 2026-10-22 by prompt 03 (ruling 9 of its orchestrator note).

- **What.** `ScalarHistory._create_functions` (`ComputeTargets/ScalarHistory.py:1677-1690` after
  prompt 03) builds each spline with z as the abscissa (`(v.z.z, getattr(v, attr))`), and wraps it
  in `ZSplineWrapper(…, log_z=True)` (`ComputeTargets/spline_wrappers.py:28-48`), which evaluates
  the spline at ln(1 + z). The bounds are read from `self.z_sample`, which `ScalarHistory` does not
  define. Since prompt 03, `v.z.z` is derived from the axis and is `inf` past 709.8 e-folds.
  `git grep -n -P '\.functions\b|ZSplineWrapper' -- '*.py'` finds no caller of
  `ScalarHistory.functions` outside the module: only the import and the one construction.
- **Impact.** None today, since nothing calls it. A caller would get wrong values, from a spline
  on z evaluated at ln(1 + z), and `inf` abscissae past 709.8 e-folds.
- **Next step.** Delete `functions`, `ModelFunctions` and `ZSplineWrapper` if they are still unread,
  or build the splines on `log_one_plus_z`. For 07, which rewrites `ScalarHistory`'s stored shape.
  Not assigned.

### 3.1 Assigned to this campaign

Assigned 2026-10-20 by the user (README §6.1, U3). Each entry stays on its own board, which carries
the **Assigned** line; the prompt named closes it there, in that board's §4, and deletes its row
from `.documents/OPEN_ISSUES.md` §1.15.

| Issue | Its board | Prompt | Why here |
|---|---|---|---|
| `[00-declare-reflects-at-origin-for-the-other-potentials]` | `integrator-remediation` | 01 — closed (`a1c88b6`) | moot once the four other potentials are deleted |
| `[00-region-properties-on-the-potentials-become-unread]` | `integrator-remediation` | 04 — closed (this commit) | `AbstractModel` does not carry the properties over |
| `[01-scalarmodelvalue-build-checks-the-stored-phi-against-pi]` | `dk-and-rpk-migration` | 03 — closed (`bd9ae42`) | the value factory is rewritten for the axis |
| `[05-the-value-factory-compares-stored-phi-against-pi]` | `science-readiness` | 03 — closed (`bd9ae42`) | the same defect, opened twice |
| `[09-redshift-lookup-at-zero-takes-abs-of-the-comparison]` | `dk-and-rpk-migration` | 03 — closed (`bd9ae42`) | the lookup moves to ln(1 + z) by absolute difference |
| `[02-scalar-stage-missing-filter-keeps-every-model]` | `dk-and-rpk-migration` | 07 | the scalar stage's lookups are rewritten for the new key |
| `[03-step-1-first-pass-lookup-filters-nothing]` | `run-integrity` | 07 | the same code |
| `[05-surfing-scan-runs-at-a-fixed-lambda]` | `surfing-temperature` | 08 | the surfing tools move onto tuned histories |

## 4. Resolved issues

- `[00-declare-reflects-at-origin-for-the-other-potentials]` — closed as moot by prompt 01
  (`a1c88b6`): the four potentials it concerned are deleted. The entry, with its **Resolved** line, is
  in `integrator-remediation`'s §4; its row is deleted from `.documents/OPEN_ISSUES.md` §1.15.
- `[01-scalarmodelvalue-build-checks-the-stored-phi-against-pi]` and
  `[05-the-value-factory-compares-stored-phi-against-pi]` (one defect, opened twice) — closed by
  prompt 03 (`bd9ae42`): `sqla_ScalarHistoryValue_factory.build` had no caller, and its body is now
  one `NotImplementedError` naming `sqla_ScalarHistoryFactory` (note 03, ruling 11). The entries,
  with their **Resolved** lines, are in `dk-and-rpk-migration`'s §4 and `science-readiness`'s §4;
  their rows are deleted from `.documents/OPEN_ISSUES.md` §1.15.
- `[09-redshift-lookup-at-zero-takes-abs-of-the-comparison]` — closed by prompt 03 (`bd9ae42`):
  the redshift lookup matches ln(1 + z) by absolute difference below 1e-10 in one branch, and a test
  shows that a lookup at 0 does not match a row at −1e-6. The entry, with its **Resolved** line, is
  in `dk-and-rpk-migration`'s §4; its row is deleted from `.documents/OPEN_ISSUES.md` §1.15.
- `[00-region-properties-on-the-potentials-become-unread]` — closed by prompt 04 (this commit): the
  five region properties are deleted from `AbstractPotential` and `ExponentialPotential`, and
  `default_max_step` from `ExponentialPotential`; the witness grep (note 04, ruling 12) finds
  nothing. The entry, with its **Resolved** line, is in `integrator-remediation`'s §4; its row is
  deleted from `.documents/OPEN_ISSUES.md` §1.15.

## 5. Dispatch record

### 5.0 Planning (2026-10-20)

Planned at the user's request, after an evaluation of the brief of 2026-10-06 (`c21cdfb`) that the
user and the planner worked through on 2026-10-20.
- **The brief checked at `ac187c8`** (README §0.2): its four sites of A hold; a fifth use, the
  super-Planckian warning, runs before φ_ref exists; the units' 2.8 × 10⁻⁴ inconsistency is real
  and is in three unit systems, not one.
- **Measured** with probes now kept in `source/planning-probes-2026-10-20/` (README §0.3):
  - the ruled fixed point converges at all thirteen grid points tried, in 6–19 integrations;
  - the V_target ≤ 0 stop is an artefact of the update order;
  - the tuned values depend on φ* only where nothing surfs (up to 1.6 % in Λ at β = 0.3);
  - the analytic minimum is accurate in location but not in amplitude, so it serves as a seed only;
  - the redshift label overflows at 709.8 e-folds.
- **R1–R14** were ruled by the user (README §6.0): the brief's U1–U5, of which U1 (match H0) was
  confirmed on 2026-10-20; one `AbstractModel`; the history finds its own normalization; two layers;
  the rename; the secant; exponential only; ln(1 + z); no seeding between histories; n in the key.
- **Suites at `ac187c8`:** 18 / 176 (61 + 115) / 78 / 84 / 61, all passing.
- **No issue** opened; eight proposed for assignment (U3).

### 5.1 Rulings (2026-10-20)

- **U1–U3 and P1–P20 ruled as proposed** ("U1 - OK", "U2 - OK", "U3 - OK", "P1-P20 seem ok"):
  the names; the rehearsal in 09; the eight issues assigned (§3.1, with an **Assigned** line on
  each owning board and the rows moved to `.documents/OPEN_ISSUES.md` §1.15).
- **U4 and U5 open.** The user asked which 79-β scan U4 means, and which units change U5 means.
  Answered in the conversation: U4's scan is `surfing-temperature`'s S1 (and its fine scan S5),
  a science input to the T_surf(β) figure run by `tools/surfing_scan.py`, not a test; U5's change
  is prompt 05's, the reduced Planck mass taken from PRyMordial's G_N (R3), which moves every
  eV-labelled quantity by about 2.8 × 10⁻⁴.
- **U4 and U5 ruled** after those answers: U4 "tune within the tool. These should be standalone
  tools, not depending on a datastore"; U5 "you just mean the numerical factor that relates eV to
  M_P. Stick with your recommendation" (05 after 04).
- **Prompts 01–10 written** the same day. Writing them found two corrections to the charters, made
  in the README: 07 removes the Λ options and `--T-stop-GeV` with their tables (nothing can
  consume them), fixes the wrapper's `scope` that reads them, and keeps the history-running tools
  on a fixed normalization until 08; and 06's comparison costs about an hour, not half.

### 5.2 After prompt 01 (2026-10-20)

Prompt 01 landed in `a1c88b6` (log [`logs/01-strip-to-the-exponential-model.md`](logs/01-strip-to-the-exponential-model.md)).
- **X1.** `ComputeTargets/tests/golden_history.py` (setup and generator), `test_golden_history.py`
  and `data/golden_history_01.json`, generated on the tree it started from (code tree `ac187c8`'s);
  the two histories take about 11 s together, and passed twice on the old tree and unchanged after
  the strip.
- **X2, X3.** The four classes, their four factories, two YAML files, ids, registry, sharding,
  spec, parser choices and dispatch removed; `Exponential` is the only `--potential-type`.
- **X4.** Tests cut to the exponential case (listed in the log).
- **X5.** `FINGERPRINT_FORMAT` 2; `SPEC_DIGESTS[2]` of 17 pairs and 14 distinct digests; a new golden
  file `store_fingerprint_jordan-normalization-01.json` beside format 1's.
- **Suites:** CosmologyModels 18, ComputeTargets 179 (61 + 118), ObjectFactories 78, tools 84,
  RunDrivers 61 (was 18 / 176 / 78 / 84 / 61: +3, the golden history's tests).
- **Closed:** `[00-declare-reflects-at-origin-for-the-other-potentials]` (moot).

**Orchestrator review of prompt 01 (2026-10-21).** Against `a1c88b6`, by note 01's ten checks. All
pass.
- **Scope.** The prompt's §5 files and the note's ruling 4, plus `tools/tests/test_inventory_report.py`,
  whose count literal (21 classes) moved with the registry; ruling 7's last sentence covers it. Ten
  deletions. The tree is clean.
- **The golden history.** The agent's generator was run by the orchestrator into the scratchpad,
  once in a temporary worktree at `701ed4c` (the generator copied in) and once at `a1c88b6`. Both
  records equal the committed JSON in every pinned item, and the SHAs equal the note's probe. So
  the JSON was made on the old tree and holds on the new one. The setup and the pinned fields are
  rulings 1 and 2, with the four wall-clock fields excluded and named.
- **The test.** `test_golden_history` passed alone twice, 11.1 s each at load 6–8. The
  orchestrator ran two probes of its own, each reverted. With one hex digit of β = 5.5's
  `samples_sha256` changed, it fails and says the difference lies between excerpts. With `RTOL`
  1.1e-8 in the generator, it fails and names sample 0's `H_Einstein`, then the counts, the
  bounce, the fixed-T values and the SHA.
- **The grep.** Note 01's command was wrong: with `-E`, git's `\b` matches nothing here, so the
  command finds no line at all. The agent ran it with `-P` and recorded the change (deviation 5).
  With `-P` the hits are format 1's pin and comment, the format-2 comment lines naming the removed
  classes, the format-1 golden file, and the three refused names in `test_plot_inputs.py`. Every
  one of them is meant to be there. **Later notes use `git grep -P`.**
- **The registries and dispatch.** One case in `main.py`, `plot_inputs.py`, the parser and
  `plot_by_beta.py`. An unknown type still raises. `compare_stores`' map is `{1: …}`, with nothing
  added to `UNSTORED_TYPE_VALUES`. `EXPONENTIAL_POTENTIAL` is still 1.
- **The tests.** Every changed or renamed method in the log matches its diff. None was deleted.
  `test_kinematic_cap_loop.py`, `test_registered_run_fingerprint.py` and the format-1 golden file
  are unchanged.
- **The format.** `SPEC_DIGESTS[1]` equals `c6a0e72`'s. The orchestrator recomputed
  `spec_digests(factories)` and it equals `SPEC_DIGESTS[2]`: 17 pairs, 14 distinct digests, 23
  classes in the registry. The new golden file is format 2, with `taken` null and no problems.
- **The breakages.** All three diffs were extracted from the log and replayed in `bash` with
  `git apply`. Each failed as the log says (1; 4; 5 failures and 4 errors), and each was reverted
  with the tree clean.
- **The prose and records.** `architecture-summary.md` and `numerical-strategies.md` each carry a
  dated note, and only the lists that describe the tree were edited. On `integrator-remediation`'s
  board the entry moved from §3 to §4. In the index the row is gone and the count is 27, with the
  integrator clause at 8. `black --check` passes on the 21 Python files.
- **The suites** at `a1c88b6`, serially: 18; 61 + 118; 78; 84; 61, all passing (load 5–17).
- **Residue, fixed in the follow-up commit.** "this commit" became `a1c88b6` in the log, this board
  and `integrator-remediation`'s §4. The note is listed in §1. The "Written?" column, which had been
  left ⬜ for all ten prompts, now marks 01 ✅ and 02–10 ✍️. The status line is updated.
- **Not residue, not opened.** The agent's observations (in its log) are 08's. These are
  `plot_by_beta.py`'s unread `potential_types`, the dead without-`n` branch of
  `plot_inputs._potential_payload`, and `main.py`'s one-case `convert_to_potential`.

### 5.3 After prompt 02 (2026-10-21)

Prompt 02 landed in `abccc23` (log [`logs/02-rename-to-scalarhistory.md`](logs/02-rename-to-scalarhistory.md)).
- **N1.** Five files moved by `git mv` (all `R` lines); classes, factories, tables, the drop group
  `scalar-history` (`--drop scalar-model` is refused), `compute_scalar_history`; the column that
  points at a history is `history_serial` on the value and tag tables and `scalar_history_serial`
  on `AdiabaticHistory` and `BBNData`.
- **N2.** `FINGERPRINT_FORMAT` 3; `SPEC_DIGESTS[3]` of 17 pairs and 14 distinct digests, with the
  digests of `ScalarHistory`, `AdiabaticHistory` and `BBNData` moved; the golden file
  `store_fingerprint_jordan-normalization-02.json`.
- **N3.** `CLAUDE.md`, `.documents/architecture-summary.md` and `.documents/numerical-strategies.md`,
  with a dated note in each. The rehearsal parser `stage_counts.py` accepts both names (the user's
  ruling after a stop).
- **N4.** The run scripts are not edited; they do not name the old driver or group (the log).
- **The golden history** passes unchanged; its JSON was not written.
- **Suites:** CosmologyModels 18, ComputeTargets 179 (61 + 118), ObjectFactories 79 (+1, the
  refusal of `--drop scalar-model`), tools 84, RunDrivers 61.
- **Opened:** `[02-the-scalar-history-parent-is-still-called-model]`.

**The stop (2026-10-21).** The agent stopped before committing. The rehearsal parser
`.documents/dk-and-rpk-migration/rehearsal/stage_counts.py` matches `main.py`'s literal
`models skipped because their ScalarModel failed`, so the rename broke
`tools/tests/test_compare_stores.py`'s stage-counts test, which loads the parser by path. Note 02's
ruling 10 had classed all the rehearsal scripts as records, so the parser could not be edited, and
that was wrong for this one: the parser is live tooling, tested here, and 09 runs it. The user
ruled "Edit the parser (Recommended)". Its `SKIPPED` regex now accepts
`(?:ScalarModel|ScalarHistory)`, so dated logs and new ones both parse. The dated outputs under
`.documents/*/out/` are unchanged. The orchestrator ruled on two more points without a stop:
- the format literal in `TestCommandLine.test_store_fingerprint` goes from 2 to 3, as 01 moved it
  from 1 to 2; ruling 6 should have named it;
- ruling 11 was wrong on the facts. The two science-run scripts name neither `plot_ScalarModel.py`
  nor `scalar-model`; each has only a comment quoting a stored failure reason. They are not edited.

**Orchestrator review of prompt 02 (2026-10-21).** Against `abccc23`, by note 02's ten checks. All
pass.
- **Scope.** 67 files, with five `R` lines (similarity 81–95 %) and no deletion. Note 02's 59
  Python files, plus `stage_counts.py` by the user's ruling, plus `CLAUDE.md`, the two documents,
  the index, this board, the golden file and the log. The tree is clean.
- **Nothing but names.** In a temporary worktree at `abccc23`, the orchestrator undid the moves,
  applied the inverse name map to the 59 code files, and diffed against `481b65e`. Every residual
  line is accounted for by one of:
  - `history_serial` on the two `ScalarHistory` child tables (ruling 2, read by eye);
  - ruling 5's test and `--drop` help;
  - format 3's docstring, `GOLDEN` and literal;
  - the parser's regex;
  - black's re-wrapping and magic trailing commas.

  The map's apparent `integrate_scalar_history` → `integrate_scalar_model` residue is an artefact of
  the map: `integrate_scalar_history` is the step loop's name since `integrator-remediation`,
  unchanged here.
- **The golden history.** The orchestrator ran the generator at `abccc23` into the scratchpad.
  `histories` equals the committed JSON in every pinned item, excerpts and SHAs included. The JSON
  is unchanged (SHA-256 `6898a432…62d6`), and `test_golden_history` passes alone.
- **The grep.** Ruling 9's command finds only its four kinds of hit, and `CLAUDE.md` has none.
  Every residual line in the two documents lies in a dated note (`> **Note, …`, `**Note added …`)
  or a dated "Added" section.
- **The columns.** The `ForeignKey`, `Parent`, query and raw-SQL uses agree with ruling 2.
  `AdiabaticHistory`'s own `history_serial` is untouched. **Probe:** with `redshift.py`'s join
  column changed to a name that does not exist, ObjectFactories, ComputeTargets (61 + 118) and
  tools all pass. Tools has one failure, in `test_registry_config`, and that failure belongs to
  running in a worktree (the registry resolves its root to the main checkout). So no test
  exercises `sqla_redshift_factory.read_table`'s `model_proxy` join, and no code passes
  `model_proxy` to it. This is an observation for 03, which rewrites the redshift factory (Z2),
  not a finding.
- **The moves and drivers.** `git log --follow` on each new path reaches its history. `main.py
  --help` and `plot_ScalarHistory.py --help` exit 0. `--drop scalar-model` exits 2, naming
  `scalar-history`, and creates no file.
- **Format 3.** The orchestrator recomputed `spec_digests(factories)` and it equals
  `SPEC_DIGESTS[3]`: 17 pairs, 14 distinct digests, 23 classes. The digests that moved are those
  of `ScalarHistory`, `AdiabaticHistory` and `BBNData`. `[1]` and `[2]` equal `d56f908`'s. The diff
  deletes only the format number and one comment line it extends. The new golden file is format 3,
  with `taken` null and no problems. The two older golden files are unchanged.
- **The breakages.** Both diffs were extracted from the log and replayed in `bash`. The first gives
  2 failures; the second gives 3 failures and 3 errors (`KeyError: 'model_serial'`). Each matches
  the log, and each was reverted with the tree clean.
- **The suites** at `abccc23`, serially: 18; 61 + 118; 79; 84; 61, all passing (load 19–64).
  `black --check` passes on the 60 Python files.
- **The records.** This board, the index (row in §1.16, 28 open) and the log are in §5.1's form,
  with every deviation classified.
- **Residue, fixed in the follow-up commit.**
  - "this commit" becomes `abccc23` in the log and on this board.
  - The index header's `jordan-normalization` clause began "none on the … board"; it now reads
    "1 on the".
  - Note 02 is listed in §1, and the status line is updated.
- **Not residue, recorded only.** The commit message's first paragraph says that `BBNData`'s own
  `history_serial` already names an `AdiabaticHistory` row. Only `AdiabaticHistory`'s child tables
  carry `history_serial`; `BBNData`'s use `bbn_serial`. The code and the log are right, and commits
  are not amended (`CLAUDE.md` invariant 1).

### 5.4 After prompt 03 (2026-10-22)

Prompt 03 landed in `bd9ae42` (log [`logs/03-the-sampling-axis.md`](logs/03-the-sampling-axis.md)),
dispatched with orchestrator note 03, whose fourteen rulings govern.
- **Z1.** `redshift(store_id, log_one_plus_z)`: ln(1 + z_E) is the stored label. `z` is a derived
  property (`expm1`, `inf` past float range), and `float()` returns ln(1 + z). Ordering, sorting and
  every truncation mode compare ln(1 + z), with tolerance 1e-10; equality and hashing stay on
  `store_id`.
- **Z2.** The table `redshift` stores `log_one_plus_z`. The lookup is one range filter of width
  ±1e-10 (`DEFAULT_LOG_ONE_PLUS_Z_PRECISION`), and `EXPLAIN QUERY PLAN` shows that it searches the
  index. 75 001 points took 7.0 s into an empty table and 5.1 s over a filled one.
  `FINGERPRINT_FORMAT` 4: 17 pairs and 14 distinct digests, with only `redshift`'s moved. Its golden
  file is `store_fingerprint_jordan-normalization-03.json`.
- **Z3.** `log_one_plus_z_grid(S, H)` builds the points `k * DELTA`. `main.py`, the golden history,
  `tools/history_and_bbn.py` and `test_fixed_T_values.py` use it. The default of
  `--log10-one-plus-z-high` is 300 (it was 35). `--log10-one-plus-z-low` accepts only 0.
- **Z4.** N_backward is the axis value. The truncation is the helper `truncate_to_history`. The
  "grid too short" error compares axis values.
- **Z5.** `log_one_plus_z_Jordan` is the last field of `SampleValues` and a non-null value column.
  The identity holds exactly at every golden sample. Against `cosmology.z`(T_J) the largest
  difference is 3.07e-7 at β = 1.5 and 3.72e-8 at β = 5.5, with bounces included.
- **Z6.** The readers are moved: `extract_common`'s event records, `plot_ScalarHistory.py`'s frame,
  `bbn_from_store.py`'s SQL and `inventory_report.py`'s label. `AdiabaticHistory` and `BBNData`
  read the redshift only as a key, and their factories now read `log_one_plus_z`.
- **Z7.** `sqla_ScalarHistoryValue_factory.build` now raises `NotImplementedError`.
- **The change in values (ruling 1).**
  - **Part A**, at fixed positions: the sample counts are equal. Every sample whose `raw_N` is
    bitwise equal on the two trees is bitwise equal in every field. `raw_N` differs by 1 ulp of
    `final_N` at 1 sample (β = 1.5) and at 2 samples (β = 5.5). The largest relative difference
    there is 6.6e-12, in `reflecting_term` at β = 5.5, sample 4803.
  - **Part B:** `n_samples` went 2722 → 2723 at β = 1.5 and stayed 4894 at β = 5.5.
- **The golden history** was regenerated as `golden_history_03.json`; 01's file is untouched.
  `test_golden_history` passes alone in 10.0 s.
- **The stop (2026-10-22).** The agent stopped before committing.
  `tools/tests/test_compare_stores.py::TestChamPBHSchema::test_14_a_store_with_champbhs_schema`
  errored on the new non-null value column, because its fixture writes ChamPBH's real schema and
  names the value columns. The orchestrator ruled that the fixture gains `"log_one_plus_z_Jordan"`,
  with no assertion changed. It also confirmed that `test_inventory_report.py:284` follows `:364`.
- **Suites:** CosmologyModels 18, ComputeTargets 188 (61 + 127, +9), ObjectFactories 86 (+7),
  tools 84, RunDrivers 61, all passing.
- **Closed:** `[01-scalarmodelvalue-build-checks-the-stored-phi-against-pi]`,
  `[05-the-value-factory-compares-stored-phi-against-pi]` and
  `[09-redshift-lookup-at-zero-takes-abs-of-the-comparison]`.
- **Opened:** `[03-scalar-history-functions-splines-z-but-evaluates-ln-one-plus-z]`.
- **Index:** `.documents/OPEN_ISSUES.md` goes from 27 to 25 open.

**Orchestrator review of prompt 03 (2026-10-22).** Against `bd9ae42`, by note 03's ten checks. All
pass.
- **Scope.** 33 files: five added, 28 modified, none deleted; only note 03's §2 files. The tree is
  clean and `git worktree list` shows only the main checkout.
- **Part A, again.** The orchestrator reran ruling 1's comparison from its own script, with the
  old tree in a temporary worktree at `9997eef` (z = `expm1(kΔ)`, k = 0 … 21250) and the new tree on
  the axis. The counts are equal (2723, 4894). Every sample with bitwise-equal `raw_N` is bitwise
  equal in every field. `raw_N` differs by exactly 1 ulp of `final_N` at samples 2715 (β = 1.5),
  4798 and 4803 (β = 5.5). Every field's largest relative difference there equals the log's,
  6.6e-12 in `reflecting_term` the largest.
- **The golden history.** The generator, run at `bd9ae42` into memory, equals
  `golden_history_03.json` in every pinned item (SHA-256 `f1fbc535…dab5`). `n_samples` are 2723 and
  4894, as note 03 predicted. `golden_history_01.json` is unchanged (`6898a432…62d6`).
  `test_golden_history` takes 11.4 s alone.
- **ln(1 + z_J),** read from the stored field: 0.0 at k = 0. Against `cosmology.z`(T_J), the largest
  difference is 3.072e-7 at sample 2168 (β = 1.5; 1.547e-7 over the 448 samples with H_J < 0) and
  3.720e-8 at sample 4349 (β = 5.5; 2.365e-8 over 1497). The note's probe gave 3.27e-7 at sample
  2167 on the old grid. The samples moved with the grid, which accounts for the difference.
- **The axis.** `test_sampling_axis` and `test_redshift_axis` (15 tests) pass under `python -W
  error`. The 400-decade test cuts at N = 800 and reads `inf` without raising. The "too short"
  raise is inline, with its test.
- **The factory.** The lookup is the one range filter. The orchestrator's own `EXPLAIN QUERY PLAN`
  gives `SEARCH redshift USING COVERING INDEX ix_redshift_log_one_plus_z`. **Probe:** with the old
  `abs((col - x) < tol)` filter put back at x = 0, issue 09's test fails and the rest pass; the
  probe was reverted.
- **Format 4.** `SPEC_DIGESTS[4]` equals `spec_digests(factories)`, recomputed: 17 pairs, 14
  distinct digests, only `redshift` moved. The diff of `config/fingerprint.py` removes only the
  format line and the one comment line it extends, so `[1]`–`[3]` are byte-identical. The new
  golden file is format 4, with `taken` null and no problems; the three older files are unchanged.
- **The readers.** The grep of note 03's check 8 finds only what rulings 4, 7, 9 and 10 leave:
  `functions` (the new issue), the plot x-coordinate, `CompareMathematica`'s frame column,
  `cosmology.z`, `as_float_list`, and the keys by `store_id`. Every `order by` on the redshift uses
  `log_one_plus_z`. `main.py --help` exits 0 and shows the default of 300. The refusal exits 2
  naming the option, and creates no file.
- **The breakages.** All three were extracted from the log and replayed in `bash`, each against
  `test_sampling_axis`, `test_redshift_axis` and `test_golden_history`. Each fails the tests the log
  names. Breakage 2 also fails both golden tests, which the log did not run against it: the wider
  tolerance keeps an extra point past `final_N`. That is more coverage, not a discrepancy. After
  each revert the tree is clean.
- **The records.** The `dk-and-rpk-migration` and `science-readiness` boards have each entry in §4
  with a **Resolved** line, and a dated note in §3. The index has 25 open (25 rows), with the
  clauses and the date. `CLAUDE.md`'s bullet is the only change there. The log is in §5.1's form,
  with seventeen deviations classified and no `UNINTENDED DRIFT`.
- **The suites** at `bd9ae42`, serially: 18; 61 + 127; 86; 84; 61, all passing (load 10–55).
  `black --check` passes on the 25 Python files of the commit.
- **Residue, fixed in the follow-up commit.**
  - "this commit" becomes `bd9ae42` in the log (×6), on this board (×8) and on the
    `dk-and-rpk-migration` (×2) and `science-readiness` (×1) boards. The index had none.
  - Note 03 is listed in §1, and the status line is updated.
- **Not residue, recorded only.**
  - The log's observation that `.documents/architecture-summary.md:636` still lists the deleted
    `DEFAULT_REDSHIFT_PRECISION` is 10's (C1).
  - `extract_common.py:421`'s string-literal comparison (`"x_coord" == "efolds"`) is offered to the
    user as housekeeping, as note 03 said.

### 5.5 After prompt 04 (2026-10-25)

Prompt 04 landed in this commit (log [`logs/04-the-model-interface.md`](logs/04-the-model-interface.md)),
dispatched with orchestrator note 04, whose sixteen rulings govern, and with two more rulings made
after a stop (below).
- **M1–M3.** `CosmologyConcepts/Models/`:
  - `AbstractModel`, with hooks that take no φ_ref;
  - `RealizedModel`, immutable, under the integrator's method names, with A(φ_ref) = 1 enforced in
    the base class;
  - `Seed`;
  - `ExponentialModel(store_id, M, n, beta, units)`, with `type_id` `EXPONENTIAL_MODEL = 1` and
    `shard_key` β;
  - the adapter `ExponentialModel.from_rows`, with normalization `4.0 * log(float(Λ))` and φ_ref 0.0.

  `effective_minimum` is the generic `brentq` (no closed form; note ruling 4). The Σ ρ_R,E term is
  left out, since `1.0 - 3.0 * cosmology.w(T_CMB)` is 0.0 exactly (w = 0.3333333333333333).
  `seed` gives tracking, else static, else plain.
- **M4, M5.** The consumers take one `RealizedModel`:
  - `ODEPolicy`, `HubblePolicy`, `PotentialDerivativePolicy` (its plain-V paths removed),
    `fixed_T_value_at`, `fixed_T_values`, `surfing_turning_points`, `compute_scalar_history` and
    `AdiabaticComputePolicy`;
  - `ScalarHistory.realized_model` is lazy and cached, and `compute_adiabatic_values` reads it;
  - `tools/history_and_bbn.py` and `tools/surfing_scan.py` pass the adapter's model.

  `BBNData`, `pipeline_selection.py`, `main.py`, `plot_by_beta.py`, `plot_ScalarHistory.py` and
  `extract_common.py` are not changed (note rulings 8–10).
- **M6.** The region properties and `default_max_step` are deleted, and the witness grep finds
  nothing. `[00-region-properties-on-the-potentials-become-unread]` is closed.
- **M7.** `ComputeTargets/tests/test_model_interface.py`, 17 tests, (a)–(h):
  - (c), the shift law: the largest difference on the scale 1 + f_m + |r| is 2.685e-14; the
    largest plain relative difference is 2.789e-11, at sample 2024;
  - (d), Eq. 7: 4.251e-14 (β = 1.5, sample 598) and 6.875e-14 (β = 5.5, sample 74).

  Seven existing test files were changed (six test modules and `golden_history.py`; listed in
  the log).
- **The golden history** passes unchanged through the adapter, and no JSON was written.
  `test_golden_history` alone took 28.4 s before (load 12) and 58.4 s and 9.5 s after (load 19 and
  10). Back to back, a0ca6a1 took 10.9 s and 24.7 s, and this tree 13.5 s and 20.3 s. The machine's
  load governs these times.
- **Breakages.**
  - Ruling 14's breakage 1, `log(exp(4 log Λ))`, is bitwise a no-op at Λ = 1 meV (and at 1001 Λ
    in 0.5–3 meV), so every test passes under it. The log records it, and replaces it with a one-ulp
    shift (`nextafter`), which fails (a) and both golden histories.
  - Breakages 2 and 3 fail (b) for both models, and (c) at every sample.
- **Suites:** CosmologyModels 18, ComputeTargets 205 (61 + 144, +17), ObjectFactories 86, tools 93,
  RunDrivers 61, all passing.
- **Closed:** `[00-region-properties-on-the-potentials-become-unread]`. **Opened:** none.
- **Index:** `.documents/OPEN_ISSUES.md` goes from 25 to 24 open.

**The stop (2026-10-25).** The agent stopped before committing, with two questions.
- **Q1.** `test_adiabatic_mass.py`'s test (e) (three tests) errored:
  - its stand-in history, a `SimpleNamespace` with `potential=None, coupling=None`, has no
    `realized_model`;
  - its `_StandInPolicy.__init__` took `(task_label, cosmology, potential, coupling)`.

  Note ruling 13 named only `_policy`'s stand-in in that file. **Ruled by the orchestrator:** the
  stand-in history carries `realized_model=None`, and `_StandInPolicy.__init__` takes
  `(task_label, cosmology, model)`. No assertion changes.
- **Q2.** Test (c) as ruling 13 built it (`fixed_T_value_at`, then 1 + r + f_m, at 1e-12 relative)
  failed on 23–31 of the 2723 samples per δ, at most by 2.8e-11, all at the T_J turning points.
  There 1 + βφ′/M_P ≈ −2.2e-3 and H_J ≈ 0, so 1 + r + f_m ≈ 5e-6 is a cancelled sum, and the
  relative difference measures r's rounding. **Ruled by the orchestrator (option A):** keep
  `fixed_T_value_at` and 1e-12, but measure against (1 + f_m + |r|) e^{−2βδ/M_P}, the rounding scale
  of the summed terms. The orchestrator reproduced 2.7e-14 on that scale with the agent's probe.
- **The flaky wall-clock test.** `test_prym_passenger.test_a_oscillating_case_completes` (a 60 s
  bound) failed before any edit: 93.9 s in the suite (load 7–12) and 76.5 s alone (load 41). It
  passed in both after-runs: in the suite, and alone in 28.4 s (load 11). The orchestrator ruled it
  not a stop: it is recorded in the log's "Observations not acted on", no issue is opened, and the
  test is not edited.
