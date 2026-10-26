# jordan-normalization campaign — implementation state

**Last updated:** 2026-10-26 · **Status: LIVE — 10 of 10 prompts written, 8 landed (01–07 reviewed;
04 and 08 each after one stop ruled by the orchestrator); R1–R14, U1–U5 and P1–P20 ruled by the user
2026-10-20 (README §6). Next: the orchestrator review of 08.**
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
| 04 | The model interface | `AbstractModel`, `ExponentialModel`, `RealizedModel`; consumers on it via an adapter; φ_ref = 0 bitwise; the shift law | Opus | ✍️ | ✅ | `597a215` | [`logs/04-the-model-interface.md`](logs/04-the-model-interface.md) |
| 05 | The Planck mass | one reduced Planck mass from PRyMordial's G_N in three unit systems; consistency tests | Sonnet | ✍️ | ✅ | `1675e73` | [`logs/05-the-planck-mass.md`](logs/05-the-planck-mass.md) |
| 06 | The tuning layer | `integrate_history` and `tune_history`: secant, seed, residuals, failure reasons, progress lines | Opus | ✍️ | ✅ | `1b83bbd` | [`logs/06-the-tuning-layer.md`](logs/06-the-tuning-layer.md) |
| 07 | The store | `ExponentialModel` table; `ScalarHistory`'s key and tuned outputs; format 5; `VERSION_LABEL` 2026.7.0; writers and readers moved | Opus | ✍️ | ✅ | `223afc6` | [`logs/07-the-store.md`](logs/07-the-store.md) |
| 08 | The driver and the tools | `main.py`'s options; the super-Planckian warning; the wrapper's scope; the tools and the surfing scan on tuned histories | Opus | ✍️ | ✅ | this commit | [`logs/08-the-driver-and-the-tools.md`](logs/08-the-driver-and-the-tools.md) |
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
- [`orchestrator/prompt-04.md`](orchestrator/prompt-04.md) (`a0ca6a1`), used for 04. Two gaps in its
  rulings were ruled after the agent stopped: the second stand-in in `test_adiabatic_mass.py`, and
  test (c)'s criterion at the T_J turning points. Its breakage 1 was a bitwise no-op (§5.5).
- [`orchestrator/prompt-05.md`](orchestrator/prompt-05.md) (`91fcbf9`), used for 05. Its probe
  found twelve existing tests pinning values on the old units, which the user ruled re-pinned in
  05 (its ruling 13); no stop (§5.6).
- [`orchestrator/prompt-06.md`](orchestrator/prompt-06.md) (`8db352b`), used for 06. The
  orchestrator built the tuning loop and ran the thirteen points with both methods before
  dispatch; the agent's comparison equals it to the digit; no stop (§5.7).
- [`orchestrator/prompt-07.md`](orchestrator/prompt-07.md) (`83569ce`), used for 07. Its probe
  showed that one integration at the tuned golden's stored values reproduces its record, so the
  round-trip test needs no re-tuning, and that SQLite ignores `Float(32)`. Its dispatch brief
  excluded `test_golden_history.py`, whose docstring ruling 5 named; the review corrected it. No
  stop (§5.8).

### 1.1 Items

| Prompt | Items |
|---|---|
| 01 | X1 the golden history · X2 the classes deleted · X3 the registries · X4 the tests · X5 format 2 |
| 02 | N1 the code, tables, columns and drop group · N2 format 3 · N3 the current documents · N4 the run scripts left as records |
| 03 | Z1 the concept ✅ · Z2 the factory and format 4 ✅ · Z3 the grid ✅ · Z4 the sampling ✅ · Z5 ln(1 + z_J) ✅ · Z6 the readers ✅ · Z7 the value factory's issues ✅ (all landed in `bd9ae42`) |
| 04 | M1 `AbstractModel` ✅ · M2 `RealizedModel` ✅ · M3 `ExponentialModel` ✅ · M4 the consumers ✅ · M5 the adapter ✅ · M6 the unread properties ✅ · M7 the tests ✅ (all landed in `597a215`) |
| 05 | K1 one constant ✅ · K2 the three unit systems ✅ · K3 the tests ✅ (all landed in `1675e73`) |
| 06 | T1 `integrate_history` ✅ · T2 the residuals ✅ · T3 the secant and its guards ✅ · T4 the seed ✅ · T5 the failures ✅ · T6 progress ✅ · T7 the outputs ✅ (all landed in `1b83bbd`) |
| 07 | S1 the model's table ✅ · S2 the key ✅ · S3 the outputs ✅ · S4 the compute path ✅ · S5 `main.py` ✅ · S6 the readers ✅ · S7 format 5 and 2026.7.0 ✅ · S8 the tests ✅ (all landed in `223afc6`) |
| 08 | D1 the options ✅ · D2 the warning ✅ · D3 the wrapper's scope ✅ · D4 `history_and_bbn.py` ✅ · D5 the surfing tools ✅ · D6 `histories.csv` ✅ · D7 the tests ✅ (all landed in this commit) |
| 09 | H1 reference · H2 resumed · H3 the comparisons · H4 the measurements · H5 the document |
| 10 | C1 the documents · C2 `CLAUDE.md` · C3 the handover · C4 the board and indexes · C5 the final suites |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Strip | only the exponential potential and coupling remain; the golden history, generated at `ac187c8`, passes bitwise | 01 | ✅ |
| Rename | no `ScalarModel` name outside the records; the golden history bitwise | 02 | ✅ |
| Axis | the axis is ln(1 + z_E) with no overflow; ln(1 + z_J) agrees with `cosmology.z(T_J)` and with the A identity; the change in values measured | 03 | ✅ |
| Model | the golden history bitwise through `ExponentialModel` at φ_ref = 0, V₀ = Λ⁴; the shift law holds | 04 | ✅ |
| Units | one Planck mass equal to PRyMordial's; 1 K, ħc, ħ and the kilogram consistent to ≤ 10⁻⁶ in each unit system | 05 | ✅ |
| Tuning | a tuned history converges inside the tolerance, deterministically; the secant is no worse than the plain update on README §0.3's grid | 06 | ✅ |
| Store | a tuned history round-trips bitwise and its realized model is rebuilt equal; the golden fingerprint at format 5 | 07 | ✅ |
| Driver | the old options are refused; the surfing scan records tuned values with its roots unchanged | 08 | ✅ |
| R | on the isolated cluster every history converged, the stored values equal the tuning layer's, and the resumed store equals the reference | 09 | ⬜ |

## 3. Active and unresolved issues

Prompt 02 opened one, prompt 03 one and prompt 08 one; prompt 07 narrowed prompt 02's. Prompt 01
closed one assigned issue, prompt 03 three, prompt 04 one, prompt 07 two and prompt 08 the last
(§3.1, §4).

### `[02-the-scalar-history-parent-is-still-called-model]`

**Opened:** 2026-10-21 by prompt 02 (ruling 3 of its orchestrator note).

- **What.** The rename changed every name that says `ScalarModel`. Lowercase names that mean a scalar
  history but do not say it were left: the parent field `"model"` in the inventory specs of
  `AdiabaticHistory` and `BBNData`; `tools/inventory_report.py`'s `model_text_of`, `model_parts`,
  `model_sort` and its printed `model: {…}`; the payload key `model_proxy` and the attribute
  `_model_proxy`; locals and parameters named `model`.
- **Impact.** None to values. Once 07 gives `model` to the Lagrangian, `model` would mean two things.
- **Next step.** 07, which rewrites these specs and readers, renames them. Not assigned yet.
- **Narrowed (2026-10-26) by prompt 07** (`223afc6`; note 07, ruling 13): the parent field
  `"model"` of the `AdiabaticHistory` and `BBNData` specs is `"history"` (the column
  `scalar_history_serial` is unchanged), and `tools/inventory_report.py` says "history" where it
  said "model" (`history_text_of`, `history_parts`, `history_sort`, `history_extras`,
  `HISTORY_FIELDS`, `HISTORY_EXTRA_FIELDS`, `HISTORY_TYPE_LEAVES`, the printed `history: {…}` and
  "common to every history"). What remains: the payload key `model_proxy`, the attribute
  `_model_proxy`, and locals and parameters that name a scalar history `model` (in `main.py`,
  `ComputeTargets/AdiabaticHistory.py`, `ComputeTargets/BBNData.py` and the plot scripts), now that
  `model` is also the Lagrangian (`ScalarHistory.model`). Impact: none to values.

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

### `[08-a-history-that-did-not-converge-is-never-retried]`

**Opened:** 2026-10-26 by prompt 08 (ruling 14 of its orchestrator note).

- **What.** `--tuning-max-integrations` (default 30) caps one history's tuning, but it is not a key
  of the history: `ScalarHistory.compute(label, max_integrations)` passes it to
  `compute_scalar_history.remote`, and the factory's key holds the tuning tolerance and no cap. A
  history that does not converge within the cap is stored as a failure under its key.
  `pipeline_selection.missing_models` does not count a stored failure as missing (prompt 07, note
  07 ruling 8), and `main.py` has no retry for the scalar stage (only `--retry-failed-bbn` for BBN).
  The option's help says so.
- **Impact.** A later run with a larger cap recomputes nothing. The only route is
  `--drop scalar-history`, which drops every history. No history has failed to converge at any
  point measured (prompt 06's thirteen points: 3–12 integrations).
- **Next step.** For the user to rule: a retry flag for the scalar stage like `--retry-failed-bbn`,
  or the cap in the key. Not assigned.

### 3.1 Assigned to this campaign

Assigned 2026-10-20 by the user (README §6.1, U3). Each entry stays on its own board, which carries
the **Assigned** line; the prompt named closes it there, in that board's §4, and deletes its row
from `.documents/OPEN_ISSUES.md` §1.15.

| Issue | Its board | Prompt | Why here |
|---|---|---|---|
| `[00-declare-reflects-at-origin-for-the-other-potentials]` | `integrator-remediation` | 01 — closed (`a1c88b6`) | moot once the four other potentials are deleted |
| `[00-region-properties-on-the-potentials-become-unread]` | `integrator-remediation` | 04 — closed (`597a215`) | `AbstractModel` does not carry the properties over |
| `[01-scalarmodelvalue-build-checks-the-stored-phi-against-pi]` | `dk-and-rpk-migration` | 03 — closed (`bd9ae42`) | the value factory is rewritten for the axis |
| `[05-the-value-factory-compares-stored-phi-against-pi]` | `science-readiness` | 03 — closed (`bd9ae42`) | the same defect, opened twice |
| `[09-redshift-lookup-at-zero-takes-abs-of-the-comparison]` | `dk-and-rpk-migration` | 03 — closed (`bd9ae42`) | the lookup moves to ln(1 + z) by absolute difference |
| `[02-scalar-stage-missing-filter-keeps-every-model]` | `dk-and-rpk-migration` | 07 — closed (`223afc6`) | the scalar stage's lookups are rewritten for the new key |
| `[03-step-1-first-pass-lookup-filters-nothing]` | `run-integrity` | 07 — closed (`223afc6`) | the same code |
| `[05-surfing-scan-runs-at-a-fixed-lambda]` | `surfing-temperature` | 08 — closed (this commit) | the surfing tools move onto tuned histories |

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
- `[00-region-properties-on-the-potentials-become-unread]` — closed by prompt 04 (`597a215`): the
  five region properties are deleted from `AbstractPotential` and `ExponentialPotential`, and
  `default_max_step` from `ExponentialPotential`; the witness grep (note 04, ruling 12) finds
  nothing. The entry, with its **Resolved** line, is in `integrator-remediation`'s §4; its row is
  deleted from `.documents/OPEN_ISSUES.md` §1.15.
- `[02-scalar-stage-missing-filter-keeps-every-model]` and
  `[03-step-1-first-pass-lookup-filters-nothing]` (one defect, opened twice) — closed by prompt 07
  (`223afc6`): the scalar stage's first pass reads its lookup results through
  `pipeline_selection.missing_models`, so a fully stored batch schedules no work, tested by
  `test_pipeline_selection.TestScalarStageFirstPass` (four of its five tests fail with the old
  logic put back). The entries, with their **Resolved** lines, are in `dk-and-rpk-migration`'s §4
  and `run-integrity`'s §4; their rows are deleted from `.documents/OPEN_ISSUES.md` §1.15.
- `[05-surfing-scan-runs-at-a-fixed-lambda]` — closed by prompt 08 (this commit): the surfing tools
  tune each β to T_CMB standalone and analyse a window ending at the first crossing of
  T_J = 1 MeV; the CSV holds the tuned values per row, and refuses a fixed-Λ CSV on resume. A
  short tuned scan (β = 1.50, 3.00, 4.00) reproduces the committed rows: roots to 5 × 10⁻¹⁵, capture
  flags and kicks exactly, every other column to at most 1.1 × 10⁻⁷. The entry, with its
  **Resolved** line, is in `surfing-temperature`'s §4; its row is deleted from
  `.documents/OPEN_ISSUES.md` §1.15, which is now empty.

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

Prompt 04 landed in `597a215` (log [`logs/04-the-model-interface.md`](logs/04-the-model-interface.md)),
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

**Orchestrator review of prompt 04 (2026-10-25).** Against `597a215`, by note 04's ten checks. All
pass.
- **Scope.** 24 files: seven added, 17 modified, none deleted; only note 04's §2 files.
  `pipeline_selection.py`, `main.py`, `plot_by_beta.py`, `plot_ScalarHistory.py`,
  `extract_common.py` and `ComputeTargets/BBNData.py` are unchanged. The tree is clean, and
  `git worktree list` shows only the main checkout.
- **The golden history.** `ComputeTargets/tests/data/` is unchanged. The orchestrator ran
  `build_record()` at `597a215` in the scratchpad, and both histories equal
  `golden_history_03.json`'s records, with `first_difference` empty. `test_golden_history` passed
  in the suite.
- **The expressions.** `ExponentialModel`'s hooks were read line by line against `a0ca6a1`'s
  `ExponentialPotential` and `ExponentialCoupling`. They are the same expressions with
  `_log_Lambda_4` replaced by the normalization, and the adapter's normalization is
  `4.0 * log(float(potential._Lambda))`. Test (a) asserts with `assertEqual`.
- **φ_ref.** It enters only `RealizedModel.__init__` (`_log_Omega_ref`) and
  `RealizedModel.log_Omega`, and no hook takes it. Test (b) runs on `_MinimalModel` as well.
- **The identities, rerun** through the realized model by the orchestrator's own probe. Eq. 7:
  4.251e-14 (β = 1.5) and 6.875e-14 (β = 5.5). The shift law, formed directly at every sample of
  both histories: 2.69e-14 and 3.08e-14. Both agree with note 04's §0 and with the log.
- **The seeds and the minimum.** `ExponentialModel.seed` gives the following:
  - β = 1.5, M = 1e-3: `tracking`, Λ = 2.110131124 meV;
  - β = 1.5, M = 0.5: `static`, 1.943956227 meV;
  - β = 3, M = 0.1: `static`, 2.050746635 meV;
  - β = −1.5, and n = 2: `plain`, φ_ref 0.

  Each equals note 04's §0 table. `effective_minimum` at β = 1.5 (Λ = 2.125913 meV, φ₀ = 0.0334 M_P)
  agrees with the orchestrator's own bisection to 1.2e-15 (φ_min/φ₀ = 1.042).
- **The consumers.** The remaining `potential` and `coupling` names in the compute layer and the two
  tools are the rows kept on the objects, the adapter, labels, comments and the tools' row
  construction. No computation reads a row. `main.py --help` exits 0.
- **M6.** The witness grep finds nothing (exit 1). The issue is in `integrator-remediation`'s §4
  with its **Resolved** line, and the index row is gone (24 open, the clauses and the date
  correct).
- **The breakages.** The log's three diffs were extracted and replayed in `bash`, against
  `test_model_interface` and `test_golden_history`, and each was reverted with the tree clean.
  - Breakage 1 (the one-ulp shift) fails (a) and both golden histories.
  - Breakages 2 and 3 each fail (c) and (b), for both models.
- **Breakage 1 as ruled was the note's error.** The orchestrator confirmed that
  `log(exp(4.0 * log(1 meV)))` equals `4.0 * log(1 meV)` bitwise (`-0x1.17df23d5ed618p+8`). The
  agent's substitution of a one-ulp shift is accepted as recorded (log Deviation 16).
- **The suites** at `597a215`, serially, at load 6–35: 18; 61 + 144; 86; 93; 61, all passing.
  `test_prym_passenger`'s wall-clock test passed. `black --check` passes on the 19 Python files.
- **Residue, fixed in the follow-up commit.**
  - "this commit" becomes `597a215` in the log (×5), on this board (×6) and on the
    `integrator-remediation` board (×1).
  - Note 04 is listed in §1, and the status line is updated.
- **Not residue, recorded only.** The log's observations are left as they are, with no issue
  opened: the dated audit harness, the unread `default_abs_tol`/`default_rel_tol`, the
  diagnostic's "potential" label, and §7.2 of `architecture-summary.md` (10's).

### 5.6 After prompt 05 (2026-10-26)

Prompt 05 landed in `1675e73` (log [`logs/05-the-planck-mass.md`](logs/05-the-planck-mass.md)),
dispatched with orchestrator note 05, whose fourteen rulings govern, and with the user's ruling
"Re-pin in 05" (ruling 13).
- **K1.** `Units/base.py`: `NEWTON_G_MEV_MINUS_2 = 6.70883e-45` (PRyMordial's `GN`, MeV⁻²) and
  `REDUCED_PLANCK_MASS_EV = 1.0e6 / sqrt(8.0 * pi * NEWTON_G_MEV_MINUS_2)` = 2.4353235955263046e+27
  (`0x1.f79cf52984fafp+90`), 2.7775e-4 below 2.436e27.
- **K2.** `Planck_units.eV` and `Mpc_units.eV` are `PlanckMass / REDUCED_PLANCK_MASS_EV`, and
  `GeV_units.PlanckMass` is `REDUCED_PLANCK_MASS_EV * eV`. No literal changed.
- **K3.** `CosmologyModels/tests/test_planck_mass.py`, 8 tests:
  - PRyMordial's `GN`, parsed from the text of `PRyM/PRyM_init.py` and compared as decimals (its
    expression is one ulp below the literal), `Mpl = 1./np.sqrt(GN)` present, and
    √(8π) M_P equal to 1e6/√G to 1e-15;
  - in each system, `PlanckMass / eV` equal to the constant to 1e-15, and 1 K, 1/eV (m, s) and
    1 kg against CODATA 2018 to 1e-6 (measured 1.70e-7, 4.08e-8, 1.58e-7, 2.13e-7; 2.78e-4 before).
- **The change in values** (both golden histories, old against new at equal index; labels equal,
  `n_samples` 2723 and 4894 unchanged). The table is the agent's, and equals note 05's §0 in every
  digit shown.

| field | β = 1.5: max (sample) | > 10⁻² | β = 5.5: max (sample) | > 10⁻² |
|---|---|---|---|---|
| `raw_N` | 1.388e-04 (0) | 0 | 6.977e-06 (0) | 0 |
| `phi_Einstein` | 4.743e-05 (2024) | 0 | 1.502e-03 (4642) | 0 |
| `pi_Einstein` | 1.388e-04 (0) | 0 | 4.105e-01 (4696) | 7 |
| `log_rhorad_Einstein` | 1.155e-05 (0) | 0 | 6.842e-05 (0) | 0 |
| `log_rhorad_Jordan` | 8.805e-06 (0) | 0 | 8.798e-06 (0) | 0 |
| `log_fm` | 1.958e-05 (2722) | 0 | 1.958e-05 (4893) | 0 |
| `H_Einstein` | 5.554e-04 (1782) | 0 | 5.552e-04 (4287) | 0 |
| `H_Jordan` | 5.719e-04 (1576) | 0 | 6.512e-04 (2440) | 0 |
| `log_T_Jordan` | 8.565e-06 (0) | 0 | 8.558e-06 (0) | 0 |
| `gstar_rho` | 1.533e-07 (2160) | 0 | 9.737e-08 (4327) | 0 |
| `gstar_s` | 1.451e-07 (2135) | 0 | 9.228e-08 (4327) | 0 |
| `dgstar_s_dlogT` | 3.883e-05 (364) | 0 | 8.184e-05 (187) | 0 |
| `dgstar_rho_dlogT` | 2.334e-05 (180) | 0 | 7.227e-05 (408) | 0 |
| `Sigma` | 4.441e-07 (2168) | 0 | 1.539e-06 (4642) | 0 |
| `friction_term` | 1.388e-04 (0) | 0 | 4.105e-01 (4696) | 7 |
| `reflecting_term` | 1.374e-04 (2024) | 0 | 1.023e-01 (4623) | 120 |
| `kicking_term` | 4.481e-07 (2168) | 0 | 1.560e-06 (4642) | 0 |
| `log_one_plus_z_Jordan` | 2.820e-08 (2314) | 0 | 1.541e-05 (4892) | 0 |

  At β = 5.5 three fields exceed 1e-2, in the tail (samples 4308–4893, φ against the wall at
  M/φ ≈ 37–60): `reflecting_term` ∝ e^(M/φ), and `pi_Einstein` and `friction_term` at turning
  points of π inside a bounce (note 05's §0). Amplifications of the 2.8e-4 input, not errors.

  | | β = 1.5 old → new | β = 5.5 old → new |
  |---|---|---|
  | `accepted_steps` | 1231 → 1233 | 26343 → 26331 |
  | `compute_steps` = `RHS_evaluations` | 9887 → 9870 | 232810 → 232530 |
  | first bounce N | 18.64397081911504 → 18.64397081911831 | 32.10765422995129 → 32.10765422995111 |
  | first bounce ln T_J | −43.35467531703388 → −43.35439760833413 | −37.03892534804545 → −37.03864763934282 |
  | `fixed_T_values` | (0.0023279537090081345, −0.05135980555622314, None, None) → (None × 4) | (None × 4) → (1.904834518987192e-05, 0.017290191260207102, None, None) |

  The 1 MeV fixed-T values flip because T_stop is `FIXED_T_JORDAN_HIGH_MEV` in the golden setup
  (a property of the setup, not of 05).
- **The golden history** is `golden_history_05.json`; `_01` and `_03` are byte-identical.
  `test_golden_history` alone took 9.57 s before (load 5.7) and 9.74 s after (load 10.2).
- **Twelve old-unit pins re-pinned** (the user's ruling), in `test_kinematic_cap_loop.py`,
  `test_integrator_exceptions.py`, `test_first_bounce.py` and `tools/tests/test_plot_surfing_histories.py`
  (the committed scan's row is compared to 1e-7, not bitwise: the shift is 2.1e-8). P2's wall bounces
  to N = 40 go from 19 to 20.
- **Not edited** (ruling 5): `test_inventory_report.py`, `test_compare_stores.py`,
  `test_store_fingerprint_golden.py`. They pass unedited.
- **Breakages.** 1: `GeV_units` at 2.436e27 fails five of `test_planck_mass`'s tests, for
  `GeV_units` only. 2: G = 6.70884e-45 fails the decimal comparison and the 1e-15 comparison, both
  golden histories and `test_integrator_exceptions.test_c_physical_values_unchanged`; ruling 4's
  four tests do not fail (7.5e-7 < 1e-6).
- **Suites:** CosmologyModels 26 (+8), ComputeTargets 205 (61 + 144), ObjectFactories 86, tools 102,
  RunDrivers 61, all passing.
- **Closed:** none. **Opened:** none. `.documents/OPEN_ISSUES.md` is unchanged.

**Before dispatch (2026-10-26).** Two things were settled before note 05 was committed (`91fcbf9`).
- **The merge's broken test.** At `5fc02a6` the tools suite ran 102 tests with one error:
  `ray-sqla-align`'s `test_ray_auth.py` read `plot_ScalarModel.py`, which this branch renamed in
  02. At the user's word ("Housekeeping commit now"), the orchestrator fixed it in `cfd2e11`, a
  commit of its own. The test, its method name, the comment in `config/ray_auth.py` and
  `CLAUDE.md`'s token-mode bullet now name `plot_ScalarHistory.py`.
- **Twelve old-unit pins.** The note's probe of the units change found twelve existing tests
  pinning values on the old units, besides the golden histories:
  - nine in `test_kinematic_cap_loop.py`;
  - three in `test_integrator_exceptions.py`;
  - one in `test_first_bounce.py`;
  - one in `tools/tests/test_plot_surfing_histories.py`, which compares bitwise with the
    committed fine scan.

  These are the integrator audit's references at 10⁻⁵ (one at 10⁻¹⁰). The units change moves
  them by 1–4 × 10⁻⁴, and P2's wall bounces go 19 → 20. The user ruled "Re-pin in 05" (note
  ruling 13, with its table of old and new literals).

**Orchestrator review of prompt 05 (2026-10-26).** Against `1675e73`, by note 05's ten checks. All
pass.
- **Scope.** 16 files: three added, 13 modified, none deleted. They are note 05's §2 files, and
  the three tests of ruling 5 are untouched. The tree is clean, and `git worktree list` shows only
  the main checkout.
- **The expressions.** The `Units/` diff is rulings 1 and 2, with no literal changed.
  `REDUCED_PLANCK_MASS_EV` is `0x1.f79cf52984fafp+90`.
- **The PRyMordial test.** The orchestrator pointed the module at a scratch copy of
  `PRyM_init.py`, and ran five probes:
  - a changed mantissa fails (a) and (c);
  - the exponents redistributed (`6.70883*1.e-38*1.e-7`) pass, since the number is the same;
  - a second `GN` line fails (a) and (c);
  - a redefined `Mpl` fails (b);
  - the unchanged copy passes.
- **The consistency.** Rerun by the orchestrator's own probe, the values are 1.700870e-7,
  4.080485e-8, 1.580230e-7 and 2.130484e-7 in each of the three systems, as the log says.
- **The golden history.** The orchestrator's dump at `1675e73` is **bitwise equal** to its probe
  tree's, in samples, labels and fixed-T values, for both histories. §0's tables therefore hold
  as measured. `first_difference` against `golden_history_05.json` is empty for both. Its
  provenance is `91fcbf9`, 05's working tree. `golden_history_01.json` and `_03.json` are
  unchanged.
- **The re-pins.** The diff of the four files changes only ruling 13's literals, the surfing
  test's two assertions, and the prose ruling 13 names. The orchestrator's re-pin probe at
  `1675e73` is byte-identical to the probe tree's, so every new literal is the tree's own value.
- **The grep.** It finds only ruling 6's stored literals: `test_inventory_report.py` at lines
  130, 273, 312, 328 and 351, and `test_store_fingerprint_golden.py:179`.
- **The breakages.** Both diffs were extracted from the log and replayed in `bash` against six
  modules (52 tests), and each was reverted with the tree clean.
  - Breakage 1 fails five of `test_planck_mass`'s tests.
  - Breakage 2 fails (a) and (c), both golden histories, and `test_c_physical_values_unchanged`.

  Both are as the log says.
- **The prose.** `CLAUDE.md` gains the one sentence. `architecture-summary.md` §8.1 gains the
  dated note, and nothing else in either file changes.
- **The suites** at `1675e73`, serially, at load 5–51: 26; 61 + 144; 86; 102; 61, all passing.
  `black --check` passes on the 11 Python files.
- **Residue, fixed in the follow-up commit.**
  - "this commit" becomes `1675e73` in the log (×5) and on this board (×3).
  - Note 05 is listed in §1, and the status line is updated: the agent's line said "awaiting
    review", over the line width.
  - This paragraph and the one before it.
- **Not residue, recorded only.** The log's three observations stand, with no issue opened:
  PRyMordial's `GN` is one ulp below the literal; the golden setup's T_stop is the 1 MeV fixed
  temperature; and the two fixtures' stored M is now 1.000278 × 10⁻³ M_P.

### 5.7 After prompt 06 (2026-10-26)

Prompt 06 landed in `1b83bbd` (log [`logs/06-the-tuning-layer.md`](logs/06-the-tuning-layer.md)),
dispatched with orchestrator note 06, whose seventeen rulings govern. No stop.
- **T1.** `integrate_history`, a plain function with `compute_scalar_history`'s signature, holds
  its body, moved unchanged but for its own name in six strings. `compute_scalar_history` stays
  the `@ray.remote`, with its signature and docstring, and calls it by name. `main.py` and every
  caller are unchanged. `TestFallbackGone` reads `integrate_history` (ruling 3).
- **T2.** `tuning_converged(residual_H, residual_phi, tolerance)`, with `residual_H = H_J/H0 − 1`
  and `residual_phi = φ_ref/φ_min − 1` from the last sample, by ruling 5's expressions.
- **T3.** `TuningSecant`, Broyden's step in (normalization coordinate, φ_ref/scale): plain first
  (B = −I, G(x) itself), then secant; the twice rule; `guard()` resets. `tune_history`'s guard on
  V_target ≤ 0 halves V(φ₀) and sets φ_ref = φ_min.
- **T4.** The seed is `model.seed(cosmology)`, per tuning, with no cache.
- **T5.** Four first clauses: "the normalization did not converge", "no normalization reaches H0",
  "the effective minimum was not found", and an integration's own reason with
  " (tuning integration k)".
- **T6.** One line per integration, `-- tune <label> it=<k> kind=<seed|plain|secant|guard> …`.
  `StageTracker` ignores it (test (c)).
- **T7.** The final payload plus `TUNING_OUTPUT_KEYS` (fifteen keys, each a `float`, `int` or
  `str`), which 07 stores.
- **The tests.** `ComputeTargets/tests/test_tuning.py`, 21 tests, (a)–(j) of ruling 11, 87.5 s
  alone at load 4.7. The tuned golden history is `golden_history_06_tuned.json`, made by
  `golden_tuned_history.py`: β = 3, M = 0.01 M_P, φ* = 5 M_P, five integrations (plain 1, secant
  3), normalization −277.4502000419897 and φ_ref 0.06222843896351175 M_P.
- **The golden history** `golden_history_05.json` passes unchanged through `integrate_history`
  (9.226 s alone after, 9.354 s before). No existing golden JSON was written.
- **The comparison** (§2 item 2, ruling 13), 26 tunings in eight lanes in 12 minutes at load
  5–16, recorded in [`logs/06-probes/`](logs/06-probes/README.md). **Both tables equal note 06's
  §0 in every integration count, step count and digit shown**, the wall times aside.

| β | M/M_P | φ* | seed | plain: integrations (steps) | secant: integrations (steps) | Λ plain (meV) | Λ secant (meV) | φ_ref/M_P plain | φ_ref/M_P secant |
|---|---|---|---|---|---|---|---|---|---|
| 1.5 | 1e-3 | 5 | tracking | 6 (plain 5) | 4 (plain 1, secant 2) | 2.125321 | 2.125322 | 3.4962045e-02 | 3.4962266e-02 |
| 25 | 1e-3 | 5 | tracking | 6 (plain 5) | 5 (plain 1, secant 3) | 1.842954 | 1.842955 | 6.8089327e-03 | 6.8089580e-03 |
| 5.5 | 1e-3 | 5 | tracking | 6 (plain 5) | 5 (plain 1, secant 3) | 2.020615 | 2.020615 | 1.6719234e-02 | 1.6719361e-02 |
| 3 | 1e-3 | 5 | tracking | 6 (plain 5) | 4 (plain 1, secant 2) | 2.073998 | 2.073996 | 2.3662575e-02 | 2.3662904e-02 |
| 1.1 | 1e-3 | 2 | tracking | 6 (plain 5) | 4 (plain 1, secant 2) | 2.144994 | 2.144996 | 4.1533670e-02 | 4.1533944e-02 |
| 0.5 | 1e-3 | 1 | tracking | 6 (plain 5) | 4 (plain 1, secant 2) | 2.184947 | 2.184949 | 6.3821123e-02 | 6.3821650e-02 |
| 3 | 1e-5 | 5 | tracking | 3 (plain 2) | 3 (plain 1, secant 1) | 2.226473 | 2.226473 | 2.6704632e-03 | 2.6704769e-03 |
| 1 | 1e-5 | 5 | tracking | 5 (plain 4) | 4 (plain 1, secant 2) | 2.236588 | 2.236588 | 4.6642085e-03 | 4.6642162e-03 |
| 3 | 0.01 | 5 | tracking | 7 (plain 6) | 5 (plain 1, secant 3) | 1.831404 | 1.831403 | 6.2228103e-02 | 6.2228439e-02 |
| 1 | 0.03 | 5 | tracking | 8 (plain 7) | 5 (plain 1, secant 3) | 1.881162 | 1.881157 | 2.0284503e-01 | 2.0284635e-01 |
| 3 | 0.1 | 5 | static | 13 (plain 12) | 9 (plain 1, secant 7) | 1.388919 | 1.388919 | 1.4772415e-01 | 1.4772170e-01 |
| 1 | 0.5 | 5 | static | 21 (plain 20) | 9 (plain 1, secant 7) | 1.332252 | 1.332252 | 6.2763553e-01 | 6.2762591e-01 |
| 3 | 0.5 | 5 | static | 23 (guard 1, plain 21) | 12 (guard 1, plain 1, secant 9) | 0.937490 | 0.937486 | 2.7085278e-01 | 2.7084733e-01 |

| β | M/M_P | φ* | \|Δ ln Λ\| | \|Δφ_ref\|/φ_ref | plain's last \|φ_ref/φ_min − 1\| | Λ_secant/Λ_planning − 1 | φ_ref,secant/φ_ref,planning − 1 | wall plain, secant (s) |
|---|---|---|---|---|---|---|---|---|
| 1.5 | 1e-3 | 5 | 3.2e-07 | 6.3e-06 | 2.9e-06 | -2.781e-04 | -3.8e-06 | 66, 43 |
| 25 | 1e-3 | 5 | 3.4e-07 | 3.7e-06 | 7.0e-07 | -2.767e-04 | -6.2e-06 | 732, 648 |
| 5.5 | 1e-3 | 5 | 1.4e-07 | 7.6e-06 | 6.1e-06 | -2.779e-04 | -8.3e-06 | 217, 173 |
| 3 | 1e-3 | 5 | 1.1e-06 | 1.4e-05 | 4.6e-06 | -2.795e-04 | +1.5e-07 | 77, 57 |
| 1.1 | 1e-3 | 2 | 7.8e-07 | 6.6e-06 | 2.7e-06 | -2.774e-04 | +1.0e-06 | 60, 40 |
| 0.5 | 1e-3 | 1 | 1.0e-06 | 8.3e-06 | 1.5e-06 | -2.768e-04 | +7.1e-06 | 36, 24 |
| 3 | 1e-5 | 5 | 2.9e-08 | 5.1e-06 | 1.2e-06 | -2.773e-04 | -8.6e-06 | 213, 214 |
| 1 | 1e-5 | 5 | 3.3e-08 | 1.6e-06 | 1.6e-06 | -2.776e-04 | +3.5e-06 | 43, 35 |
| 3 | 0.01 | 5 | 5.7e-07 | 5.4e-06 | 1.3e-07 | -2.783e-04 | +3.8e-06 | 64, 46 |
| 1 | 0.03 | 5 | 2.2e-06 | 6.5e-06 | 1.1e-06 | -2.894e-04 | +2.7e-06 | 20, 14 |
| 3 | 0.1 | 5 | 1.2e-07 | 1.7e-05 | 9.7e-06 | -2.780e-04 | +1.6e-05 | 77, 56 |
| 1 | 0.5 | 5 | 2.9e-07 | 1.5e-05 | 7.1e-06 | -2.772e-04 | +1.6e-05 | 34, 14 |
| 3 | 0.5 | 5 | 4.2e-06 | 2.0e-05 | 8.2e-06 | -2.647e-04 | +1.2e-05 | 69, 32 |

  The wall times are this machine's at load 5–16. The secant needs fewer integrations at twelve
  points and as many at one (β = 3, M = 10⁻⁵): 73 against 116. The methods differ by at most
  4.2e-6 in ln Λ and 2.0e-5 in φ_ref, against the 1e-4 stop; the four differences above 1e-5 are
  the plain update's slack at M ≥ 0.1 and at β = 3, M = 10⁻³. Λ is 2.77–2.80 × 10⁻⁴ below the
  planning probes at eleven points (05's units). The guard fires only at β = 3, M = 0.5, at
  integration 0, and the twice rule never fires.
- **Breakages.** Convergence on `residual_H` alone fails (a) twice. A seed cached at module level
  fails (h). A `guard()` that keeps B fails (b)'s after-guard test.
- **Suites:** CosmologyModels 26, ComputeTargets 226 (61 + 165, +21), ObjectFactories 86, tools
  102, RunDrivers 61, all passing.
- **Closed:** none. **Opened:** none. `.documents/OPEN_ISSUES.md` is unchanged.

**Before dispatch (2026-10-26).** The orchestrator built the tuning loop as a prototype in the
session scratchpad, at `65af27d`, and ran README §0.3's thirteen points with the plain update and
with the secant (note 06's §0). The secant needed no more integrations at any point, so the
prompt's stop did not arise, and the note fixed the expressions so that the agent's table had to
equal the prototype's. The probe also found that the prompt's `>> tune` line would be read as a
stage banner, that two existing tests read `compute_scalar_history`'s body by name, and that the
prompt's breakage of the guard's reset would fail nothing on the grid. The note ruled all three
(rulings 3, 6, 9 and 14). Baselines at `65af27d`: 26; 61 + 144; 86; 102; 61.

**Orchestrator review of prompt 06 (2026-10-26).** Against `1b83bbd`, by note 06's ten checks. All
pass.
- **Scope.** 39 files, all note 06's §2 files: `ScalarHistory.py`, `test_integrator_exceptions.py`,
  the new `test_tuning.py`, `golden_tuned_history.py` and `golden_history_06_tuned.json`, this board,
  the log and `logs/06-probes/` (the script, `run_all.sh`, `table.py`, the 26 logs, `driver.log`,
  `table.txt` and a README). `CosmologyConcepts/Models/` and `pipeline_selection.py` are unchanged.
  The tree is clean, and `git worktree list` shows only the main checkout.
- **The move.** `integrate_history`'s body is `compute_scalar_history`'s, with the function's name
  changed in its five printed or raised strings and in the assertion's message (Deviation 1,
  accepted: it names the function it is in). `compute_scalar_history` passes every argument by name.
  `golden_history.py`, `test_golden_history.py` and `golden_history_01/03/05.json` are unchanged,
  and the golden history passes in the suite.
- **The loop.** `tune_history`, `_tuning_state`, `TuningSecant` and `tuning_converged` were read
  against rulings 5–10. The expressions are the prototype's, and the order is ruling 7's. The model
  is read only through ruling 4's methods.
- **The comparison, rerun in part.** The orchestrator ran `1b83bbd`'s `tune_history` on β = 3 at
  M = 0.01, 0.1 and 0.5, with both methods. Each result equals the prototype's by `repr`, with 7/5,
  13/9 and 23/12 integrations (plain/secant). Each of the 69 progress lines equals the agent's log
  line for line, the wall time aside.
- **The tests.** `test_tuning.py`'s 21 tests are ruling 11's (a)–(j). They passed in the suite;
  the agent's time alone was 87.5 s at load 4.7.
- **The tuned golden.** Its normalization −277.4502000419897 and φ_ref 0.06222843896351175 equal
  the prototype's at β = 3, M = 0.01 by `repr`, with five integrations (plain 1, secant 3) from the
  tracking seed. Test (e) compares the file with a fresh tuning and passed in the suite.
- **The progress line.** The 189 `-- tune` lines of `logs/06-probes/`, fed to `StageTracker`,
  move no stage, and none matches `_STAGE_BANNER`, `_QUEUE_PROGRESS` or `_QUEUE_COMPLETE`.
  `>> tune …` would match the banner.
- **The breakages.** The log's three diffs were extracted and replayed in `bash`, and each was
  reverted with the tree clean. Breakage 1 fails (a) twice. Breakage 3 fails (b)'s after-guard
  test. Breakage 2 fails (h) with `'-277.581681683' != '-276.998824803'`. All three are as the log
  says.
- **The record.** The orchestrator's own `table.py`, run on its prototype's logs, gives the first
  table identical to `logs/06-probes/table.txt`, and the second identical except the wall times.
- **The suites** at `1b83bbd`, serially, at load 10–23: 26; 61 + 165; 86; 102; 61, all passing,
  `test_prym_passenger`'s wall-clock test included. `black --check` passes on the six Python files.
- **Residue, fixed in the follow-up commit.**
  - "this commit" becomes `1b83bbd` in the log (×6) and on this board (×3).
  - Note 06 is listed in §1, and the status line is updated.
  - This paragraph and the one before it.
- **Not residue, recorded only (for 07's note).**
  - Ruling 8's clauses cover the failures that tuning was expected to meet. But `_tuning_state`
    catches every `ValueError` from the state, not only `effective_minimum`'s, under "the effective
    minimum was not found". The state's other arithmetic, and `renormalize`, can raise things that
    no clause catches: a `ComputationFailureError` from `log_V`, or a `ZeroDivisionError` at
    1 + βφ′/M_P = 0. None arises at any measured point, and none is plausible at T_CMB. When 07
    makes `tune_history` the remote's body, an exception that escapes would reach the driver
    instead of a failure row. No issue is opened.
  - The log's observations stand: Ray's deduplication of worker lines, which 09 sees; the static
    seed's distance from H0 at M ≥ 0.1; and the plain update's slack.

### 5.8 After prompt 07 (2026-10-26)

Prompt 07 landed in `223afc6` (log [`logs/07-the-store.md`](logs/07-the-store.md)), dispatched with
orchestrator note 07, whose sixteen rulings govern. No stop; the run was cut off once by an API rate
limit and resumed on the same working tree.
- **S1.** `ObjectFactories/ExponentialModel.py` (M, n, β; sharded by β, batch 50) in
  `ExponentialPotential`'s place in the registry; `Lambda_value`, `ExponentialPotential`,
  `ExponentialCoupling`, `CosmologyConcepts/Potentials/`, `ConformalCouplings/`, `Lambda_value.py`
  and `ExponentialModel.from_rows` deleted.
- **S2, S3.** `ScalarHistory` is keyed by version, cosmology, model (`model_type`, `model_serial`),
  φ*, π*, T_init, atol, rtol and `tuning_tol`; no `T_Jordan_stop` column. Fifteen nullable tuning
  columns; `ScalarHistory.tuning` (`TuningValues`), `realized_model` (tuned, cached),
  `T_Jordan_stop` a property at T_CMB for the BBN pre-check.
- **S4.** `compute_scalar_history` calls `tune_history`; `integrate_history` is the primitive for the
  golden setup, the tests and the tools that run their own histories, which keep 4 ln(1 meV),
  φ_ref = 0 until 08. `AdiabaticHistory` and `BBNData` hold the history's `model`.
- **S5.** `main.py`'s grid is `ExponentialModel` over (M, n = 1, β); the Λ options and
  `--T-stop-GeV` are refused (exit 2); the tuning tolerance is `DEFAULT_TUNING_TOLERANCE`;
  `pipeline_selection.missing_models` decides the first pass. The wrapper's scope has no Λ.
- **S6.** `plot_inputs.exponential_models` and `parameters(…, tuning_tol)`; the plot scripts by
  (M, n) and per model; `histories.csv`'s `n`; `compare_stores`, `inventory_report` (the "history"
  names, ruling 13) and `bbn_from_store` on the new tables.
- **S7.** `FINGERPRINT_FORMAT` 5 (15 pairs, 13 distinct digests),
  `store_fingerprint_jordan-normalization-07.json`; `VERSION_LABEL` `2026.7.0`.
- **S8.** `ObjectFactories/tests/test_tuned_history_round_trip.py` (6 tests; one integration at the
  tuned golden's stored normalization and φ_ref, whose record equals the JSON's): every tuning
  column and all 5960 samples round-trip bitwise, the realized model is rebuilt equal, the key
  finds the row and no other, a failure row stores NULLs, `_do_not_populate` gives `tuning`.
  `TestScalarStageFirstPass` (5); the tuning tolerance's miss in `test_plot_inputs` (1). Removed:
  the adapter's two tests and five of `test_plot_inputs` (T_stop and couplings), each named in the
  log.
- **The golden histories** pass bitwise: `test_golden_history` 11.6 s and the tuned golden's test (e)
  26.8 s alone (load 6–7), against 12.2 s and 30.1 s before. The round trip takes 7.6 s alone.
- **Breakages.** The tolerance out of the key fails (b)'s "another tuning tolerance"; `Float(32)`
  fails nothing (SQLite); the value rounded to float32 fails (a)'s columns, (a)'s realized model and
  (d); the first-pass filter reverted fails four of `TestScalarStageFirstPass`.
- **Suites:** CosmologyModels 26, ComputeTargets 225 (61 + 164, −1), ObjectFactories 92 (+6), tools
  102, RunDrivers 61, all passing.
- **Closed:** `[02-scalar-stage-missing-filter-keeps-every-model]`,
  `[03-step-1-first-pass-lookup-filters-nothing]`. **Narrowed:**
  `[02-the-scalar-history-parent-is-still-called-model]`. **Opened:** none.
- **Index:** `.documents/OPEN_ISSUES.md` goes from 24 to 22 open.
- **For the review.** The log's Deviation 8: `test_golden_history.py`'s docstring still names
  `compute_scalar_history._function`, since the dispatch brief excluded the file that ruling 5 named.

**Before dispatch (2026-10-26).** The orchestrator read every writer and reader of the store at
`1a66efc` before writing note 07, and ruled sixteen points: six callers of
`compute_scalar_history._function` and nine files using the adapter move to `integrate_history`
and a directly built model at 4 ln Λ, φ_ref = 0, bitwise; `ScalarHistory` keeps a derived
`T_Jordan_stop` for the BBN pre-check; the super-Planckian warning iterates models at φ_ref = 0;
n takes Λ's place in paths, labels and `histories.csv`; the adiabatic and BBN specs' parent becomes
`"history"`. Two probes: one `integrate_history` at `golden_history_06_tuned.json`'s stored
normalization and φ_ref reproduces its record with no difference (5.3 s), and SQLite stores a
`Float(32)` column's value unrounded, so the prompt's float32 breakage was ruled as a rounding of
the value. Baselines at `1a66efc`: 26; 61 + 165; 86; 102; 61.

**Orchestrator review of prompt 07 (2026-10-26).** Against `223afc6`, by note 07's eleven checks.
All pass.
- **Scope.** 71 files: note 07's §2 files and ruling 1's deletions (`git rm`; git pairs the deleted
  `ObjectFactories/ExponentialPotential.py` with the new `ExponentialModel.py` as a rename).
  `ComputeTargets/BBNData.py` changes only its object (ruling 6); its pre-check is untouched. The
  tree is clean and no Ray process runs.
- **The schema.** The three factories, `config/datastore.py` and `config/sharding.py` read against
  rulings 1–3: the model's columns, spec and match; the history's key, its fifteen nullable columns
  with their units, and `build`'s filter (the key, `validated` and the version only).
- **The object and the compute path.** `ScalarHistory`, `compute_scalar_history` (tune_history's
  order, every argument by name), `compute` and `store` against rulings 4–5; `AdiabaticHistory` and
  `BBNData` against ruling 6. No caller passes the old signature; the remaining mentions of
  `compute_scalar_history._function` are docstrings that say what changed.
- **The golden histories.** `test_golden_history` 11.1 s and the tuned golden's test (e) 27.2 s
  alone, both passing; the four golden-history JSONs, their generators and the four older
  fingerprint files are unchanged from `83569ce`.
- **The round trip.** `test_tuned_history_round_trip.py` read against ruling 14 and run alone
  (6 tests, 8.2 s). The orchestrator's own probe tuned the golden point through
  `compute_scalar_history._function` (26.9 s): its history record equals the JSON's with no
  difference and its tuning record equals the JSON's; written through the test's harness and read
  back, all fifteen tuning values and the realized model (normalization, φ_ref, A(φ_ref) = 1, and
  four methods at three field values) equal the tuning's own, bitwise.
- **`main.py` and the issues.** The grid (M outer, β inner, n = 1), the tuning tolerance row, the
  three stages and `missing_models` read against rulings 7–8.
- **The readers.** `plot_inputs.exponential_models` and `parameters(…, tuning_tol)`; the plot
  scripts' `M=…eV_n=…` paths and the tuned V in `plot_ScalarHistory.py`; the tools against
  ruling 11. The history tools' tests pass in the suite.
- **The fingerprint.** `spec_digests(config.datastore.factories)` equals `SPEC_DIGESTS[5]` (15
  pairs, 13 distinct digests); pins 1–4 kept; `GOLDEN` names the new file; `VERSION_LABEL`
  `2026.7.0`.
- **The drivers.** `main.py`, both plot scripts and the wrapper `--help` exit 0; `--Lambda-values-eV`
  and `--T-stop-GeV` are refused with exit 2, creating no file. The agent's `main.py --inventory` on
  `test_inventory_report`'s temporary store shows 44 records in 15 classes, `log10_tol` −10, −8, −5.
- **The breakages.** The log's four diffs (checked equal to the agent's scratch diffs) replayed in
  `bash` and reverted, the tree clean after each: the tolerance out of the filter fails (b)'s
  "another tuning tolerance"; `Float(32)` fails nothing; the float32 rounding fails three tests
  with `'-0x1.1573400000000p+8' != '-0x1.1573404f5900fp+8'`; the old filter fails four of
  `TestScalarStageFirstPass`. All as the log says.
- **The records and the suites.** This board, the `dk-and-rpk-migration` and `run-integrity` boards,
  the index (22 open) and the log in §5.1's form, every deviation classified. §2 item 3's grep
  finds only the hits the log lists, with `ObjectFactories/ExponentialModel.py`'s docstring and spec
  comment (which say what the table replaces) besides. The suites at `223afc6`, serially, at load
  3–5: 26; 61 + 164; 92; 102; 61, all passing, `test_prym_passenger`'s wall-clock test included.
  `black --check` passes on the 53 changed or new Python files.
- **Residue, fixed in the follow-up commit.**
  - "this commit" becomes `223afc6` in the log (×5), on this board (×7) and on the
    `dk-and-rpk-migration` and `run-integrity` boards (×1 each).
  - `test_golden_history.py`'s docstring names `integrate_history` (Deviation 8). Ruling 5 named the
    file and the dispatch brief excluded it; the contradiction was the orchestrator's.
  - Note 07 is listed in §1, and the status line is updated.
  - This paragraph and the one before it.
- **Not residue, recorded only (for 08's note).**
  - The log's Observation 2 stands: an exception that escapes `tune_history`'s arithmetic would now
    reach the driver through the remote rather than become a failure row. None arises at any
    measured point. 09's rehearsal is the first run of the remote under Ray.
  - The log's other observations stand: `bbn_from_store`'s join does not check `model_type`; the
    comment in `config/sharding.py` still names Λ.

### 5.9 After prompt 08 (2026-10-26)

Prompt 08 landed in this commit (log [`logs/08-the-driver-and-the-tools.md`](logs/08-the-driver-and-the-tools.md)),
dispatched with orchestrator note 08, whose sixteen rulings govern. One stop, ruled by the
orchestrator: `test_scalarhistory_failure_reason.py` borrowed `tools.history_and_bbn.LAMBDA_EV`,
which ruling 8 removes, and was not in the note's file list; ruled option A, the test holds its own
`LAMBDA_EV = 1.0e-3`.
- **D1.** The Λ options, `--T-stop-GeV` and `--potential-type` are registered hidden with
  `config.argument_parser.RemovedOption`, whose `parser.error` names the prompt and the
  replacement (exit 2, on the command line and in a `--config` file, and as the wrapper's refusal).
  New: `--tuning-tolerance` (1e-5, a key), `--tuning-max-integrations` (30, not a key; its help says
  a failure is not recomputed), `--n-values` (`[1]`, `nargs="+"`, no `extend`). `main.py` builds
  (M, n, β), its tuning tolerance from the option, and passes the cap to `ScalarHistory.compute`.
  `exponential.yaml` loses `potential-type` and `log10-one-plus-z-high: 85`. `plot_inputs` matches
  `--n-values` (an n with no stored model is `PlotOptionNotStored`) and `--tuning-tolerance`.
- **D2.** The super-Planckian warning takes the cosmology, evaluates ln A(φ*) at the seed's φ_ref,
  runs once per cosmology, and its lines name β, M, n, φ*, the seed's φ_ref and kind; thresholds at
  M = 10⁻³: β > 6.506 (φ* = 5), 32.61 (φ* = 1).
- **D3.** The scope drops the potential and gains `n …` and `tuning tolerance …`; tune lines beat and
  are never a stage (a streams test and an in-process `"tune"` stand-in run).
- **D4.** `tools/history_and_bbn.py` tunes by default and prints a `tuning` line of the fifteen
  outputs (repr), bitwise the tuned golden's at β = 3, M = 0.01 (22 s);
  `--fixed-normalization NORM PHI_REF_MP` runs `integrate_history`; `--T-stop-GeV` only with it;
  the axis is main.py's 300 decades.
- **D5.** The surfing tools tune each β (`run_history(…, T_window_low_GeV, tolerance,
  max_integrations)`), with the window `in_window` at the first crossing of T_J =
  `--T-window-low-GeV`; the CSV's new columns; old-format CSVs refused by the scan and by the plot;
  `--T-stop-GeV` refused. Closes `[05-surfing-scan-runs-at-a-fixed-lambda]`.
- **D6.** `histories.csv` gains the normalization, V_ref (eV⁴), φ_ref and φ_0 (M_P), tracking, both
  residuals and the integrations.
- **D7.** New `ComputeTargets/tests/test_driver_options.py` (14) and `tools/tests/test_history_and_bbn.py`
  (3); further tests in `test_initial_field_option` (2), `test_plot_inputs` (2, one removed),
  `test_extraction` (1), the round trip (1), `test_surfing_scan` (6), `test_plot_surfing_histories`
  (1), `test_plot_surfing_temperature` (3) and RunDrivers (3).
- **Short scan** (β = 1.50, 3.00, 4.00, at a load falling from 8 to 5): 20.1, 31.7 and 65.3 s; the
  tuned values equal the probe's bitwise; roots to 5.1 × 10⁻¹⁵; flags and kicks unchanged. Predicted
  S1 about 55 min, S5 about 35 min.
- **Golden histories** pass: 9.3 s and 23.1 s alone.
- **Breakages:** five, in the log. Breakage 1's first replay ran the real wrapper and registered
  `var/runs/rrk-adoption-03-unit-20261026T151946`, left stale for the user to end (log,
  Observations); its replay now runs in-process tests only.
- **Suites:** CosmologyModels 26, ComputeTargets 243 (76 + 167, +18), ObjectFactories 93 (+1),
  tools 115 (+13), RunDrivers 64 (+3), all passing.
- **Closed:** `[05-surfing-scan-runs-at-a-fixed-lambda]`. **Opened:**
  `[08-a-history-that-did-not-converge-is-never-retried]`.
- **Index:** `.documents/OPEN_ISSUES.md` stays at 22 open (one closed, one opened); §1.15 is empty.
