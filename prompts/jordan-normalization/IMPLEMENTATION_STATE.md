# jordan-normalization campaign — implementation state

**Last updated:** 2026-10-20 · **Status: LIVE — 10 of 10 prompts written, 0 landed; R1–R14, U1–U5
and P1–P20 ruled by the user 2026-10-20 (README §6). Next: the orchestrator note for 01.**
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
| 01 | Strip to the exponential model | four potentials and their configs removed; a golden short history; format 2 | Sonnet | ⬜ | ⬜ | — | — |
| 02 | Rename to `ScalarHistory` | classes, tables, drop group, proxies, documents; format 3 | Sonnet | ⬜ | ⬜ | — | — |
| 03 | The sampling axis | ln(1 + z_E) on the axis, exact-multiple grid, absolute lookup, ln(1 + z_J) per sample; format 4 | Opus | ⬜ | ⬜ | — | — |
| 04 | The model interface | `AbstractModel`, `ExponentialModel`, `RealizedModel`; consumers on it via an adapter; φ_ref = 0 bitwise; the shift law | Opus | ⬜ | ⬜ | — | — |
| 05 | The Planck mass | one reduced Planck mass from PRyMordial's G_N in three unit systems; consistency tests | Sonnet | ⬜ | ⬜ | — | — |
| 06 | The tuning layer | `integrate_history` and `tune_history`: secant, seed, residuals, failure reasons, progress lines | Opus | ⬜ | ⬜ | — | — |
| 07 | The store | `ExponentialModel` table; `ScalarHistory`'s key and tuned outputs; format 5; `VERSION_LABEL` 2026.7.0; writers and readers moved | Opus | ⬜ | ⬜ | — | — |
| 08 | The driver and the tools | `main.py`'s options; the super-Planckian warning; the wrapper's scope; the tools and the surfing scan on tuned histories | Opus | ⬜ | ⬜ | — | — |
| 09 | Rehearse a tuned build | registered pilot build on the isolated cluster, interrupted and resumed, compared bitwise (if U2) | Opus | ⬜ | ⬜ | — | — |
| 10 | Close | documents, `CLAUDE.md`, the handover for the science rebuild, indexes, board | Sonnet | ⬜ | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

**Written 2026-10-20** after the rulings (README §2.1). Each is dispatched with an orchestrator
note in `orchestrator/`, written after the previous prompt's review.

### 1.1 Items

| Prompt | Items |
|---|---|
| 01 | X1 the golden history · X2 the classes deleted · X3 the registries · X4 the tests · X5 format 2 |
| 02 | N1 the code, tables, columns and drop group · N2 format 3 · N3 the current documents · N4 the run scripts left as records |
| 03 | Z1 the concept · Z2 the factory and format 4 · Z3 the grid · Z4 the sampling · Z5 ln(1 + z_J) · Z6 the readers · Z7 the value factory's issues |
| 04 | M1 `AbstractModel` · M2 `RealizedModel` · M3 `ExponentialModel` · M4 the consumers · M5 the adapter · M6 the unread properties · M7 the tests |
| 05 | K1 one constant · K2 the three unit systems · K3 the tests |
| 06 | T1 `integrate_history` · T2 the residuals · T3 the secant and its guards · T4 the seed · T5 the failures · T6 progress · T7 the outputs |
| 07 | S1 the model's table · S2 the key · S3 the outputs · S4 the compute path · S5 `main.py` · S6 the readers · S7 format 5 and 2026.7.0 · S8 the tests |
| 08 | D1 the options · D2 the warning · D3 the wrapper's scope · D4 `history_and_bbn.py` · D5 the surfing tools · D6 `histories.csv` · D7 the tests |
| 09 | H1 reference · H2 resumed · H3 the comparisons · H4 the measurements · H5 the document |
| 10 | C1 the documents · C2 `CLAUDE.md` · C3 the handover · C4 the board and indexes · C5 the final suites |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Strip | only the exponential potential and coupling remain; the golden history, generated at `ac187c8`, passes bitwise | 01 | ⬜ |
| Rename | no `ScalarModel` name outside the records; the golden history bitwise | 02 | ⬜ |
| Axis | the axis is ln(1 + z_E) with no overflow; ln(1 + z_J) agrees with `cosmology.z(T_J)` and with the A identity; the change in values measured | 03 | ⬜ |
| Model | the golden history bitwise through `ExponentialModel` at φ_ref = 0, V₀ = Λ⁴; the shift law holds | 04 | ⬜ |
| Units | one Planck mass equal to PRyMordial's; 1 K, ħc, ħ and the kilogram consistent to ≤ 10⁻⁶ in each unit system | 05 | ⬜ |
| Tuning | a tuned history converges inside the tolerance, deterministically; the secant is no worse than the plain update on README §0.3's grid | 06 | ⬜ |
| Store | a tuned history round-trips bitwise and its realized model is rebuilt equal; the golden fingerprint at format 5 | 07 | ⬜ |
| Driver | the old options are refused; the surfing scan records tuned values with its roots unchanged | 08 | ⬜ |
| R | on the isolated cluster every history converged, the stored values equal the tuning layer's, and the resumed store equals the reference | 09 | ⬜ |

## 3. Active and unresolved issues

None opened by this campaign.

### 3.1 Assigned to this campaign

Assigned 2026-10-20 by the user (README §6.1, U3). Each entry stays on its own board, which carries
the **Assigned** line; the prompt named closes it there, in that board's §4, and deletes its row
from `.documents/OPEN_ISSUES.md` §1.15.

| Issue | Its board | Prompt | Why here |
|---|---|---|---|
| `[00-declare-reflects-at-origin-for-the-other-potentials]` | `integrator-remediation` | 01 | moot once the four other potentials are deleted |
| `[00-region-properties-on-the-potentials-become-unread]` | `integrator-remediation` | 04 | `AbstractModel` does not carry the properties over |
| `[01-scalarmodelvalue-build-checks-the-stored-phi-against-pi]` | `dk-and-rpk-migration` | 03 | the value factory is rewritten for the axis |
| `[05-the-value-factory-compares-stored-phi-against-pi]` | `science-readiness` | 03 | the same defect, opened twice |
| `[09-redshift-lookup-at-zero-takes-abs-of-the-comparison]` | `dk-and-rpk-migration` | 03 | the lookup moves to ln(1 + z) by absolute difference |
| `[02-scalar-stage-missing-filter-keeps-every-model]` | `dk-and-rpk-migration` | 07 | the scalar stage's lookups are rewritten for the new key |
| `[03-step-1-first-pass-lookup-filters-nothing]` | `run-integrity` | 07 | the same code |
| `[05-surfing-scan-runs-at-a-fixed-lambda]` | `surfing-temperature` | 08 | the surfing tools move onto tuned histories |

## 4. Resolved issues

None.

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
