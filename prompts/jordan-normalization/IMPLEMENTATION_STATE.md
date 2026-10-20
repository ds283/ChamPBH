# jordan-normalization campaign — implementation state

**Last updated:** 2026-10-20 · **Status: PLANNED — 0 of 10 prompts written; R1–R14 ruled by the
user (README §6.0); awaiting the rulings on README §6.1 (U1–U5) and §6.2 (P1–P20).**
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
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) (no section yet: none
opened or assigned)

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

**Held, by design (README §2.1).** 01–10 are written once the user has ruled on README §6.1 and
§6.2.

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

None yet. README §6.1 U3 proposes eight, for the user's ruling.

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
