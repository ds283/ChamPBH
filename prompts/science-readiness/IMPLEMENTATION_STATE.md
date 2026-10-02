# Science-readiness campaign — implementation state

**Last updated:** 2026-10-02 · **Status: IN PROGRESS — 6 of 9 landed** (prompt 01, with
deviations; prompt 02; prompt 03, with deviations; prompt 04, with deviations; prompt 05,
measurement only, the averaging withdrawn; prompt 06). Planned on 2026-10-01 against
`main` at `6aaa706`, from a Claude Science re-evaluation kept at
[`source/campaign_reevaluation_2026-10-01.md`](source/campaign_reevaluation_2026-10-01.md) and
checked against the tree by the planner (README §0.3). Suites at `6aaa706`: CosmologyModels 18,
ComputeTargets 67, Datastore 17.
**Target branch** `science-readiness`, to be cut from `6aaa706`; planning and orchestration commits
land on it.
**`VERSION_LABEL` is `"2026.6.0"`** since prompt 01 (from `"2026.5.0"`), bumped once. Every store
made before 2026.6.0 is invalid, and the science run needs a fresh datastore file (columns are
added with no migration). `PRYM_VERSION` is `"bf24c3d+ri02+sr01"`. Suites after prompt 01:
CosmologyModels 18, ComputeTargets 71, Datastore 17. After prompt 02: 18, 75, 21. After
prompt 03: 18, 82, 26. After prompt 04: 18, 86, 26. After prompt 05 (no code): 18, 86, 26. After prompt 06: 18, 88, 26.

The campaign hands the scalar field to PRyMordial through the expansion rate alone, with a patched
PRyMordial that has no fictitious new-physics temperature and an optional wall-clock limit. It
checks PRyMordial's output before storing it. It stores a `ScalarModel`'s failure reason and first
bounce. It makes φ\* a run option, with a warning for a super-Planckian start. It replaces the aliased point
samples BBN reads with bounce averages on the dense output. It narrows the BBN spline to the range
PRyMordial reads, and adds the extraction and the four figures of the science run.

**Campaign:** [`README.md`](README.md) ·
**Code (planned):**
- `PRyM/PRyM_init.py`, `PRyM/PRyM_main.py` (the Hubble flag, the wall-clock limit, the `cham03`
  revert; prompt 01);
- `ComputeTargets/BBNData.py` (01, 05, 06); `ComputeTargets/ScalarModel.py` (02, 03, 05);
- `Datastore/SQL/ObjectFactories/BBNData.py` (01), `…/ScalarModel.py` (02, 03, 05);
- `main.py` (01, 02, 04), `plot_by_beta.py` (02, 04, 07), `plot_ScalarModel.py` (01, 04),
  `extract_common.py` (07), `pipeline_selection.py` (04), `config/argument_parser.py` (01, 04,
  07), `config/version.py` (01);
- `tools/history_and_bbn.py` (new in 01; 03, 04, 05);
- `ComputeTargets/tests/`, `Datastore/tests/`;
- `.documents/numerical-strategies.md`, `numerical-methods-for-paper.md`,
  `architecture-summary.md`, `paper-corrections-numerical-section.md` (08, additive);
  `.documents/review-remediation-verification.md` (09, additive).

**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) §1.7–§1.8.

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) is updated **in the same
> commit**: the row is added, moved or deleted, and the count and date in its header are
> corrected. The index is an index: one line per issue, pointing at the board that holds it. Where
> the two disagree, the board is right. See `CLAUDE.md`.

### Decisions

- **2026-10-01, the user (planning conversation): README §0.2 U1–U5.**
  - U1: replace the `NP_thermo_flag` route outright by patching PRyMordial, and remove `T_NP`.
  - U2: the wall-clock limit is an optional parameter of the PRyMordial patch, returned as a
    failure.
  - U3: bounce averages by Gauss–Legendre on the dense output at sampling time.
  - U4: the first bounce in dedicated columns.
  - U5: the Phase D extraction and figures in this campaign.
- **2026-10-01, the planner: README §0.2 P1–P9 proposed.** P1 removes `p_NP` everywhere; P2
  reverts `cham03`; P3 is the 600 s default and the option; P4 folds in two `run-integrity`
  issues; P5 defines the first bounce; P6 (as proposed) skipped super-Planckian couplings; P7
  defines the cell; P8 sets the floor at 0.2 keV; P9 is one bump and a fresh datastore.
- **2026-10-01, the user: P1–P5 and P7–P9 accepted as proposed; P6 overruled.** A coupling with
  Ω(φ*) T* > M_P is warned about and computed, never skipped or refused. The user's reason:
  there can be numerical reasons to start slightly super-Planckian (to kill transients), the
  premise is that the final state does not depend on the initial one, and dropping such points
  brings the proposed science run no benefit. README §0.2 P6, §2 (h) and prompt 04 carry the
  ruling. With this line the precondition of orchestrator 01 §1.1 is met. The two `run-integrity`
  issues of P4 are now firmly assigned to prompt 01. The user had earlier asked how close the
  probes came to P3's 600 s; the answer (the Hubble-only solves took 9.7–10.0 s, about 1.7 % of
  the limit, unloaded) left P3 as proposed.
- **2026-10-01, the user: prompt 01's deviation 3 accepted as is.** `test_bbn_callbacks (h)` no
  longer bounds the builder's constant-ratio callback against the exact constant family. It bounds
  that callback through the Hubble-only route against the same callback through the "honly" route
  on `7b518c9` (`BUILDER_CONST_HONLY_FULL_*`), at 1e-6. The 1.06e-4 D/H offset from the exact
  family is PRyMordial's sensitivity to ulp-level changes in ρ_NP
  (`[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]`, `review-remediation`). It is
  printed and not bounded. Log 01, deviation 3 holds the measurements. The orchestrator's review
  of `1bc8977` passed all ten checks of `orchestrator/prompt-01.md` §3, and this ruling was the
  only open question.
- **2026-10-01, the user: prompt 02's deviation 1 accepted as is.** `summarise_failure_reasons`
  and `NO_FAILURE_REASON` live in `pipeline_selection.py`, outside prompt 02's allowed files,
  because `main.py` parses `sys.argv` at import and a test cannot import from it. `main.py`
  imports the function. The change to `pipeline_selection.py` is those 21 added lines and nothing
  else. The orchestrator's review of `77a7e0c` failed only check 1 of
  `orchestrator/prompt-02.md` §3 (allowed files), on this file; checks 2–6 passed, and this
  ruling was the only open question. Log 02, deviation 1 holds the reasoning.
- **2026-10-02, the user: prompt 04's test (c) accepted as is.** Test (c) of
  `ComputeTargets/tests/test_initial_field_option.py` parses `["--database", "unused.db"]`
  rather than the prompt's `[]`, because `--database` is `required=True` in
  `config/argument_parser.py`. Log 04 does not record this as a deviation. The orchestrator's
  review of `0b58eb8` passed the five checks of `orchestrator/prompt-04.md` §3; this unrecorded
  deviation was the only open question. Also unrecorded in log 04, and noted by the orchestrator:
  test (b) stops at the first driver it fails on, so on `HEAD~1` it reports `main.py:862` alone;
  the orchestrator checked out each of the other two drivers from `HEAD~1` separately and the
  test caught `plot_by_beta.py:895` and `plot_ScalarModel.py:1611`.
- **2026-10-02, the user: prompt 05's averaging withdrawn (U3 and P7); prompt 05 lands as
  measurement only.** The implementation agent built the cell means and stopped before
  committing, on prompt 05 §5's first stop condition. The breakage witness fell only to 0.79× and
  0.76× at β = 1.6, M = 10⁻⁵, against a target of ≤ 0.1. The orchestrator confirmed this on the
  built tree. It counted the half-periods of π per sample cell: a median of 0 from 100 MeV to
  about 100 eV, on β = 1.6 and 2 at M = 10⁻⁵ and β = 2 at M = 0.5. Aliasing begins only below
  that. So the jumps in the ratio below 3 keV are resolved bounces. The cell means also:
  - biased `H_J²` by +5.7e-5;
  - moved D/H at β = 2, M = 0.5 by 1.57e-3;
  - made β = 2, M = 10⁻⁵ fail in PRyMordial.

  Point input completes BBN on all four histories. The user's reason: in PRyMordial's window the
  point samples are the behaviour of H on the solution. If PRyMordial fails on that input, it
  does not cope with the model, and that is a finding, not a reason to change the input. The
  agent's commit `c242f64` has no code, schema or test. The orchestrator reviewed it: records
  only, deviations classified, board and index in the same commit, suites 18 / 86 / 26 unchanged.
  This ruling is carried by the README §0.2 amendment and dated notes in §2 (i), (k), §3.1, §6.6,
  §6.7 and §7. Prompts 06–09 and `orchestrator/prompt-08.md` carry dated amendments. The two
  issues prompt 05 opened (`[05-…]`) are unassigned.
- **2026-10-02, the user: prompt 07's `a2deb00` reverted (`8fcb295`); the fixed-`T` values move
  to the `ScalarModel` row; prompt 07 to be re-planned.** Figure 4 and `histories.csv` need φ and
  ρ_NP/ρ_R,J at T_J = 1 MeV and 70 keV. `a2deb00` interpolated them from every stored sample at
  plot time. To do so it removed `_do_not_populate` from the `ScalarModel` and `BBNData` lookups
  in `plot_by_beta.py`'s `build_plot_work`, so every lookup loaded each history's whole sample
  table to yield four numbers. That is prompt 07 §5's first stop condition (the lookups cannot
  return the values without a factory change). Log 07 tagged it `STRUCTURALLY REQUIRED`
  (deviation 3) and did not stop; the orchestrator's first report also missed the stop condition.
  The user's reason: the four values are properties of the whole history, not of a sample. They
  belong on the parent `ScalarModel` row, as the first bounce does (prompt 03), and are read with
  `_do_not_populate` kept. ρ_NP/ρ_R,J is built from `ScalarModelValue` fields alone
  (`3 M_P² H_J² − ρ_R,J (1 + f_m)`, `ComputeTargets/BBNData.py`), so both can be computed in
  `compute_scalar_model`. Also ruled, for the re-plan: log 07's deviation 2 is accepted.
  `build_beta_plot` returns plain per-history records and `run_pipeline` gathers them across
  potentials (`store_results=True`) for figure 2 and `histories.csv`. This is a change in how
  data are marshalled, and figure 2 needs it. The orchestrator's review of `a2deb00` otherwise
  passed its five checks: allowed files; pure functions; tests (a)–(e), 13 OK; existing figures
  unchanged but for the caption; suites 18 / 101 / 26. Prompt 07 is suspended until re-planned;
  G and `[00-no-extraction-for-the-science-figures]` stay open.

Decisions the prompts may surface, each a stop-and-ask in its prompt:

- the patched route does not reproduce the reference (prompt 01, §5);
- a callback exception does not propagate out of LSODA (prompt 01, §5);
- the first negative-to-positive turning point is not a wall bounce on some history (prompt 03);
- the noise below 3 keV does not fall as required with averaged input (prompt 05);
- the `N`-average is not within `1e-3` of the time-average (prompt 05).

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [The Hubble-only BBN route, the wall-clock limit and the output checks](01-hubble-only-bbn-route.md) | **R**, **W**, **O**, version bump, driver | Opus | ✍️ 2026-10-01 | ✅ 2026-10-01 (with deviations) | see `git log` ("Hand the scalar field to PRyMordial through H alone") | [`logs/01-hubble-only-bbn-route.md`](logs/01-hubble-only-bbn-route.md) |
| 02 | [A failure reason on `ScalarModel` rows](02-scalarmodel-failure-reasons.md) | **F** | Sonnet | ✍️ 2026-10-01 | ✅ 2026-10-01 (with deviations) | see `git log` ("Store why a ScalarModel history failed") | [`logs/02-scalarmodel-failure-reasons.md`](logs/02-scalarmodel-failure-reasons.md) |
| 03 | [Store the first bounce](03-first-bounce.md) | **T** | Opus | ✍️ 2026-10-01 | ✅ 2026-10-02 (with deviations) | see `git log` ("Store the first bounce of every ScalarModel") | [`logs/03-first-bounce.md`](logs/03-first-bounce.md) |
| 04 | [The initial field as a run option, with a super-Planckian warning](04-initial-field-option.md) | **P** | Sonnet | ✍️ 2026-10-01 | ✅ 2026-10-02 (with deviations) | see `git log` ("Make the initial field a run option and warn on a super-Planckian start") | [`logs/04-initial-field-option.md`](logs/04-initial-field-option.md) |
| 05 | [Bounce averages on the dense output](05-bounce-averages.md) | **A** | Opus | ✍️ 2026-10-01 | ✅ 2026-10-02 (measurement only; the averaging withdrawn by the user's ruling) | see `git log` ("Withdraw the bounce averages: BBN's window is not aliased") | [`logs/05-bounce-averages.md`](logs/05-bounce-averages.md) |
| 06 | [Narrow the BBN spline to PRyMordial's range](06-bbn-spline-floor.md) | **L** | Sonnet | ✍️ 2026-10-01 | ✅ 2026-10-02 | see `git log` ("Narrow the BBN spline floor to 0.2 keV") | [`logs/06-bbn-spline-floor.md`](logs/06-bbn-spline-floor.md) |
| 07 | [Extraction and the science figures](07-extraction-and-figures.md) | **G** | Sonnet | ✍️ 2026-10-01 | — | — | — |
| 08 | [Documents](08-documents.md) | **D** | Sonnet | ✍️ 2026-10-01 | — | — | — |
| 09 | [Close-out verification and handover](09-close-out-verification.md) | close-out | Sonnet | ✍️ 2026-10-01 | — | — | — |

---

## 2. Items

| Item | Kind | Description | Prompt | Status |
|---|---|---|---|---|
| R | **DEFECT, medium** | The `NP_thermo_flag` route integrates a fictitious `T_NP`, and adds −3H(ρ_NP + p_NP) and dρ_NP/dT to the plasma equation, cancelling only to `≈ r·1.2×10⁻³`. Replace it with a Hubble-only patch. Closes `[00-bbn-route-integrates-a-fictitious-np-temperature]`. | 01 | **done** 2026-10-01 (log 01) |
| W | **GAP** | No bound on a PRyMordial solve's wall time. Closes `[00-prymordial-has-no-wall-clock-limit]`. | 01 | **done** 2026-10-01 (log 01) |
| O | **GAP** | A successful PRyMordial return is stored unchecked. Closes `[00-prymordial-output-is-stored-unchecked]`; with P4, also closes `[02-a-short-bbn-sample-grid-escapes-compute-bbn-data]` and `[02-the-bbn-callbacks-do-not-check-their-values-for-finiteness]` (assigned). | 01 | **done** 2026-10-01 (log 01) |
| F | **GAP** | A `ScalarModel` failure row carries no reason. Closes `[00-scalarmodel-failure-rows-carry-no-reason]` (assigned). | 02 | **done** 2026-10-01 (log 02) |
| T | **GAP** | The first bounce is not stored and cannot be read from the samples at small `M`. Closes `[00-first-bounce-is-not-stored]`. | 03 | **done** 2026-10-02 (log 03) |
| P | **GAP** | φ\* is a literal in three drivers, with no super-Planckian warning. Closes `[00-initial-field-value-is-hard-coded-and-unchecked]` (assigned). | 04 | **done** 2026-10-02 (log 04) |
| A | **DEFECT, medium at M ≲ 10⁻⁴** | BBN integrates a spline through random-phase samples of the bounces below a few keV. Closes `[00-bbn-input-is-aliased-at-small-M]`; narrows `[00-stored-samples-alias-the-rebounds]` (assigned). | 05 | **withdrawn** 2026-10-02 (log 05): premise measured false; the user's ruling |
| L | **DEFECT, low** | The BBN spline reaches 0.1 eV, while PRyMordial reads to 0.363 keV. Closes `[00-bbn-spline-domain-is-far-wider-than-prymordial-uses]` (assigned). | 06 | **done** 2026-10-02 (log 06) |
| G | **GAP** | No extraction or figures for the science run. Closes `[00-no-extraction-for-the-science-figures]`. | 07 | open |
| D | documents | Three documents describe the `NP_thermo_flag` route. Closes `[00-documents-describe-the-thermo-route]` and `[04-numerical-strategies-describes-the-removed-asinh-bbn-interface]` (assigned). | 08 | open |

---

## 3. Active and unresolved issues

Seven opened by the planner on 2026-10-01, one per campaign item that has no issue on another
board; prompt 01 closed three, prompt 03 one and prompt 05 one (§4), and two are open. Prompt 05
opened two more (`[05-…]`). Seven more were assigned from other boards (§3.1); prompts 01, 02, 04
and 06 closed five of them, prompt 05 narrowed one, and two are open.

- **[00-no-extraction-for-the-science-figures]** *(README §1 G)*.
  - **What.** None of the four Phase D figures, nor a per-history table, can be produced from a
    store.
  - **Next step.** Prompt 07.
- **[00-documents-describe-the-thermo-route]** *(README §1 D)*.
  - **What.** `numerical-strategies.md` §7, `numerical-methods-for-paper.md` §4 and
    `architecture-summary.md` §7.4 describe the `NP_thermo_flag` route and the pressure callback.
  - **Next step.** Prompt 08, once the code is final.
- **[05-the-ratio-spline-may-ring-at-resolved-bounce-jumps]** *(log 05, Observations)*.
  - **What.** In PRyMordial's window the ratio `ρ_NP/ρ_R,J` is a resolved sawtooth: it jumps by
    about +0.005 to +0.007 at each bounce (β = 1.6, M = 10⁻⁵, 0.3–3 keV) and drifts smoothly in
    between. Each jump falls between two samples 0.0092 e-folds apart, and `build_rho_NP_callback`
    puts a cubic spline through them. Its overshoot or ringing between samples has not been
    measured.
  - **Impact.** Until it is, a PRyMordial failure on point input cannot be attributed to the true
    H rather than to our interpolation.
  - **Next step.** On one small-M history, compare the spline's r between samples with r from the
    dense output at the same T_J. Not assigned.
- **[05-the-value-factory-compares-stored-phi-against-pi]** *(log 05, Observations)*.
  - **What.** `sqla_ScalarModelValue_factory.build`, on finding an existing row, checks
    `fabs(row_data.phi_Einstein_Mp - pi_Einstein_Mp)` (`Datastore/SQL/ObjectFactories/ScalarModel.py:1008`
    on `a522005`) under a message about π: the stored φ against the supplied π, where
    `row_data.pi_Einstein_Mp` was meant.
  - **Impact.** The path raises a spurious `ValueError` whenever it is reached with φ ≠ π; nothing
    in the tree calls it today (the `ScalarModel` factory reads values directly).
  - **Next step.** One-word fix with a test of the existing-row path. Not assigned.

### 3.1 Assigned to this campaign from other boards

Each entry stays on its own board, which carries an **Assigned (2026-10-01)** line. The prompt
that closes one adds a dated **Resolved** line there, deletes the index row, and records it in §4
here (README §5 rule 4).

| Issue | Board | Prompt |
|---|---|---|
| `[00-initial-field-value-is-hard-coded-and-unchecked]` | `review-remediation` | 04 — **resolved 2026-10-02** (§4) |
| `[00-bbn-spline-domain-is-far-wider-than-prymordial-uses]` | `review-remediation` | 06 — **resolved 2026-10-02** (§4) |
| `[04-numerical-strategies-describes-the-removed-asinh-bbn-interface]` | `review-remediation` | 08 |
| `[02-a-short-bbn-sample-grid-escapes-compute-bbn-data]` | `run-integrity` | 01 (P4) — **resolved 2026-10-01** (§4) |
| `[02-the-bbn-callbacks-do-not-check-their-values-for-finiteness]` | `run-integrity` | 01 (P4) — **resolved 2026-10-01** (§4) |
| `[00-scalarmodel-failure-rows-carry-no-reason]` | `integrator-remediation` | 02 — **resolved 2026-10-01** (§4) |
| `[00-stored-samples-alias-the-rebounds]` | `integrator-remediation` | 05 — **narrowed 2026-10-02**: it has no BBN half in PRyMordial's window, where the z grid resolves the bounces (above about 100 eV); the averaging was withdrawn (log 05). The adiabatic half stays open there |

---

## 4. Resolved issues

- **[00-bbn-spline-domain-is-far-wider-than-prymordial-uses]** *(assigned from
  `review-remediation`; §3.1)*. **Resolved (2026-10-02):** by prompt 06 (log 06).
  `compute_BBN_data`'s `T_BBN_keV_spline_min` defaults to 0.2 keV (was 1e-4); the pre-check rule is
  unchanged, so a history must reach 20 eV (was 0.01 eV). PRyMordial's lowest callback query is
  0.3628 keV (`test_bbn_spline_floor (b)`). The driver on β = 2 at M = 0.5 and 10⁻³ reproduces log
  05's abundances to every printed digit. The dated **Resolved** line is also on the
  `review-remediation` board.

- **[00-bbn-route-integrates-a-fictitious-np-temperature]** *(README §0.3, §1 R; `PRyM_main.py`
  `:95–201` on `6aaa706`)*.
  - **What.** `_configure_PRyMordial` sets `NP_thermo_flag`, which puts ρ_NP into `Hubble`, which
    is wanted. It also adds −3H(ρ_NP + p_NP) and dρ_NP/dT to dT_γ/dt and integrates a third
    variable `T_NP` that no output reads (`cham03` makes its equation inert). The plasma obeys the
    Standard-Model equation only through a cancellation between two spline-derived terms.
  - **Impact.** README §6.1 shows the cost of the route on real histories.
  - **Next step.** Prompt 01.
  - **Resolved (2026-10-01):** by prompt 01 (log 01). `PRyM_init.NP_hubble_flag` adds ρ_NP to
    `Hubble` and nowhere else; `_configure_PRyMordial` sets `NP_thermo_flag = False` and checks
    the flags the route relies on; the thermodynamic solve integrates (T_γ, T_ν) only and ρ_NP is
    called only from `Hubble` (`test_bbn_solver_failures (f)`); `cham03` is reverted. The route
    reproduces README §6.1's "honly" rows to every printed digit (β = 2 at M = 0.5 and 10⁻³; the
    constant family, small and full network), and ρ_NP ≡ 0 is plain PRyMordial exactly.
    `PRYM_VERSION = "bf24c3d+ri02+sr01"`, `VERSION_LABEL = "2026.6.0"`.
- **[00-prymordial-has-no-wall-clock-limit]** *(README §1 W)*.
  - **What.** A solve that slows without failing occupies a Ray worker indefinitely; `run-integrity`
    declined a Ray timeout (its README §0.4).
  - **Impact.** One slow history stalls a survey's BBN stage.
  - **Next step.** Prompt 01, as a PRyMordial parameter (U2).
  - **Resolved (2026-10-01):** by prompt 01 (log 01). `PRyMclass(…, wall_clock_limit=None)`;
    every `fun` and `jac` of the eight `solve_ivp` calls is wrapped and the deadline is checked
    before each stage; past it, `PRyMWallClockLimitError(stage, elapsed, limit)`, which
    `_run_PRyMordial` returns as a failure row. 600 s by default (`DEFAULT_BBN_WALL_CLOCK_LIMIT`),
    `--bbn-wall-clock-limit SECS` on `main.py` (0 disables it); `compute_SM_baseline` has none. At
    1e-3 s the constant family fails in 0.001 s naming `thermodynamics (no NP)`.
- **[00-prymordial-output-is-stored-unchecked]** *(README §1 O)*.
  - **What.** `_run_PRyMordial` returns `res[4:8]` as they come. `plot_by_beta.py` drops
    non-positive abundances at plot time, after storing them.
  - **Impact.** An unphysical result is a stored success.
  - **Next step.** Prompt 01.
  - **Resolved (2026-10-01):** by prompt 01 (log 01). `_check_abundances`: all four finite,
    0 < Yp < 0.5, D/H, ³He/H, ⁷Li/H > 0; otherwise `"PRyMordial output: …"` failure rows
    (`test_bbn_solver_failures (h)`).
- **[02-a-short-bbn-sample-grid-escapes-compute-bbn-data]** *(assigned from `run-integrity`;
  §3.1)*. **Resolved (2026-10-01):** by prompt 01 (log 01). `build_rho_NP_callback` raises
  `ComputationFailureError` for fewer than four samples, naming the count, so the case is a
  `"BBN callbacks: …"` failure row (`test_bbn_solver_failures (i)`; before, `ValueError`). The
  dated **Resolved** line is also on the `run-integrity` board.
- **[02-the-bbn-callbacks-do-not-check-their-values-for-finiteness]** *(assigned from
  `run-integrity`; §3.1)*. **Resolved (2026-10-01):** by prompt 01 (log 01). The ρ_NP callback
  raises `ComputationFailureError` for a non-finite value at a finite in-domain T, for example
  from a NaN `G_rho` (`test_bbn_solver_failures (i)`; before, `nan` was returned to PRyMordial).
  The dated **Resolved** line is also on the `run-integrity` board.
- **[00-scalarmodel-failure-rows-carry-no-reason]** *(assigned from `integrator-remediation`;
  §3.1)*. **Resolved (2026-10-01):** by prompt 02 (log 02). `compute_scalar_model` returns
  `{"failure": True, "failure_reason": …}` from both failure exits (the `ComputationFailureError`
  message; `"sampling: overflow when assembling sample values: …"`), truncated to 256.
  `ScalarModel.failure_reason` reads the new nullable `ScalarModel.failure_reason String(256)`
  column, on a failure row, and is `None` on a success. `main.py` prints this run's failures
  grouped by first clause (`pipeline_selection.summarise_failure_reasons`); `plot_by_beta.py`
  reports models dropped because their `ScalarModel` failed, with the reason. The dated
  **Resolved** line is also on the `integrator-remediation` board.
- **[00-first-bounce-is-not-stored]** *(README §1 T; the source §2)*.
  - **What.** The dense-output first bounce (`N`, `T_J`, `φ_min`) exists only during the
    integration. The source's sample-based detector returns 0.138 GeV for 0.747 GeV at small `M`.
  - **Impact.** `T_deliver(β)`, a figure of the science run, cannot be made from a store.
  - **Next step.** Prompt 03.
  - **Resolved (2026-10-02):** by prompt 03 (log 03). `first_bounce(result)` returns
    `FirstBounce(N, phi_Einstein, log_T_Jordan, reflected)`: the root of π on the first accepted
    step whose interpolant has π(t_k) < 0 < π(t_{k+1}), or the first reflection if it comes
    earlier, or `None`. `compute_scalar_model` returns it, and four nullable columns
    (`first_bounce_N`, `first_bounce_log_T_Jordan`, `first_bounce_phi_Einstein`,
    `first_bounce_reflected`) store it; `ScalarModel.first_bounce` reads it back. On β = 2 it
    gives 20.34302685 / 746.63 MeV (M = 0.5) and 20.35208223 / 746.69 MeV (M = 10⁻³), as
    verification §4.8; β = 1.6 at 10⁻⁵: 18.97443372 / 420.76 MeV. It is the first `φ < 1.5 M`
    wall bounce on every window and history it was run on (P5's premise). The sample-based
    detector gives 742.79 MeV at M = 10⁻³ and 0.39 MeV at 10⁻⁵ on the same histories.
- **[00-initial-field-value-is-hard-coded-and-unchecked]** *(assigned from `review-remediation`;
  §3.1)*. **Resolved (2026-10-02):** by prompt 04 (log 04). `--phi-init-Mp` (default 5.0) in the
  shared parser; `main.py`, `plot_by_beta.py` and `plot_ScalarModel.py` build φ\* from it, with no
  `5.0 * units.PlanckMass` literal left. `pipeline_selection.super_planckian_couplings` returns
  the couplings with ln Ω(φ\*) + ln T\* > ln M_P, and `warn_super_planckian` (called by
  `main.py` before step 1) prints one warning per such coupling and the count and returns the
  coupling array unchanged: warn, never skip or refuse (P6). The dated **Resolved** line is also
  on the `review-remediation` board.
- **[00-bbn-input-is-aliased-at-small-M]** *(README §1 A; the source §2; README §6.1)*.
  - **What.** Below a few keV, `ρ_NP/ρ_R,J` jumps from sample to sample at small `M`, because the
    samples catch the bounces at random phase: ±0.4 % at M = 10⁻⁵ and ±0.85 % at 10⁻³, around a
    median ten times smaller (README §6.1). The source reports one PRyMordial failure from it
    (β = 1.6, M = 10⁻⁵, low-T network); the planner did not reproduce it on `6aaa706`.
  - **Impact.** Noise of the order of the signal in the input PRyMordial integrates below 3 keV.
  - **Next step.** Prompt 05.
  - **Resolved (2026-10-02):** by prompt 05 (log 05), as **not a defect** in PRyMordial's window:
    the jumps are resolved bounces, not aliasing. At β = 1.6, M = 10⁻⁵ only 6 of 130 cells in
    [0.3, 1) keV and 2 of 120 in [1, 3) keV contain a sign change of π; the ratio is a resolved
    sawtooth whose 10 largest steps carry 95 % and 99 % of the rms². The orchestrator's count
    (log 05, Verification) finds a median of 0 half-periods per cell from 100 MeV to 100 eV on
    β = 1.6 and 2 at M = 10⁻⁵; aliasing begins below about 100 eV. Cell means of H_J² on the dense
    output cut the rms step only to 0.71–0.84× and made β = 2, M = 10⁻⁵ fail in PRyMordial; the
    user withdrew them (2026-10-02). Point input completes BBN on all four histories of log 05
    (β = 2 at M = 0.5, 10⁻³, 10⁻⁵; β = 1.6 at 10⁻⁵).
