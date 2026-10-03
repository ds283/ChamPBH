# BBN-tolerance campaign — implementation state

**Last updated:** 2026-10-03 · **Status: IN PROGRESS — re-planned around the small network (U3);
prompt 01 committed BLOCKED and ruled; 01c next, once P10–P13 are accepted** (01b added
2026-10-03, U2; 01c added 2026-10-03, U3). Planned on 2026-10-03 against
`main` at `4ae25b4`, from a Claude Science brief kept at
[`source/brief_prym_lowT_failures.md`](source/brief_prym_lowT_failures.md) and checked against the
tree by the planner (README §0.3). Suites at `8efc50f` (no code changed through `4ae25b4`):
CosmologyModels 18, ComputeTargets 103, Datastore 31.
**Target branch** `bbn-tolerance`, cut from `4ae25b4`; planning and orchestration commits land on
it.
**`VERSION_LABEL` is `"2026.6.0"`** and does not change in this campaign (README §0.2 P5).
**`PRYM_VERSION` is `"bf24c3d+ri02+sr01"`**; prompt 02 makes it `"bf24c3d+ri02+sr01+bt02"`.

The campaign finds out why PRyMordial's low-temperature network fails on 11 of 684 φ\* = 5
histories of the 2026.6.0 science run, and why D/H scatters by up to 2.2×10⁻³ under ulp-level input
changes. It chooses a tolerance by measurement, patches it in, warns when a store serves BBN rows
from another PRyMordial version, and hands the user the route to refresh BBN without recomputing
histories.

**Campaign:** [`README.md`](README.md) ·
**Code (planned):**
- `tools/bbn_from_store.py` (new in 01);
- `PRyM/PRyM_main.py` (the two low-T calls; 02); `ComputeTargets/BBNData.py` (`PRYM_VERSION`;
  02);
- `pipeline_selection.py`, `main.py`, `plot_by_beta.py` (the warning; 02);
- `extract_common.py` (`kick_threshold_curve`; 01b), `plot_by_beta.py` (a comment; 01b);
- `ComputeTargets/tests/` (01, 02);
- `.documents/numerical-strategies.md`, `numerical-methods-for-paper.md`,
  `paper-corrections-numerical-section.md`, `review-remediation-verification.md` (03, additive).

**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) §1.9–§1.10.

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) is updated **in the same
> commit**: the row is added, moved or deleted, and the count and date in its header are
> corrected. The index is an index: one line per issue, pointing at the board that holds it. Where
> the two disagree, the board is right. See `CLAUDE.md`.

### Decisions

- **2026-10-03, the user (planning conversation): README §0.2 U1.** Prompt 01 may run the full
  tolerance scan (about 350 solves) in parallel, 8–10 at a time, as a one-off exception to
  `CLAUDE.md`'s limit of a few PRyMordial solves per prompt. Wall times are measured separately,
  serially, on an idle machine. The store stays read-only.
- **2026-10-03, the planner: README §0.2 P1–P9 proposed.**
  - P1: prompt 01 changes no production code and nothing in `PRyM/`; tolerances are changed by
    interception.
  - P2: the scan grid.
  - P3: the rule for choosing the setting.
  - P4: the patch to both low-T calls, and `PRYM_VERSION` `+bt02`.
  - P5: no `VERSION_LABEL` bump; refresh by copy and `--drop bbn-data`.
  - P6: warn on a foreign `PRyM_version`, never filter.
  - P7: re-pin pinned constants, never loosen a bound.
  - P8: never accept a partial solve.
  - P9: residuals are measurements.

- **2026-10-03, the user: P1–P9 accepted as proposed.** With this line the precondition of
  orchestrator 01 §1.1 is met. P3's rule stands; the setting it selects is ruled separately after
  log 01, as orchestrator 02 §1.1 requires.
- **2026-10-03, the user: U2, add prompt 01b (item B).** The `T_deliver` figure's kick-threshold
  curve uses β_th = 1/√(3Σ). The paper's reachability condition gives 1/√(3Σ_eff) =
  √((2 + Σ)/(6Σ)). The fix is a separate prompt, independent of the PRyMordial work. The Claude
  Science note that raised it also said the overlay uses a different Σ from the integration. The
  planner checked this and found it wrong: both call the history's own `QCD_Cosmology.w`, the
  `Xav_EOS_data.csv` spline (README §2 (h)). The issue
  `[00-the-kick-threshold-overlay-uses-sigma-not-sigma-eff]` is opened for 01b.

- **2026-10-03, prompt 01 (log 01): STOP — no setting in the grid meets P3. Awaiting the user.**
  - **Criterion 3** (the SM moves by ≤ 1e-3 in D/H) fails at every converged setting. The
    default's own SM D/H is 1.35e-3 above the converged value, which agrees to 2.1e-5 at rtol
    1e-5, 1e-6 and 1e-8.
  - **Criterion 2** (D/H spread < 1e-4 on all 16 histories) misses on β = 2, M = 10⁻⁵ at every
    setting (1.04–1.22e-4). That floor is set by PRyMordial's thermodynamic stage.
  - **Criterion 1** is met by 1e-4 and 1e-5 in S1, but not guaranteed by any tolerance. The
    failures come from the Li8(p,d)Li7 rate, `[01-prymordial-li8-p-d-li7-rate-rings-near-1-kev]`,
    and appeared at 1e-6, at 1e-8, at both S2 settings, and once at 1e-5 in the Yp-floor runs.
  - **Criterion 4** (serial cost ≤ 3×) holds at 1e-5 (2.2–2.5×) and fails at 1e-6 (3.7–4.1×).
  - With criterion 3 waived and criterion 2 measured against the floor, P3 would select low-T
    **rtol 1e-5, atol unchanged**. Log 01 measured Yp's floor and the P7 values there,
    provisionally.
  - The ruling needed:
    - the setting;
    - criteria 2 and 3;
    - whether a PRyMordial rate patch (outside P4) is allowed;
    - re-pinning `test_bbn_callbacks`' `README_BASELINE`, which fails at any converged setting
      (D/H 1.38e-3 against its 1e-4 bound).

- **2026-10-03, the user: U3, re-plan around the small network; ruling on log 01's stop.**
  - **The ruling.** The full network's failures come from its lithium network, which we must
    live with or work around. The lithium abundance is not used for cosmological constraints
    (the PDG declines to quote one); what matters is computing Yp and D/H reliably. Production
    is to move to PRyMordial's small network, which has no Li8. **The first step is a
    measurement of the small network**: prompt 01c (README §0.2 U3, §2 (c′)).
  - **Log 01's stop is resolved by this ruling.** No full-network setting is ruled. Prompts 02
    and 03 are rewritten after the ruling on log 01c and carry a notice until then.
  - **Log 01's deviations accepted:** 2 (the tool imports `Datastore` indirectly, through
    `ComputeTargets.BBNData`; it opens the store only read-only) and 9 (the cost run under
    background load; "not much we can do about this").
- **2026-10-03, the planner: README §0.2 P10–P13 proposed after U3. Awaiting the user.**
  - P10: prompt 01c measures and changes nothing, the tool included; about 340 solves, 8–10 at
    a time, as U1.
  - P11: the rule for the small network's low-T setting: reliability, scatter, convergence
    against `rtol` 1e-8, and cost ≤ 3× today's full-network default.
  - P12: the small network's offset from the full one is a measurement, never a bound.
  - P13: what the rewrite of prompt 02 must settle, ruled with log 01c.

  Orchestrator 01c §1.1 requires the user's acceptance of P10–P13 recorded here.

Decisions the prompts may surface, each a stop-and-ask in its prompt:

- the tool does not reproduce the stored outcome (prompt 01, §8);
- the mechanism points outside the low-T stage (prompt 01, §8);
- no setting meets P3 (prompt 01, §6);
- **the setting itself** (P3): the user rules on log 01's recommendation before prompt 02;
- the patched tree does not reproduce log 01 digit for digit (prompt 02, §5);
- a re-pinned test fails its unchanged bound (prompt 02, §5);
- log 01c's reproduction of log 01's S3 fails, a small-network solve fails, or no setting meets
  P11 (prompt 01c, §6);
- **the small network's setting** (P11) and P13: the user rules on log 01c before prompt 02 is
  rewritten.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [The mechanism and the low-T tolerance scan](01-mechanism-and-tolerance-scan.md) | **M**; measures **S**, **N** | Opus | ✍️ 2026-10-03 | ⛔ 2026-10-03, **BLOCKED** (no setting meets P3; the user rules) | see `git log` ("Add bbn_from_store and measure PRyMordial's low-T failures") | [`logs/01-mechanism-and-tolerance-scan.md`](logs/01-mechanism-and-tolerance-scan.md) |
| 01c | [Measure the small network](01c-small-network-scan.md) (added 2026-10-03, U3) | **Q** | Opus | ✍️ 2026-10-03 | — | — | — |
| 01b | [Draw the kick threshold with Σ_eff](01b-kick-threshold-sigma-eff.md) (added 2026-10-03) | **B** | Sonnet | ✍️ 2026-10-03 | — | — | — |
| 02 | [Set the low-T tolerance and warn on a stale PRyMordial version](02-low-T-tolerance-patch.md) | **S**, **N**, **K** | Opus | ✍️ 2026-10-03; **to be rewritten after log 01c** (U3) | — | — | — |
| 03 | [Documents and close-out](03-documents-and-close-out.md) | **D** | Sonnet | ✍️ 2026-10-03; **to be revised with 02** (U3) | — | — | — |

---

## 2. Items

| Item | Kind | Description | Prompt | Status |
|---|---|---|---|---|
| M | measurement | The mechanism, the tolerance scan, Yp's floor, upstream's defaults, and a recommended setting. | 01 | **measured** (log 01); recommendation withheld: no setting meets P3 |
| Q | measurement | The small network on the roster and a breadth sample: reliability, scatter, convergence, cost, its offset from the full network, and a recommended setting by P11. Added 2026-10-03 (U3). | 01c | open |
| S | **DEFECT, medium** | The full low-T network fails near T_J = 1 keV on 11 of 684 φ\* = 5 histories, depending on ulp-level details of the input. Closes `[00-the-low-T-network-fails-near-1-keV-on-ulp-level-input]`. | 01, 02 | open; measured (log 01): the cause is a PRyMordial rate, not the tolerance as such |
| N | **DEFECT, low–medium** | D/H moves by up to 2.2×10⁻³ under a 10⁻¹² change to ρ_NP at the default tolerance. Closes `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]` (assigned). | 01, 02, 03 | open; measured (log 01) |
| B | **DEFECT, low** | The `T_deliver` figure's kick threshold uses 1/√(3Σ), not the paper's 1/√(3Σ_eff); its minimum is 1.0295 where the paper says 1.11. Closes `[00-the-kick-threshold-overlay-uses-sigma-not-sigma-eff]`. Added 2026-10-03 (U2). | 01b | open |
| K | **GAP** | A store that was not refreshed serves BBN rows from an older PRyMordial with no warning. Closes `[00-a-store-serves-bbn-rows-from-another-prym-version-silently]`. | 02 | open |
| D | documents | The tolerance, its measurements, the refresh route, the handover. | 03 | open |

---

## 3. Active and unresolved issues

Two opened by the planner on 2026-10-03, one per defect or gap with no issue on another board, and
one more the same day for prompt 01b (U2). Two opened by prompt 01 the same day (`[01-…]`). One
assigned from another board (§3.1).

- **[00-the-low-T-network-fails-near-1-keV-on-ulp-level-input]** *(README §1 S; the brief §1–§2)*.
  - **What.** In the 2026.6.0 science store, 11 of 684 φ\* = 5 histories have a `BBNData` failure
    row with `PRyMSolverFailureError: solve_ivp failed in stage 'low-T nuclear network (full)':
    status=-1, message='Required step size is less than spacing between numbers.'`. Each failed at
    96–99 % of the stage's end time, at T_J just above 1 keV. 9 are surfing histories and 2
    non-surfing (README §6.0). Their neighbours in β and M complete. The brief reports that any
    change to the input of 10⁻¹² relative or more cures every one.
  - **Measured (the brief, 2026-10-03; not yet re-measured in this repository).** The low-T calls
    pass no `rtol`, so SciPy's `1e-3` applies. With `rtol = 1e-6` on the full call, four failures
    and the control complete.
  - **Impact.** 11 rows are "not assessed", including β = 1.345 inside the threshold zoom and five
    points of the convergence-in-M comparison. Whether a history fails is not reproducible across
    harnesses (β = 1.6, M = 10⁻⁵ failed on 2026-10-01 and completed in the `science-readiness`
    close-out).
  - **Next step.** Prompt 01 measures; prompt 02 fixes.
    **Re-planned (2026-10-03, U3):** prompt 01c measures the small network, which lacks the
    faulty rate; prompt 02, rewritten after the ruling on log 01c, moves production to it.
  - **Narrowed (2026-10-03, prompt 01).**
    - **Reproduced.** All 11 fail in the tool exactly as stored: same stage, same `t reached`,
      identical reason.
    - **The mechanism is in the low-T stage, but it is not the error test.** On β = 1.6,
      M = 10⁻⁵ and β = 1.05, M = 0.01:
      1. BDF's Newton iteration fails at every step size, from 1e4 s down to 3e-9 s, with
         contraction rate → 1.07–1.09.
      2. The Jacobian it holds was refreshed 1e4–3e4 s ahead, where
         ∂f_Li8/∂Y_Li8 = +1.6–2.3e15 s⁻¹. At the retried t it is −1.5–2.0e14 s⁻¹.
      3. The cause is PRyMordial's Li8(p,d)Li7 reverse rate, α·exp(γ/T9) times a global
         quadratic spline of the forward-rate table. The spline rings in sign at ~1e-54, where
         the table is ~1e-245. See `[01-prymordial-li8-p-d-li7-rate-rings-near-1-kev]`.
      4. f is smooth, and no `T_of_t` breakpoint falls in the collapsing steps.
    - **Tightening rtol does not remove it.** In the scan:
      - 1e-4 and 1e-5 complete all 11 (0 failures in 49 solves each);
      - 1e-6 fails β = 1.1, M = 0.03 (prod);
      - 1e-8 fails the control β = 1.2, M = 10⁻³;
      - each S2 setting fails one solve;
      - 1 of 109 solves at 1e-5 failed (in the Yp-floor runs).
    - **The provisional setting.** At rtol 1e-5 (not ruled; see Decisions) the cost is
      2.2–2.5× the default's. The SM baseline moves −1.36e-3 in D/H (the default's own error)
      and −2.0e-6 in Yp.
- **[01-prymordial-li8-p-d-li7-rate-rings-near-1-kev]** *(log 01, Verification item 6; Observations 1)*.
  - **What.** `Li7dLi8p_bkwrd` (`PRyM/PRyM_nuclear_net63.py:1142–1147`; upstream `bf24c3d` is
    identical) is α·T9^β·exp(γ/T9)·spline(T9), with γ = 2.2274. The spline is
    `interp1d(kind="quadratic", fill_value="extrapolate")` (`:197`) through a 500-node table of
    the forward rate.
    - Near T9 = 0.0116 (T ≈ 1 keV) the table is 1e-264 to 3e-241. The spline gives −6.9e-55 to
      +3.9e-54, negative on 51 % of T9 ∈ [0.0105, 0.015].
    - The reverse rate therefore reaches |1.45e39| and flips sign between neighbouring
      temperatures, where it should be about 1e-160.
    - It is the only splined rate in `PRyM_init.py` with γ > 0 (a regex scan).
  - **Impact.** It causes the 11 low-T failures of `[00-the-low-T-network-fails-near-1-keV-on-ulp-level-input]`,
    and it keeps causing about 1 % of solves to fail at every tolerance in the grid. Li8 is
    tiny, so its effect on the abundances of a completed solve is not established.
  - **Re-planned (2026-10-03, U3).** The user ruled to work around this rate, not patch it.
    Production moves to the small network, which has no Li8, subject to log 01c. The issue stays
    open: the full network keeps the defect for anyone who selects it.
  - **Next step (as first recorded).** The user rules. A patch to a PRyMordial rate is outside P4
    and is a stop for prompt 02 (README §4). Options:
    - interpolate the table in log space, or return 0 below its significant range;
    - clamp the reverse rate;
    - use the shadowed analytic forward rate at `:886–889`.
- **[01-prymordial-dYB8dtLT-unpacks-Y-in-the-superseded-order]** *(log 01, Observations 2)*.
  - **What.** `dYB8dtLT` (`PRyM/PRyM_nuclear_net63.py:1480`; upstream identical) unpacks Y in the
    old species order, which every other low-T equation has commented out. So in B8's equation
    He6 ← Y[Li7], Li6 ← Y[Be7], Li7 ← Y[He6] and Be7 ← Y[Li6]. B8's right-hand side is wrong and
    does not match the Jacobian's B8 row.
  - **Impact.** Not measured. B8 stays below 1e-16 and enters no reported abundance, so it is
    probably negligible.
  - **Next step.** Measure it by a runtime override of `dYB8dtLT` in a probe; patch only if the
    user asks.
- **[00-a-store-serves-bbn-rows-from-another-prym-version-silently]** *(README §0.3, §1 K)*.
  - **What.** `BBNData` lookups are keyed on `VERSION_LABEL` and ignore `PRyM_version`
    (`Datastore/SQL/ObjectFactories/BBNData.py` `build`). Only `inventory` lists the versions
    present.
  - **Impact.** After prompt 02, `plot_by_beta.py` on a store that was not refreshed would plot
    old-tolerance abundances with no sign that they are old.
  - **Next step.** Prompt 02 (README §0.2 P6): warn, do not filter.
- **[00-the-kick-threshold-overlay-uses-sigma-not-sigma-eff]** *(README §0.2 U2, §1 B, §2 (h))*.
  - **What.** `extract_common.kick_threshold_curve` returns 1/√(3Σ), with the same text in
    `plot_T_deliver`'s legend and in a comment in `plot_by_beta.py`. The paper's reachability
    condition gives β_th = 1/√(3Σ_eff) = √((2 + Σ)/(6Σ)). The Σ itself is the integration's.
  - **Measured** (planner, `d78c9f8`). The curve's minimum is 1.0295 at the QCD peak
    (Σ = 0.31453, 0.182 GeV); with Σ_eff it is 1.1074, the paper's 1.11.
  - **Impact.** The dashed curve on figure 3 sits about 7 % low in β, so delivery points between
    1.03 and 1.11 look as if they sit above the threshold.
  - **Next step.** Prompt 01b.

### 3.1 Assigned to this campaign from other boards

Each entry stays on its own board, which carries an **Assigned (2026-10-03)** line. The prompt
that closes one adds a dated **Resolved** line there, deletes the index row, and records it in §4
here (README §5 rule 4).

| Issue | Board | Prompt |
|---|---|---|
| `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]` | `review-remediation` | 01 (measures), 02 (closes) |

- **Narrowed (2026-10-03, prompt 01), `[03-…]`.**
  - **The scatter at the default.** At the default low-T tolerance the D/H spread over prod,
    pert12 and pert9 is 1.3e-4–2.8e-3 on the five controls (median 1.45e-3).
  - **Most of it is the low-T rtol.** It falls to a median of 8.4e-5 at rtol 1e-4, 4.1e-5 at
    1e-5, and 1.8–2.0e-5 at 1e-6–1e-8.
  - **What remains is set by other stages.**
    - The Yp spread, 1.5–4.5e-5, does not move with the low-T rtol. It falls about 100× only
      when the thermodynamics, a(T), high-T and mid-T stages are all tightened.
    - β = 2, M = 10⁻⁵ keeps a D/H spread of 1.04e-4 at every low-T rtol. It falls to 2.4e-5 with
      the thermodynamic stage tightened.
  - **A systematic offset.** The a(T) stage at its rtol 1e-6 shifts D/H by about +4.5e-4.
  - These are measurements (P9), handed to prompt 03.

---

## 4. Resolved issues

None yet.
