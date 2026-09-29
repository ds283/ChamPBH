# Open issues — project-wide index

**Last updated:** 2026-09-30 · **15 open**, all on the `review-remediation` board (closed
2026-09-30); 4 of them assigned to `production-readiness` (planned 2026-09-30).

This file exists so that an issue opened by one campaign is not lost when that campaign closes.
It is an **index, not a record**: one line per issue, pointing at the campaign status board that
holds the measurements, the impact statement and the next step. Never put issue content here — if
the two disagree, the board is right.

> **Maintenance rule.** Whenever you add, narrow or close an entry in a campaign board's
> §3 (Active and unresolved issues) or §4 (Resolved issues), update this file **in the same
> commit**. Add the line, move it between sections here, or delete it, and correct the count and
> the date above. See `CLAUDE.md`.

**Boards.** [`review-remediation`](../prompts/review-remediation/IMPLEMENTATION_STATE.md) ·
[`production-readiness`](../prompts/production-readiness/IMPLEMENTATION_STATE.md)

## 1. Open, by owning board

### 1.1 `review-remediation` — [board §3](../prompts/review-remediation/IMPLEMENTATION_STATE.md)

Seven seeded at planning on 2026-09-29 from `.documents/audit-2026-09-29/README.md`. The two
opened after that on 2026-09-29 are resolved (board §4). One opened by prompt 02, four by prompt 03,
one by prompt 04, one by prompt 05, one by prompt 06. The campaign closed on 2026-09-30; these rows
stay here until a later campaign takes them. Four were assigned on 2026-09-30 and are listed
in §1.2.

- `[00-initial-field-value-is-hard-coded-and-unchecked]` — `main.py:814` fixes φ* = 5 M_P with no
  A*T* ≲ M_P guard (review H8). Out of this campaign's scope.
- `[00-kicking-function-table-has-no-provenance-in-the-repository]` —
  `CosmologyModels/GenericEOS/Xav_EOS_data.csv` was added in commit `1759515` without the script
  that built it.
- `[00-datastore-lookups-ignore-the-version-column]` — `ScalarModel`, `AdiabaticHistory` and
  `BBNData` rows carry a `version` but no lookup filters on it, so a numerical fix returns stale
  rows from an old store without complaint.
- `[00-two-files-are-not-black-clean]` — `Datastore/SQL/ObjectFactories/base.py` and
  `CosmologyModels/LambdaCDM/Planck.py` at `f5896bb`; housekeeping.
- `[00-bbn-spline-domain-is-far-wider-than-prymordial-uses]` — `BBNData.py:47` tabulates down to
  0.1 eV where PRyMordial stops near 0.3 keV; harmless at 250 knots per decade, but it includes
  the matter-era re-delivery oscillations.
- `[02-stale-derivative-and-T_LO-comments-in-the-EOS-package]` — three wrong comments in
  `CosmologyModels/GenericEOS/` (the base `dG_s_dlogT` docstring, the jax class's "1/GeV", the
  "600 keV" above `SAIKAWA_SHIRAI_T_LO`); comment-only.
- `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]` — PRyMordial's Yp and D/H
  move by 1e-5–7e-4 under 1e-9–1e-8 changes to ρ_NP; prompt 04's 1e-4 D/H test passes at 8.85e-5
  inside that band.
- `[03-bbn-solver-failures-are-undetected-and-some-exceptions-escape]` — PRyMordial never checks
  `solve_ivp`'s status, and `compute_BBN_data` catches only three exception types.
- `[03-main-recomputes-failed-bbn-rows-on-every-run]` — `main.py` looks up successes only, so a
  deterministic BBN failure is recomputed and re-stored each run.
- `[04-numerical-strategies-describes-the-removed-asinh-bbn-interface]` —
  `.documents/numerical-strategies.md` §7.2–7.4 still describe the asinh transform and the sort
  that prompt 04 removed; needs a dated addendum.
- `[05-kicking-table-and-saikawa-shirai-gs-disagree-through-qcd-and-ew]` — the table's Σ peaks
  (QCD 0.3145, EW 0.0374) match neither the Σ the g's imply by conservation (0.299, 0.058) nor the
  4g_s/(3g_ρ) − 1 formula (0.249, 0.0374 at 46 GeV); a decision for the authors.

### 1.2 Assigned to `production-readiness` — [its board](../prompts/production-readiness/IMPLEMENTATION_STATE.md)

Assigned 2026-09-30 by the user, to clear before the production run. The entries stay on the
`review-remediation` board (§3), which carries the **Assigned** line; the prompt that closes one
deletes its row here.

- `[06-hard-reflection-caption-reads-the-wrong-key-and-always-prints-zero]` — `extract_common.py:216`
  looks up `hard_reflections` but the count is stored as `number_hard_reflections`, so every
  `plot_ScalarModel.py` caption says 0.
- `[00-hard-reflection-count-is-stored-but-never-reported]` — the hard-reflection count is on every
  `ScalarModel` row; no survey summary reports it, and the one caption that tries reads it wrongly
  (review N1; qualified by prompt 06).
- `[03-small-network-flag-is-never-read-by-prymordial]` — `BBNData.py` sets
  `small_network_flag`, PRyMordial reads `smallnet_flag`; every BBN solve so far used the full
  network.
- `[00-adiabaticity-diagnostic-omits-the-source-response-term]` — `AdiabaticHistory.py:103`
  gives zero conformal mass for the exponential coupling; the (Ω′)² response term is missing
  (review H5).
