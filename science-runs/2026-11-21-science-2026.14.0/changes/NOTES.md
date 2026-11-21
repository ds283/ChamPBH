# changes — what moved between 2026.10.0 and 2026.11.0

**Of 2026-11-14**, PLAN.md P5 (ruled as proposed by the user, 2026-11-13): "a short `changes/` analysis joins this
run's `histories.csv` with 2026.10.0's on (M, β, row) and reports what moved … It sets no threshold." Written and run by
a Claude Code session (the run's orchestrator) at the user's request ("write the record and commit any outstanding work,
then the analyses"). **Not yet seen by the user.** It is new: no earlier run has one.

```bash
./venv/bin/python science-runs/2026-11-13-science-2026.11.0/changes/changes.py
```

It reads this run's main-store export (`../fig6/inputs/histories.csv`), 2026.10.0's
(`../../2026-11-08-science-2026.10.0/fig6/inputs/histories.csv`) and electroweak-eos 02's 48-history comparison
(`.documents/electroweak-eos/effect/out/effect.csv`), all committed. It needs no Ray and no store, and writes
`changes.csv` (one row per (M, β, row), each observable old, new, difference and relative difference), `changes.txt`
(the summary below, in full), `changes.pdf`/`.png` and `changes.provenance.json`. Committed in `b1804b3`, the commit the
provenance file records (clean tree). Measurements, not bounds.

**Choices of the session, not ruled:** the delivery class (below 10 MeV "not at the wall", as Figure 6's beige
region; 10 MeV–10 GeV "QCD"; above 10 GeV "EW", electroweak-eos verification §2.4.5's label) is a label, not a test;
"the edge" is the dense grid's fine segment, β = 2.950–3.040; the figure's layout (four rows of |new − old| against β,
coloured by block).

## Beside electroweak-eos's 48

electroweak-eos 02 built 48 of 2026.10.0's histories on the new table (verification §2.4). **47 of them are in this
run's main store, and every observable of every one is identical** — T_deliver, Y_p, D/H, the shifts, ΔN_eff today and
at 1 MeV, the parking temperature, ln R at the first impact, the reflections and the tuning iterations, bit for bit.
The 48th is R-ladder's (10⁻¹⁶ M_P), in the reflection store. So that prompt's measurements of the change are this run's,
and its old side equalled 2026.10.0's export (§2.4.4): the two runs differ by the table alone.

## What moved

Of 4758 histories in 2026.10.0 and 4818 here, 4452 are ok in both.

- **Status.** Four histories changed status, all at M_crit and all at the 30-integration tuning cap: 10⁻¹⁵ β = 1.1 and
  1.2 (A = 1) now converge; 10⁻¹⁵ β = 1.4 (A = 1) and 10⁻¹² β = 1.0 (α = 0.01) now fail. The 60 new histories (β =
  2.950–2.975 at laboratory M) are all ok. Every other failure is the same kind as before (PRyMordial's floor fails at a
  `T_in_MeV` that differs in the fifth digit).
- **Delivery class.**
  - **QCD → EW, 129 histories**: at laboratory M, β = 2.985, 2.99, 2.995 and 3.0 in every row (48; 1.68–1.93 GeV →
    20.6–48.5 GeV); and β = 3.0 at every other M of the store (B 72, C 6, D 3; 1.68 → 48.5 GeV). This is the move of
    the electroweak-capture edge from (3.000, 3.005] to (2.980, 2.985] that electroweak-eos measured.
  - **QCD → not at the wall, 12 histories**: β = 1.0675 at laboratory M in every row, T_deliver 10.13 → 9.88 MeV,
    across Figure 6's 10 MeV label. Figure 6's beige edge, computed from the data, moves from 1.066 to 1.069 with it.
  - The rest are the four status changes at M_crit.
- **At the edge** (β 2.950–3.040, ok in both): T_deliver moves by up to a factor 28.9 (2.985–3.0) and by a median 26%
  at laboratory M; ΔY_p by up to 0.26 pp and Δ(D/H) by up to 0.66 pp; ΔN_eff today by up to 0.049 (absolute; up to
  +241% relative, at laboratory M); ln R at the first impact by up to 14.9. None of these moves a Figure 6 test.
- **Above the edge, the EW deliveries move by tens of percent.** At laboratory M, A = 1, T_deliver rises by 9–12% at
  β = 3.04–4 (60.0 → 65.6 GeV at 3.04; 97.6 → 109.5 at 3.5), by 2.7% at 5, and then falls: −6% at 6, −21% at 8,
  −32% at 10, −51% at 15 and **−61% at β = 20 (846 → 328 GeV)**, the largest move away from the edge in every block.
  ln R at the first impact moves with it (+0.5 at 3.5, −1.7 at 10, −4.0 at 20) and ΔN_eff today by −20% to +32% (+32% at
  β = 20). These are the histories that deliver above 100 GeV, where the new table follows Laine & Meyer and the old
  one was a free gas at vacuum masses; electroweak-eos sampled only β = 5.0 there (+2.7%). Their yields hardly move
  (at β ≥ 3.25, |ΔΔY_p| ≤ 0.05 pp and |ΔΔ(D/H)| ≤ 0.04 pp over every row and block; 0.007 and 0.023 pp at
  laboratory M, A = 1), and they are far inside the tests.
- **Below the edge, away from the wall**: medians of 7×10⁻⁵ in T_deliver, 0.004 pp in ΔY_p, 0.008 pp in Δ(D/H) and
  2×10⁻⁴ relative in ΔN_eff today, in every block. The largest moves are at the non-surfing β ≤ 1.0 (Δ(D/H) up to 2.9
  pp, and 4.2 pp in block D at β = 0.9, where the shifts are hundreds of percent), in the transition window just above the wall
  (ΔN_eff today up to +123% at β = 1.0625, A = 0.05, from 2.6×10⁻⁴), and at M_crit, where the drain is tiny (ΔN_eff
  today ×94 at 10⁻¹², β = 2.8, A = 1, from about 10⁻⁷; parking T by a factor up to 10⁵) and the reflection counts
  change by up to 10⁴ in the histories near the tuning cap.

## Figure 6's tests

Over the histories ok in both runs, by Figure 6's tests (|ΔY_p| > 2.45%, |Δ(D/H)| > 2.37%, ΔN_eff today > 0.30):

| Block | excluded, 2026.10.0 | excluded, 2026.11.0 | become excluded | cease to be |
|---|---|---|---|---|
| A | 803 | 810 | 10 | 3 |
| B | 437 | 436 | 0 | 1 |
| C | 32 | 32 | 0 | 0 |
| D | 170 | 170 | 0 | 0 |

All 14 flips are near a test's edge, at β = 1.0625–1.25 (A's 13 are listed in `../fig6/NOTES.md`; B's is 10⁻³², β =
1.25, A = 2, whose Δ(D/H) moves from 2.380% to 2.374%, just inside 2.374%). The 60 new histories are none of them
excluded (`../fig6/NOTES.md`).

## What it does not cover

The reflection store (44 histories; its export is not committed for 2026.10.0, and its histories are drawn whole
rather than compared), the per-history `fields.csv` and `bounces.csv` (electroweak-eos verification §2.4.6 compared
the bounce rows of its 37 one-loop histories), and the derived measures (compared in `../relativistic/NOTES.md`).
