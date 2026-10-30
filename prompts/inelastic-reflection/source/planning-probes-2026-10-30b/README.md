# Probes for prompt 08, 2026-10-30 (b)

Read-only. The orchestrator ran these at `79091b1` (prompt 07b landed and reviewed) at the user's word
("Please write prompt 08"). Nothing is stored, no Ray is started, and nothing in the tree is changed. Each
probe is `../planning-probes-2026-10-30/peaks.py` with a block added at its end, so every history uses that
probe's setup: `test_injection`'s at α = 0.1, untuned. Its reading of the generations, the estimate by 𝓡
and the margins is in that directory's README §0. The numbers are measurements; no threshold was set in
advance. The load was 4–30.

All commands run from the repository root. Pickles go to `$PROBE_OUT`, or the system's temporary directory
if it is not set.

## 1. Three readings of the tracker margin (`track3.py`)

    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-30b/track3.py BETA M [ALPHA]

Each history is reported as its worst margin over the weighted generations (share ≥ 10⁻³, U9), with the
share of ρ_χ in weighted generations below ln 10. Today is included in every reading.
- **A**: the tracker's mass m_eff(φ_min) at every stored sample after each generation's birth (peaks.py's
  reading);
- **B**: the tracker's mass at the samples after the history's last generation only (`N_late` is
  `Parking.N`, or the last generation's N_turn for a history that was released or never parked);
- **C**: the field's own m_eff(φ) at every sample after birth (scoping §3.4 read literally).

**`track3.out`**, M = 0.5 at β = 2 and 5, M = 10⁻³ at β = 1.2, and β = 2 at M = 10⁻¹² and 10⁻³⁰. At
M = 0.5, C repeats the birth margin (about −0.70) at samples that land near a wall. A lies between, and B
passes by 8. At M ≤ 10⁻³ every reading passes. The user chose B (README §8.1 R20).

## 2. Do the turnarounds cover the time before the late tracker? (`cover.py`)

    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-30b/cover.py BETA M [ALPHA]

R20's proviso. Here `N_late` is the last turning point of either kind or the last generation's N_turn,
whichever is later.
- For each weighted generation, the field's own mass margin is formed at every stored sample with
  N_turn < N ≤ N_late.
- Its minimum is compared with the smallest of three margins, all at the masses peaks.py uses:
  - the birth margin;
  - the margins at every later candidate's turnaround (the generation's mass by 𝓡);
  - the margins at every later plateau turning point (the field's own m_eff on the dense output there).
- The script prints the smallest difference over the generations, the number of generations below the
  bound, and the same count without the plateau turning points.

**`cover.out`**, M = 0.5 at β = 2, 5 and 25; M = 10⁻³ at β = 1.2; β = 2 at M = 10⁻¹², 10⁻¹⁶ and 10⁻³⁰;
β = 3 at 10⁻¹⁶:
- **The turnarounds bound the samples** on seven of eight histories, by +0.0003 (M = 0.5, β = 5) to +42.
- **At M = 0.5, β = 25 one generation's samples fall 3×10⁻⁴ below the bound.** That is the size of the
  momentum's redshift during one swing. The bound holds only up to that redshift, and prompt 08 §1.6
  derives it.
- **The plateau turning points never bind** on these histories: each count is the same without them.
