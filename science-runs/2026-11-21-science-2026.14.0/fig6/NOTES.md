# fig6 — Figure 6 at the laboratory mass, 2026.14.0, one figure per wall

**Ported 2026-11-20, before the run** (PLAN.md §5, P9), by a Claude Code session at the user's request ("Port the
analysis scripts while the probe runs"). No figure of this run exists yet; this note is rewritten when the run's
figures are made. Below the line is 2026.11.0's note, unchanged, for the design and rulings this figure keeps.

```bash
./venv/bin/python science-runs/<folder>/fig6/fig6.py --wall {exp,pl1,pl2} [--points]
```

**What the port changed** (`fig6.py`'s docstring lists the same):
- one figure per wall (`--wall`; the `WALLS` table): `exp` (exponential, n = 1, `one-loop-exponential-v2`), `pl1` and
  `pl2` (inverse power law, n = 1 and 2, `one-loop-inverse-power-v1`), each reading `inputs/<wall>/histories.csv`, a copy
  of that wall's `plot_by_beta.py` export, and writing `fig6-<wall>.*`;
- the one-loop rows are the wall's own drain model in place of the retired `one-loop-measured-v1`, and rows are filtered
  by the export's `model` and `n` columns;
- **`EW_BRACKET` is per wall and starts as `None`** (no electroweak region drawn). At every run the script prints the
  adjacent pair of β in 2.9–3.1 with the largest jump in the median delivery temperature, and the provenance records it
  as `ew_candidate`; the bracket is set from that, wall by wall, once the run's exports exist, and stated here;
- `--inputs`, `--sidecar` and `--out` point it at another export, for testing.

**Checked.** On 2026.11.0's own input with a `model` column added and `one-loop-measured-v1` renamed, both by editing the text so that no float is re-parsed,
`one-loop-exponential-v2`, `--wall exp` reproduces every count of 2026.11.0's `fig6.provenance.json` (the wall's edge,
the β without BBN, the parking count, the A count per β, every exclusion count by A and by α), and its `ew_candidate`
is β = 2.980–2.985 (×10.6 in T_deliver), the bracket 2026.11.0 set by hand.

---

# fig6 — Figure 6 at the laboratory mass, 2026.11.0

**Of 2026-11-14**, made by a Claude Code session (the run's orchestrator) at the user's request ("write the record and
commit any outstanding work, then the analyses"). **Not yet seen by the user.** It repeats the 2026.10.0 Figure 6
([`../../2026-11-08-science-2026.10.0/fig6/`](../../2026-11-08-science-2026.10.0/fig6/NOTES.md)) on this run's
store, with that folder's rulings (the user's of 2026-11-09: the band over A = 0.05–3, A = 1 black, A = 3, 5 and 10
as green long-dashed lines, the fixed law at α = 0.01 and 0.1 as blue dash-dot lines, Planck's one-tailed
ΔN_eff < 0.30, no point for each history) and no new choice. That folder's NOTES.md says what each element is and
where each number comes from; it is not repeated here.

```bash
./venv/bin/python science-runs/2026-11-13-science-2026.11.0/fig6/fig6.py [--points]
```

It reads only `inputs/histories.csv`, an unchanged copy of `var/science-2026.11.0/out/main/QCD_Cosmology/histories.csv`
(SHA-256 `fb6a8dbb…`, written by the run script's `plot_by_beta.py` step, 09:12–09:18 UTC on 2026-11-14), and the
main store's sidecar in `../registry/stores/`. It writes `fig6.pdf`, `fig6.png` and `fig6.provenance.json`.

## What changed in the script

`fig6.py` is 2026.10.0's with three changes, listed in its docstring and committed in `ad7df2d`, the commit its
provenance file records (clean tree):
- the run's name in the docstring, the title and the PDF metadata;
- the sidecar it cites, `science-2026.11.0.manifest.json`;
- **`EW_BRACKET`**, the grey "surfs the electroweak kick" region, which marks where the delivery temperature jumps at
  laboratory M. In this run's export it jumps between β = 2.980 (1.95 GeV) and 2.985 (20.6 GeV) at every A and α,
  where 2026.10.0's jumped between 3.000 and 3.005; the script's constant is set to the new bracket. This is the move
  `electroweak-eos` measured (its verification §2.4) and the reason PLAN.md §0.1 moved the dense grid's fine segment.

The beige "not at the wall" region's edge is computed by the script from the data (the largest β delivered below
10 MeV); it reads β < 1.069 here.

## What it says

Counts for β ≥ 1.06 (187 histories at each A or α: 2026.10.0's 182 and the five new β 2.950–2.975), as in 2026.10.0's
table. The same counting reproduces that table exactly from that run's input. (The provenance file's own counts include
β = 1.0, so they are one larger.)

| | A = 0.05 | 0.1 | 0.2 | 0.5 | 1 | 1.5 | 2 | 3 | 5 | 10 | α = 0.01 | α = 0.1 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| excluded | 77 | 78 | 70 | 68 | 61 | 49 | 39 | 37 | 88 | 93 | 77 | 64 |
| (2026.10.0) | 76 | 76 | 69 | 68 | 61 | 48 | 39 | 37 | 88 | 93 | 75 | 64 |
| largest β excluded | 1.92 | 1.94 | 1.80 | 1.70 | 1.50 | 1.38 | 1.23 | 1.18 | 1.74 | 1.84 | 1.94 | 1.50 |
| max ΔN_eff today | 0.009 | 0.013 | 0.020 | 0.039 | 0.071 | 0.10 | 0.13 | 0.20 | 0.34 | 0.63 | 0.014 | 0.082 |

The largest β excluded and the largest ΔN_eff are 2026.10.0's at every A and α. **Every finding of 2026.10.0's
NOTES.md stands**, with these counts:
- the exclusions fall with A up to A = 3 (β ≤ 1.92 to β ≤ 1.18), then rise at A = 5 and 10 (1.74, 1.84), from the dark
  radiation present during BBN;
- 186 one-loop histories park above 10 keV and are excluded (8 at A = 3, 85 at A = 5, 93 at A = 10; 2026.10.0: 184,
  with 6 at A = 3); the fixed law has none;
- ΔN_eff today passes 0.30 only at A = 5 (β = 1.115–1.155, 17 histories, up to 0.335) and A = 10 (β = 1.095–1.50, 73, up
  to 0.63), and never excludes a history the BBN tests did not already exclude;
- the band alone (A ≤ 3) has 97 β excluded at some A, up to 1.94 (96 in 2026.10.0); 19 β are excluded at every A, all
  below 1.2 (19); 105 at some A, up to 1.94 (104); above β ≈ 1.94 nothing is excluded;
- A = 1 (61, up to 1.50) is close to α = 0.1 (64, up to 1.50), and A = 0.05–0.1 match α = 0.01 (77–78, up to 1.92–1.94);
- 257 one-loop histories have a negative shift (253), none below −0.46% in ΔY_p or −0.36% in Δ(D/H), all well inside
  the tests.

## What moved since 2026.10.0

Joined on (row, β) at laboratory M, over the 182 β both runs hold:

- **The five new β (2.950–2.975), 60 histories, are none of them excluded.**
- **13 histories change class**, all at β = 1.0625–1.23, in the transition window where the shifts are jagged in β:
  10 become excluded (α = 0.01 at β = 1.0625 and 1.08; A = 0.05, 0.1, 0.2 at 1.0625; A = 0.1 at 1.08; A = 0.5 at
  1.0825 and 1.085; A = 1.5 at 1.23; A = 3 at 1.12) and 3 cease to be (A = 0.5 at 1.0675 and 1.1375; A = 3 at 1.0675).
  Each sits near a test's edge in one run or the other, and the largest moves are in Δ(D/H) at β = 1.0625 (+1.5 to +1.6
  pp, from 1.5–2.2% to 3.2–3.7%), where the field reaches the wall at about 0.3 MeV.
- **Away from the edge** (β < 2.9), the median move is 0.005 pp in ΔY_p and 0.009 pp in Δ(D/H); the largest are 0.23 pp
  and 1.9 pp, both in the transition window.
- **At the electroweak edge** the grey region moves left by 0.02 in β, and β = 2.985–3.000 now deliver above 10 GeV;
  their shifts were already well inside the tests and stay so (over β = 2.950–3.040, |ΔY_p| ≤ 0.41%,
  |Δ(D/H)| ≤ 1.0%, ΔN_eff ≤ 0.076, against 0.40%, 0.99% and 0.075 in 2026.10.0).

The comparison across the whole store is PLAN.md P5's `changes/` analysis.
