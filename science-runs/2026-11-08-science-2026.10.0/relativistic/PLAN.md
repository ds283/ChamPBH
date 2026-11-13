# relativistic — do the quanta made at reflections stay relativistic? (plan)

**Of 2026-11-09. A plan only: nothing here is ruled, no script is written, nothing is committed.** Written by a
Claude Code session at the user's request ("Could you plan such a folder and its layout? We should do this so later
campaigns automatically pick up that they should do the corresponding analysis"). The proposals marked **P** and
the questions marked **Q** wait for the user's ruling; §5 lists them.

## 0. Why

The `2026.10.0` run was built with the instrumentation of `one-loop-drain` (README §8.3) so that this question could
be answered. The post-build script (`4ade10b`) ran `tools/quanta_measures.py` and the two figure tools on
2026-11-09, but nobody looked at the output, nothing was copied here, and the run's README names no figure for it.
What the output says, from a read-only survey of the same day (scratch only):

| M / M_P | max E_field (any history, any A) | max r_χ E_field (share of ρ_R) | worst tracker margin | histories with > 1% of K in quanta failing at a wall |
|---|---|---|---|---|
| 10⁻³⁰ | 1.3×10⁻²⁸ | 1.5×10⁻³² | 29.4 | 5 / 278 |
| 4×10⁻³¹ | 8.2×10⁻²⁹ | 8.9×10⁻³³ | 30.1 | 41 / 1838 (38 at A = 0.05, β 1.06–1.42) |
| 10⁻³³ | 6.8×10⁻³¹ | 6.3×10⁻³⁵ | 32.5 | 15 / 284 |
| 10⁻⁴² | 1.8×10⁻²⁸ | 1.4×10⁻³¹ | 28.5 | 38 / 288 (β ≤ 1.7) |
| 10⁻¹⁵ (A = 1) | 1.6×10⁻¹⁶ | 1.2×10⁻¹⁹ | −26.0 | 17 / 24 |

(Margins are ln(p/m); the integrator warns below ln 10. E is the matter-like excess of ρ_χ over its radiation value,
`ComputeTargets/quanta_spectrum.py`, G(y).) E is unchanged to three figures at c_a = 0 and 10.

So under the one-loop law the answer is yes, by many orders of magnitude, at every M the store holds. The per-wall
margins fail in a window of low β, because those quanta are born soft and carry almost no energy; the energy measure
E is the one that bounds the physics. **What the output does not cover** is as important and the folder must state it:

1. **The fixed law (α = 0.01, 0.1; the blue lines of Fig 6).** By design (`one-loop-drain` README §8.1, §8.3) the
   all-generation margins and the bounce rows are written "under the one-loop law only", so the fixed-law histories
   have no `all_worst_*` columns, no bounce rows and no E. Only the old weighted shares exist, and they are poor at low
   β: at 4×10⁻³¹ the share of K failing at a wall reaches 0.86 (α = 0.01) and 0.99 (α = 0.1), for β ≤ 1.0–1.43.
2. **The parking leftover**, which has no spectrum (`[00-the-parking-leftover-has-no-spectrum]`): ≤ 5.5% of K at
   laboratory M, about all of it at M_crit.
3. **The 18 histories that fail the K check** (C, 10⁻¹², A = 1; undiagnosed), for which the tool writes nothing, and
   the 304 failed histories.

## 1. The folder

```
science-runs/2026-11-08-science-2026.10.0/relativistic/
  PLAN.md                       this file; kept as the record of the plan once NOTES.md exists
  NOTES.md                      rulings in the user's words, what each figure shows and says, what it does not cover
  relativistic.py               the figures, from inputs/ only (no store, no Ray), as fig6.py
  inputs/
    quanta_histories.csv        unchanged copies of the export's, c_a = 1 (1.7 MB) ...
    quanta_histories-assoc0.csv   ... and the sensitivity scales (1.7 MB each)
    quanta_histories-assoc10.csv
    samples/<A>/<beta>/<M>/quanta_samples.csv, quanta_bounces.csv   the histories of rel-history only (P5)
  rel-lab.{pdf,png}             §2.1
  rel-M.{pdf,png}               §2.2
  rel-history.{pdf,png}         §2.3
  relativistic.provenance.json  as fig6's: script and input SHA-256, commit, tree state, store digest, the counts of §2.4
```

`histories.csv` is not copied again: the script reads `../fig6/inputs/histories.csv`, as `m-trend/` and `fig6b/` do
(the margins and shares are its columns). Script first, renders second, two commits, so the provenance names a clean
commit holding the script (the pattern of `fig6/`).

## 2. The layout

**P1 — styling follows Fig 6:** β on a log axis, the band over A = 0.05–3, A = 1 black, A = 3, 5, 10 green long
dashes, split at the wall's edge (β < 1.066 beige). Not the categorical heat map of `plot_quanta_grid.py`, which
cannot be read when the dense and sparse β grids share an axis (Q4).

### 2.1 `rel-lab` — the answer at laboratory M (the headline)

M = 4×10⁻³¹ M_P, the 186 dense β, one column, four rows sharing the β axis:

| Row | Quantity | Axis | Why |
|---|---|---|---|
| (a) | r_χ E_field,max, the largest share of ρ_R that is matter-like | log | **the test**: what the mass of the quanta can do to the expansion |
| (b) | E_field today and E_tracker,max | log | the same at the end of the history, and against the tracker's mass in place of the field's |
| (c) | the worst all-generation birth, reflection and tracker margins, ln(p/m) | linear, a zero line and an ln 10 line | where individual generations fail, and how badly |
| (d) | the share of K that fails at birth / at a wall, and the parking share | log | how much energy the failures of (c) carry, and how much is not covered at all |

**Q1 — the fixed law in this figure.** (a) and (b) cannot hold it (no E). Options: (i) draw only its weighted
failing shares in row (d), dash-dot blue as in Fig 6, labelled "weighted generations only"; (ii) leave it out and
say so in the caption; (iii) cover it properly, which needs the integrator to write the bounce rows and margins under
the fixed law and a rebuild (§4.2). Recommended: (i) now and (iii) as an issue for the next physics campaign.

**Q2 — a reference line in (a).** None is proposed: the result is a measurement. If one is wanted, the natural scale
is the precision of ΔN_eff (a matter-like share of ρ_R of order 10⁻³ at recombination), which (a) misses by about
thirty decades.

### 2.2 `rel-M` — the trend in M and the M_crit block

A = 1, the sparse 30 β, one line per M: B's six (10⁻³⁰ … 10⁻⁴²) and 4×10⁻³¹ in one sequential colour map, and
M_crit's two (10⁻¹⁵, 10⁻¹²) in a contrasting pair, since they are in the classical regime and differ in kind. Rows (a),
(c) and (d) of §2.1 (three rows). **P2:** A = 1 alone, as the m-trend figure; the other A are in the provenance counts.
The 18 K-check failures at 10⁻¹² are crossed and counted, not drawn.

### 2.3 `rel-history` — inside the histories

**P5:** three histories of the main store at 4×10⁻³¹, chosen as the worst cases of §0: A = 0.05 at the β of the
largest wall-failing share, A = 1 at β = 2.0 (a typical surfing history), and A = 10 at β = 2.0. One column each;
rows: r_χ E_field(N) and r_χ E_tracker(N) against T_J (log), and the birth ln(p/m) of each generation against T_J
at birth, the size of each point its ΔK/K_final. Read from the copied `quanta_samples.csv` and `quanta_bounces.csv`
(about 2–3 MB together). The 15 `quanta_history` figures of the reflection store are not copied: they are diagnostics.

### 2.4 Numbers in the provenance and NOTES

For each (M, A, and α where it applies): measured, failed, K-check failed, max r_χ E, max E today, worst margins, the
largest failing shares and the largest parking share; the c_a = 0 and 10 values beside the c_a = 1 ones as the
spectrum's sensitivity (no figure, **P3**). δ_stim and n̄ are **not** part of this analysis (P4): they belong to the
stimulated-emission bound, which needs its own analysis (one history at 10⁻³³, A = 0.5, β = 0.9, has δ_stim = 7×10⁴,
and 24 at M_crit have n̄ > 1). NOTES records that it saw them.

## 3. What the session would do, once ruled

1. Copy the inputs (§1), write `relativistic.py` (black-formatted when written, as fig6.py), draw the three figures,
   show them, iterate on the user's comments, write NOTES.md with the rulings in the user's words.
2. Commit the script, plan, notes and inputs; then the renders, on a clean tree.
3. Add the folder to the run README's "Figures made from it" and a row to §4's standard-analyses table.

## 4. So that later runs and campaigns do it without being asked

The 2026.10.0 analysis was missed for two reasons: nothing listed it as owed, and the run script could not make its
inputs (the measures need both exports in one root; the post-build script repaired that). Proposed:

### 4.1 A list of standard analyses (P6)

A new section of `science-runs/README.md`:

> **Standard analyses.** A run of record makes each of these from its stores, or says in its README why it does not.
> Each names the folder of its latest instance, whose script is the template: copy the folder to the new run, copy
> the new inputs, re-run, and write a new NOTES.md. A run's PLAN.md lists them, and its run script makes every input
> they read.
>
> | Analysis | Question | Inputs it needs from the run | Latest |
> |---|---|---|---|
> | Figure 6 | Which β are excluded by Y_p, D/H and ΔN_eff at laboratory M? | `histories.csv` | `2026-11-08-science-2026.10.0/fig6/` |
> | Relativistic quanta | Do the quanta produced at reflections stay relativistic, and how much is uncovered? | `histories.csv`, `quanta_histories*.csv`, each history's `quanta_samples.csv`/`quanta_bounces.csv` (so: both exports in one root and `tools/quanta_measures.py` at c_a = 1, 0, 10) | `2026-11-08-science-2026.10.0/relativistic/` |

**Q3:** whether Fig 6b (M_crit) and the M trend also belong on the list. Recommended: no, they answer questions
of this run's grid.

And each run's README gains a section **"Standard analyses"**, one line per row of that table: made (folder), or not
made and why.

### 4.2 A pointer where a session will see it (P7; CLAUDE.md, needs the user's agreement)

In CLAUDE.md, "Repository mechanics", the **Science runs** bullet gains one sentence:

> A run of record makes the standard analyses listed in `science-runs/README.md`, or its README says why not; its
> plan names them and its run script makes their inputs.

And in "Campaign conventions" (also needs agreement), one sentence:

> A campaign that changes what a standard analysis reads (the drain law, the spectrum model, the bounce rows, the
> margins, or the export layout) says in its README which standard analyses its rebuild must repeat, and updates
> that analysis's row in `science-runs/README.md` if its inputs change.

### 4.3 The gaps, as issues (P8)

The open-issues index holds campaign boards only; a science run has none. Proposed: record these two in the run
README's own "Open" list and, when the user picks the next campaign, open them on its board (and so in
`.documents/OPEN_ISSUES.md`):
- **the fixed law has no relativistic test**: bounce rows and all-generation margins are written under the one-loop
  law only (one-loop-drain §8.1); covering it needs an integrator change and a rebuild (Q1 (iii));
- **`plot_quanta_grid.py`'s β axis is categorical** and unreadable when dense and sparse grids mix (Q4: fix the tool,
  or retire the grid figure in favour of this folder's).

The K-check failures at 10⁻¹² are already listed as undiagnosed in the run README and need no new entry.

## 5. For the user to rule

| | Question | Proposed |
|---|---|---|
| P1 | Styling and axes as Fig 6 (β log axis, band A 0.05–3, A = 1 black, A = 3, 5, 10 green) | yes |
| Q1 | The fixed law: (i) weighted shares only in row (d), (ii) left out, (iii) rebuild | (i) now, (iii) as an issue |
| Q2 | A reference line in rel-lab (a) | none |
| P2 | rel-M at A = 1 only, M_crit as a contrasting pair | yes |
| P3 | The c_a sensitivity as numbers, no figure | yes |
| P4 | δ_stim and n̄ left to a separate analysis | yes |
| P5 | The three histories of rel-history | as §2.3 |
| P6 | The standard-analyses list in `science-runs/README.md` and a section in each run README | yes |
| Q3 | Fig 6b and the M trend on that list | no |
| P7 | The two CLAUDE.md sentences of §4.2 | the user's call |
| P8 | The two gaps recorded in the run README, opened on the next campaign's board | yes |
| Q4 | `plot_quanta_grid.py`: fix its axis, or retire it | retire for figures; keep as a diagnostic |

## 6. Ruled by the user, 2026-11-09

- **The fixed law (Q1):** "I'm not sure we want to go to the trouble of doing these estimates carefully for the fixed
  law. We know that's wrong, and it's there for comparison purposes. Let's produce whatever figures we can, and
  leave it at that." So (i): the fixed law is drawn where the store has something for it (its weighted failing
  shares), and (iii) is withdrawn. Its gap is stated in NOTES.md and is **not** opened as an issue (P8 is narrowed to
  the grid tool's axis).
- **A reference line (Q2):** "I don't see anything obvious to include here." None.
- **CLAUDE.md (P7):** "The proposed sentences for CLAUDE.md sound fine."
- The rest (P1–P6, P8 as narrowed, Q3, Q4): "So far this sounds fine", taken as ruled as proposed.
