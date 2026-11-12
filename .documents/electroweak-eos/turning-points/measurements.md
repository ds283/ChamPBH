# Turning points resolved above the solver's tolerance — measurements (electroweak-eos prompt 01a)

Taken on 2026-11-12, by prompt 01a, in its worktree on branch `electroweak-eos-01a`, cut from `0479587`. "Before" is
the code at `0479587`; "after" is 01a's change. "The old table" is the worktree's own CSV, `ec6bca8`'s (SHA-256
`28edd854…29a8`). "The new table" is prompt 01's, given as a copy (SHA-256 `690090c8…0119d`) and read through a
redirect of `Xav_EOS_spline`'s `pandas.read_csv`; it was never installed. Times are under a machine load of 6 to 50
(other projects' work), recorded with each output. The log is `prompts/electroweak-eos/logs/01a-resolved-turning-points.md`.

## The probes

| Script | What it does |
|---|---|
| `scan_turning_points.py` | one or all of the 36 histories of 01's two scans (the union of its 32- and 20-history grids), run to T_CMB at α = 0, atol = rtol = 1e-8; writes JSON with every float in hex: the turning points (and, for a history that fails, every TurningPoint constructed before the failure), `first_bounce`, the nodes, the interpolants' SHA-256, the final state, the counts, `surfing_turning_points`, and after the change each dropped pair's step, nodes' π, excursions and τ, and the closest pair |
| `scan_all.sh` | runs the 36 histories, one process each, P at a time |
| `compare_scans.py` | compares two runs, history by history, to the last bit (`--mode old` or `new`) |
| `run_tests_new_table.py` | runs the 22 tests prompt 01's table breaks (`tests_broken_by_the_table.txt`, note 01 §0) on a given table and records each integration |
| `summarize_tests.py` | the 22 tests before and after, from two runs of the above |
| `sigma_switched_off.py` | the early pair made on the old table by a patched Σ (the integration test's history) |
| `item11_pair.py` | test_injection item (11)'s interior pair: its samples, excursion and τ |
| `surfing_survey.py` | what `surfing_turning_points` records on the scans |

The raw JSON (about 30 MB a run) stays in the session scratchpad; `out/` holds text only.

## What each output shows

**`out/compare_old.txt` — the old table, before against after.** 36 histories, 241,380 turning points before and
241,376 after. 14 histories fail at small M on both trees: the step budget, and at 10.0/1e-12 and 5.5/1e-12 φ ≤ 0 (the step
cap violated), at 3.0/1e-12 the guard on a reflection requested inside the wall. They are compared through the TurningPoints constructed before the failure
and the failure message, which is the same text in two runs of the unchanged code. Everything else is bitwise equal in
every history: `first_bounce` (22 histories have one), the nodes, the interpolants, `N_final`, the final state, nfev,
the steps, the reflections, the warnings, `dropped_drains` and `surfing_turning_points`. That leaves the turning points of
two histories, each of which loses one pair:

| History | Step | π at the nodes | Smallest π | τ | Excursion/τ | Removed |
|---|---|---|---|---|---|---|
| β = 10, M = 10⁻³ M_P | [78.552502, 78.553501], T_J ≈ 23.7 eV | +8.19e-7, +7.94e-8 | −1.75e-9 | 1.0000008e-8 | 0.175 | plateau N = 78.553228, wall N = 78.553303 |
| β = 20, M = 10⁻³ M_P | [125.583817, 125.585772], T_J ≈ 0.461 keV | +2.25e-7, +6.93e-8 | −9.52e-9 | 1.0000002e-8 | 0.95 | plateau N = 125.584798, wall N = 125.585312 |

Both are near-stalls of an outgoing field: π > 0 and falling, a dip below zero smaller than τ, then rising. In each, a similar
plateau/wall pair one step earlier is kept (2.45 τ and 2.72 τ). Both lie far below 100 GeV, the end of 01's scans.
The report prints two verdicts:
- *the prompt's gate as written* ("no pair dropped"): **FAILS**, on these two pairs. 01a stopped on this;
- *the gate as the user ruled it on 2026-11-12, ruling (a)*: no pair dropped down to 100 GeV, everything else
  bitwise, the late pairs recorded. It **HOLDS**.

**`out/closest_pairs_old.txt` — how near the threshold the old table's pairs lie.** Only the nine M = 10⁻³ M_P histories
have a step that shows two or more sign changes. The closest pair kept is 1.10 τ, at β = 4, N = 52.066. A pair that
close can cross the threshold if the tolerance changes. The next closest are 2.45 τ and 2.72 τ (beside the two dropped
pairs), then 8.9 τ and 10.3 τ.

**`out/compare_new.txt` — the new table, before against after, at α = 0.** 231,904 turning points before and
231,854 after. The early wall/plateau pair (T_J 385–389 GeV, |φ − φ*| ≤ 2 × 10⁻⁹ M_P, π < 0 at both nodes,
excursions 0.008 τ to 0.23 τ) is dropped in every one of the 24 histories that had it. That covers 01's 14 of 20 at
M = 4 × 10⁻³¹ and 10⁻³, and all four M at β = 1.2, 1.5, 3, 5.5 and 10. There is one other drop: a late near-stall pair at
β = 20, M = 10⁻³ (N = 126.278, 0.59 τ). Everything else is bitwise equal, with three permitted exceptions: the turning
points (after = before less the dropped pairs, in order), the new count, and `dropped_drains`. `dropped_drains` goes from
1 to 0 wherever the early pair's wall had armed a drain that its plateau dropped. The user ruled on 2026-11-12 that it
may differ ("add to the allowed differences"), since U9 has a dropped pair neither arm nor drop a drain. `first_bounce`
is unchanged everywhere. Verdict: HOLDS.

**`out/tests_new_table.txt` — the 22 tests on the new table.** Before the change all 22 fail (21 fail, 1 error). After it,
4 pass: test_injection 1, 2 and 2, and test_drain_store's extremes. The other 18 still fail, each with its first
failing assertion listed. The 33 integrations the tests make show the early pair in 31 before the change and drop
exactly one pair in exactly those 31 after it. The two without the pair are test_injection's β = 10, M = 0.5 M_P (item 11's
history) and test_wall_rounding's β = 2, M = 10⁻¹⁶ M_P. The probe wraps test_release's own reference to
`integrate_scalar_history`, so its two histories are counted, which 01's run missed.

**`out/item11_pair_{before,after}.txt` — test_injection item (11).** The plateau/wall pair inside the step [63.852649,
63.854452] (π at the nodes +5.14e-8 and +8.29e-6) has a dip of 1.1761e-6 against τ = 1.000008e-8, which is 117.6 τ. It is
kept, and no pair is dropped in that history.

**`out/sigma_switched_off_{before,after}.txt` — the integration test's history.** With Σ patched to zero above 386.7 GeV
on the old table (β = 1.5, 2, 3 at M = 10⁻³ and β = 10 at M = 0.5 M_P, to 100 GeV), the unchanged code records a wall and a
plateau at T_J 387–388 GeV on the step where Σ switches on, with excursions of 0.06 τ to 0.77 τ. With the change there is
no turning point, the count is 1, and `dropped_drains` is 0. The test uses β = 1.5: excursion 6.17e-10, τ 1.0000000017e-8.

**`out/surfing_survey.txt` — `surfing_turning_points` (prompt §2).** It cannot misfire in the early pair's circumstances.
There π ≈ 10⁻⁹ M_P, so g = 1 + βπ/M_P ≈ 1, and no crossing lies within 385–389 GeV in any history. But it keeps every sign
change of g. In the captured histories (β = 10 and 20, M = 10⁻³ and 0.5 M_P, both tables) it records 22 to 138
crossings, most at one T_J to 10⁻⁹ or better over tens of e-folds: g held at zero on the surfing attractor, its sign set by
the solver's error. Opened as `[01a-surfing-turning-points-keeps-crossings-below-the-solver-tolerance]`.

**`out/suites_{before,after}.txt` and `out/breakage.txt`.** The suites and the breakage record, as the log's §3 and §4 state
them.
