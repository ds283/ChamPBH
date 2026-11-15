# Orchestrator — prompt 04, measure a sample

**Written** 2026-11-15 with the prompt, at the user's request: "No need to argue about D12. Leave it as it is. Please
write prompt 04 and its orchestrator note." Not dispatched. P24–P32 were ruled by the user the same day, as proposed
(README §6.7); the user dispatches it from a fresh session, whose brief is [`session-04.md`](session-04.md).

Read [`../README.md`](../README.md) first: §2, §4, §5, and §6 as ruled (R1–R10, U1–U9) and as proposed (§6.7, P24–P32).
Then read the prompt. This note binds 04 only. The conventions of earlier notes bind here as well: a tolerance is a
number with its reason, timing is taken under load, and the suites run serially.

**You do not write code.** You may:
- run the suites and the scripts the log names;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`04-measure-a-sample.md`](../04-measure-a-sample.md) · **Gate:** "Sample" · **Closes:** nothing ·
**Opens:** only what the work finds · **Model:** Opus · **Network:** none. Nothing is pushed.

**When:** written 2026-11-15 at the user's request. The user dispatches it from a fresh session ("I'll do that in a
fresh session"), which re-checks the Gate (§1).

**Gate**, as measured by the planner on 2026-11-15 at 18:49 local, before this note and the prompt were committed:
- **The worktree.** `.claude/worktrees/power-law-wall` is on `power-law-wall` at `99cc79b` (review of 03), on top of
  `d924275` (03). `git status --porcelain` shows `?? venv`, plus this note, the prompt and README §6.7 before their
  commit.
- **The main checkout** is on `main` at `9df9e13`, clean. Nothing of this campaign has touched it.
- **The registry.** `runregistrykit list`: 62 runs, all finished, none `running` or stale, none of this campaign.
- **The rehearsal lock** is free.
- **The code.** `DRAIN_MODELS` has three models; `InversePowerModel` defines `_drain_fraction` and
  `one_loop_drain_model`; the verification document has §1–§3; `.documents/power-law-wall/sample-04/` does not exist.
- **Load and disk.** Load averages 4.42 / 7.34 / 5.75; 122 GB free.
- **The suites.** Not re-run by the planner. At `d924275` 03's agent measured 26, 732 (219 + 513), 163, 302, 76, with only
  the two worktree failures of note 01's ruling 7. Only text has changed since.

## 0. What the planner found before writing 04

A serial probe at `99cc79b`, run from the worktree root on 2026-11-15, 18:49–18:59 (load 4.1 at its start, 19.1 at its
peak from another session). Its script and outputs are in this session's scratchpad, not committed:
`/private/tmp/claude-35086/-Users-ds283--claude-spares-spare-wmEFHY/9625fd26-7ba4-4730-ba1d-b5c91c282d18/scratchpad/plan04/`
(`probe.sh`, `probe.log`, `surf-*.csv`, `ol-*.txt`). That directory may not survive the session; the numbers below are
the record. The probe runs the committed tools unchanged.

- **Surfing at 10⁻³ M_P** (`tools/surfing_scan.py --M 1e-3 --beta-values 1.11 3.0 3.01`):

  | wall | β = 1.11 | β = 3.0 | β = 3.01 | wall time (3 β) |
  |---|---|---|---|---|
  | exponential | no capture, T_deliver 0.16301 GeV, 0 reflections | no capture, 48.4902 GeV, 0 | EW, 55.4693 GeV, 0 | 104 s |
  | power, n = 2 | no capture, 0.16301 GeV, 2 reflections | no capture, 48.4902 GeV, 167 | EW, 55.4693 GeV, 196 | 268 s |
  | power, n = 1 | no capture, 0.16301 GeV, 12 reflections | fails at G2, N = 39.68 | fails at G2, N = 41.42 | 120 s |

  - n = 2 surfs as the exponential does at these three β: the same capture, and T_deliver equal to about 10⁻⁶. It
    reflects off the wall 167–196 times after the delivery, and costs about 3 times as much per β (118–122 s at β ≈ 3,
    27 s at 1.11).
  - n = 1 fails at the guard G2 at β = 3.0 and 3.01, as R5 recorded in natural mode at this M ("W/(π²/2) = 1.12 | 1.08").
    At 1.11 it tunes. So at n = 1 the EW edge may not be bracketable at 10⁻³ M_P; the prompt treats that as a measurement.
  - From these, a scan costs about 20–40 s per β on the exponential and at n = 1, and up to about 2 min per β at n = 2
    near β = 3. The prompt's six scans and Q-exp (≤ 49 + 51 + 101 β per n, 51 for the exponential) take about 2–4 h of
    wall time if run three at a time.
- **One-loop histories by the tool route**, beside the exponential's 2026.11.0 export rows:

  | history | n = 1 | n = 2 | exponential (export) |
  |---|---|---|---|
  | 10⁻¹⁵ M_P, β 2, A 1: injections; ΔN_eff at 1 MeV; ℓ_min; ln R first impact | 38; 0.0392; 12.0; 388.75 | 23; 0.0595; 5.01; 178.87 | 27; 5.79e-8; —; 0.546 (all classical) |
  | 10⁻³³ M_P, β 2, A 1 | 31; 0.0273; 23.4; 554.54 | not probed | 34; 0.0363; 25.7; 166.33 |
  | 4×10⁻³¹, β 1.2, A 0.05 | not probed | 152; 0.00196; 3.92; 308.99 | 140; 0.00253; 4.24; 138.17 |
  | 4×10⁻³¹, β 2.9, A 10 | 13; 0.0575; 37.5; 545.64 | not probed | 7; 0.0605; 30.2; 145.60 |

  - Every power-law candidate is one-loop and unresolved; each tuned in one integration from the tracking seed, in
    12–23 s with BBN.
  - **At 10⁻¹⁵ M_P the walls differ most**: the exponential is classical there (its M_crit) and drains almost nothing,
    while the power wall drains fully one-loop, more than at laboratory M.
- **The forty's cost.** From 01's runs and this probe, every laboratory-M and 10⁻¹⁵ / 10⁻³³ M_P history takes 12–25 s;
  natural mode at 10⁻³ M_P takes 3–11 s at n = 1 (G2 failures) and 49–95 s at n = 2. The forty take about 15 min run
  serially.
- **R9's share, from 02's maps** (`.documents/power-law-wall/wall-02/map_summary.txt`, fixed law): over every mapped
  power-law candidate ln(E/V₀) ≥ 33.30 (laboratory M, β = 1.2, α = 0.1), and its range along a path does not depend on M
  (54.999–106.043 at β = 2, α = 0.01, at 10⁻³³, 4×10⁻³¹ and 10⁻¹⁵ M_P alike). The parking cut ends the cascade there,
  so the planner expects no K from E/V₀ < 100 and a table error 1/s_t − 1 < 10⁻¹⁴. Natural mode drains nothing.
- **M_crit, estimated from 02's path formula** (ln R scales as −4 ln M at fixed x, and x does not depend on M):

  | history (02's map) | M at which the smallest ln R reaches 0 | M at which the first impact's reaches 0 |
  |---|---|---|
  | power n = 1, β 2, α 0.01 | 0.32 M_P | 1.6×10²⁷ M_P |
  | power n = 2, β 2, α 0.01 | 6.2×10⁻¹³ M_P | 2.6×10⁴ M_P |
  | power n = 1, β 1.2, α 0.1 | 5.2×10⁻¹³ M_P | 1.2×10²⁵ M_P |
  | power n = 2, β 1.2, α 0.1 | 5.3×10⁻²⁰ M_P | 1.4×10³ M_P |
  | exponential, β 2, α 0.1 | 1.2×10⁻²¹ M_P | 1.15×10⁻¹⁵ M_P |
  | exponential, β 2.9, α 0.1 | 3.0×10⁻¹⁹ M_P | 2.6×10⁻¹⁵ M_P |

  The exponential's first-impact column reproduces its known M_crit ≈ 10⁻¹⁵ M_P. On the power wall the first impact is
  one-loop at every M up to M_P, at both n. The estimate is the planner's; 04 makes it from the sample (prompt §1.4 item
  5).
- **The counterparts.** All twenty exponential rows are in the main export, `var/science-2026.11.0/out/main/
  QCD_Cosmology/histories.csv` on `main` (3 913 790 bytes, SHA-256
  `fb6a8dbb41fa7bbd7a0dd6b6076e25c3f9623dec9328b3e07b41b5e4fdb04253`), each with a normalization and no failure. Nineteen
  are also in the reflection export (the 10⁻³³ row is not). The laboratory row at β = 2, A = 1 (45 injections, ΔN_eff at
  1 MeV 0.041888) equals 03's exponential tool route.
- **The edges at laboratory M** are not scanned (P29). 01 found the power wall's laboratory-M histories under the fixed
  law equal to the exponential's in reflections, injections, first bounce and ΔN_eff, and n = 2's surfing at 10⁻³ M_P
  equals the exponential's above, so the planner expects the exponential's laboratory-M edges to hold there.

**Rulings.** Each is recorded in the log, with its class.
1. **The worktree and the main checkout.** As note 01's ruling 1: the agent works only in the worktree, from its root,
   with `./venv/bin/python` and `PYTHONPATH=.`. It never stages `venv`, never reads or touches the main checkout (the
   export and the run record come as copies), and never names `/tmp/ray-cpbh`. *STRUCTURALLY REQUIRED.*
2. **The sample is U7's.** No history is added or dropped. A further run made only to explain a measurement (one
   history, at fixed normalization or with the runner's instrumentation) is allowed in scratch, stated in the log, and
   not committed as part of the forty. *STRUCTURALLY REQUIRED* (U7 as ruled).
3. **The overlaps and the tool route.** "Identical" means every line equal once the timings (`wall=`, `PRyM_time=`,
   `uptime` lines) are masked. A difference in an overlap (prompt §1.1) or in the tool route against the export
   (§1.2, at the tool's printed digits) is a stop. *IMPLEMENTATION CHOICE, ruled here.*
4. **Failures are measurements.** R5's failures recur and are noted. Any other failure is not a stop: it is recorded with
   its reason, N and field, and named first in the hand-back. *IMPLEMENTATION CHOICE, ruled here* (README §5,
   "Measurements, not bounds").
5. **The surfing scans.** The grids are read from each coarse scan as the prompt says (§1.5). The stops are the prompt's.
   At most three scans run at once, and no scan runs while the forty run. *IMPLEMENTATION CHOICE, ruled here.*
6. **The machine.** The forty run one at a time; the load is recorded beside every timing. *IMPLEMENTATION CHOICE, ruled
   here.*
7. **The two worktree failures** of note 01's §0.1 are expected in the base suites. Any other failure there is a stop.
   *IMPLEMENTATION CHOICE, ruled here.*
8. **A finding about production** is recorded and reported, not fixed. The agent may open a §3 issue for it, with
   `.documents/OPEN_ISSUES.md` in the same commit. *IMPLEMENTATION CHOICE, ruled here.*
9. **No option chosen.** Where a threshold's use on this wall is in question, the hand-back states the evidence and the
   options (prompt §1.6), and recommends none as the agent's ruling. *STRUCTURALLY REQUIRED* (README §2 row 04).
10. **No file but the prompt's §2 paths.** *STRUCTURALLY REQUIRED.*

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before committing.
  Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit", and the orchestrator substitutes it.
- **Order.**
  1. The Gate, and the five suites on the base: serially, ComputeTargets in its two halves, the `test_[g-z]` half and
     `tools/tests` in the background, waiting for each notification.
  2. The runner and its check against the plain tool (prompt §1.1); the tool route against the export (§1.2).
  3. The forty, serially, in the background, waited for without polling; then the overlaps.
  4. The surfing scans (§1.5): the coarse scans and Q-exp, then the fine scans read from them; the three `f_new.csv` rows.
  5. The tables, the thresholds, the documents, discovery counts, and the commit.
- **Scratch.** Probes and suite outputs go in the agent's own subdirectory of the session scratchpad. Nothing goes in
  `/tmp`.
- **No Ray.** Nothing starts a cluster, and no `ray` command is run. No store is written, and nothing is registered.
- **Stop conditions are real:**
  - a test failing on the base other than the two known;
  - an overlap, or the tool route against the export, that is not identical (ruling 3);
  - a surfing grid the prompt's §1.5 stops on;
  - a production change becoming necessary.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

1. **The Gate above**, on the commit that adds this note, or a later one.
2. **`runregistrykit list`**, the rehearsal lock (`./venv/bin/python -m raypoolkit.rehearsal_lock status`), and the load
   average. A cluster or lock of another session is left alone; 04 needs neither.
3. **The copies.** Copy into a dispatch directory of the session scratchpad, read-only, from the main checkout:
   - `var/science-2026.11.0/out/main/QCD_Cosmology/histories.csv`, and check its SHA-256 against §0's;
   - `science-runs/2026-11-13-science-2026.11.0/README.md`.
   Copy nothing else from the main checkout, and write nothing there.

## 2. Dispatch

Start one fresh-context subagent, on **Opus**, in the worktree, in the background. Give it:
- the prompt, the README and the board;
- `HEAD`, and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad;
- the two copies of §1 item 3, and the planner's probe outputs of §0, copied beside it, as data (they are not to be
  committed).

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the files of ruling 10. Its log is `logs/04-measure-a-sample.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **Stop and ask** on any of the conventions' stop conditions.
- **At the end it replies with:**
  - the log's path and the message as committed; `git show --stat` of its commit;
  - the suites at the base, with counts, times and `uptime`, and the discovery counts after;
  - the runner's check, the overlaps and the tool route against the export;
  - the forty: outcome, integrations, wall time, injections, ΔN_eff at 1 MeV, Y_p and D/H, each beside the
    exponential's; every failure with its reason;
  - R9's share, and the smallest ln(E/V₀) reached;
  - each threshold of prompt §1.4, with the numbers;
  - the surfing edges per n beside the exponential's, with the failed rows, and each scan's wall time and load;
  - the M_crit estimate per n;
  - the options it puts to the user, if any;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — six checks

1. **Scope.**
   - `git show --stat <agent>` lists only ruling 10's paths, and `venv` is not in it.
   - `git diff <base> <agent>` outside `prompts/power-law-wall/`, `.documents/power-law-wall/`,
     `.documents/power-law-wall-verification.md` and `.documents/OPEN_ISSUES.md` is empty.
   - The commit's parent is this note's commit, or a later one that is not the agent's.
   - The main checkout is untouched. Nothing is pushed.
2. **The sample is U7's.** Count the run files: twenty per n, named as the prompt names them, each with a command, an
   exit status and `uptime`. `summary.csv` has forty rows, built by the committed script from the run files; rebuild it
   myself and `cmp`.
3. **Re-run two histories myself**, serially, timed with the load: n = 2 at 10⁻¹⁵ M_P, β = 2, A = 1 (against the run file
   and §0's probe), and one laboratory-M history the agent's checks did not cover, through the runner. Each equals the
   committed file with timings masked.
4. **The counterparts.** The extract's twenty rows equal the export copy's (`cmp` of the rows, SHA-256 of the source);
   re-run the exponential's 10⁻¹⁵ M_P tool route and match the export row at the printed digits.
5. **The thresholds and R9.** For two histories, recompute from the runner's per-candidate table: the largest φ_t, the
   smallest ln R, ρ_kin/ρ_R and m_eff/H at the cut, the smallest ln(E/V₀) and the share of K below E/V₀ = 100. Check
   the M_crit estimate's arithmetic for one row.
6. **The surfing and the records.**
   - Re-run one row of each n's fine scan near an edge (or the nearest failure, at n = 1) and match it, `wall_s` aside.
   - The edges stated match the CSVs (`capture_kick` read by a scratch script).
   - The log is in §5.1's form, every deviation classed; verification §4 exists; `sample-04/` and `source/sample/` exist,
     with a README.
   - The board has row 04, its items and the gate Sample. Any issue opened is on §3 and in `.documents/OPEN_ISSUES.md`.
   - `CLAUDE.md` is unchanged, and discovery counts match the base's.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 04 landed, the gate Sample held, and this note marked "used for 04";
- an *Orchestrator review of prompt 04* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

**Report to the user:**
- the agent's commit and the review commit;
- the gate Sample;
- the forty against the exponential, above all at 10⁻¹⁵ M_P, and every failure;
- each threshold on this wall, and the options the agent puts for any in question (README §2 row 04: the user rules
  them before 05);
- the surfing edges per n;
- R9's share and the M_crit estimate;
- that 05 is written only when the user asks for it.
