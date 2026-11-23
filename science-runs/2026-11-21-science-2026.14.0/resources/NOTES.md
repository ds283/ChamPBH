# resources — what the 2026.14.0 run cost in time, CPU and storage

**Of 2026-11-23**, made by a Claude Code session at the user's request ("can you generate some figures for the resource
requirements of the `2026.14.0` run?": wall-clock time, CPU time, the histories per potential, the storage of the
datastores and of the derived products, and any other useful metric). **Not yet seen by the user.** Measurements, not
bounds. Not one of the standard analyses; nothing here sets a threshold.

```bash
./venv/bin/python science-runs/2026-11-21-science-2026.14.0/resources/resources.py
```

It needs no Ray and no store. It writes `resources-time`, `resources-histories` and `resources-storage` (`.pdf`, `.png`),
`resources.txt` (every number below, in full) and `resources.provenance.json` (input hashes, git SHA).

## Where the inputs came from

The stores and the exports are on the Mac Studio. `extract_remote.sh` read them on 2026-11-23 over SSH, read-only:
every database was opened with `?mode=ro&immutable=1`, so sqlite took no lock and wrote nothing, and nothing was
written there. Run as its header says, it gives:
- `inputs/histories.csv`: one row per stored `ScalarHistory` of the three stores (13 806), with its model, its α row,
  the integrator's statistics, its sample rows, and the timings its `BBNData` and `AdiabaticHistory` rows record;
- `inputs/tables.csv`: bytes per table and index, per shard and primary, from sqlite's `dbstat`;
- `inputs/files.csv`: bytes of every file of the three stores;
- `inputs/outputs.csv`: the 91 279 files under `var/science-2026.14.0/out/`, grouped by root and kind.

`inputs/steps.txt` holds the run script's `>> <time> <step>` lines, from `run.out` on the Mac Studio. The stage times
come from the committed `../logs/`.

## What the timings are

- **Wall-clock** is the run script's (slots, exports) or the pipeline's log line (stages within a slot).
- **Reserved CPU** is 8 × a stage's wall-clock: the Ray cluster had 8 CPUs, and a stage holds them until it finishes.
- **Recorded CPU** is what the store holds, and it is a lower bound:
  - `ScalarHistory.compute_time` is the integrator's time for a history's final integration only, so the figures use
    `compute_time × tuning_iterations` to estimate all of the tuning's integrations;
  - `BBNData` records PRyMordial's time and the new-physics setup's (the second is 0.02% of the first);
  - `AdiabaticHistory` records its own `compute_time`;
  - a failed history records no time, and nothing outside the integrator is timed.
- **A history's span is not its CPU time.** The label's timestamp is when the driver *built* the task, and up to 24
  tasks are in flight on 8 CPUs (`main.py`, `max_task_queue=24`). From label to stored row the spans add up to 202 h,
  more than the 144 CPU-h the stage reserved; the median span is 3.6 times the history's recorded integration time.
  The spans are used only for their **end** times, in panel (d).

## What it says

**Time.** Launch to finish took 25 h 45 min: 22 h 48 min of build, 3 min of inventories and 2 h 54 min of exports,
measures and figures (about 1 h per wall).
- By stage (`resources-time` (b)), the build was 18.0 h of scalar histories, 4.0 h of BBN and 0.7 h of adiabatic
  parameters.
- The power-law slots, which build n = 1 and 2 together, took 1.5× the matching exponential slots' time for 2× the
  histories.
- The build ran at 606 histories an hour, or 76 per reserved CPU-hour.

**CPU** (`resources-time` (c)).
- **Reserved:** 181 CPU-h for the build (144 scalar histories, 32 BBN, 5.5 adiabatic).
- **Recorded:** at least 73 CPU-h (41.9 integration, 30.7 PRyMordial, 0.3 adiabatic).
  - By wall: exp 20.5 + 10.9, pl2 11.3 + 10.1 and pl1 10.1 + 9.8 (integration + BBN).
  - exp's integration time includes blocks C and D. Block C alone recorded 3.2 h of final integrations for 180
    histories.
- **The BBN stage used 97% of its reservation**, which is a check that the recorded times mean what they say.
- **The scalar-history stage recorded only 29% of its reservation.** Most of the gap is idle CPUs (panel (d)); the
  rest is untimed work: failed histories, and anything outside the integrator.

**Stragglers** (`resources-time` (d)).
- Each pipeline row (one α or A of one slot) must finish before the next starts. In blocks A–D, **49% of the
  scalar-history stage's wall-clock was spent after 95% of a row's histories were stored**: 8.4 h of 17.3 h.
- During that tail most of the 8 CPUs are idle. In 34 of 52 rows the tail was longer than the rest of the row.
- It is worst in A-exp (63%) and B-exp (60%). In a one-loop row of A-exp, 95% of the 191 histories are stored within
  3.3–6.1 min of the row's first task, and the last within 9.7–16.9 min.
- **The history that ends a row is at β ≤ 1.0 in 36 of the 52 rows, and fails in 35 of them**: 27 at the 2 × 10⁶-step
  budget and the rest at the effective-minimum search. These β lie below the wall edge (β ≈ 1.069), outside Figure 6's
  range. Block C's one-loop row is the exception: there the tail is the M_crit histories themselves (95% at 129 min,
  last at 158 min).
- This is the largest single inefficiency in the run. It is recorded, not acted on.

**Histories** (`resources-histories` (a)).

| wall | histories | the history failed | its BBN failed |
|---|---|---|---|
| `exp` | 4862 (A 2292, B 2160, C 180, D 186, R 44) | 194 | 115 |
| `pl2` | 4472 (A 2292, B 2160, R 20) | 128 | 157 |
| `pl1` | 4472 (A 2292, B 2160, R 20) | 297 | 146 |
| all | 13 806 | 619 | 418 |

The 1037 together are the README's "Failed histories".

**Cost per history** (`resources-histories` (b), (c); blocks A and B, successes).
- **The three walls cost the same.** The median recorded integration time is 5.5 s (`exp`) and 5.9 s (`pl2`, `pl1`),
  with a 99th percentile of 50–56 s. A history takes about 58 000–60 000 right-hand-side evaluations at about 25 µs
  each. It stores about 5300 samples and 63–67 bounce rows.
- **PRyMordial costs about as much as the integration**: a median of 8.5 s per history on every wall.
- **The cost is not concentrated.** The top 1% of histories take 8.6% of the integration time.
- **By β** (block A, one-loop law), the median cost over A is lowest just below the wall edge (about 1.2–1.5 s at
  β = 1.0). It rises through the transition window to 10–15 s at β ≳ 3, where the field surfs the electroweak kick.
  No β = 0.5 history succeeds there on `exp` or `pl1`, so their lines start at 0.75.
- These are successes' costs. The failures below the wall edge are not timed, but they set the tails above.

**Storage** (`resources-storage`).
- **Datastores: 24.5 GiB** in 51 files (exponential 8.8, inverse-power 15.5, reflections 0.23).
  - The field samples (`ScalarHistoryValue` and its indexes) are about two thirds of each store. The adiabatic samples
    are about a quarter, and the BBN samples about 6%.
  - The bounce rows, the one-loop law's instrumentation, are 0.9–1.5%.
  - The redshift grid is 75 MB in each store, a third of the reflection store.
- **Derived products: 35.7 GiB in 91 279 files**: `exp` 12.6, `pl2` 11.9, `pl1` 11.2, reflection roots included.
  - `tools/quanta_measures.py`'s `quanta_samples.csv` are **62%** of it (22.4 GiB). They are written three times, at
    c_a = 0, 1 and 10, and none is compressed.
  - The gzipped `fields.csv` are 33% (11.9 GiB).
  - The bounce files are 2.6% and the figures 1.2%.
- **Per history:** about 1.9 MB of datastore and 2.8 MB of derived products, on every wall.

## Observations, not acted on

- **The row barrier.** Half the scalar-history stage waited on the last 5% of each row, and the last history of a row is
  nearly always a β ≤ 1.0 history that fails below the wall edge. Letting rows overlap (one queue across α and A), or
  submitting the slowest β first, would recover most of the 8.4 h of tail. That is a change to `main.py`'s pipeline or
  to RayPoolKit's queue, and none is proposed here.
- **`quanta_samples.csv` is uncompressed and written three times.** Compressing it as P11 compresses `fields.csv`, or
  keeping one scale's in full, would roughly halve the derived products.
- **The recorded CPU is a lower bound.** The tuning's earlier integrations are estimated as copies of the last, and a
  failed history's time is not recorded at all. Reading the true CPU time would need a timer per task in the store or
  the logs; none exists.
