# Log 09 — rehearse a tuned build

**Subject:** Rehearse a registered, interrupted and resumed tuned build
**Commit:** this commit, on branch `jordan-normalization`, on top of `4dc31ea`
**Date:** 2026-10-26 (the rehearsal, 19:43–20:37 GMT), by Claude Opus 5.5
**Result:** **COMPLETE.** H1–H5 shipped; the gate "R" holds. No stop condition was met.
- Seven builds through the wrapper on ChamPBH's isolated cluster: `reference-a`, `-b`, `-c`
  `done`/0 into the reference store; `resumed-a` `killed`/143 by `SIGTERM` 90 s into its scalar
  stage, with β = 0.5, 1.5 and 3 stored and validated; `resumed-a2` (`--prune-unvalidated`),
  `resumed-b`, `resumed-c` `done`/0 into the resumed store.
- **Seven histories, six models; every history converged** (3–9 integrations, |residual| < 10⁻⁵).
- **Every stored tuned value equals `tools/history_and_bbn.py`'s bitwise**: 7 histories × 15 keys
  × 2 stores, by `float.hex`; the inputs are the tool's.
- **`compare_stores`: `RESULT: equal`**; both stores fingerprint to `3b92d064…15a7`.
- Six histories' normalization and φ_ref equal 06's probes' RESULT hex, the seventh note 09's
  probe line. Against README §0.3, Λ lower by 2.768–2.795 × 10⁻⁴ and φ_ref within 1.6 × 10⁻⁵.
- **The longest heartbeat silence: 58 s** (`reference-c`, scalar stage), against 900 s.
- `histories.csv` SHA-256 `02400fd7…7c77d`; the reference store unchanged by the plot.
- Suites before and after: 26; 76 + 167; 93; 115; 64, all passing.
- No code changed. Opened one issue,
  `[09-ray-deduplicates-the-tune-lines-of-parallel-histories]`; the index 24 → 25.

The orchestrator's note for 09 (its Gate, §0, 21 rulings) governed where it differs from the
prompt; each ruling is classified below as it says. The record is
[`.documents/jordan-normalization-verification.md`](../../../.documents/jordan-normalization-verification.md) §1;
the figures below are its.

## What shipped

- **H1 — `reference`.** Runs `a` (`jordan-normalization-09-reference-a-20261026T195544`, pid
  35782, 3 min 41 s), `b` (`…-reference-b-20261026T200014`, 36512, 1 min 3 s) and `c`
  (`…-reference-c-20261026T200132`, 36956, 3 min 7 s) into
  `var/datastores/jordan-normalization-09-reference.db`, each `done`/0, each log ending
  `** driver exit status 0`, `** launcher saw exit status 0`. After `a`, `store create`
  (`store_id` `b5721a5e4c544f02bfb78e1b9ea7e9d9`); `b` and `c` carry it as `results_store_id` and
  wrote their fingerprints to the sidecar. Then `plot_by_beta.py --phi-init-Mp 5` (unregistered,
  read-only), exit 0, into `var/jordan-normalization-09/plot-reference/`.
- **H2 — `resumed`.** Run `a` (`…-resumed-a-20261026T200457`, pid 37417) into
  `…-resumed.db`, `SIGTERM`ed by `interrupt_when.py --after 90`: `killed`/143. Three rows stored,
  all validated. `store create` (`5f9ffeeba8994e4ba41504eb24040cc0`). Then `a` again with
  `--prune-unvalidated` (`resumed-a2`, 37955: computed only β = 5.5, no prune report), `b`
  (`resumed-b`, 38353) and `c` (`resumed-c`, 38606), each `done`/0 with the sidecar written.
- **H3 — the comparisons.**
  - `compare_stores`: `RESULT: equal`, `problems: none`, `ExponentialModel` 6, `ScalarHistory` 7,
    `AdiabaticHistory` 7, `BBNData` 7, `ScalarHistoryValue` 39397, `redshift` 75001. `store
    fingerprint` on each closed store: `3b92d064512079e30bb2291d6d82732fed10c187e50c61e4b3d54cb47b3415a7`,
    each matching its sidecar.
  - The standalone tool, seven runs after the stop: inputs, convergence and the 210 key comparisons
    all as expected (`SUMMARY: inputs all as the tool; converged all; bitwise all 7 x 15 x 2 equal`).
  - Every history converged: `failure` 0, `validated` 1, `abs(residual) < 1e-05` for both.
  - Against §0.3: the table in the document §1.8.
- **H4 — measured.** The histories' integrations and tuning walls (document §1.3); the stage times
  (§1.2); the longest heartbeat silence per run, from a recorder, against 900 s (§1.10); the Ray
  processes after the kill (`ray_cluster.sh status`: the 19 baseline processes and nothing else;
  0 workers alive, 0 actors `ALIVE`); `git_dirty` **false** in every manifest; the RayPoolKit wait,
  seen as a computed history waiting up to 102.5 s to be stored (§1.10).
- **H5 — the document.** `.documents/jordan-normalization-verification.md` (new), §1; the outputs
  under `.documents/jordan-normalization/out/09-*` (67 files, 548 KB, the largest 25.9 KB): the seven
  launch logs, the plot log and the interrupter's log; each run's `manifest.json` and `status.json`;
  both sidecars; the recorders' outputs and `silences.py`'s; the readers' outputs; the seven tool
  outputs; `compare_stores`', `stage_counts.py`'s, the `store` commands', the comparison's and the
  manifests' summaries; `histories.csv`; the cluster's record; `ray_state.py`'s; the Gate, the
  load, the checks, the token greps and the suites' summary; the helpers as `.txt` and the two
  adaptation diffs.
- **The board**: row 09, H1–H5, the gate "R", the status line, §3's new entry and §5.10.
  `.documents/OPEN_ISSUES.md`: the new row, 24 → 25.

## Deviations from the prompt

1. **`STRUCTURALLY REQUIRED`: seven histories, six models** (ruling 1). The prompt's §1.1 says
   "Eight histories"; the runs give seven, and `b`'s model is `a`'s (β = 0.5, M = 10⁻³, n = 1).
   Found: `ExponentialModel` 6, `ScalarHistory` 7 in each store.
2. **`IMPLEMENTATION CHOICE`: the runs as ruled** (ruling 2). Seven builds, slugs `reference-a`,
   `-b`, `-c`, `resumed-a`, `-a2`, `-b`, `-c`, each with `--job-name jordan-normalization-09-<slug>`
   (the prompt's `<store>-<run>`, with `a2` for the second `a`), `S` and `L` absolute. `b` and `c`
   have the same arguments in both builds but the job name. The order was the table's.
3. **`IMPLEMENTATION CHOICE`: `store create` after each store's first close** (ruling 3). After
   `reference-a` and after `resumed-a`. `results_store_id` `null` for those two and the store's id
   for the other five; `fingerprint_sidecar` `written` for those five, as expected. No `store
   fingerprint --write` was run.
4. **`IMPLEMENTATION CHOICE`: the launch line** (ruling 4), one Bash call per launch exporting
   `ADDRESS`, `S` and `L`, recorded verbatim in the document §1.14; the recorder and, for
   `resumed-a`, `interrupt_when.py` each launched with `nohup … &` in a Bash call of its own
   straight after. The load at each launch was taken by `uptime` in the Bash call just before the
   launch call (not inside it), so the launch lines are exactly the ruling's.
5. **`IMPLEMENTATION CHOICE`: every wait was `wait_for_line.py`**, not the prompt's `Monitor`
   (ruling 5), one foreground call each, the Bash tool at 600 000 ms: `--timeout 120` for the
   registration lines (each returned after 2.0 s) and `--timeout 570` for the launcher's line.
   **No wait timed out**; the longest was 214.9 s (`reference-a`). No `sleep` loop, `pgrep` or
   `tail -f`.
6. **`IMPLEMENTATION CHOICE`: the helpers** (ruling 6). `wait_for_line.py` and `ray_state.py`
   copied unchanged (`cmp` 0 each); `check.sh` and `suite.sh` adapted to `var/jordan-normalization-09`
   and `09-` names (the diffs committed as `09-check.sh.diff` and `09-suite.sh.diff`: comments, `L`
   and the output file names only). Written: `beat_times.py`, `silences.py`, `histories.py` (stdlib
   only) and a fourth, `compare_tool.py`, for rulings 13–15, which imports `Units` for the unit
   conversions (the ruling leaves the fourth's imports open). `ray_state.py` always ran as
   `RAY_AUTH_MODE=token ./venv/bin/python …`.
7. **`IMPLEMENTATION CHOICE`: the recorder** (ruling 7) measured what H4 asks of `list`. Each ran
   detached beside its build, wrote one `L` line per log line and one `H` line per change of
   (heartbeat, state, stage), and exited after the launcher's line (all seven: `exit: the launcher's
   line was seen; status.json read once more`), before the stop. `silences.py` gives (a)–(d).
   **(d)'s tune-line counts are short by one for β = 1.5 in `reference-a` and `resumed-a`**: Ray's
   log deduplication swallowed that history's seed line (Observations, item 1). The tuning walls in
   the document are the lines seen, and say so.
8. **`IMPLEMENTATION CHOICE`: the delay** (ruling 8). From `reference-a`'s recorder: t₁ = 5.5 s,
   t₂ = 24.7 s, t₃ = 36.8 s, t₄ = 138.1 s after the scalar banner; t₄ − t₃ = 101.3 s ≥ 60 s;
   (t₃ + t₄)/2 = 87.45 s, rounded to **`<s>` = 90 s**. The outcome was the expected one: β = 0.5,
   1.5 and 3 stored (all validated), β = 5.5 not; `resumed-a` `killed`/143.
9. **`NOTED`: the signal** (ruling 9). As expected: `sent SIGTERM` at 20:06:58 UTC, the handler's
   `** SIGTERM at 2026-10-26 20:06:59 +0000: recorded killed, exit code 143; …` (1 s), `** SIGTERM:
   main.py ended with SystemExit(1); recording killed, exit code 143`, `** driver exit status 143`,
   the launcher's 143. Signal → final heartbeat 26 s. No `*** SIGTERM received` banner, no alarm
   line. One line not in the expectation: β = 5.5's `it=3` tune line, after the handler's two
   (Observations, item 3). `resumed-a2`'s open printed **no prune report**: no row was unvalidated.
10. **`STRUCTURALLY REQUIRED`: the cluster** (ruling 10). `start`, `stop`, `status` and the lock's
    `status` each alone in their Bash call, output to a file under `$L`, exit by `echo $?`; no
    command line named `/tmp/ray-cpbh`; every Ray CLI command with `RAY_AUTH_MODE=token`; no `ray
    stop`; the token file not printed, copied or changed. `stop` ran after every driver's pid
    answered `kill -0` with `no such process`, every recorder had exited and `interrupt_when.py` had
    printed its `sent` line. `start` exit 0; `stop` exit 0 with `SIGKILL` to the `gcs_server` (35393)
    and 35394 after the grace, as at `ray-sqla-align` 02, then no process and the ports free and the
    lock released; `status` exit 0, all free. Also run, not ruled: `ps -E -ww -o command= -p PID |
    grep -o 'RAY_AUTH_MODE=[a-z]*'` on the `gcs_server` and the `raylet` (`ray-sqla-align` 02's
    check), which printed `RAY_AUTH_MODE=token` for each and nothing else.
11. **`IMPLEMENTATION CHOICE`: the Ray state** (ruling 11) after `start`, after `resumed-a`'s
    launcher line and before `stop`; `ray_cluster.sh status` after `resumed-a` too. After
    `resumed-a`: 0 workers alive, 0 actors `ALIVE`, the 19 baseline processes; nothing to wait for.
    The prompt's H4 asks for the Ray processes "after each kill"; there was one kill.
12. **`NOTED` and `IMPLEMENTATION CHOICE`: the load and the suites** (ruling 12). One-minute load
    3.59 before `start` (under 20), 2.44–5.90 at the launches, 4.99 at the plot, 6.21 before `stop`,
    6.61–10.73 at the tool runs. The suites before `start` and after `stop` only, as ruled; all
    passed at the baselines; nothing was re-run.
13. **`IMPLEMENTATION CHOICE`: the stored values** (ruling 13), read by `histories.py` through
    `file:…?mode=ro` URIs on the shards only. The inputs are the tool's, and every row converged by
    `tuning_converged`'s inequality, `abs(residual_H) < tolerance and abs(residual_phi) < tolerance`
    (`ComputeTargets/ScalarHistory.py:1423-1429`), strict. T* is stored in GeV as `20000.0`, which is
    (2 × 10⁴ × `GeV`)/`GeV` by `float.hex`, and the stored value times `GeV` is 2 × 10⁴ × `GeV` by
    `float.hex`: the ruling's "equal by `float.hex` to `20000 * Planck_units().GeV`" read through
    the column's unit.
14. **`NOTED`: the bitwise comparison** (ruling 14). Seven tool runs after the stop, serially; all
    210 comparisons equal; no NaN printed; 06's six RESULT hex and note 09's probe line equal. The
    tool's run for β = 0.5 at φ* = 5 exited 1, its BBN failing as §0 shows; the others exited 0.
15. **`NOTED`: §0.3** (ruling 15). Λ/Λ_§0.3 − 1 from −2.7954 × 10⁻⁴ to −2.7679 × 10⁻⁴; φ_ref within
    1.6 × 10⁻⁵, as 06's table.
16. **`NOTED`: the BBN stage** (ruling 16). One failure, β = 0.5 at φ* = 5, with §0's reason,
    identical in both stores; no other.
17. **`NOTED`: the stores** (ruling 17). As expected, with one difference from the ruling's
    expectation: `compare_stores`' bookkeeping list holds no `extra_data` entry (the ruling's
    "`extra_data`'s timing"); it lists the timestamps, the labels, `ScalarHistory`'s `compute_time`
    and three RHS times, `AdiabaticHistory.compute_time` and `BBNData`'s two compute times.
    `stage_counts.py`: `reference-a` 4 / 4 / 4 / 4 in the scalar stage, `resumed-a2` 1 / 1 / 1 / 1.
18. **`NOTED`: the plot** (ruling 18). Exit 0; six histories at φ* = 5 read, six combinations
    skipped with a warning each; β = 0.5 dropped from the BBN plots with §0's reason;
    `histories.csv` six rows, each `normalization` and `phi_ref_Mp` equal by `float.hex` to the
    store; the store's files and sidecar unchanged by the plot.
19. **`NOTED`: the manifests** (ruling 19). Each as expected: `script` `main.py`, `script_sha256`
    `ec6144aa…f87d`, `git_head` `4dc31eae6d3a523abd58346aa9628ea24ad97cfe`, `git_dirty` false,
    the seven packages, `scope` as 08's `describe_scope`. No super-Planckian warning in any log.
20. **`STRUCTURALLY REQUIRED`: the token grep** (ruling 20), after every record was written:
    exit 1.
21. **`STRUCTURALLY REQUIRED`: the records** (ruling 21), and, since §3 gained an entry,
    `.documents/OPEN_ISSUES.md`.
22. **`UNINTENDED DRIFT`: the Gate's record was written into `var/jordan-normalization-09/`,
    created in the same Bash call**, so `09-gate.txt` cannot show that the directory did not exist
    before. It did not: `ls var/` two minutes earlier (19:41) listed `datastores`,
    `ray-sqla-align-01`, `ray-sqla-align-02` and `runs` only. Also under `$L`: a self-test of the
    recorder and summariser on a hand-written log (`var/jordan-normalization-09/selftest/`), which
    read no store and started nothing; left in place, since nothing under `var/` is deleted.

## Verification performed

**The Gate** (19:43:42, [`09-gate.txt`](../../../.documents/jordan-normalization/out/09-gate.txt)):
branch `jordan-normalization`, `HEAD` `4dc31eae6d3a523abd58346aa9628ea24ad97cfe`, `git status
--porcelain --untracked-files=all` 0 lines; `git diff a5e7850 4dc31ea --stat` note 09 alone; `main.py`
SHA-256 `ec6144aa…f87d`; `ray_cluster.sh status` exit 0, the lock free, no process, the four ports
free; the lock module's `status` exit 0, `lock: free`; `list`: four runs, all finished (`lost`,
`done`, `killed`, `done`); neither store, nor `var/jordan-normalization-09/` (deviation 22); load
3.45.

**The suites**, serially, in the foreground, never with the cluster up
([`09-suites.txt`](../../../.documents/jordan-normalization/out/09-suites.txt)); `unittest`'s time
and the one-minute load before the root:

| Suite | Before `start` | After `stop` |
|---|---|---|
| CosmologyModels | 26 OK, 67.0 s, load 3.07 | 26 OK, 64.4 s, load 3.31 |
| ComputeTargets `test_[a-f]*` | 76 OK, 95.2 s, load 3.06 | 76 OK, 94.4 s, load 4.29 |
| ComputeTargets `test_[g-z]*` | 167 OK, 216.2 s, load 2.92 | 167 OK, 214.9 s, load 3.76 |
| ObjectFactories | 93 OK, 14.2 s, load 8.36 | 93 OK, 14.3 s, load 7.73 |
| tools | 115 OK, 113.9 s, load 7.69 | 115 OK, 115.8 s, load 6.40 |
| RunDrivers | 64 OK, 23.3 s, load 3.40 | 64 OK, 24.6 s, load 6.02 |

The baselines exactly; none fell. `var/runs` unchanged by either RunDrivers run.

**The cluster**: `start` exit 0, 8 s, `gcs_server` pid 35393; `ray status` exit 0, `0.0/10.0 CPU`,
`0.0/1.0 GPU`, `0B/6.57GiB memory`, `0B/2.00GiB object_store_memory`; `stop` exit 0, 20 s;
`status` after, exit 0, all free; the lock module's `status`, exit 0, `lock: free`. `ray stop` never
run.

**The builds, the stores, the comparisons**: the document §1.2–§1.11, every figure a line in
`.documents/jordan-normalization/out/09-*`.

**The token grep**: `grep -r -F -l -f ~/.ray/auth_token .documents/jordan-normalization/
.documents/jordan-normalization-verification.md prompts/jordan-normalization/logs/09-rehearse-a-tuned-build.md`,
after every record was written: no output, exit 1.

**Acceptance (prompt §3):**
1. Every history converged; the stored tuned values equal the standalone tool's bitwise; the
   resumed store equals the reference (`RESULT: equal`, one fingerprint).
2. H4's measurements and H5's document: as above.
3. One commit with the log and the document; the board's row 09, H1–H5, the gate "R" and §5.10.

`git status --short` before staging: the records of ruling 21 and nothing else; `var/` is ignored.
`main.py`'s SHA-256 unchanged at the end.

## The deliberate-breakage record

None. This prompt changes no code, so there is nothing to break; the deliberate interruption is
the rehearsal's `SIGTERM`.

## Observations not acted on

1. **Ray's log deduplication drops tune lines of parallel histories.** Ray's deduplicator
   (`ray/_private/ray_logging/__init__.py`, `_canonicalise_log_line` and `LogDeduplicator`) keys a
   forwarded line on its words that hold no digit, and a line whose key another process printed
   within 5 s (`RAY_DEDUP_LOGS_AGG_WINDOW_S`) is held back; only the latest such line is printed,
   with `[repeated Nx across cluster]`. Every tune line has the same key. In `reference-a` and
   `resumed-a`, β = 1.5's seed line was lost this way inside β = 3's `… it=0 kind=seed … wall=8.8
   [repeated 2x across cluster]` (8.5 in `resumed-a`). The stored values are not affected, and the
   heartbeat still beats, but P12's "one line per integration" does not reach the log whole when
   histories run in parallel, and a cost read from the log undercounts. Opened as
   `[09-ray-deduplicates-the-tune-lines-of-parallel-histories]`; not assigned.
2. **The RayPoolKit wait (P19) took the form "computed, not stored"**: run `c`'s M = 0.1 history
   finished 102.0 s (reference) and 102.5 s (resumed) before it was stored, with M = 10⁻⁵'s; run
   `a`'s β = 0.5 waited 19.5 s and 18.9 s for β = 1.5's. An interruption in such a window loses the
   history. No row was stored and left unvalidated. For 10's handover, with
   `[04-the-pool-waits-for-every-in-flight-item-when-fewer-than-three]` (`rrk-adoption`).
3. **A `SIGTERM`ed build's worker runs on** for the history in flight: β = 5.5's `it=3` line
   arrived 20 s after the `killed` record and refreshed the heartbeat with the state `killed`. By
   20:07:45 no worker was alive. `resumed-a2` recomputed the history from its seed (107 s).
4. **The fingerprint at finish takes 23–25 s** per run here, against 7–8 s on `ray-sqla-align`
   02's store; the `redshift` table holds 75001 rows against 21250. It is the longest silence after
   the BBN stage.
5. **`plot_by_beta.py`'s shard warning**, `number of shards read from database (=16) does not match
   specified number of shards (=10)`, despite `--shards 16`, as in the two earlier rehearsals' plot
   logs.
6. **`histories.csv`'s `M_Mp` for M = 10⁻⁵ prints `9.999999999999999e-06`**, the stored eV value
   converted back; cosmetic.
7. **The stop needed `SIGKILL`** for 35393 and 35394 after its grace, as at `ray-sqla-align` 02;
   `ray status` reports `0.0/1.0 GPU`, as before.

## State handed to the next prompt

- **Tree:** this commit, on `jordan-normalization`, on top of `4dc31ea`. No code changed since
  `e624f1a`. `git status` clean after the commit; nothing pushed.
- **The stores**, kept for the user (README §3, §7):
  - `var/datastores/jordan-normalization-09-reference.db`, 16 shards, sidecar `store_id`
    `b5721a5e4c544f02bfb78e1b9ea7e9d9`;
  - `var/datastores/jordan-normalization-09-resumed.db`, 16 shards, sidecar `store_id`
    `5f9ffeeba8994e4ba41504eb24040cc0`;
  - each fingerprint `3b92d064512079e30bb2291d6d82732fed10c187e50c61e4b3d54cb47b3415a7`, version
    label `2026.7.0`; seven histories, six models, seven BBN rows (one failure, β = 0.5 at φ* = 5).
- **`var/runs/`**: seven runs of 09, all finished — six `done`, `resumed-a` `killed`; with the four
  earlier runs, `list` shows eleven, none running or stale.
- **`var/jordan-normalization-09/`**: the launch logs, pid files, recorders, the plot's outputs, the
  tool outputs and the helpers.
- The cluster is stopped and the rehearsal lock free.
- **For 10's handover**, quoting the document §1:
  - the cost: the seven histories' tuning took 337.5 s of integrations by the tune lines seen
    (3–9 integrations; 0.8–45 s each), at a one-minute load of 2–7; M = 10⁻⁵ and β = 5.5 are
    the slowest (130 s, 113 s);
  - the heartbeat's longest silence: 58 s, in the scalar stage, against 900 s; the fingerprint at
    finish adds 23–25 s of silence after the BBN stage;
  - the RayPoolKit wait: a computed history waited up to 102.5 s to be stored;
  - Ray's deduplication of tune lines (the new issue);
  - the resume after `SIGTERM` works for tuned histories: `--prune-unvalidated` on the next run,
    which computes only what is missing.
- **Counts:** CosmologyModels 26, ComputeTargets 243 (76 + 167), ObjectFactories 93, tools 115,
  RunDrivers 64.
- **Open issues.** Opened `[09-ray-deduplicates-the-tune-lines-of-parallel-histories]`. Index: 25
  open.
