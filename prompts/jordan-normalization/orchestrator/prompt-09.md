# Orchestrator — prompt 09, rehearse a tuned build

Read [`../README.md`](../README.md) first: §0.3 items 1, 2 and 7, §2.2 (09's charter), §3, §4, §5,
§6.1 U2, §6.2 P12 and P19. Then the prompt, the board, 06's and 08's logs (06's table of the secant
against the plain update, `logs/06-probes/table.txt`, and its `*_secant.log` RESULT lines; 08's
"State handed to the next prompt"). The rehearsal this prompt repeats was ruled twice before:
`rrk-adoption`'s note for its 04 and `ray-sqla-align`'s note for its 02
([`../../ray-sqla-align/orchestrator/prompt-02.md`](../../ray-sqla-align/orchestrator/prompt-02.md)).
Where their rulings still apply they are restated in §0, so that the agent need not read them.

**You do not write code.** You may:
- run the suites, `compare_stores.py`, `stage_counts.py`, the agent's helpers in
  `var/jordan-normalization-09/tools/`, `tools/history_and_bbn.py`, and the registry's read-only
  commands (`list`, `store show`, `store fingerprint` without `--write`) on 09's own stores and runs;
  `ray_cluster.sh status` and the lock's `status`, each alone in its Bash call;
- read the agent's outputs under `var/jordan-normalization-09/`, `var/runs/` and
  `.documents/jordan-normalization/out/`;
- run a probe from the session scratchpad, never committing one, and never starting Ray;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`09-rehearse-a-tuned-build.md`](../09-rehearse-a-tuned-build.md)
**Board items:** H1–H5 · **Gate:** "R" · **Closes:** nothing · **Opens:** only what the work finds ·
**Model:** Opus, as the prompt recommends · **Network:** none. Nothing is pushed.

**Gate**, checked at `a5e7850` on 2026-10-26 at 19:38 GMT, before this note was committed:
- 08 landed (`e624f1a`) and reviewed (`a5e7850`); the gate "Driver" holds (board §2).
  `git diff e624f1a a5e7850 --stat` touches four records and no code: the code the builds run is
  08's. `main.py`'s SHA-256 is **`ec6144aa02f5014dbc1aaecee81dd22efa030b2ae507c4eb6b7965b9e489f87d`**.
- `jordan-normalization` checked out in the main checkout; `git status --porcelain
  --untracked-files=all` empty. `HEAD` is the commit that adds this note, which touches only
  `orchestrator/`: it is the prompt's **dispatch commit**, and every manifest's `git_head` will be it.
- `ray_cluster.sh status`, exit 0: the lock free (`~/.raypoolkit/rehearsal-lock` does not exist), no
  Ray process of the cluster, nothing `stop` would signal, ports 6380, 6381, 8267 and 52367 free.
- `./venv/bin/python -m runregistrykit list`: four runs, all finished — `ray-sqla-align` 02's three
  (`done`, `killed`, `done`) and `rrk-adoption-03-unit-20261026T151946` `lost` (ended by the user
  after 08's review). None `running`, stale or unknown.
- `var/` holds `datastores/` (four sidecars of dropped stores: `rrk-adoption-04-*` and
  `ray-sqla-align-02-*`, `.manifest.json` only), `ray-sqla-align-01/`, `ray-sqla-align-02/` and
  `runs/`. None of 09's store paths, and no `var/jordan-normalization-09/`. The four sidecars are not
  09's, are not opened, changed or removed.
- The one-minute load was 2.66 (2.66 / 3.38 / 3.28). Other workloads share the machine.

## 0. What makes this prompt unusual

**It is the only end-to-end test of tuned histories before the science rebuild**: real Ray, the
wrapper, the store, the adiabatic and BBN stages and the plot, all reading what the tuning wrote. Its
claims are of two kinds: equalities (bitwise, by `float.hex`; `compare_stores`' `RESULT: equal`) and
measurements (times, silences, integrations). **Every claim in its record is a line the agent saw,
quoted, with the command that produced it.**

The orchestrator read every file the prompt names at `a5e7850` and ran two probes before writing
this note. What it found, where the prompt is silent, loose or wrong for this tree:

- **The grid is seven histories and six models, not eight.** Run `a` gives four (β 0.5, 1.5, 3, 5.5
  at M = 10⁻³ M_P, φ* = 5), `b` one (β 0.5, M = 10⁻³, φ* = 1), `c` two (β 3 at M = 0.1 and 10⁻⁵,
  φ* = 5); README §2.2 lists the same seven. `b`'s model (M = 10⁻³, n = 1, β = 0.5) is `a`'s, so the
  `ExponentialModel` table holds six rows.
- **The YAML's `M-values-Mp: 0.5` is overridden, not extended.** A scratch parse with `main.py`'s own
  parser (`create_argument_parser`) of each run's arguments gives `M_values_Mp` `[0.001]`,
  `[0.1, 1e-05]` and `[0.001]`, `M_values_eV` `[]`, the β values as given, n `[1]`, tolerance 1e-05,
  cap 30, atol = rtol = 1e-08, T* 20000 GeV, 300 decades at 250 per decade, 16 shards.
  **`--prune-unvalidated` defaults to true** (`BooleanOptionalAction`), so giving it is explicit,
  not a change.
- **The tool reproduces prompt 06 bitwise.** `tools/history_and_bbn.py 0.5 1e-3 --phi-init-Mp 1`
  (load 7 → 6) printed, in 21 s with its PRyMordial solve:

  ```
  tuning beta=0.5 M=0.001: normalization=-276.74415964649995 V_ref=6.581780291902322e-121 phi_ref=0.06382165048150604 phi_0=0.05436359501460411 pi_0=0.061562388505771254 tracking=0.754946252655946 residual_H=9.519404822189159e-07 residual_phi=3.82648485230952e-06 tuning_iterations=4 plain_steps=1 secant_steps=2 guard_steps=0 seed_kind=tracking seed_normalization=-276.7841498114068 seed_phi_ref=0.062129003658643864
  ```

  whose normalization and φ_ref are `-0x1.14be813f20b86p+8` and `0x1.0569d9d98458ap-4`, the hex of
  `logs/06-probes/b0.5_M1e-3_phi1_secant.log`'s RESULT line. 06's probes therefore predict six of the
  seven stored histories' normalization and φ_ref to the bit (all but β = 0.5 at φ* = 5).
- **β = 0.5 at φ* = 5 tunes quickly and its BBN fails, as it did in 2026.6.0.** The same tool with
  `--phi-init-Mp 5`, in 6 s:

  ```
  tuning beta=0.5 M=0.001: normalization=-276.7305358530105 V_ref=6.671430241172124e-121 phi_ref=0.06421012992791278 phi_0=0.05568261410958742 pi_0=0.05291204161274784 tracking=0.6334956605858231 residual_H=-6.581715529141441e-06 residual_phi=6.279225380323439e-06 tuning_iterations=4 plain_steps=1 secant_steps=2 guard_steps=0 seed_kind=tracking seed_normalization=-276.7841498114068 seed_phi_ref=0.062129003658643864
  bounce beta=0.5 M=0.001: N=36.955071745 T_J=0.000000 MeV phi=6.996332e-05 reflected=False
  fixed_T beta=0.5 M=0.001: 1MeV crossings=1 phi=3.835253226e+00 ratio=4.191912797e+01 …
  bbn beta=0.5 M=0.001: FAILURE network=small wall=1.0 s reason=PRyMordial: ComputationFailureError: T_in_MeV=0.00019824 MeV is smaller than T_BBN_spline_min=0.0002 MeV
  ```

  The field has not rolled by 1 MeV (φ = 3.84 M_P, ρ_NP/ρ_R,J ≈ 42), the history converges in four
  integrations, and PRyMordial's input temperature falls below the spline floor. The 2026.6.0 science
  run's `plot-phi5.log` has the same reason at β = 0.5–0.9, M = 10⁻³ M_P (lines 139–147). It is a
  property of a non-surfing history at large φ*, not a defect of this campaign: **a stored BBN
  failure for that history is expected**, deterministic, and the same in both stores.
- **Prompt 06's costs place the histories far apart in run `a`.** 06's secant walls (load 6–12):
  β = 1.5 43 s, 3 57 s, 5.5 173 s; β = 0.5 at φ* = 5 6 s here. Run `c`: M = 10⁻⁵ 214 s, M = 0.1 56 s.
  `b`: 24 s (06), 21 s here. So each build's scalar stage is about 3–4 minutes in `a` and in `c`.
- **`compute_time` is not the tuning's time.** `tune_history`'s payload is the final integration's
  (`dict(data)`), so the stored `compute_time` (`supervisor.integration_time`) is that integration's
  alone. A history's tuning time is the sum of its `-- tune` lines' `wall=` values (each line's wall
  is that integration's), or the arrival of its last line after the scalar banner. **The delay for
  `resumed-a` cannot be chosen from `compute_time`s**, as the earlier rehearsals' was.
- **The longest heartbeat silence cannot be read from `list`.** `status.json` keeps only the latest
  beat; `list`'s `AGE` is the run's age since `created`, not the heartbeat's (`rrk-adoption` 04,
  deviation 13); and the agent may not poll a run. The wrapper beats on a stdout line at most once
  every 30 s, or at once on a stage change (`RunDrivers/registered_run.py:304-312`); tune lines are
  printed with `flush=True` (`ComputeTargets/ScalarHistory.py:1672-1677`) and Ray forwards them. A
  silence is therefore measured by a **detached recorder** that notes each line's arrival and each
  change of `status.json`'s `heartbeat` (ruling 7). It is a process, not the agent's polling.
- **`04-compute_times.py` reads the old schema** (`ScalarModel`, `ExponentialCoupling`). The stored
  rows are now `ScalarHistory` (`model_type` 1 → `ExponentialModel` by `model_serial`; `M_serial`,
  `n_value`, `beta_serial`; `phi_Einstein_init_serial`, `T_Jordan_init_serial`, `atol_serial`,
  `rtol_serial`, `tuning_tol_serial`; the fifteen columns of `TUNING_OUTPUT_KEYS`, `z_samples`,
  `compute_time`, `validated`, `failure`, `failure_reason`, `timestamp`;
  `ObjectFactories/ScalarHistory.py:155-280`, `ObjectFactories/ExponentialModel.py:41-62`). A new
  reader replaces it (ruling 13).
- **`tools/compare_stores.py` knows the new schema** (`ScalarHistory`'s bookkeeping, the
  polymorphic `model_serial`), and `stage_counts.py`'s and `interrupt_when.py`'s banner, `CALCULATE
  SCALAR FIELD HISTORIES FOR SAMPLE GRID`, is still `main.py`'s (`main.py:325`).
- **The plot reads the store's grid.** `plot_inputs.stored_grid` gives every stored β and M, and
  `exponential_models` looks up their product: 3 M × 4 β = 12 combinations, six stored at φ* = 5,
  six skipped with a warning line each. φ* = 1's history is not read (the plot is at φ* = 5).
- **The scalar stage waits for three returns** (`process_batch_size=3`, `main.py:330`; the RayPoolKit
  wait of P19). With four in flight in run `a`, the first store comes after the third history
  completes. "Once at least one history is stored" therefore means after the third.
- **No super-Planckian warning is expected.** The thresholds at the seed's φ_ref are β > 6.506 at
  M = 10⁻³, φ* = 5 (08's log), and higher at larger M; no point here reaches them.
- **The RunDrivers suite runs the real wrapper as a subprocess on the real `var/runs`**
  (`[08-the-wrappers-refusal-tests-run-the-real-wrapper-on-var-runs]`). A refusal that failed would
  attach to a cluster at 6380. **No suite runs while the cluster is up.**

**Baselines** at `e624f1a`, from 08's review (serially, load 3–5; `a5e7850` changed only records):
CosmologyModels **26**, ComputeTargets **76 + 167**, ObjectFactories **93**, tools **115**,
RunDrivers **64**, all passing.

**These rulings bind the agent.** Each is recorded in the log as a deviation of the class given,
where it reads the prompt otherwise; `NOTED` ones are expectations, recorded as found.

1. **Seven histories, six models** (`STRUCTURALLY REQUIRED`). The prompt's "eight histories" is a
   miscount (§0). The record says seven.

2. **The runs** (`IMPLEMENTATION CHOICE`, ruled). `ADDRESS=127.0.0.1:6380`,
   `S=$PWD/var/datastores`, `L=$PWD/var/jordan-normalization-09`, **absolute**, exported in each
   launch call. Seven builds, each through the wrapper with `--campaign jordan-normalization --prompt
   09 --register <slug> --purpose "…"`, and after `--` the prompt's common arguments
   (`--config exponential.yaml --database $S/jordan-normalization-09-<store>.db --shards 16
   --job-name jordan-normalization-09-<slug> --ray-address $ADDRESS`) and the run's own:

   | slug | store | run | extra |
   |---|---|---|---|
   | `reference-a` | `reference` | `a`: `--M-values-Mp 1e-3 --beta-values 0.5 1.5 3 5.5 --phi-init-Mp 5` | — |
   | `reference-b` | `reference` | `b`: `--M-values-Mp 1e-3 --beta-values 0.5 --phi-init-Mp 1` | — |
   | `reference-c` | `reference` | `c`: `--M-values-Mp 0.1 1e-5 --beta-values 3 --phi-init-Mp 5` | — |
   | `resumed-a` | `resumed` | `a` | `SIGTERM`ed (ruling 8) |
   | `resumed-a2` | `resumed` | `a` | `--prune-unvalidated` |
   | `resumed-b` | `resumed` | `b` | — |
   | `resumed-c` | `resumed` | `c` | — |

   `b` and `c` have the same arguments in both builds (the job name aside), so that the interruption
   is the only difference. The order is the table's: the reference build whole, then the resumed.

3. **`store create` after each store's first close** (`IMPLEMENTATION CHOICE`, ruled; the prompt puts
   it after `c`). After `reference-a`: `store create $S/jordan-normalization-09-reference.db
   --purpose "…"` and `store show`; after `resumed-a`: the same for the resumed store. Then the later
   runs on each store record its `store_id` as `results_store_id` and write their fingerprint to the
   sidecar at finish, as the registry's convention has it (`CLAUDE.md`; `rrk-adoption` 04). Expected:
   `results_store_id` `null` for `reference-a` and `resumed-a`, the store's `store_id` for the other
   five; `fingerprint_sidecar` "written" for those five. Any other value is a stop. No `store
   fingerprint --write` is needed; at the end, `store fingerprint` **without `--write`** on each
   closed store matches its sidecar.

4. **The launch line** (`IMPLEMENTATION CHOICE`; the earlier rehearsals' ruling 5). One Bash call per
   launch, which exports `ADDRESS`, `S` and `L` and runs:

   ```
   nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u RunDrivers/registered_run.py --campaign jordan-normalization --prompt 09 --register <slug> --purpose "<one line>" -- <arguments> > $L/<slug>.log 2>&1 & echo $! > $L/<slug>.pid; wait $!; echo "** launcher saw exit status $?" >> $L/<slug>.log' > /dev/null 2>&1 &
   ```

   with the slug spelt out. Record each exactly as run, the exports included. The recorder (ruling 7)
   and, for `resumed-a`, `interrupt_when.py` (ruling 8) are each launched with `nohup … &` in a Bash
   call of their own straight after.

5. **The waits** (`IMPLEMENTATION CHOICE`; the earlier rehearsals' ruling 3). A subagent cannot end
   its turn to be woken by a `Monitor` (`rrk-adoption` 04, deviation 3), so each wait is **one
   foreground call** of `wait_for_line.py` (ruling 6), the Bash tool's timeout at 600 000 ms: for
   `"** registered_run: registered as"` with `--timeout 120`; for `"** launcher saw exit status"`
   with `--timeout 570`. **An end wait that times out may be repeated once**, and the log says so;
   a registration wait that times out, or a second end-wait time-out, is a stop. No `sleep` loop, no
   `pgrep`, no `tail -f`.

6. **The helpers** (`IMPLEMENTATION CHOICE`, ruled). Copy unchanged
   `.documents/ray-sqla-align/out/02-wait_for_line.py.txt` and `02-ray_state.py.txt` to
   `$L/tools/wait_for_line.py` and `ray_state.py` (`cmp` each against its source), and adapt
   `02-check.sh.txt` and `02-suite.sh.txt` to `$L` and `09-` names (the diff recorded). `ray_state.py`
   runs `./venv/bin/ray list` in a subprocess, so always as **`RAY_AUTH_MODE=token ./venv/bin/python
   $L/tools/ray_state.py`**. The agent writes three more, stdlib only, in `$L/tools/`: the recorder
   and its summariser (ruling 7) and the stored-values reader (ruling 13), with a fourth for the
   comparison of ruling 14 if it wants one. Every helper is committed as `out/09-<name>.txt`.

7. **The recorder** (`IMPLEMENTATION CHOICE`, ruled; it measures what the prompt's H4 asks of `list`,
   which `list` cannot give). `beat_times.py LOG OUT`, launched detached (`nohup`) straight after
   each build's launch, and reading only:
   - it waits for LOG to exist, then appends to OUT one line per complete line of LOG as it appears,
     `<UTC ISO time, ms> L <the line, ANSI stripped>`, polling every 0.5 s;
   - once LOG holds `** registered_run: registered as <RUN_ID>`, it also reads
     `var/runs/<RUN_ID>/status.json` at each poll (a failed read is skipped) and appends
     `<UTC> H <heartbeat> <state> <stage>` whenever (heartbeat, state, stage) changes;
   - it exits after LOG's `** launcher saw exit status` line and one last read of `status.json`, or
     after 3600 s whatever happens, writing why it exited as its last line.

   `silences.py OUT…` then gives, per run: (a) **the longest heartbeat silence**: the largest
   difference between consecutive recorded `heartbeat` values while `state` was `running`, and from
   the last of them to the run's end, with the stage it fell in; (b) the longest gap between
   consecutive log lines, per stage; (c) each stage's time, from its banner's arrival to its `ALL
   WORK ITEMS COMPLETE` line's; (d) per history, its label, its `-- tune` lines' count, the sum of
   their `wall=`, and its last tune line's arrival after the scalar banner. The heartbeat values
   are the registry's own, to the second, so the 0.5 s poll does not limit (a). Every recorder has
   exited (its last line written) before `stop`. The 900 s window is not changed (P12); (a) is
   reported against it.

8. **The delay for `resumed-a`** (`IMPLEMENTATION CHOICE`, ruled). From `reference-a`'s recorder,
   the four histories' last-tune-line arrivals after the scalar banner, sorted, t₁ ≤ t₂ ≤ t₃ ≤ t₄.
   **`<s>` is (t₃ + t₄)/2, rounded to 10 s**: after the third history, at which the first store
   comes (§0), and well before the fourth. At 06's costs that is about 100–120 s. **If t₄ − t₃ is
   under 60 s, stop and ask.** Launch `interrupt_when.py $L/resumed-a.log $L/resumed-a.pid --banner
   "CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID" --after <s>` detached, straight after
   `resumed-a`'s launch, to `$L/resumed-a.interrupt.log`. Expected: β = 0.5, 1.5 and 3 stored
   (validated or not), β = 5.5 not. **A different outcome is recorded, not a stop**, so long as
   `resumed-a` is `killed`/143; one that stored no history is recorded and reported, and a
   `resumed-a` that reaches `done` is a stop. After it, the reader (ruling 13) on the resumed store,
   with each row's `validated`; any row stored and unvalidated at the signal is the RayPoolKit wait
   of P19, measured from the row's `timestamp` to the signal.

9. **The signal** (`NOTED`; the earlier rehearsals' ruling 7). Expected after the banner: the
   handler's `** SIGTERM at …: recorded killed, exit code 143; …`, then `** SIGTERM: main.py ended
   with SystemExit(1); recording killed, exit code 143`, the marker `** driver exit status 143`, the
   launcher's 143. `interrupt_when.py` prints UTC to the second and the handler local time with its
   offset: 0 or 1 s between them is the expectation met. Give also the time from the signal to the
   final `status.json` heartbeat. A `*** SIGTERM received` banner or a `** cancelled an alarm` line
   is recorded and explained, and a stop only if the run is not `killed`/143. `resumed-a2`'s open
   prints an `INTEGRITY WARNING` and `** Note: these values have been pruned from the datastore.`
   for each unvalidated row; quote it, or say there was none.

10. **The cluster** (`STRUCTURALLY REQUIRED`; `ray-sqla-align` note 02, ruling 10, restated).
    - **`ray_cluster.sh start`, `stop` and `status`, and the lock module's `status`, are each the only
      process of their Bash call**, output redirected to a file under `$L` (`> F 2>&1`), never piped,
      the exit taken with `echo $?` in the same call, the file read in a later call.
    - **No command line of the agent's names `/tmp/ray-cpbh`**, from `start` to the end of `stop`.
    - **Every Ray CLI command carries `RAY_AUTH_MODE=token`.** **No `ray stop`, ever.** The token file
      `~/.ray/auth_token` is never printed, copied or changed.
    - Builds, waits, recorders, `interrupt_when.py`, `ray status` and `ray_state.py` each run in Bash
      calls of their own, never with a script or lock command. **`stop` runs only once every
      driver's pid answers `kill -0` with `no such process`** (by pid, from the pid files), every
      recorder has exited, and `interrupt_when.py` has printed its `sent` line.
    - `start` exits 3 (the lock held, or its old location present) or 4 (a port held, a process of
      the cluster alive): **either is a stop and ask**, never a wait, a retry or a removal.
    - `stop` needing `SIGKILL` after its grace for the `gcs_server` is not a stop (`ray-sqla-align`
      02), so long as `stop` exits 0, confirms no process and the ports free, and releases the lock.

11. **The Ray state** (`IMPLEMENTATION CHOICE`, ruled). `ray_state.py` after `start` (the baseline),
    after `resumed-a`'s launcher line, and before `stop`; `ray_cluster.sh status` (alone, to a file)
    after `resumed-a`'s launcher line too (the prompt's H4). After `resumed-a` no worker or actor of
    its job is alive; if one is, wait for it **by pid** with `kill -0` alone, at most ten minutes,
    and record it. Ray's `SUCCEEDED` for a `SIGTERM`ed driver's job is known, not a fault.

12. **The load and the suites** (`NOTED` and `IMPLEMENTATION CHOICE`). `uptime` before the suites,
    before `start`, at each launch, at the plot, before `stop`, and at each tool run. **If the
    one-minute load before `start` is over 20, stop and ask** (a BBN solve that meets
    `--bbn-wall-clock-limit` under load in one build only is the named cause of a store difference).
    The suites: `CLAUDE.md`'s commands, serially, in the foreground, `ComputeTargets` in its two
    halves, each call at 600 000 ms, output to `$L/suites/`, the summary to `$L/09-suites.txt`;
    **before `start` and after `stop` only, never with the cluster up** (§0). Expected at both
    points: the baselines above. A failure is re-run alone before it is called real
    (`test_prym_passenger`'s 60 s wall-clock test fails under load); an existing test that fails is a
    stop.

13. **The stored values** (`IMPLEMENTATION CHOICE`, ruled). `histories.py STORE_STEM` reads every
    shard through `sqlite3` with `file:…?mode=ro` URIs only, and prints per `ScalarHistory` row: M,
    n, β, φ*, π*, T* (from their rows, the floats as stored, and their `float.hex`); atol, rtol and
    the tuning tolerance (from `tolerance`); `validated`, `failure`, `failure_reason`, `z_samples`,
    `compute_time`, `timestamp`; and the fifteen `TUNING_OUTPUT_KEYS` columns, floats by `float.hex`,
    ints and `seed_kind` as stored. It also counts the rows of `ExponentialModel`,
    `AdiabaticHistory` and `BBNData` and gives each `BBNData` row's failure and reason. It writes no
    file but its output.
    - **The inputs are the tool's** (the prompt's "if the tool cannot be given exactly the build's
      inputs, stop"): every row's atol and rtol `1e-08`, tolerance `1e-05`, T* equal by `float.hex` to
      `20000 * Planck_units().GeV`, π* 0.0, φ* 5.0 or 1.0, M and β as given; and each row's
      `z_samples` equal to the tool's `samples=` for that history (the axis, 250 per decade to 300).
    - **Converged** (H3): every row `failure` 0, `validated` 1, and |`residual_H`| and
      |`residual_phi`| below the tolerance, as `tuning_converged` tests it (read the function and say
      which inequality).

14. **The bitwise comparison** (`NOTED` where it agrees with the prompt). After `stop` and before
    the suites again, `tools/history_and_bbn.py BETA M --phi-init-Mp X` once per history, seven runs,
    serially, each alone in its Bash call (the tool reads `PRyMrates/` from the working directory;
    "one invocation at a time"), with every other option at its default (which are `main.py`'s),
    output to `$L/tool/<β>-<M>-phi<X>.txt`, with `uptime`. Each `tuning` line's fifteen values
    against **both** stores' row for that history: floats by `float.hex` of the printed repr, ints and
    the string exactly; a NaN printed where the store holds NULL counts as equal and is said (SQLite
    stores NaN as NULL). **Any difference is a stop.** Measured beside it, `NOTED`: each stored
    normalization and φ_ref against 06's `*_secant.log` RESULT hex for the six points 06 ran
    (expected equal), and β = 0.5 at φ* = 5 against §0's probe line above (expected equal). A
    difference there with the tool equal is recorded and reported, not a stop. The tool's `bbn`
    line for β = 0.5, φ* = 5 is expected to fail as §0 shows.

15. **The tuned values against README §0.3** (`NOTED`; a measurement). Λ = V₀^¼ with V₀ = exp
    (`normalization`) M_P⁴, in meV with `REDUCED_PLANCK_MASS_EV` (`Units/base.py`): Λ/meV =
    exp(normalization/4) × `REDUCED_PLANCK_MASS_EV` × 10³. §0.3's "tuned Λ" is that same V₀^¼, not
    V_ref^¼ (08's probe compared V_ref^¼, which differs by exp((M/φ_ref)/4)). For each of the six
    points §0.3 lists (β = 0.5 at φ* = 1; 1.5, 3, 5.5 at M = 10⁻³; 3 at 10⁻⁵ and 0.1): Λ/Λ_§0.3 − 1
    and φ_ref/φ_ref,§0.3 − 1. Expected, from 06's table: Λ −2.77 to −2.80 × 10⁻⁴ (05's units) and
    φ_ref within 2 × 10⁻⁵ (the tolerance). β = 0.5 at φ* = 5 has no §0.3 entry. Report how well; no
    threshold.

16. **The BBN stage** (`NOTED`). The stored `BBNData` for β = 0.5, M = 10⁻³, φ* = 5 is expected to be
    a failure with §0's reason, identical in both stores (the reason's `T_in_MeV` is deterministic).
    Any other BBN failure is recorded with its reason. A BBN failure in one store and not the other is
    a store difference, and a stop.

17. **The stores** (`NOTED`). `compare_stores.py` on the two: `RESULT: equal`, `problems: none`, its
    row counts quoted (expected `ExponentialModel` 6, `ScalarHistory` 7), its bookkeeping list
    quoted (labels with the job names, timestamps, compute times; `extra_data`'s timing). `store
    fingerprint` without `--write` on each closed store: each matches its sidecar; the two digests
    are expected equal, and recorded either way. `store show` on each: the runs that name it, with
    their states. `stage_counts.py` on the seven logs: `reference-a` 4 / 4 / 4 / 4 in the scalar
    stage, `resumed-a2` only what `resumed-a` left missing.

18. **The plot** (`NOTED`). `plot_by_beta.py --config exponential.yaml --database
    $S/jordan-normalization-09-reference.db --shards 16 --phi-init-Mp 5 --output $L/plot-reference
    --ray-address $ADDRESS`, unregistered, to `$L/plot-reference.log`, with `shasum -a 256` of the
    reference store's files and sidecar before and after (unchanged: it is read-only). Expected: exit
    0; six histories read at φ* = 5 and six (M, β) combinations skipped with a warning each; β = 0.5
    dropped from the BBN plots with §0's reason. `histories.csv`: its SHA-256, its six rows, and each
    row's `normalization` and `phi_ref_Mp` equal by `float.hex` to the stored values (Planck units:
    M_P = 1.0, so `phi_ref_Mp` is `phi_ref`).

19. **The manifests** (`NOTED`, the expected values). `script` `main.py`, `script_sha256`
    `ec6144aa…f87d`; `git_head` the full SHA of this note's commit; `git_dirty` **false**; `packages`
    seven, as `ray-sqla-align` 02 recorded them (`runregistrykit` 0.2.0, `datastorekit` 0.4.0,
    `raypoolkit` 0.3.0, each not editable; `ray` 2.59.0, `SQLAlchemy` 2.0.55, numpy, scipy);
    `scope` as 08's `describe_scope` writes it (`config exponential.yaml; β …; M … Mp; n 1; φ* … Mp;
    tuning tolerance 1e-05; 16 shards; job …`). **`git_dirty` true is a stop**: the tree moved.
    No super-Planckian warning line in any launch log (§0); one that appears is quoted.

20. **The token grep** (`STRUCTURALLY REQUIRED`). After every record is written: `grep -r -F -l -f
    ~/.ray/auth_token .documents/jordan-normalization/ .documents/jordan-normalization-verification.md
    prompts/jordan-normalization/logs/09-rehearse-a-tuned-build.md`. Exit 1 is the pass; exit 0 lists
    a file (remove that copy, report it, do not commit it); exit 2 is a stop.

21. **The records** (`STRUCTURALLY REQUIRED`).
    - **`.documents/jordan-normalization-verification.md`** (new, additive), its header like
      `.documents/ray-sqla-align-verification.md`'s, and its §1 "The rehearsal (prompt 09, <date>)":
      the tree; the result; the cluster and the load; the builds' table (run id, slug, store, pid,
      state, exit, launched → ended, wall, `results_store_id`, fingerprint at finish, load at
      launch); the histories' table (β, M, φ*, integrations, plain/secant/guard, seed, tuning wall
      from the tune lines, last-line arrival, `compute_time`, Λ in meV, φ_ref/M_P, both residuals);
      the delay and the signal; the resume and the stores; the bitwise comparison; §0.3's
      comparison; the silences and the stage times; the BBN stage; the plot; the token check; the
      suites; observed, not acted on; the commands, verbatim, with `<scratch>` for `$L/tools/`.
    - **`.documents/jordan-normalization/out/09-*`**: the seven launch logs, the plot log and the
      interrupter's log (ANSI stripped, `sed -E 's/\x1b\[[0-9;]*m//g'`); each run's
      `manifest.json` and `status.json`; both sidecars; the recorders' outputs and `silences.py`'s;
      the readers' outputs; the seven tool outputs; `compare_stores`', `stage_counts.py`'s and the
      `store` commands' outputs; `histories.csv`; the cluster's record; `ray_state.py`'s; the suites'
      summary; the helpers as `.txt`. Not the run directories' `stdout.log` and `stderr.log`, which
      repeat the launch logs. A file over about 1 MB is reported before committing.
    - **The board**: row 09 ✅ with "this commit"; H1–H5 as found; the gate "R" as found; the status
      line ("9 of 10 landed, 09 awaiting review", or words to that effect); `### 5.10 After prompt 09
      (<date>)` in §5.9's form. `.documents/OPEN_ISSUES.md` only if §3 gains an entry.
    - **The log** `logs/09-rehearse-a-tuned-build.md`, in `dk-and-rpk-migration` README §5.1's form.

**What moves on purpose:** in the repository, exactly the records of ruling 21. Outside the tree:
`var/datastores/` (09's two stores, their shards and sidecars), `var/runs/` (seven run directories),
`var/jordan-normalization-09/`, the lock under `~/.raypoolkit/` (taken by `start`, released by
`stop`), and `/tmp/ray-cpbh/`.

**What must not move:** every file of the tree outside ruling 21's list, code and tests above all;
`orchestrator/`; `prompts/INDEX.md`; `CLAUDE.md`; every other campaign's records; the four
sidecars of dropped stores in `var/datastores/`; the other runs under `var/runs/`; the venv; the
token file; SGK, SI and the three packages. Nothing in `var/` is deleted. No store but 09's two is
opened.

**The stop conditions.** The prompt's §4, with these added: rulings 3, 5, 8, 10, 12, 13, 14, 16, 19
and 20 where they say so; a build that fails other than by its signal; `start` refusing. **Stop
with care:** the cluster is stopped first, unless the stop is what failed or a build is still
running; a stop with the cluster up, the lock held or a run `running` leaves them so and reports
`status`'s output and the run's pid and heartbeat. A stop goes to the user through the orchestrator;
the agent resumes by message.

**Conventions.** All of these bind the agent.
- **Order.** The Gate (the prompt's, re-checked); the helpers; the suites; `start`, `ray status`,
  `ray_state.py`; `reference-a` with its recorder, `store create`; `reference-b`; `reference-c`; the
  delay from `reference-a`'s record; `resumed-a` with its recorder and `interrupt_when.py`, the Ray
  state, the reader, `store create`; `resumed-a2`; `resumed-b`; `resumed-c`; the plot; `ray_state.py`;
  `stop` and `status`; `compare_stores`, the fingerprints, `store show`, `stage_counts.py`; the
  readers on both stores; the seven tool runs and the comparisons; `silences.py`; the suites again;
  the records, the token grep, the document, the log, the board.
- **Staging and reporting.** One commit, staged by explicit path only, after showing `git diff
  --cached --name-only`, which is ruling 21's list exactly. Check `git log -1` before committing:
  `HEAD` must still be this note's commit. Count, then paste. A record states only what a quoted line
  shows. "this commit" where the SHA goes; the orchestrator substitutes it.
- **Scratch.** Everything saved while working goes under `$L`; helpers in `$L/tools/`. Never `/tmp`,
  never the session scratchpad, never the tree.
- **Not touched:** `orchestrator/`, `prompts/INDEX.md`, `CLAUDE.md`, the board beyond ruling 21, any
  other campaign's records.

## 1. Before you dispatch

Done at `a5e7850`, before this note was committed: the Gate above; the scratch parse of the three
runs' arguments; the two tool runs of §0 (`orch09/probe-b0.5-phi5.txt`, `probe-b0.5-phi1.txt` in
the session scratchpad); `ray_cluster.sh status` (free). The suites' baselines are 08's review's.

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout. Give it: the prompt; the README; the
board; 06's and 08's logs; `rrk-adoption`'s prompt 04 and its log; `.documents/ray-sqla-align-verification.md`
§1.10 (the commands); `HEAD`; the Gate as measured; and §0 of this note up to "## 1. Before you
dispatch", as a copy in the session scratchpad. The agent does not read `orchestrator/` or prompt 10.

Tell it plainly:
- **One commit, at the end**, of records only, with exactly ruling 21's list.
- **§0's rulings bind it**, each recorded with its class.
- **It starts Ray only through the script**, once, opens no store but 09's two, and runs no suite
  with the cluster up.
- **The token is never printed, copied or changed.**
- **Stop and ask** on any of the prompt's §4 conditions or §0's.
- **It replies with:**
  - the log's path; the message as committed; `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - `start`'s exit; `ray status`'s resources; `ray_state.py` at its three points; `stop`'s and
    `status`'s exits and what `status` said;
  - the builds' table, with the load at each launch;
  - the histories' table (ruling 21);
  - t₁…t₄, `<s>` and how ruling 8 gave it, and what `resumed-a` stored and validated;
  - the signal: `interrupt_when.py`'s lines, the log's lines after the banner, the two intervals;
  - `resumed-a2`'s prune report (or none) and `stage_counts.py` on the seven logs;
  - `compare_stores`' result and row counts; the two `store fingerprint` digests; `histories.csv`'s
    SHA-256 and the reference store's `shasum`s before and after the plot;
  - the inputs check and the convergence check (ruling 13); the bitwise comparison, history by
    history and key by key (ruling 14), and the measurements against 06's hex and §0's probe;
  - §0.3's comparison (ruling 15);
  - the longest heartbeat silence per run with its stage, against 900 s; the stage times; the
    longest line gaps; the RayPoolKit wait if seen;
  - the BBN stage's failures with their reasons;
  - the manifests' `git_head`, `git_dirty`, `script_sha256`, `packages` and `scope`;
  - the token grep's exit;
  - every deviation with its class; "Observations not acted on" and "State handed to the next
    prompt", verbatim.

## 3. The review — eleven checks

1. **Scope.** `git show --stat <agent>` lists exactly ruling 21's files; its parent is this note's
   commit. `git diff <dispatch> <agent> -- '*.py' '*.sh' '*.toml' '*.yaml' requirements.txt
   CLAUDE.md prompts/INDEX.md` is empty. `git status --porcelain --untracked-files=all` empty;
   `var/` in no commit; nothing pushed.
2. **The cluster and the token, here.** `ray_cluster.sh status` (alone): the lock free, no process,
   the ports free. The lock module's `status`: free. Ruling 20's grep exits 1, and so does `git grep
   -F -l -f ~/.ray/auth_token <agent>`.
3. **The runs, from the registry.** `list`: seven runs of 09, `resumed-a` `killed`, the other six
   `done`. Each `manifest.json` and `status.json` under `var/runs/` read against the committed copies
   and the document's table: state, exit code, `script_sha256` against `shasum -a 256 main.py`,
   `results`, `results_store_id` (ruling 3), `git_head` this note's commit, `git_dirty` false,
   `packages`, `scope` (ruling 19).
4. **The order and the signal.** In each launch log the three `** registered_run:` lines precede
   `>> packages:`. `resumed-a`'s log: the handler's line, the `SystemExit` line, the marker 143, the
   launcher's 143; the interval recomputed from the quoted lines.
5. **The delay.** `silences.py` re-run here on `reference-a`'s recorder output: t₁…t₄ as logged, and
   `<s>` as ruling 8 gives them. The account of what `resumed-a` stored matches the reader's output
   on the resumed store at that point (committed), `resumed-a2`'s prune report and its stage counts.
6. **The stores, re-measured.** `compare_stores.py` on the two: `RESULT: equal`, the logged row
   counts. `store fingerprint` on each, without `--write`: as logged, each matching its sidecar.
   `store show` on each: the runs as logged. `stage_counts.py` on the seven logs: as logged.
7. **The stored values, re-measured.** `histories.py` re-run here on both stores: the inputs and the
   convergence as ruling 13 says. **Two tool runs here**, chosen from the seven (one at M = 10⁻³
   and the M = 10⁻⁵ one), their `tuning` lines equal by `float.hex` to both stores' rows; the other
   five checked against the committed tool outputs. 06's hex for the six points, against the stores.
8. **§0.3 and the silences.** Ruling 15's ratios recomputed here for two points from the stored
   hex. The longest heartbeat silence of one run recomputed here from its committed recorder output.
9. **The plot.** `shasum -a 256 $L/plot-reference/QCD_Cosmology/histories.csv` as logged; the
   reference store's files' `shasum`s as logged after the plot; the CSV's `normalization` and
   `phi_ref_Mp` against the stores.
10. **The records.** The document's §1 against the outputs under `out/` and the commands as run;
    nothing claimed that no quoted line shows. The board (ruling 21) and the log in §5.1's form,
    every deviation classed, §0's among them. The message in `CLAUDE.md`'s form.
11. **The suites** at `<agent>`, serially, no cluster up: 26 / 76 + 167 / 93 / 115 / 64. No `.py`
    file is committed, so `black` has nothing to check; the helpers' copies are `.txt`.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, on the board, in the log and in the document;
- the board's list of orchestrator notes, with this one marked "used for 09";
- `### 5.10`'s *Orchestrator review of prompt 09* paragraph, holding what each check found;
- the status line: 9 of 10 landed; next, the orchestrator note for 10.

Residue the review finds is fixed in that commit or a follow-up of its own. Anything that is not
residue is opened on the board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:** the commit and the review commit; the gate "R" (convergence, the bitwise
comparison, `compare_stores`); the seven runs; the measurements (the longest silence against 900 s,
the histories' costs, the RayPoolKit wait); the rulings that read the prompt otherwise than it was
written (seven histories; `store create` after each store's first close; the recorder in place of
`list` for the silences; the delay from the tune lines, not `compute_time`; the expected BBN failure
at β = 0.5, φ* = 5); and that 09's two stores are kept for the user under `var/datastores/`.
