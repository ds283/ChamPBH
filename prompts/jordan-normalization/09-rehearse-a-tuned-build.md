# Prompt 09 — rehearse a tuned build

**Campaign:** [`README.md`](README.md) · **Board items:** **H1**–**H5** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-20, after the user's rulings (README §6: U2, the rehearsal; R13, deterministic
seeds; P12). It follows `rrk-adoption` prompt 04's protocol for registered, interrupted and resumed
builds. Its orchestrator note, written after 08's review, governs where it differs from this file.
**Gate:**
- 08 landed and reviewed, the gate "Driver" holding;
- on branch `jordan-normalization`, no tracked change in `git status`;
- `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status`: the lock free, no ChamPBH
  process, the ports free;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- neither `var/datastores/jordan-normalization-09-reference.db` nor `…-resumed.db` exists, nor
  `var/jordan-normalization-09/`.

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Opus**. Real builds of tuned histories under real Ray, one interrupted on
purpose, read against the tuning layer called directly.

**Read first:**

1. [`README.md`](README.md): §0.3 items 1 and 7 (the points and their costs), §2.2 (09's charter),
   §3, §5 (the one-off exception; "No agent polls a run"), §6.1 U2, §6.2 P12, P19.
2. `CLAUDE.md`: "Long-running jobs — the run registry" (the rules, the wrapper), "Ray on this
   machine".
3. `prompts/rrk-adoption/04-rehearse-registered-runs.md` and its log: the launch (detached, through
   `sh`), `interrupt_when.py`, the `** driver exit status` marker, `store create`, `compare_stores`,
   and what was measured.
4. The rehearsal tools, run unchanged: `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh`,
   `interrupt_when.py`, `stage_counts.py`; `tools/compare_stores.py`.
5. 06's, 07's and 08's logs: the tuning lines, the stored columns, the options, and 06's costs at
   these points.

---

## 1. What is wanted

The proof that the tests cannot give: under real Ray, through the wrapper, tuned histories are
computed, stored, read back by the adiabatic and BBN stages and by `plot_by_beta.py`, and a build
interrupted in its scalar stage resumes to a store equal to an uninterrupted one. This is a one-off
exception to `CLAUDE.md`'s rule that no campaign prompt starts a `main.py` run with a datastore
(U2).

### 1.1 The protocol

- `ADDRESS=127.0.0.1:6380`, `S=var/datastores`, `L=var/jordan-normalization-09`.
- **Three runs make one build**, into one store, each through the wrapper (`--campaign
  jordan-normalization --prompt 09 --register <slug> --purpose "…"`), after `--`:
  - `a`: `--M-values-Mp 1e-3 --beta-values 0.5 1.5 3 5.5 --phi-init-Mp 5`;
  - `b`: `--M-values-Mp 1e-3 --beta-values 0.5 --phi-init-Mp 1`;
  - `c`: `--M-values-Mp 0.1 1e-5 --beta-values 3 --phi-init-Mp 5`;
  each with `--config exponential.yaml --database $S/jordan-normalization-09-<store>.db --shards 16
  --job-name jordan-normalization-09-<store>-<run> --ray-address $ADDRESS`. Eight histories, about
  ten CPU-minutes of tuning at 06's costs, plus their adiabatic and BBN stages.
- **Every run is launched detached** (rule 3), checked once that it started, and waited on by a
  `Monitor` on its log's `** driver exit status` line, never by polling (rule 4).

### 1.2 The items

- **H1 — `reference`.** Runs `a`, `b`, `c` into `…-reference.db`, then `store create --purpose …`
  and `plot_by_beta.py --phi-init-Mp 5` (unregistered, read-only) into `$L/plot-reference/`.
- **H2 — `resumed`.** Run `a` into `…-resumed.db`, sent `SIGTERM` by `interrupt_when.py` during the
  scalar stage once at least one history is stored; recorded `killed`; then run `a` again with
  `--prune-unvalidated`, then `b` and `c`. `store create`.
- **H3 — the comparisons:**
  - `tools/compare_stores.py` on the two stores: equal (deterministic seeds, R13), or differing only
    where `rrk-adoption` 04's protocol says a resume may;
  - for every stored history, `tools/history_and_bbn.py` run on the same inputs (06's tuning,
    standalone, no Ray): the stored normalization, φ_ref, residuals and iteration count equal its
    output **bitwise**. If the tool cannot be given exactly the build's inputs (grid, tolerances,
    T_init), stop;
  - every stored history converged: both stored residuals inside the tolerance;
  - the tuned values against README §0.3's: they should differ by 05's units (about 2.8 × 10⁻⁴ in
    the normalization's Λ-equivalent, V₀^¼) and agree otherwise. Report how well; this is a
    measurement.
- **H4 — measured:** each history's integrations and wall time; each stage's time; the longest
  heartbeat silence `list` reported, against the 900 s window (P12); the Ray processes after each
  kill (`ray_cluster.sh status`); `git_dirty` as recorded; the RayPoolKit wait of P19, if seen
  (a stored history left unvalidated behind a slower one in flight).
- **H5 — the document.** `.documents/jordan-normalization-verification.md`, with §1 for this
  rehearsal: the commands, the times, H3's results and H4's measurements; small text outputs under
  `.documents/jordan-normalization/out/`. The stores stay under `var/datastores/` for the user (§3).

---

## 2. Verification

H3 is the verification. Also: the five suites unchanged (no code changes in this prompt); no
ChamPBH Ray process outlives `ray_cluster.sh stop`.

## 3. Acceptance

1. Every history converged; the stored tuned values equal the standalone tool's bitwise; the
   resumed store equals the reference.
2. H4's measurements and H5's document.
3. **One commit**, with the log `logs/09-rehearse-a-tuned-build.md` and the verification document.
   The board: row 09, H1–H5, the gate "R", `### 5.10 After prompt 09 (<date>)`.

## 4. Stop conditions: stop and ask

- The lock is held, or a ChamPBH Ray process is running (rule 16: a stop, never a wait).
- A history does not converge, or `V_target ≤ 0` persists (the brief's §5).
- A stored value differs from the standalone tool's.
- The resumed store differs from the reference beyond what a resume may change.
- A defect in the wrapper or the driver (README §4: a lettered prompt fixes it, not this one).

## 5. What this prompt does not do

- It changes no code. It builds only the two stores of §1.1, under `var/datastores/`.
- It does not run `ray stop`, a production run, or a full surfing scan.
- **Files it changes:** `.documents/jordan-normalization-verification.md` and
  `.documents/jordan-normalization/out/` (new); the board; the log. Nothing under `var/` is
  committed.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only.
