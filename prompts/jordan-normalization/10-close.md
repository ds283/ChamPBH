# Prompt 10 — close

**Campaign:** [`README.md`](README.md) · **Board items:** **C1**–**C5** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-20, after the user's rulings (README §6: R5, the science rebuild is the user's;
P15, P19). Its orchestrator note, written after 09's review, governs where it differs from this
file.
**Gate:**
- 09 landed and reviewed, the gate "R" holding;
- on branch `jordan-normalization`, no tracked change in `git status`;
- no ChamPBH Ray process is running. You start none.

**Closes:** nothing (every assigned issue was closed by its prompt; check that none remains in
`.documents/OPEN_ISSUES.md` §1.15). **Opens:** only what the work finds.
**Recommended model:** **Sonnet**. Documents, a handover and the indexes, from the logs.

**Read first:**

1. [`README.md`](README.md): §1, §2.2 (10's charter), §7.
2. Every log of this campaign (`logs/01`–`09`) and its board, and
   `.documents/jordan-normalization-verification.md`.
3. `.documents/architecture-summary.md`, `.documents/numerical-strategies.md`, `CLAUDE.md`.
4. `.documents/review-remediation-verification.md` §4.12–§4.15 (the earlier handovers: what in the
   run scripts changes, the registered launch, the held rebuild).
5. `science-runs/2026-10-03-science-2026.6.0/full_run_2026.6.0.sh` and `-refresh.sh` (records: read,
   not edited), and `science-runs/README.md`.

---

## 1. What is wanted

The repository says what the campaign changed, and the user can run the science rebuild from one
section without reading the campaign.

### 1.1 The items

- **C1 — the documents.** `architecture-summary.md` and `numerical-strategies.md`: the model
  interface (`AbstractModel`, `ExponentialModel`, `RealizedModel`, A(φ_ref) = 1 in the base class,
  the normalization the model interprets); the two layers (`integrate_history`, `tune_history`);
  the conditions and the secant with its guards; the seed; the stored tuning outputs; the sampling
  axis (ln(1 + z_E), the grid, ln(1 + z_J)); the Planck mass. And the brief's R6 audit of the
  documents: every statement that Planck units "scale out" M_P, that A(0) = 1, or that z is stored,
  corrected where it describes the present and left where it is a dated record.
- **C2 — `CLAUDE.md`.** The "Units" and "Redshift" bullets as 03 and 05 left them, checked; the
  names after 02; the plot-scripts bullet; the suite counts; anything a log listed as made false
  and not yet fixed.
- **C3 — the handover**, a dated addendum to `.documents/review-remediation-verification.md`,
  §4.16, in the form of §4.12–§4.15:
  1. **The rebuild is unheld** (the user's ruling of 2026-10-13 waited on this campaign).
  2. **The run scripts' changes**, block by block against `full_run_2026.6.0.sh`: the options that
     are gone (`--Lambda-values-eV`, `--potential-type`, `--T-stop-GeV`), the new ones and their
     defaults, `--drop scalar-history`, `plot_ScalarHistory.py`, every block through the wrapper
     with `--campaign jordan-normalization --prompt 10 --register <job>`, and stores under
     `var/datastores/`. A new folder under `science-runs/` holds the new scripts when the user
     writes them; 10 does not write them.
  3. **The cost**, from README §0.3 item 7 and 09's measurements: per block, and the slowest
     histories; the heartbeat's longest silence and what `list --stale-after` to use.
  4. **The surfing scans** (U4): the S1 and S5 commands on the new tool, their predicted times from
     08, and that their committed outputs are records of the old normalization.
  5. **The RayPoolKit wait** (P19), restated with 09's measurements, for the user to rule upstream.
  6. **What this campaign deferred** (README §7): other models; per-history surfing columns; the
     deceleration parameter as a test.
- **C4 — the board closed**: every row landed, the gates, §3 and §4 as they stand, a `### 5.11
  After prompt 10 (<date>)` and a closing paragraph; `prompts/INDEX.md`'s row marked complete with
  its commits; `.documents/OPEN_ISSUES.md`'s header and §1.15–§1.16.
- **C5 — the final suites**, all five, with their counts, which `CLAUDE.md` then states.

---

## 2. Verification

1. The five suites at the counts 09 handed on, and both golden histories.
2. Every "made false" line in logs 01–09 is either fixed or named in the log as left, with why.
3. Every command in C3 runs as written: `--help` for each, and `main.py --inventory` on 09's
   reference store (no Ray).

## 3. Acceptance

1. C1–C5 as §1.1 says.
2. **One commit**, with the log `logs/10-close.md`. The merge into `main` is the user's (P15).

## 4. Stop conditions: stop and ask

- An assigned issue is still open in §1.15.
- A log's "made false" line cannot be fixed without a code change.

## 5. What this prompt does not do

- It changes no code, builds no store, and runs no surfing scan or production run.
- It does not edit `science-runs/`'s records or write the new run scripts.
- **Files it changes:** `.documents/architecture-summary.md`, `.documents/numerical-strategies.md`,
  `.documents/review-remediation-verification.md` (the addendum only), `CLAUDE.md`, the board,
  `prompts/INDEX.md`, `.documents/OPEN_ISSUES.md`, the log.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only.
