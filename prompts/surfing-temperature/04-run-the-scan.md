# Prompt 04 — run the scan

**Campaign:** [`README.md`](README.md) · **Board items:** **S1**–**S4** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-14, with 01–03 and 05, after the user's rulings. Its orchestrator note, written
after 03's review, hands it 02's cost projection and the chunk plan. It governs where it differs
from this file.
**Gate:**
- 01, 02 and 03 landed and reviewed, with the gates "Roots" and "Turning points" holding;
- 02's projection of the full scan at or below P7's 60 minutes (board §5.3). If it is above, this
  prompt is not dispatched without the user's ruling;
- on branch `surfing-temperature`, no tracked change in `git status` (the untracked set is 01's gate
  list);
- you start no Ray, and open no store.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Opus**. A long run that must be resumable and reproducible, and a
verification document that reports measurements without judging them against the paper.

**This prompt runs the scan as a one-off exception** to `CLAUDE.md`'s "nothing a prompt runs should
take longer than a few PRyMordial solves", ruled by the user (U4, README §5 rule 12). It runs no
PRyMordial, and no Ray.

**Read first:**

1. [`README.md`](README.md): §0.2, §0.4, §2.2 (04's charter), §4, §5 (rules 10 and 12), §6.
2. Logs 01–03: "What shipped", "Observations not acted on" and "State handed to the next prompt".
   Log 02's T5 table and projection in particular.
3. `tools/surfing_scan.py` and `tools/plot_surfing_temperature.py`: their options, the CSV, the
   sidecar and resume.
4. `CLAUDE.md`: "Repository mechanics", in particular run from the root, the ten-minute Bash cap, and
   "Long-running jobs".

---

## 1. What is wanted

The scan the user asked for, its figure, and a verification document that says what was measured
and how.

### 1.1 The items

- **S1 — the scan.**
  - **The command:** `tools/surfing_scan.py --beta-min 1.10 --beta-max 5.00 --beta-step 0.05`
    with `--phi-init-Mp 5`, `--M 1e-3` and the default `--T-stop-GeV`. That is 79 values.
  - **The output:** `--out .documents/surfing-temperature/out/surfing_scan_phi5_M1e-3.csv` (P8).
  - **Where and how.** From the repository root, in the foreground, in chunks of β (by
    `--beta-values`, or by sub-ranges of the same grid).
    - Each chunk is sized from 02's per-history times to finish within about eight minutes, under
      the ten-minute Bash cap.
    - The CSV is resumable, so a chunk the cap cuts off loses only the history in progress. Re-run
      the chunk; its finished rows are skipped.
    - Record the load (`uptime`) at the start of each chunk.
  - **A history that alone cannot finish within a chunk** is a stop (§4). Do not raise the step
    budget, change the step control, or skip the β.
- **S2 — the figure.** `tools/plot_surfing_temperature.py --csv <the CSV> --out
  .documents/surfing-temperature/out/surfing_temperature_phi5_M1e-3`. Look at the PNG. If something
  in it is hard to read, record it in the log; a change to the plot script is out of scope here.
- **S3 — reproducibility.**
  - Re-run two β, 1.50 and 4.00, into a scratch CSV.
  - Every column but `wall_s` must equal the scan's row **as a string**.
  - A difference is a stop: the histories are expected to be bitwise reproducible, as R0 was
    (`dk-and-rpk-migration`).
- **S4 — `.documents/surfing-temperature-verification.md`**, created, with a §1 in the additive form
  of the other verification documents:
  1. **The tree and the run.** `HEAD` (from the sidecar) and the command lines. The chunk plan and
     each chunk's wall time and load. The total.
  2. **Every failure**, with its reason, and every row with a flag: no bounce above T_stop, or
     `multiple_crossings` from 01.
  3. **The dynamical thresholds.** For each kick, the smallest β on the grid whose first episode it
     captures, beside 01's analytic threshold. The paper says the dynamical thresholds lie "slightly
     above" the analytic ones. Report the difference; do not test it.
  4. **The measured temperatures against β**, summarised:
     - the range of each of the four ratios T/T_hot per kick;
     - where a second dip first appears and where it stops appearing;
     - any β at which a series jumps. A jump is reported with the rows either side, not explained
       away.
  5. **Comparisons, as measurements** (README §5 rule 10):
     - the overshoot T_max/T_capture against the integrator audit's 12–13 % (F1);
     - where an EW capture has several turning points, the e-fold spacing and decay of successive
       extrema against `eq:capture-oscillator`'s prediction (decay e^(−N/2)).

     A difference is reported, and opened as an issue only if it shows a defect of the code.
  6. **The figure**, linked (the PNG), with a two-line description of what each panel shows.
  7. **What would move after `jordan-normalization`**, restated from README §0.4, with this run's
     Λ.

---

## 2. Verification

1. **Before the scan:** the four roots at the board's counts, serially, with the load. They are
   re-run after only if this prompt changes code. It should not.
2. **The CSV:**
   - 79 data rows and one header;
   - the β column equal to the grid as strings;
   - no duplicate β;
   - the sidecar's options equal to the command's.

   Show the check's command and output in the log.
3. **S3's comparison**, its command and output.
4. **The outputs' sizes.** CSV, sidecar, PDF and PNG together are expected under 1 MB. If they are
   larger, say so before committing.

## 3. Acceptance

1. S1–S4 as §1.1 says.
2. §2's checks, each with its command and output in the log.
3. Suites unchanged from 03's.
4. **One commit** containing:
   - the four outputs under `.documents/surfing-temperature/out/`;
   - the verification document;
   - the log `logs/04-run-the-scan.md` in the template of `dk-and-rpk-migration` README §5.1;
   - the board: row 04, S1–S4, the gate "Scan", and a new `### 5.5 After prompt 04 (<date>)`.

   Write "this commit" where the SHA goes. S3's scratch CSV is not committed.

## 4. Stop conditions: stop and ask

- **The scan's actual time exceeds 60 minutes** (P7) before it is finished: stop between chunks,
  with the rows so far kept.
- **One history cannot finish inside a chunk.**
- **A history has a captured first episode and no bounce above T_stop.**
- **S3 finds a difference.**
- **More than five histories fail.** Fewer is recorded, not a stop.
- Anything would need a change to code, or an edit outside §5's list.

## 5. What this prompt does not do

- **Files it changes:**
  - the four new files under `.documents/surfing-temperature/out/`;
  - `.documents/surfing-temperature-verification.md`;
  - this board, and the log.
- It changes no code: not the tools, not `extract_common.py`, not `ScalarModel.py`.
- It runs no PRyMordial, starts no Ray, and opens no store. It runs only the scan, S3's two
  histories and the plot.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
