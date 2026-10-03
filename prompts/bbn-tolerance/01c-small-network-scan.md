# Prompt 01c — Measure the small network

**Campaign:** [`README.md`](README.md) · **Board item:** **Q** · **Board:**
`IMPLEMENTATION_STATE.md`. Update your row and Q.
**Closes:** nothing. Adds a dated **Narrowed** line under
`[00-the-low-T-network-fails-near-1-keV-on-ulp-level-input]` with what the small network does on
the 11.
**Recommended model:** **Opus.** A scan with many cells, a breadth sample, a cost measurement,
and a recommendation the user rules on.

> Added 2026-10-03 by the user's ruling U3 (README §0.2). Prompt 01 found that the full network's
> low-T failures come from a PRyMordial lithium rate that no tolerance removes. The user ruled to
> re-plan around the small network, which does not contain that rate, and to measure it first.

**Read first:**

1. [`README.md`](README.md) §0.2 (U1, U3, P7, P9, P10–P12), §2 (a), (c), (c′), (d), §4, §5,
   §6.0, §6.1c.
2. [`logs/01-mechanism-and-tolerance-scan.md`](logs/01-mechanism-and-tolerance-scan.md) in full,
   and the probe scripts in `logs/01-probes/` you reuse. Log 01 is evidence, not instructions.
3. `tools/bbn_from_store.py`: its command line, `--small-network`, `lowT_tolerance_override`, and
   `connect_ro`.
4. `ComputeTargets/tests/test_prym_passenger.py` (c) and `test_network_flag.py` (b), for the
   pinned small-network values.

---

## 1. What this prompt does

It measures; it changes nothing (P10).
- No production code, nothing in `PRyM/`, no write to any store.
- **The tool is used as prompt 01 left it.** If a measurement needs a change to
  `tools/bbn_from_store.py`, stop and ask.
- Probe scripts go in `logs/01c-probes/`. A script from `logs/01-probes/` that you adapt is
  copied there, not edited in place.

## 2. Reproduce first

Run the four inputs of log 01's S3, `prod`, small network, at all five `rtol`. These are the SM
baseline, the control β = 1.6, M = 10⁻³, β = 1.6, M = 10⁻⁵ and β = 2.4, M = 10⁻⁵. Each must equal
its row in `logs/01-probes/scan.csv` **to every printed digit**. These runs are T1's own rows for
those cells; they are not run twice. **If any differs, stop.**

## 3. Measure

- **T1** (README §2 (c′)): the small network at the five `rtol` on the 16 histories × 3 variants
  plus the SM baseline, 245 solves. Run 8–10 at a time, into `logs/01c-probes/scan.csv`, with the
  columns of log 01's `scan.csv` and a `block` column.
- **Choose T2's setting by P11 from T1.** Use criteria 1–3, and criterion 4 from a provisional
  serial timing if one is needed to separate two candidates. Say in the log which setting and why.
- **T2**, the breadth sample, `prod` only, at that setting:
  - every 10th φ\* = 5 history in (M, β) order;
  - every history whose φ\* ≠ 5.

  Enumerate the histories with a read-only probe through the tool's `connect_ro`, and save the
  list in `logs/01c-probes/breadth.csv` so that it can be re-run. Say how many histories each
  part gave.
- **Failures.** Any failure, in T1 or T2, is recorded with its stage, `t reached / t target` and
  reason. Finish the grid, then stop (README §4). For a failure, say whether it is in the low-T
  stage and whether the step-size history resembles log 01's mechanism. Instrument it as log 01
  did only if that takes no change to the tool.
- **Cost** (README §2 (d)): serial, after T1 and T2 have finished, three repeats, medians. Cover
  the small network at each T1 setting and the **full network at the default**, on the SM baseline
  and the control. Record the load average as log 01 did.
- **The offset from the full network** (P12). For each input common to T1 and log 01's S1, at
  `rtol` 1e-5, 1e-6 and 1e-8, give the relative difference small − full in Yp and D/H. Also give
  the median and the maximum over inputs. This is a measurement. Do not bound it, and do not
  open an issue for it.
- **The pinned small-network values** (P7) at the recommended setting:
  - `test_prym_passenger`'s `CONST_HONLY_SMALL_*` (bound 1e-6);
  - `test_network_flag (b)`'s ⁷Li/H shift between the networks (≥ 5e-3).

  Quote the pinned value, the value now, the bound, and whether it would pass. Re-derive the
  constants on `7b518c9` in a temporary worktree, as log 01 item 11 did, and remove the worktree
  afterwards.

## 4. The recommendation

Apply P11 and name **one** small-network setting: low-T `rtol`, with `atol` 1e-11. Give a table
with each criterion's value at every T1 setting. If none meets P11, say which criterion fails
where, name the setting that meets 1 and 4 and comes closest on 2 and 3, and **stop**: the user
rules.

## 5. What this prompt does not do

- It changes no code. It does not change the tool or `PRyM/`, and it does not run `main.py`.
- It does not choose how production selects the network, or anything else in P13. Those are
  ruled with this log, and written into the rewritten prompt 02.
- It does not bound the small network's offset from the full one, or its scatter (P9, P12).
- It does not measure the full network further, beyond the cost reference.

## 6. Stop conditions — stop and ask the user

- §2's reproduction fails.
- Any small-network solve fails. Finish the grid first, so the user has the whole picture.
- No setting meets P11.
- A measurement needs a change to `tools/bbn_from_store.py`, a production file, or `PRyM/`.

## 7. The log, the board and the index

- `logs/01c-small-network-scan.md`, in the README §5.1 template. Probes and CSVs go in
  `logs/01c-probes/`.
- The board:
  - your row in §1, and item Q;
  - under `[00-the-low-T-network-fails-near-1-keV-on-ulp-level-input]` (§3), a dated
    **Narrowed (2026-…, prompt 01c)** line: what the small network does on the 11 and on the
    breadth sample.
- The index: that row's hook, and the date. Add a row for any issue you open.
- Suites before and after: unchanged at 18, 106 and 31 (no code changes).

**Allowed files:**
- `prompts/bbn-tolerance/logs/` (the log and `01c-probes/`);
- `prompts/bbn-tolerance/IMPLEMENTATION_STATE.md`;
- `.documents/OPEN_ISSUES.md`.
