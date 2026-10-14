# Log — surfing-temperature 05: close

**Subject:** Z1–Z4, the documents, the handover and the records that close the campaign. **Commit:**
this commit · **Date:** 2026-10-14 · **Branch:** `surfing-temperature`, parent `2958234` ·
**Model:** Claude Sonnet 5.5 · **Result:** landed. No stop condition fired. No Python changed.

## What shipped

- **Z1, `.documents/architecture-summary.md`** (36 lines added, none removed):
  - a `tools/` block in the §2 tree, between `PRyM/` and `config/`: `surfing_scan.py` and
    `plot_surfing_temperature.py`, each with a comment, and a `...` line (note 05 §0.4);
  - a dated note, "Note, 2026-10-14 (`surfing-temperature`, prompts 01 to 04b)", after the
    `read-only-plots` note and before the section's `---`: `find_kicks` and `reachability_roots` (they
    read the integrator's own w), `surfing_turning_points` (production does not call it), and the two
    tools (neither starts Ray or opens a store);
  - a short dated note in §7.2, after the 2026-10-02 `ScalarModel` note and before
    `**ScalarModelProxy:**`: `surfing_turning_points` walks the interpolants `first_bounce` walks, and
    T_J turns where β π/M_P = −1, from the temperature law's positive denominator.
- **Z2, `CLAUDE.md`:** three counts, ComputeTargets 152 → 176, its second half 91 → 115, and tools
  34 → 70. The first half (61), CosmologyModels (18), ObjectFactories (69) and the line "(The counts are
  those of 2026-10-14.)" are as they were.
- **Z3, `.documents/review-remediation-verification.md` §4.14**, after §4.13's last paragraph and
  before the `---` (91 lines added): the opening paragraph, "This supersedes" (nothing), and six
  numbered points: where the results are; the re-run commands; what `jordan-normalization` would move;
  that per-history values are not stored; the suites; the one open issue.
- **Z4:** the board (status line COMPLETE, row 05, Z1–Z4 ✅, a new §5.6); `prompts/INDEX.md` (the
  `surfing-temperature` row, and the header count 9 campaigns: 0 planned, 0 live, 9 closed);
  `.documents/OPEN_ISSUES.md`, **narrative only** (the header sentence and §1.13's first line). Its
  count, 27, and its date are unchanged, and the issue `[05-surfing-scan-runs-at-a-fixed-lambda]` is
  neither re-opened nor closed (note 05 §0.1).
- **This log.**

## Deviations from the prompt

The note's §0 (U9, the qualified capture claim, the re-run command naming a new file, the
placements, the counts) is the brief these were measured against. Differences from the prompt that
the note itself rules are not listed.

| # | Deviation | Tag |
|---|---|---|
| D1 | The note says §4.14 states that the re-run "runs at Λ = 1 meV and φ_ref = 0, whatever the tree". The board's issue says "φ_ref does not exist in the code". I wrote "it runs at Λ = 1 meV, whatever the tree. … it has no φ_ref", so as not to assert a value of a quantity the tool does not have. | IMPLEMENTATION CHOICE |
| D2 | §4.14 has six points. Z3 lists the points to say; I added the suites (point 5) as a separate point, and made the open issue point 6, since the note says §4.14 names it. | IMPLEMENTATION CHOICE |
| D3 | The tree's `tools/` block is introduced in the dated note by a sentence saying that it lists only the two new files and that the directory holds more. The prompt asks for the comments in the tree, not for that sentence. | IMPLEMENTATION CHOICE |
| D4 | §4.14's "written at" SHA is `2958234`, the parent, which was `HEAD` when it was written. | IMPLEMENTATION CHOICE |

There is no UNINTENDED DRIFT. (A first `sed -i -e` on `CLAUDE.md` failed under macOS's `sed` and
changed nothing; the second, with `-i ''`, made the three edits above.)

## Verification performed

**The final suites** (prompt §2 item 1), serially, in the foreground, each output in the scratch
directory `…/scratchpad/agent05/` (`cm.out`, `ca.out`, `cb.out`, `of.out`, `tl.out`). `uptime` is the
load averages (1 / 5 / 15 min) at each run's start:

| Suite | Result | Wall | Load at start |
|---|---|---|---|
| CosmologyModels | 18 OK | 79 s (unittest: 77.4 s) | 14.95 / 8.97 / 7.69 |
| ComputeTargets `test_[a-f]*.py` | 61 OK | 116 s (unittest: 113.3 s) | 5.20 / 7.27 / 7.17 |
| ComputeTargets `test_[g-z]*.py` | 115 OK | 123 s (unittest: 120.7 s) | 2.29 / 5.54 / 6.50 |
| ObjectFactories | 69 OK | 7 s (unittest: 5.7 s) | 2.54 / 4.57 / 5.97 |
| tools | 70 OK | 14 s (unittest: 12.5 s) | 2.41 / 4.51 / 5.95 |

These are the board's counts (18 / 61 + 115 / 69 / 70) and the orchestrator's baselines. No count
fell, no test failed, and nothing was re-run. ComputeTargets took about 239 s in two halves, so
`CLAUDE.md`'s "about 200 s" stands as written.

**The handover's command, read and not run** (prompt §2 item 2):
- the flags in §4.14 point 2 (`--beta-min`, `--beta-max`, `--beta-step`, `--phi-init-Mp`, `--M`,
  `--out` for the scan, and `--csv`, `--out` for the plot) are the `add_argument` names in
  `tools/surfing_scan.py` and `tools/plot_surfing_temperature.py` (`grep`);
- its options, β = 1.10–5.00 by 0.05, φ* and M, with `--T-stop-GeV` left at its default, are those
  of the sidecar `surfing_scan_phi5_M1e-3.csv.meta.json` (`beta_min` 1.1, `beta_max` 5.0,
  `beta_step` 0.05, `phi_init_Mp` 5.0, `M` 0.001, `T_stop_GeV` 0.001); the fine scan's are
  verification §1.1's S5 command;
- the output file's name carries the options, as the committed names do, so the scan neither skips
  every β nor exits 2 on `_INPUT_MATCH` (`tools/surfing_scan.py:155`), which includes `Lambda_eV`;
- `LAMBDA_EV` is imported from `tools/history_and_bbn.py` (`tools/surfing_scan.py:106-112`, `:213`)
  and no option sets it.

**Numbers in the records, each against its source:**
- the commits: 01 `2c98370`, 02 `f166b31`, 03 `1f40b53`, 03b `ee4a2df`, 04 `90d44f8`, 04b `a65bd74`,
  the plan `8adb806`, U9 `29e34db`: board §1 and note 05 §0.6;
- the costs, 287 s (coarse scan), 155 s (fine scan), 52 s and 4 s (figures): verification §1.1;
- QCD captures to β = 2.998, EW from 3.020, nothing at 2.999–3.019: verification §1.8 and §1.3;
- the merge of QCD and μ/π above β = 2.154: README §0.3;
- the suites, 18 / 176 / 69 / 70: the table above;
- the index's count, 27, and date: `.documents/OPEN_ISSUES.md` header, unchanged.

**Scope** (prompt §2 item 3), `git diff --stat` of the commit, the files below and nothing else:
- `.documents/architecture-summary.md`, `CLAUDE.md`, `.documents/review-remediation-verification.md`,
  `prompts/INDEX.md`, `.documents/OPEN_ISSUES.md`, `prompts/surfing-temperature/IMPLEMENTATION_STATE.md`,
  and this log.

**The dated blocks:** `git diff .documents/architecture-summary.md` has no `-` line (36 insertions,
0 deletions), so every existing `> **Note, …**` block is as it was. `git diff` of
`.documents/review-remediation-verification.md` has no `-` line: §4.1–§4.13 and §5 are untouched.
`.documents/OPEN_ISSUES.md`'s diff changes two sentences of narrative and nothing in its header
count or date.

## The deliberate-breakage record

None. The prompt has no tests to break; it changes no code.

## Observations not acted on

- **`prompts/INDEX.md`'s `read-only-plots` row** still says "not yet merged into `main`", which has
  been stale since `5740690` (note 05 §0.6). It is not 05's to fix.
- **No statement of `CLAUDE.md` other than the counts was made false** by the campaign. Its "Repository
  mechanics" says nothing of `tools/surfing_scan.py` or the plot, and it needs nothing.
- The CosmologyModels run started at load 14.95, above the orchestrator's baseline load (5.6) and
  took 79 s against its 83 s; nothing else was affected.

## State handed to the next prompt

- **The branch.** `surfing-temperature`, this commit on top of `2958234`. Nothing is staged or
  modified after it. The untracked entries are the user's, as before: 17 `pilot-2026.6.0*.db`,
  `pilot-main.log`, `pilot-plot.log`, `pilot-out/`, `full_run_2026.6.0.sh`,
  `full_run_2026.6.0-refresh.sh`, `prompts/jordan-normalization/`.
- **The campaign is closed on its board** (COMPLETE, 7 of 7 landed). The branch is **not merged into
  `main`**: that is after this prompt's review (P10), by the orchestrator with the user's go-ahead.
- **Counts at the close:** CosmologyModels 18, ComputeTargets 176 (61 + 115), ObjectFactories 69,
  tools 70. `CLAUDE.md` says so.
- **One issue is open:** `[05-surfing-scan-runs-at-a-fixed-lambda]`, on the board's §3 and in
  `.documents/OPEN_ISSUES.md` §1.13; unassigned (`jordan-normalization` has no board yet). The index
  count is 27 and its date is unchanged.
- **The handover** is `.documents/review-remediation-verification.md` §4.14; its re-run commands
  name a new output file, and run at Λ = 1 meV until the tool is changed.
- **For the review (E1–E4):**
  - the notes table on the board is the orchestrator's, and has no row for note 05;
  - the SHA "this commit" appears in the board's status line and row 05, §5.6 and this log, and is
    the orchestrator's to substitute;
  - `INDEX.md`'s `surfing-temperature` row does not carry the commit of 05 (it says "05, the
    documents and the handover").
