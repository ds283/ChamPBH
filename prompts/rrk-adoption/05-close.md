# Prompt 05 — close

**Campaign:** [`README.md`](README.md) · **Board items:** **Z1**–**Z5** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-19, with 01–04, after the user's rulings. Its orchestrator note, written after
04's review, hands it the counts and findings of 01–04, and governs where it differs from this file.
**Gate:**
- 01–04 landed and reviewed, with the gates "Pin", "Fingerprint", "Wrapper" and "R" holding
  (board §2);
- on branch `rrk-adoption`, no tracked change in `git status`;
- no ChamPBH Ray process is running, and `./venv/bin/python -m runregistrykit list` shows nothing
  `running`, stale or unknown. You start nothing.

**Closes:** the campaign; no §3 issue. **Opens:** what 01–04's logs left as observations, if the
orchestrator note says to open them.
**Recommended model:** **Sonnet**. Prose and records, two drafts for another repository, and one
final measurement.

**Read first:**

1. [`README.md`](README.md): §1, §2.2 (05's charter), §5, §6.0, §6.2 (P12), §7.
2. This board in full; logs 01–04: "What shipped", "Observations not acted on" and "State handed
   to the next prompt".
3. `.documents/rrk-adoption-verification.md` §1, which 04 created.
4. `.documents/architecture-summary.md`:
   - the tree (§2);
   - §4.1, the paragraph at `:294-299` on the `v0.4.0` pin, which says the fingerprint module "is
     there for the RunRegistry component";
   - §9, "The Main Pipeline (`main.py`)";
   - §10, "Utilities".
5. `.documents/review-remediation-verification.md` §4.12–§4.14 (the pattern; §4.12 point 3 lists the
   run scripts' lines).
6. RunRegistryKit at `v0.2.0` (read, never edit):
   - `prompts/extraction/IMPLEMENTATION_STATE.md` §2, row **G3**;
   - `prompts/interrupted-runs/IMPLEMENTATION_STATE.md` §2, row **G2**;
   - the rows **G2** and **G1** above each, SecondaryGWKit's, for the form;
   - `docs/adoption/champbh.md` item 9, the acceptance a G3 record quotes.
7. `.documents/dk-and-rpk-migration-verification.md` §4.3–§4.4: how this repository drafted two G3
   records for its packages' boards.
8. `CLAUDE.md`; [`prompts/INDEX.md`](../INDEX.md); [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md).
9. The user's run scripts, `full_run_2026.6.0.sh` and `full_run_2026.6.0-refresh.sh` (untracked):
   read them; never edit, copy or run them.

---

## 1. What is wanted

The repository says what the campaign changed. The user's next long run, `jordan-normalization`'s
rebuild among them, knows how to launch through the registry and what to do after a hard kill.
RunRegistryKit's two gates have their records drafted for the user to commit there.

### 1.1 The items

- **Z1 — `.documents/architecture-summary.md`.** This file describes the code. It is not a dated
  record, but its existing `> **Note, …**` blocks are dated records, and are kept byte for byte.
  - The tree gains `RunDrivers/` (the wrapper and its tests), `runregistrykit.toml`,
    `config/fingerprint.py` and `var/` (ignored: `var/runs/`, `var/datastores/`), each with a
    comment.
  - A dated note, in the form of the existing ones, says:
    - what the registry records and where;
    - that a registered run is launched through `RunDrivers/registered_run.py`, and `main.py`
      stays launchable without it;
    - what the heartbeat means;
    - the fingerprint format and its guard;
    - that the store's sidecar is made by a person after its first close.
  - The paragraph at `:294-299` is descriptive prose, not a note. Its last sentence becomes true of
    the tree: the fingerprint module is used, through RunRegistryKit's store layer.
- **Z2 — `CLAUDE.md`.**
  - The section 01 wrote names the wrapper definitely, with its usage line.
  - The test commands' counts are 04's final ones.
  - Nothing else changes.
- **Z3 — the handover**, as `.documents/review-remediation-verification.md` **§4.15**, after §4.14,
  in its form. It says:
  - what the campaign did, and what it supersedes by statement: §4.12's and later sections' lines on
    where stores live and how runs are launched;
  - **the run scripts' changes**, by line of each script (README §7):
    - `STORE_DIR` becomes `var/datastores`;
    - each block runs through the wrapper, with its four flags and the `--job-name` as slug;
    - each block runs detached, through `sh` as 04 launched them;
    - `store create --purpose …` after the first block closes;
    - after a hard kill, `python -m runregistrykit end RUN_ID --reason …`, then the block again with
      `--prune-unvalidated`. A `SIGTERM`ed block needs no `end`: it is recorded `killed`;
    - the plot loop and `--inventory` stay unregistered;
  - **the root's untracked files** and the old stores, which the user removes so that `git_dirty`
    means something (R5, R6), and 04's stores and runs under `var/`, kept until the user removes
    them;
  - **the production machine**: pull, reinstall the venv from `requirements.txt`, and run `list` at
    the start of a session there;
  - that `jordan-normalization` comes next (R7), and that its rebuild is the first production run
    through the wrapper.
- **Z4 — RunRegistryKit's gate records, drafted (P12).** A new §2 of
  `.documents/rrk-adoption-verification.md`, in the form of `dk-and-rpk-migration-verification.md`
  §4.3–§4.4. It holds two drafts, each a table row's text ready to paste, naming this repository's
  commits:
  - **`extraction` board, G3**: ChamPBH adopted a release, `v0.2.0` directly. It quotes the
    checklist's item 9, point by point, with where each holds: the pin and the venv (01); the
    config and `var/` (01); one `begin` … `finish` run listed with its `packages` (04); the helper
    not substituted (U1: the checklist's point 4 does not apply, and why); the store layer, with a
    fingerprinter that loads and `store fingerprint` reading a closed store (02, 04);
  - **`interrupted-runs` board, G2**: ChamPBH adopted `v0.2.0`, its pin moved in a commit of its own
    (01's). It says which of the `v0.2.0` note's §3 checks applied (none of ChamPBH's code predates
    the adoption), and that the hard-kill route was rehearsed (04, H3).

  **Nothing is written in RunRegistryKit.** The user commits the records there.
- **Z5 — the records.**
  - **This board closes.** The status line says **COMPLETE**, with the final suites. Every §1 row is
    ✅; Z1–Z5; §2's gates, with RunRegistryKit's left "drafted, the user's"; §3 and §4 as they
    stand. A final `### 5.6 After prompt 05 (<date>)` lists what is left to the user:
    - the merge into `main` (P10);
    - the two gate records (Z4);
    - the run scripts (Z3);
    - the files at the root, the old stores, and 04's stores and runs;
    - the docstring of `ObjectFactories/tests/test_inventory_specs.py:25-29`, which still says
      ChamPBH "is expected to adopt" RunRegistryKit. Existing tests are not modified (README §5),
      so it is recorded here for the user, not edited.
  - **`prompts/INDEX.md`**: the row **complete**, in `read-only-plots`' form, with the date, the
    final suites and the open-issue column from counting; the header's count and date.
  - **`.documents/OPEN_ISSUES.md`**: the header's description of this campaign (closed; what it
    opened and closed), and the count by counting.

---

## 2. Verification

1. **The suites**, all five roots, serially, before and after: the counts 04 handed on.
2. **The documents are additive** where they are records. `git diff --numstat` on `.documents/`
   shows no deletions but in `architecture-summary.md`'s descriptive lines and in
   `OPEN_ISSUES.md`'s header. Extract every `> **Note, …**` block of `architecture-summary.md` at
   `HEAD` and after your edits, and `diff` them: identical but for the note Z1 adds.
3. **Every citation exists.** Every path, function, option, run id and line that Z1–Z4 name exists
   in the tree, in `var/runs/`, in the run scripts or in RunRegistryKit at `v0.2.0`. Give the list
   in the log.
4. **The index.** `.documents/OPEN_ISSUES.md`'s count equals its rows by counting, and
   `prompts/INDEX.md` agrees with this board.
5. `./venv/bin/python -m runregistrykit list`: the four runs of 04, as 04 recorded them, and nothing
   new.

## 3. Acceptance

1. Z1–Z5 as §1.1 says, and §2 in the log.
2. **No deliberate breakage**: 05 adds no code and no test.
3. **One commit**, with the log `logs/05-close.md` in the template of `dk-and-rpk-migration`
   README §5.1. Write "this commit" where the SHA goes.

## 4. Stop conditions: stop and ask

- A gate does not hold when you start.
- A sentence of a dated record would need rewriting rather than adding to.
- A count moves.
- A draft of Z4 cannot be written true: a point of the checklist's item 9 does not hold and is not
  explained by a ruling.
- Anything would need an edit to the run scripts, to RunRegistryKit, or outside §5.

## 5. What this prompt does not do

- **Files it changes:**
  - `.documents/architecture-summary.md`;
  - `CLAUDE.md`;
  - `.documents/review-remediation-verification.md` (§4.15 appended);
  - `.documents/rrk-adoption-verification.md` (§2 added);
  - `.documents/OPEN_ISSUES.md`;
  - `prompts/INDEX.md`;
  - this board, and the log.
- No code, no test, no script, no store, no run. It does not merge the branch, and writes nothing
  in another repository.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
