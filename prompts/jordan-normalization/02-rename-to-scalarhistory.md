# Prompt 02 — rename to `ScalarHistory`

**Campaign:** [`README.md`](README.md) · **Board items:** **N1**–**N4** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-20, after the user's rulings (README §6: R9, "model" kept for the Lagrangian;
U1, the names). Its orchestrator note, written after 01's review, hands it 01's counts and governs
where it differs from this file.
**Gate:**
- 01 landed and reviewed, the gate "Strip" holding (board §2);
- on branch `jordan-normalization`, no tracked change in `git status`;
- no ChamPBH Ray process is running. You start none.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Sonnet**. A large mechanical rename, done with `git mv` and a reviewed
search-and-replace, with the golden history as the witness that nothing else changed.

**Read first:**

1. [`README.md`](README.md): §0.3 item 6 (what the rename touches), §2.2 (02's charter), §5, §6.0
   R9, §6.1 U1, §6.2 P2, P3.
2. `CLAUDE.md`: "Repository mechanics", in particular the paragraph on the rename of 2026-10-13
   (`Datastore/` into `ObjectFactories/`, `StoreFixtures/`, `tools/tests/`): campaign records and
   dated documents keep the old names.
3. 01's log and the golden history (`ComputeTargets/tests/test_golden_history.py`).
4. `config/datastore.py` (`factories`, `drop_groups`, `tables_to_drop`), `config/sharding.py`, and
   the specs that name `ScalarModel` as a `Parent` (`ObjectFactories/AdiabaticHistory.py`,
   `ObjectFactories/BBNData.py`).

---

## 1. What is wanted

Every current name that says `ScalarModel` says `ScalarHistory`, so that "model" is free for the
Lagrangian (04, 07). Nothing computed or stored changes, other than the table and column names.

### 1.1 The items

- **N1 — the code.** By `git mv` and then edit, so that `git log --follow` keeps each file's history:
  - `ComputeTargets/ScalarModel.py` → `ComputeTargets/ScalarHistory.py`;
    `ObjectFactories/ScalarModel.py` → `ObjectFactories/ScalarHistory.py`;
    `plot_ScalarModel.py` → `plot_ScalarHistory.py`;
    test modules whose names carry it (`test_scalarmodel_failure_reason.py` →
    `test_scalarhistory_failure_reason.py`, and any other);
  - classes and functions: `ScalarModel` → `ScalarHistory`, `ScalarModelValue` →
    `ScalarHistoryValue`, `ScalarModelProxy` → `ScalarHistoryProxy`, `compute_scalar_model` →
    `compute_scalar_history`, the factories `sqla_ScalarModel*` → `sqla_ScalarHistory*`, and
    every import, annotation, docstring and message that names them;
  - **the tables**: `ScalarModel`, `ScalarModel_tags`, `ScalarModelValue` → `ScalarHistory`,
    `ScalarHistory_tags`, `ScalarHistoryValue`, in the registry, the sharding configuration, the
    batch sizes, and the `Parent` references of the specs;
  - **the columns that point at a history**: `model_serial` on the value, tag, `AdiabaticHistory`
    and `BBNData` tables → `history_serial`, so that 07's `model_serial` can mean the Lagrangian's
    row. Every query that names them follows;
  - **the drop group** `scalar-model` → `scalar-history`; `--drop` accepts only the new name, and
    its help says so.
  - What does **not** change: `SampleValues`, `ModelFunctions` (07 or 04 may rename them; not
    here), the potential and coupling classes, `AdiabaticHistory`, `BBNData`.
- **N2 — fingerprint format 3.** Table names and `Parent` references move, so the specs' digests
  move. `FINGERPRINT_FORMAT` 3 with its history line, `SPEC_DIGESTS[3]` generated, and a new golden
  file.
- **N3 — the current documents.** `.documents/architecture-summary.md`,
  `.documents/numerical-strategies.md`, `CLAUDE.md` (its "Redshift" bullet names
  `ScalarModelValue`; its plot-scripts bullet names `plot_ScalarModel.py`), `prompts/INDEX.md` only
  where it describes the future. Not the campaign records, the dated verification documents, the
  audits or `science-runs/` (as 2026-10-13's rename did). Where a current document cites a record
  by its old name, leave the citation.
- **N4 — the run scripts are records.** `science-runs/2026-10-03-science-2026.6.0/*.sh` name
  `plot_ScalarModel.py` and `--drop scalar-model`; they are not edited. 10's handover says what
  changes in a new run script.

---

## 2. Verification

1. **Before any edit:** the five suites at the counts 01 handed on.
2. **After:**
   - the golden history passes **unchanged** (its JSON is not regenerated);
   - the five suites, with every count unchanged unless a test was renamed into another root (say
     so if one was);
   - `grep -rn "ScalarModel\|scalar_model\|scalar-model\|plot_ScalarModel"` outside `prompts/`,
     `science-runs/`, `.documents/` (bar N3's current documents) and the old golden files: nothing.
     List what remains in the excluded places by count, not by line;
   - `git log --follow --oneline -1` on each moved file reaches its pre-move history;
   - `PYTHONPATH=. ./venv/bin/python main.py --help` and `plot_ScalarHistory.py --help` exit 0;
   - `black --check` on every changed file.
3. **Deliberate breakages**, each failing a named test:
   - `--drop scalar-model` passed: refused (exit 2) with a message naming the new group;
   - one `Parent("model_serial", …)` left unrenamed: the format guard or the golden fingerprint
     fails.

## 3. Acceptance

1. N1–N4 as §1.1 says.
2. §2's checks in the log.
3. **One commit**, with the log `logs/02-rename-to-scalarhistory.md`. The board: row 02, N1–N4,
   the gate "Rename", `### 5.3 After prompt 02 (<date>)`.

## 4. Stop conditions: stop and ask

- The golden history changes.
- A name cannot be changed without changing what something computes or how a lookup matches.
- DatastoreKit requires a table or column name it does not accept (a package question, not a
  workaround).

## 5. What this prompt does not do

- No change to any computation, option (other than `--drop`'s group name), store or package.
- **Files it changes:** the moved files of N1 and every file that imports or names them; `config/`;
  the specs; the tests; `CLAUDE.md`, `.documents/architecture-summary.md`,
  `.documents/numerical-strategies.md`; the board; the log.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only (moves included), after showing
`git diff --cached --name-status`.
