# Prompt 02b — `tools/history_and_bbn.py` runs production's network

**Campaign:** [`README.md`](README.md) · **Board item:** **H** · **Board:**
`IMPLEMENTATION_STATE.md`. Update your row and H.
**Closes:** `[02-two-places-still-say-production-runs-the-full-network]`.
**Recommended model:** **Sonnet.** One default, its help text, a comment, and a test extended.

> Added 2026-10-03 by the user's ruling U5 (README §0.2), on log 02's Observations 1. Prompt 02
> moved production to PRyMordial's small network and made the other production defaults follow
> `main.py` (P15). `tools/history_and_bbn.py`, which runs one history and its BBN as production
> does, was not in prompt 02's allowed files and still defaults to the full network.

**Precondition (the orchestrator checks it):** prompt 02 has landed, and the board's Decisions
record U5.

**Read first:**

1. [`README.md`](README.md) §0.2 (U4, P15, U5), §5, §6.2b.
2. `IMPLEMENTATION_STATE.md`: Decisions U5, and the §3 entry
   `[02-two-places-still-say-production-runs-the-full-network]`.
3. `tools/history_and_bbn.py`: the `--small-network` argument (`:254–259` on `5a72871`) and where
   it is used (`:367`, `:372`).
4. `tools/bbn_baseline.py`: its `--small-network` argument, which prompt 02 changed; the pattern
   to follow.
5. `ComputeTargets/BBNData.py`: the comment in `_configure_PRyMordial` above
   `PRyMini.smallnet_flag = small_network` (`:255–259` on `5a72871`).
6. `ComputeTargets/tests/test_network_flag.py`: `test_c_production_defaults_are_the_small_network`,
   and how it reads `tools/bbn_baseline.py`'s default from the source.

---

## 1. The changes

- **`tools/history_and_bbn.py`.** The `--small-network` argument's `default` becomes `True`. Its
  help text says the default is the small network, as `main.py` runs, and that
  `--no-small-network` selects the full network, in the words prompt 02 used for
  `tools/bbn_baseline.py`. Nothing else in the tool changes.
- **`ComputeTargets/BBNData.py`, `_configure_PRyMordial`.** The comment that says "False
  (production) runs the full network" is corrected: `True` (production since bbn-tolerance
  prompt 02) runs the small network, `False` the full one. Keep the rest of the comment, including
  the production-readiness history. **Comment text only**; no code in this file changes.
- Nothing else. In particular, not `tools/bbn_from_store.py`, which keeps the full network by
  P15, and not `tools/bbn_baseline.py`'s module docstring.

## 2. Tests

- **`test_network_flag` (c), extended.** It reads `tools/history_and_bbn.py`'s `--small-network`
  default from the source, exactly as it reads `tools/bbn_baseline.py`'s, and asserts `[True]`.
  Add one sentence to the docstring naming the tool. **This fails on `HEAD~1`**, on the value
  (`[False]`), not on an import.
- No new test module; the ComputeTargets count is unchanged at 113.

## 3. What this prompt does not do

- No change to `main.py`, `plot_by_beta.py`, `PRyM/`, the `BBNData` lookup or any tolerance.
- No PRyMordial solve beyond what the suites already run, and no store opened.
- No documents beyond the log, the board and the index. Prompt 03 writes them.

## 4. Acceptance

README §6.2b, every row. All three suites pass at 18, 113 and 31. `black --check` is clean on the
files you changed.

## 5. Stop conditions — stop and ask the user

- Any test other than `test_network_flag` (c) depends on `tools/history_and_bbn.py`'s default.
- The change would need a file outside the allowed list.

## 6. The log, the board and the index

- `logs/02b-history-tool-follows-production.md`, in the README §5.1 template.
- The board: H done; the issue moved to §4 with a **Resolved** line naming both places.
- The index: delete the row; correct the count and date.

**Allowed files:**
- `tools/history_and_bbn.py` (the `--small-network` default and help only);
- `ComputeTargets/BBNData.py` (the one comment in `_configure_PRyMordial` only);
- `ComputeTargets/tests/test_network_flag.py` (test (c) only);
- the log; this campaign's board; `.documents/OPEN_ISSUES.md`.
