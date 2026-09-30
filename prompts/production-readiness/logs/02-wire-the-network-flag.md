# Log 02 — Wire the network flag; run the full network; bump the version

**Prompt:** prompts/production-readiness/02-wire-the-network-flag.md
**Commit:** the commit that adds this file ("Wire the BBN network flag to PRyMordial and run the full network"); its SHA is in `git log`
**Model:** Opus 5.5
**Date:** 2026-09-30
**Result:** COMPLETE WITH DEVIATIONS

The deviation that sets the Result: README §6.2's Yp bound (small against full network ≤ 1e-5
relative) is **missed**: it is 6.2e-5 on the fixture's constant family. The bound was not
rewritten. It is kept as an expected-failure test and opened as board §3
`[02-network-shift-bounds-sit-inside-prymordial-noise]`. Every other §6.2 row is met.

## What shipped

- **N1.** `ComputeTargets/BBNData.py:259–264` (`_configure_PRyMordial`):
  `PRyMini.small_network_flag = small_network` → `PRyMini.smallnet_flag = small_network`. The
  comment above now says that True is the 12-reaction network (faster, unreliable for ⁷Li), that
  False (production) is the full network, that PRyMordial reads `smallnet_flag` when the solve runs,
  and that until this prompt the unread `small_network_flag` was set.
- **N2.** `small_network=False` in:
  - `main.py:752`, the BBN payload (`{"small_network": True}` before);
  - `ComputeTargets/BBNData.py:301`, the `compute_BBN_data` default (`True` before);
  - `plot_by_beta.py:903`, the `compute_SM_baseline` call, and its comment at `:899` ("small_network=False
    is what main.py passes");
  - `tools/bbn_baseline.py:50–51`, the `--small-network` default and help text ("default: False, the
    full network, as main.py passes"). The docstring's usage example now shows `--small-network`
    (the non-default option) instead of `--no-small-network`.
  - `BBNData.compute`'s own payload fallback (`BBNData.py:698–700`) was already `False`; unchanged.
  - The switch is kept.
- **N3.** `ComputeTargets/tests/prym_fixtures.py`, `run_prym`:
  - default `small_network: bool = False` (was `True`);
  - it now calls `ComputeTargets.BBNData._configure_PRyMordial(small_network)` (imported inside the
    function) and then sets `NP_thermo_flag` from its argument. The three hand-copied assignments are
    gone;
  - it saves and restores `smallnet_flag`, not `small_network_flag`;
  - the docstring now says the flags come from `_configure_PRyMordial`, and that before this prompt
    every solve used the full network, so every pin taken from it is a full-network value.
  - `ComputeTargets/tests/test_bbn_callbacks.py`: `_SavedPRyMGlobals._init_names` saves
    `smallnet_flag`. Test (i) calls `compute_SM_baseline(False)` (was `True`) and
    `assertFalse(baseline["small_network"])` (was `assertTrue`); its docstring follows.
  - **No pinned abundance was re-taken or edited.**
- **N4.** `extract_common.py:149`: `small_network is "True" or small_network is "Multiple"` →
  `==` for both. Nothing else in `add_BBN_info_labels` or the file changed.
- **N5.** `VERSION_LABEL` **`"2026.2.0"` → `"2026.3.0"`** in `main.py:86` and `plot_by_beta.py:79`.
  The new comment in `main.py` sits under the 2026-09-29 sentence, verbatim:
  ```
  # On 2026-09-30 (production-readiness prompt 02) the small_network switch was wired to the flag
  # PRyMordial reads: from 2026.3.0 the stored small_network describes the network that ran, and
  # production runs the full network (small_network=False).
  ```
  **Consequence: every store made before 2026.3.0 is invalid.** This is written on the board's
  header and on P2's row.
- **N6.** Dated notes, additive only:
  - `.documents/numerical-strategies.md` §7.5, a "Note added 2026-09-30" paragraph after the
    small-network sentence;
  - `.documents/architecture-summary.md`, a note after the Stage 3 code block (the `True` inside
    the block is left as it was);
  - `.documents/numerical-methods-for-paper.md` §4, a dated sub-bullet under "The network" caveat.
    It says the defect is fixed and that production uses the full network.
- **Test.** New `ComputeTargets/tests/test_network_flag.py`, 4 methods:
  - `test_a_flag_reaches_prymordial`;
  - `test_b_flag_selects_the_network`;
  - `test_b_prime_Yp_within_1e_5` (`@unittest.expectedFailure`);
  - `test_c_production_defaults_are_the_full_network`.
  - Helpers: `_SavedPRyMGlobals`, `_network_solves() -> dict` (the two solves, cached per process),
    `_calls_named(path, name)` and `_StubPRyMclass`.
- `PRyM/` untouched; `PRYM_VERSION` still `"bf24c3d+cham03"`. No schema or lookup change.

## Deviations from the prompt

### The Yp bound of test (b) is missed and kept as an expected failure — STRUCTURALLY REQUIRED

- **What the prompt assumed.** Prompt §2 (b) and README §6.2 give Yp ≤ 1e-5 relative, small
  against full, from the board's 1.5e-6. README §2 (b) quotes the same board figures as "What moves".
- **What was there.** Through the fixed flag, on the fixture's `CONSTANT` family, Yp moves by
  **6.200e-5**.
- **Where the board's figure came from.** It was measured on another construction of the same
  family: the raw fit `_raw_G_rho`, 3.38 below 10 keV, on `47c50ae`. I re-measured on this tree
  (scratch, below):
  - **the raw-fit construction:** Yp moves by 2.3e-6, which passes, but D/H moves by 1.2e-3, which
    would fail the D/H bound;
  - **the SM case (ρ_NP ≡ 0):** Yp 2.2e-5, D/H 1.7e-3.
  - So which bound is missed depends on a construction that changes ρ_NP by 2.2e-10. That is
    PRyMordial's known noise (`[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]`).
  - The ⁷Li/H shift is 1.0–1.2 % in all three cases.
- **What was done.** README §4 says a miss is "never a rewritten threshold", so the 1e-5 bound is
  unchanged.
  - Test (b) asserts ⁷Li/H ≥ 5e-3, D/H ≤ 1e-3, and the full run against the pins; all pass.
  - The Yp bound is its own method, `test_b_prime_Yp_within_1e_5`, under
    `@unittest.expectedFailure`. The suite stays green and reports the miss on every run. If the
    bound is ever met, it becomes an unexpected success, which fails the suite.
  - Both methods share one pair of solves (`_network_solves`), so the module still runs PRyMordial
    twice.
- **Alternatives rejected.**
  - A failing test in the suite: the suites are an acceptance item, and the next prompt would start
    from a red suite.
  - Loosening the bound: forbidden.
  - Changing the fixture's construction to the raw fit: it moves the pins, and trades the Yp miss
    for a D/H miss.
- **Touches a §2 design fact:** yes, §2 (b)'s "What moves" figures. That is a README §4 stop for
  the orchestrator. Opened as board §3 `[02-network-shift-bounds-sit-inside-prymordial-noise]`.

### `run_prym` calls `_configure_PRyMordial` — IMPLEMENTATION CHOICE

- **Alternatives.**
  - Keep the hand-copied assignments, with `smallnet_flag` corrected;
  - or call the production function and override `NP_thermo_flag`.
- **Picked:** the call.
  - The defect was exactly a fixture that copied the production settings faithfully, bug
    included. With one source of truth, test (b) exercises the production path.
- **Cost.** `prym_fixtures` now imports `ComputeTargets.BBNData`, and with it Ray, but no cluster.
  - The import is inside the function, so the module still loads without Ray until a solve is asked
    for.
  - The script driver still works: `python -m ComputeTargets.tests.prym_fixtures zero` gave Yp
    0.2468872958, D/H 2.462251065, 8.3 s.

### Test (c) reads three call sites from source, and stubs the solve — IMPLEMENTATION CHOICE

- **Why read from source.** `plot_by_beta.py` and `main.py` run `argparse` and `ray.init` at import,
  so neither can be imported.
- **What (c) does.**
  - It `ast`-parses `plot_by_beta.py` for its single `compute_SM_baseline(...)` call and
    `literal_eval`s its arguments.
  - It calls `compute_SM_baseline` with them, with `PRyM.PRyM_main.PRyMclass` patched to a stub, so
    no solve runs.
  - It asserts the returned `small_network` and `PRyM_init.smallnet_flag` are both `False`.
- **Beyond the prompt's two assertions.** (c) also checks `main.py`'s BBN payload and
  `tools/bbn_baseline.py`'s `--small-network` default in the same way. They are README §6.2 row 3
  witnesses, and they cost nothing.
- **Alternative:** a grep in the log only. Rejected, because it would not guard against a later
  edit.

### Test (i) in `test_bbn_callbacks.py`: the call argument follows too — IMPLEMENTATION CHOICE

- **What the prompt says.** "its `assertTrue(baseline["small_network"])` follow[s]".
- **What was done.** I also changed the call from `compute_SM_baseline(True)` to `(False)`.
- **Why.** With the flag now effective, `(True)` would run the small network, and ⁷Li/H would miss
  its 1e-4 pin by 1.2 %. The pin was taken on the full network. Keeping `(True)` would have meant
  re-pinning, which is forbidden.

### `tools/bbn_baseline.py` docstring example — IMPLEMENTATION CHOICE

- The usage line `--no-small-network` became `--small-network`, so that the example still shows the
  non-default option.
- The prompt names "its default and its help text"; I treat the docstring's usage line as help
  text.

## Verification performed

All from the repository root with `venv/bin/python`, on `cf773b2` plus this diff unless stated.

- **Suites.**
  - Before, on `cf773b2`: `CosmologyModels/tests` 12 OK; `ComputeTargets/tests` 18 OK (41 s).
  - After: `CosmologyModels/tests` 12 OK; `ComputeTargets/tests` **22**, `OK (expected
    failures=1)`, 63 s. +4 methods, all in `test_network_flag.py`.
- **Pins unchanged** (I ran these and they passed; the figures are the tests' printout on the full
  suite run):
  - `test_prym_passenger` (c): constant family Yp and D/H against 0.2540937879 / 2.671500711 at
    1e-5. Pass.
  - `test_bbn_callbacks` (h): Yp 0.2540933067 (1.89e-06), D/H 2.671263588 (8.85e-05). These are
    identical to the `cf773b2` run.
  - `test_bbn_callbacks` (i): baseline Yp 0.2468872958, D/H 2.462251065, ³He/H 1.042050273,
    ⁷Li/H 5.423441017. These are identical to the `cf773b2` run and to
    `.documents/numerical-methods-for-paper.md`'s baseline.
- **Test (a) fails on `HEAD~1`.**
  - How it was shown: I copied this diff's `ComputeTargets/BBNData.py` aside and wrote
    `git show cf773b2:ComputeTargets/BBNData.py` over it. `cf773b2` is `HEAD~1` of this commit.
    I ran tests (a) and (c) against it and then restored the file.
  - `test_a_flag_reaches_prymordial` failed: `AssertionError: False is not True`, at
    `assertIs(PRyMini.smallnet_flag, True)`.
  - `test_c_...` failed: `True is not False`, on the `compute_BBN_data` default.
  - After restoring, both pass.
- **Test (b), the four numbers** (constant 0.08 ρ_SM fixture; `test_network_flag` (b) printout, full
  suite run):

  | Quantity | small | full | relative shift | bound | verdict |
  |---|---|---|---|---|---|
  | ⁷Li/H × 1e10 | 5.1424297 | 5.091224307 | **1.006e-2** | ≥ 5e-3 | pass |
  | D/H × 1e5 | 2.670892604 | 2.671499971 | **2.274e-4** | ≤ 1e-3 | pass |
  | Yp | 0.2540780344 | 0.2540937879 | **6.200e-5** | ≤ 1e-5 | **MISS** (expected failure) |
  | full vs pins | — | — | Yp **7.52e-11**, D/H **2.77e-7** | ≤ 1e-5 | pass |

  - **Wall-clocks.** Small 5.2 s, full 7.9 s in the suite run; 5.0 s and 6.7 s run alone. The board
    had 5.9 s and 9.1 s.
- **Scratch probes**, scratchpad only and not committed, on this tree. Each run goes through
  `run_prym(..., small_network=...)`:
  - **Determinism and the SM case** (`probe_network.py`).
    - The constant family, small network, repeated: Yp 0.2540780344, D/H 2.670892604, ⁷Li/H
      5.1424297. That is bit-identical to test (b).
    - `ZERO`, small against full: Yp 0.2468818826 / 0.2468872958 (2.193e-5); D/H 2.457976999 /
      2.462251065 (1.736e-3); ⁷Li/H 5.486812924 / 5.423441017 (1.168e-2).
  - **The raw-fit construction** (`probe_rawfit.py`).
    - The family: ρ_NP = 0.08 · (π²/30) · `_raw_G_rho(T/1e3)` · T⁴ with p = ρ/3. Its derivative is
      a central difference with half-step 1e-4 T, and ρ = 0 for T ≤ 0.
    - Small: Yp 0.2540895184, D/H 2.668169518, ⁷Li/H 5.151838607.
    - Full: 0.2540901113, 2.671382773, 5.091075577.
    - Shifts: Yp 2.333e-6, D/H 1.203e-3, ⁷Li/H 1.194e-2.
- **PRyMordial reads the flag at run time** (read, not run). `PRyM_main.py` imports `PRyM_init` as
  a module (`import PRyM.PRyM_init as PRyMini`, `:17`, inside `PRyMclass.__init__`). It tests
  `PRyMini.smallnet_flag` inside `__init__` at `:602` (which nuclear-rate module), `:887` (which
  network RHS), `:985, 992` (MT initial conditions and solve) and `:1164, 1170` (LT).
  - Setting the module attribute before constructing `PRyMclass` is therefore enough. Test (b)'s
    ⁷Li/H shift confirms it.
  - `grep -rn small_network_flag PRyM/` finds nothing.
- **README §6.2, row by row.**

  | Row | Measured |
  |---|---|
  | `smallnet_flag` after `_configure_PRyMordial(True)` / `(False)` | True / False (test (a)); False / False on `HEAD~1` |
  | `grep -rn "small_network_flag" --include='*.py' ComputeTargets/ tools/ main.py plot_by_beta.py` | only history in comments and docstrings: `BBNData.py:263`, `prym_fixtures.py:193`, `test_network_flag.py:21, 178` |
  | `small_network` in `main.py`, the `compute_BBN_data` default, `plot_by_beta.py`, `bbn_baseline.py`, `run_prym` | False in all five (`main.py:752`, `BBNData.py:301`, `plot_by_beta.py:903`, `bbn_baseline.py:50`, `prym_fixtures.py:174`); test (c) asserts the first four |
  | pinned abundances | pass, unchanged |
  | constant family small vs full | ⁷Li/H 1.006e-2 ✓, D/H 2.274e-4 ✓, Yp 6.200e-5 ✗ |
  | `add_BBN_info_labels` | `==` (`extract_common.py:149`) |
  | `VERSION_LABEL` | `"2026.3.0"` at `main.py:86` and `plot_by_beta.py:79` |

- **Formatting.** `black --check` is clean on all eight changed Python files.

## Observations not acted on

- **`BBNData.build()` does not key on `small_network`.**
  `Datastore/SQL/ObjectFactories/BBNData.py:143–149` filters on `model_serial` and, optionally,
  `failure` only. An old store's `small_network = True` rows would be returned to a
  `small_network=False` run.
  - This is covered by `[00-datastore-lookups-ignore-the-version-column]` and by the fresh-database
    rule, since every store before 2026.3.0 is invalid anyway. Nothing new opened.
- **The SM baseline on the small network** moves D/H by 1.7e-3 and ⁷Li/H by 1.2 %. This supports
  the full-network decision, and is recorded in the new §3 issue.
- **Old documents.** `.documents/architecture-summary.md`'s Stage 3 code block still shows
  `payload={"small_network": True}`. It is left as written, with the dated note after it (rule 6).
- **Stale line numbers in the prompt.** `PRyM_main.py:546` is `Y_prime_HT` and does not read the
  flag. `plot_by_beta.py:74` is now `:79` after prompt 01. Neither affected the work.

## State handed to the next prompt

- `VERSION_LABEL = "2026.3.0"` at `main.py:86` and `plot_by_beta.py:79`. `main.py`'s comment is
  `:80–85`: the 2026-09-29 sentence, then the 2026-09-30 prompt-02 sentence. Prompt 03 adds its
  reason beneath that and does **not** bump the label.
- Suite counts after this prompt: `CosmologyModels/tests` **12**, `ComputeTargets/tests` **22**,
  reported as `OK (expected failures=1)`. The expected failure is `test_network_flag`
  `test_b_prime_Yp_within_1e_5`. A later prompt should count 22 and expect that line.
- `run_prym` defaults to `small_network=False` and takes its flags from
  `ComputeTargets.BBNData._configure_PRyMordial`. That function now sets `PRyM_init.smallnet_flag`.
  Save and restore `smallnet_flag`, not `small_network_flag`, if you touch PRyMordial's globals.
- Every pinned BBN abundance is unchanged and is a full-network value.
- Open for the user: board §3 `[02-network-shift-bounds-sit-inside-prymordial-noise]`, which asks
  whether to accept the Yp miss or restate the §6.2 Yp and D/H rows. Prompt 04 re-measures §6.2 and
  will find the same miss.
- Reproduce test (b)'s numbers:
  `PYTHONPATH=. ./venv/bin/python -m unittest ComputeTargets.tests.test_network_flag -v` (about
  13 s).

## Addendum 2026-09-30 — the user's decision on the Yp bound

Added after the orchestrator's review of `8503fe7`. The record above is unchanged.

The orchestrator's rerun of test (b) reproduced the numbers above exactly:
- ⁷Li/H 1.006e-2, D/H 2.274e-4 and Yp 6.200e-5 relative, small against full;
- 5.7 s for the small network and 7.7 s for the full one.

The user decided that the small network's Yp and D/H offsets are not an issue. They are a property
of PRyMordial, which never promised to hold them at any level. The 1e-5 and 1e-3 bounds were this
campaign's, not a contract PRyMordial made.

- `test_b_prime_Yp_within_1e_5` (the expected failure) and test (b)'s D/H bound are removed. Test
  (b) keeps the ⁷Li/H ≥ 5e-3 bound and the full-network pins, and still prints the Yp and D/H
  shifts.
- `ComputeTargets/tests` is now **21**, all `OK`. The count falls by the one removed method, by
  the user's decision.
- `[02-network-shift-bounds-sit-inside-prymordial-noise]` is withdrawn: board §4, and its row is
  deleted from `.documents/OPEN_ISSUES.md`. README header and §6.2 amended.
- The shifts stay recorded in `.documents/numerical-strategies.md` §7.5, as something to be aware
  of.

This supersedes the "State handed to the next prompt" bullets on the suite count (22, one expected
failure) and on the open decision. The next prompt should count **12 / 21**, all `OK`.
