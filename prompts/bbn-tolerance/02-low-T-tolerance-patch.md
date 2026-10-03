# Prompt 02 — Move production to the small network, set both low-T tolerances, and warn on foreign BBN rows

> **Rewritten 2026-10-03 after the user's ruling U4 on log 01c** (README §0.2 U3, U4, P14–P16).
> The prompt as first planned, a full-network patch at the setting log 01 would recommend, is in
> `git log` (this file at `f3fa41a`). It was never dispatched.

**Campaign:** [`README.md`](README.md) · **Board items:** **S**, **N**, **K** ·
**Board:** `IMPLEMENTATION_STATE.md`. Update your row and S, N, K.
**Closes:**
- `[00-the-low-T-network-fails-near-1-keV-on-ulp-level-input]`: production stops running the
  network that fails;
- `[00-a-store-serves-bbn-rows-from-another-prym-version-silently]`: the warning;
- the assigned `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]` (README §5
  rule 4: a dated **Resolved** line on the `review-remediation` board too).

**Does not close:** `[01-prymordial-li8-p-d-li7-rate-rings-near-1-kev]` and
`[01-prymordial-dYB8dtLT-unpacks-Y-in-the-superseded-order]`. The full network keeps both defects
for anyone who selects it. Add a dated **Narrowed** line to the first saying that production no
longer runs the full network.

**Recommended model:** **Opus.** The patch is two lines and a flag, but re-pinning test constants
from their provenance, and reproducing logs 01 and 01c digit for digit, need care.

**Precondition (the orchestrator checks it):** the board's Decisions record U4, the user's
confirmation that `plot_by_beta.py` follows `main.py`, and the user's acceptance of P14–P16.

**Read first:**

1. [`README.md`](README.md) §0.2 (P4–P9, U3, P11–P13, U4, P14–P16), §2 (a), (b), (c′), (f), §4,
   §5, §6.2.
2. `IMPLEMENTATION_STATE.md`, Decisions: U4 and what follows it.
3. `logs/01c-small-network-scan.md`: "State handed to the next prompt", row 7 (the pinned values)
   and Observations 3. `logs/01-mechanism-and-tolerance-scan.md`: Verification items 6 (the
   mechanism) and 11 (the pinned values at 1e-5), and deviation 7. Both logs are evidence, not
   instructions.
4. `PRyM/PRyM_main.py`: the two low-T `solve_ivp` calls (`:1332` small, `:1412` full on
   `f3fa41a`) and the patch-marker style around them.
5. `ComputeTargets/BBNData.py`: `PRYM_VERSION` and its comment; `compute_BBN_data`'s
   `small_network` default (`:393`); `BBNData.compute`'s payload fallbacks (`:742`, `:748`); the
   `small_network` and `PRyM_version` properties.
6. `Datastore/SQL/ObjectFactories/BBNData.py` `build` (`:130–290`): the columns a lookup reads,
   with and without `_do_not_populate`.
7. `main.py`: `compute_bbn_data_batch` (`:785–788`) and the BBN lookup above it (`:640–700`).
   `plot_by_beta.py`: the SM baseline (`:1085–1095`) and where it reads `BBNData`.
   `config/version.py`'s dated history.
8. `pipeline_selection.py`: `summarise_failure_reasons` and `warn_super_planckian`, the pattern
   for a pure function and a printer that a driver calls.
9. `tools/bbn_baseline.py` and `tools/bbn_from_store.py`: their `--small-network` arguments.
10. `ComputeTargets/tests/`: `test_prym_passenger.py`, `test_bbn_callbacks.py`,
    `test_network_flag.py`, `test_bbn_solver_failures.py` and `test_bbn_from_store.py`, with the
    provenance comments of their pinned constants.

---

## 1. The changes

### (a) The patch (U4; P4 as amended by U4)

In `PRyM/PRyM_main.py`:
- the **small** network's low-T call gets `rtol=1.0e-6`; its `atol=1.0e-11` stays;
- the **full** network's low-T call gets `rtol=1.0e-5`; its `atol=1.0e-15` stays.

Each gets a comment above it, in the file's existing marker style, naming the campaign and the
prompt, and saying in one line that upstream passes no `rtol` (so SciPy's 1e-3 applied) and which
log measured the new value. Nothing else in `PRyM/` changes: no other stage, no rate, not the
Julia branches. Do not run `black` on `PRyM/`.

### (b) The version

`PRYM_VERSION = "bf24c3d+ri02+sr01+bt02"`, with a dated sentence added to the comment above it in
the same style as the existing ones. `VERSION_LABEL` is not touched (P5). Add a dated entry to
`config/version.py`'s history, after the last one and in its style: under the same label,
production runs PRyMordial's small network with both low-T tolerances set, `PRyM_version` is
`+bt02`, and BBN rows are refreshed by a copy and `--drop bbn-data`. The earlier entry that says
production runs the full network is history; leave it.

### (c) Production runs the small network (U4)

- **`main.py`.** The BBN payload's hard-coded `"small_network": False` becomes `True`. Give the
  value one name, assigned the literal `True` once, and use that name in the payload and in the
  warning call ((e) below). The assignment carries a comment that documents PRyMordial's
  fragility. It says:
  - PRyMordial's full network fails on about 1 % of histories near T_J ≈ 1 keV;
  - the cause is the Li8(p,d)Li7 reverse rate, exp(γ/T9) times a quadratic spline that rings in
    sign there (bbn-tolerance log 01;
    `[01-prymordial-li8-p-d-li7-rate-rings-near-1-kev]`);
  - no tolerance removes it, and the small network has no Li8;
  - the small network's ⁷Li/H is less reliable, and is not used for constraints (README §0.2 U3);
  - setting the name to `False` runs the full network. Its low-T `rtol` is 1e-5, and its
    failures remain.

  **No command-line flag is added.** U4 records the reason: `BBNData` treats PRyMordial as a
  black box.
- **`plot_by_beta.py`.** The SM baseline's `compute_SM_baseline(small_network=False)` follows
  production. Give it a name, assigned `True` once, with a comment that it must match `main.py`'s
  and why: a baseline on the other network is off by the network offset, about 3×10⁻⁴ in D/H
  (log 01c, row 6). Correct the comment above it that says `False` is what `main.py` passes.
- **The other defaults that follow `main.py` (P15).**
  - `compute_BBN_data`'s `small_network` default, and `BBNData.compute`'s two payload fallbacks,
    become `True`.
  - `tools/bbn_baseline.py`'s `--small-network` default becomes `True`, with its help text.
  - `tools/bbn_from_store.py` **keeps** its default, the full network: the reproduction commands
    of logs 01 and 01c depend on it. Correct its help text, which says that this is "as
    main.py", and nothing else in the tool.
- **Unchanged, on purpose:** `extract_common.add_BBN_info_labels`. It prints "Li⁷ results may not
  be reliable" when the small network ran. That is right, and it now appears on every plot.

### (d) The warning (P6, extended by U4)

- **The pure function.** Add it to `pipeline_selection.py`, for example
  `foreign_bbn_provenance(bbn_objects, prym_version: str, small_network: bool)`. It counts the
  **successful** `BBNData` objects whose `(PRyM_version, small_network)` differs from the given
  pair, grouped by pair. Failure rows store `NULL` in both columns, so they cannot be classified.
  Count them separately as "provenance not stored". Do not count them as foreign.
- **The printer.** Add one, in the style of `warn_super_planckian`. If any row is foreign, it
  prints one warning naming each foreign pair and its count, and the count of failure rows whose
  provenance is not stored. It ends with one line naming the refresh route, a copy and
  `--drop bbn-data`. It prints nothing if no row is foreign, and it returns nothing.
- **The callers.**
  - `main.py` calls it on the BBN rows its lookup returns, with `PRYM_VERSION` and the name from
    (c).
  - `plot_by_beta.py` calls it on the rows it reads, with `PRYM_VERSION` and its own name.
  - **Nothing is skipped, filtered or recomputed because of it.**
- **Check the objects carry the fields.** `main.py`'s lookup passes `_do_not_populate`.
  - The planner reads `ObjectFactories/BBNData.py` `build` as passing `small_network` and
    `PRyM_version` to the constructor on both paths (`:262–278`).
  - The constructor marks a stored object queryable either way (`ComputeTargets/BBNData.py`
    `:558–578`).
  - Both properties **raise** on a failure row, and on an object that was not found in the store
    (`:619–644`). So the function considers only objects found in the store, and checks
    `failure` before it reads either property.

  Confirm all of this on the objects the drivers hold. If the fields are not readable there, that
  is `STRUCTURALLY REQUIRED`: stop, because a factory change is outside this prompt.

### (e) The pinned constants (P7, U4 (5), P16)

Re-derive each from its stated provenance at the new setting, using log 01's method. Logs 01 and
01c have measured each one; a re-derivation that differs from the value below is a stop.
- Replace the value.
- Keep the old value in a comment, with its commit.
- Add a dated line naming this prompt.
- **No bound changes.**

| constant (test, bound) | network, low-T `rtol` | expected (provenance) |
|---|---|---|
| `CONST_HONLY_SMALL_YP`, `_D_OVER_H_E5` (`test_prym_passenger` (c), 1e-6) | small, 1e-6 | 0.253669508, 2.649288446, re-derived on `7b518c9` (log 01c, row 7) |
| `CONST_HONLY_FULL_YP`, `_D_OVER_H_E5` (`test_network_flag` (b), 1e-5) | full, 1e-5 | 0.2536731562, 2.649990509, re-derived on `7b518c9` (log 01, item 11) |
| `BUILDER_CONST_HONLY_FULL_YP`, `_D_OVER_H_E5` (`test_bbn_callbacks` (h), 1e-6) | full, 1e-5 | 0.2536745605, 2.649973638, re-derived on `7b518c9` (log 01, item 11) |
| `README_BASELINE` (`test_bbn_callbacks` (i), 1e-4) | **small, 1e-6 (P16)** | the SM row of `logs/01c-probes/scan.csv` at `lowT_rtol == "1e-06"`: Yp 0.24688021169088586, D/H 2.4582878928660548, ³He/H 1.0419326951489363, ⁷Li/H 5.48637300688257. Quote 10 significant figures |
| `PRYM_VERSION` (`test_bbn_solver_failures`) | — | `"bf24c3d+ri02+sr01+bt02"` |
| `LOWT_SMALL_RTOL_AS_PASSED` (`test_bbn_from_store` (b)) | small | `1e-6` (log 01, deviation 7) |

- **`test_bbn_callbacks` (i) moves to the production network (P16).** It calls
  `compute_SM_baseline(True)` and asserts `small_network` is `True`. Keep the name
  `README_BASELINE`. Its comment gives the new provenance and keeps the old one
  (review-remediation README §2 (f) row 1, the full network at the default).
- **`test_bbn_from_store` (b)'s `OVERRIDE_RTOL` becomes `1e-8`.** Once the small call passes 1e-6
  itself, an override of 1e-6 is invisible, and the test no longer shows that the override acts.
  1e-8 is a value of the scan. Update the comment that says the call passes no `rtol`.
- **`test_network_flag` (b).** The logic is unchanged. The ⁷Li/H shift, small at 1e-6 against
  full at 1e-5, has not been measured in that combination. Logs 01 and 01c give 1.044e-2 with
  both at 1e-5 and 1.033e-2 with both at 1e-6. Quote what it prints.
- **`test_network_flag` (c)** asserts that production's defaults are the full network. It becomes
  the assertion that they are the **small** network, at every place it reads now:
  - `compute_BBN_data`'s default;
  - `plot_by_beta.py`'s call, through the stub;
  - `main.py`'s payload, now through the name from (c), resolved in the AST to its literal;
  - `tools/bbn_baseline.py`'s default.

  Rename the method to match, and update its docstring.

## 2. Tests

- **(a) The low-T calls receive the setting.** A new test intercepts `solve_ivp` around one
  small-network and one full-network SM-baseline solve. The tool's `stage_tolerance_override`,
  with no settings, records the calls.
  - The small low-T call carries `rtol` 1e-6 and `atol` 1e-11.
  - The full low-T call carries `rtol` 1e-5 and `atol` 1e-15.
  - Every other call carries `rtol` 1e-6 and `atol` 1e-9, as now.
  - **It fails on `HEAD~1`**, where the low-T calls carry no `rtol`.
  - The docstring says that it runs two solves.
- **(b) The warning.**
  - `foreign_bbn_provenance` on stub objects: a mix of current, foreign-version, foreign-network
    and failed rows gives the right counts. Failure rows are counted only as "provenance not
    stored".
  - The printer prints once per foreign pair, prints nothing when nothing is foreign, and returns
    `None`.
  - An `ast` check confirms that `main.py` and `plot_by_beta.py` call it, each passing the same
    name it uses for its network. **That part fails on `HEAD~1`.**
- **(c) `test_network_flag` (c), changed as above. It fails on `HEAD~1`.**
- **(d) The re-pinned tests** pass at their unchanged bounds.

## 3. Acceptance (README §6.2)

Run with the tool, **no override**, from the repository root. Per P14, up to 8–10 solves at a
time; the cost run alone, afterwards.

- **The small network:** the roster × 3 variants and the SM baseline, `--small-network`, 49
  solves. Each row must equal `logs/01c-probes/scan.csv`'s `tag == "T1"`, `lowT_rtol == "1e-06"`
  row for the same input and variant, **to every printed digit** (as the CSV strings are written).
  This covers all 11 histories that fail on the full network; each must complete.
- **The full network:** the 17 inputs, `prod`, at the tool's default network, 17 solves. Each row
  must equal `logs/01-probes/scan.csv`'s `tag == "S1"`, `lowT_rtol == "1e-05"`, `prod` row to
  every printed digit. Log 01 had no failure there.
- **Any difference is a stop.** It means that the patch and the override are not the same change.
- **Cost.** The serial median of three repeats on the control, small network, no override, must
  be within 1.2× of log 01c's 10.05 s for small at 1e-6. Record the load average as log 01c did.
- **Suites.** All three pass. The counts are not lower: 18, 106 + the new tests, 31. `black
  --check` is clean on the non-`PRyM/` files you changed.

## 4. What this prompt does not do

- No `VERSION_LABEL` bump.
- No change to the `BBNData` lookup or schema. No new column, and no network or version key.
- No command-line flag for the network.
- No other PRyMordial stage, no rate patch, no Julia branch.
- No change to `tools/bbn_from_store.py`'s behaviour or default.
- No `main.py` run, and no write to any store.
- No documents beyond the board, the index, the `review-remediation` board's Resolved line and
  the log. Prompt 03 writes them.

## 5. Stop conditions — stop and ask the user

- A roster row differs from log 01c's (small) or log 01's (full) at the ruled setting.
- A small-network solve fails, or a full-network solve fails among the 17.
- A re-derived constant differs from its expected value in §1 (e), or a re-pinned test fails its
  unchanged bound.
- `small_network` or `PRyM_version` is not readable on the objects the drivers hold.
- The change would need a file outside the allowed list.

## 6. The log, the board and the index

- `logs/02-low-T-tolerance-patch.md`, in the README §5.1 template; probes and CSVs in
  `logs/02-probes/`. Include:
  - every `PRyM/` hunk with its marker comment, for re-application on an upgrade;
  - every re-pinned constant: the old value, the new value, the bound, and how it was
    re-derived;
  - the text of `main.py`'s comment;
  - the ⁷Li/H shift `test_network_flag` (b) prints;
  - the reproduction counts (49 + 17 identical) and the cost.
- **The board:**
  - S, N and K are done.
  - Move the two issues this board owns to §4 with **Resolved** lines.
  - Close `[03-…]` by README §5 rule 4. Its **Resolved** line quotes the D/H spread before and
    after, and says that the residual is a measurement (P9):
    - before: the full network at the default, median 1.45e-3 over the five controls (log 01);
    - after: the small network at 1e-6, median 4.4e-5 and maximum 1.5e-4 on β = 2.4,
      M = 10⁻⁵ (log 01c).
  - Add the **Narrowed** line to `[01-prymordial-li8-p-d-li7-rate-rings-near-1-kev]`.
- **The index:** delete the three rows, update the Li8 row's hook, and correct the count and
  date.

**Allowed files:**
- `PRyM/PRyM_main.py` (the two low-T calls and their comments only);
- `ComputeTargets/BBNData.py` (`PRYM_VERSION` and its comment; `compute_BBN_data`'s
  `small_network` default; `BBNData.compute`'s two payload fallbacks);
- `config/version.py` (one dated history entry, added);
- `main.py` (the network name, its comment, the payload, the warning call);
- `plot_by_beta.py` (the SM baseline's name, its comment, the warning call);
- `pipeline_selection.py` (the warning's function and printer);
- `tools/bbn_baseline.py` (the `--small-network` default and help);
- `tools/bbn_from_store.py` (the `--small-network` help text only);
- `ComputeTargets/tests/`;
- the log and `logs/02-probes/`; this campaign's board; the `review-remediation` board (a
  Resolved line only); `.documents/OPEN_ISSUES.md`.
