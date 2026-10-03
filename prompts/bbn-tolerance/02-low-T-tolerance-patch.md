# Prompt 02 — Set the low-T tolerance and warn on a stale PRyMordial version

**Campaign:** [`README.md`](README.md) · **Board items:** **S**, **N**, **K** ·
**Board:** `IMPLEMENTATION_STATE.md`. Update your row and S, N, K.
**Closes:** `[00-the-low-T-network-fails-near-1-keV-on-ulp-level-input]`,
`[00-a-store-serves-bbn-rows-from-another-prym-version-silently]`, and the assigned
`[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]` (README §5 rule 4: a dated
**Resolved** line on the `review-remediation` board too).
**Recommended model:** **Opus.** The patch is two lines, but re-pinning test constants from their
provenance and reproducing log 01 digit for digit need care.

**Precondition (the orchestrator checks it):** the board's Decisions record the user's ruling on
log 01's recommendation (P3) and on P4–P7. **The setting below is the ruled one, not log 01's
recommendation, if the two differ.**

**Read first:**

1. [`README.md`](README.md) §0.2 (P4–P9), §2 (a), (b), (f), §4, §5, §6.2.
2. `IMPLEMENTATION_STATE.md`, Decisions: the ruled setting.
3. `logs/01-mechanism-and-tolerance-scan.md`: "State handed to the next prompt", the scan table at
   the ruled setting, and the pinned-value measurements.
4. `PRyM/PRyM_main.py`: the two low-T `solve_ivp` calls and the patch-marker style around them.
5. `ComputeTargets/BBNData.py`: `PRYM_VERSION` and the `BBNData` object's `PRyM_version`.
6. `main.py`'s BBN stage, and `plot_by_beta.py` where it reads `BBNData`.
7. `pipeline_selection.py`: `summarise_failure_reasons` and `warn_super_planckian`, which are the
   pattern for a pure function a driver calls.
8. `ComputeTargets/tests/test_prym_passenger.py`, `test_bbn_callbacks.py`, `test_network_flag.py`,
   `test_bbn_solver_failures.py`: the pinned constants and their provenance comments.

---

## 1. The changes

- **(a) The patch (P4).** In `PRyM/PRyM_main.py`, add the ruled `rtol` (and `atol`, if ruled) to
  the full and small low-T `solve_ivp` calls. Each gets a comment above it, in the file's existing
  style:
  `# ChamPBH bbn-tolerance prompt 02: rtol (upstream passes none, so SciPy's 1e-3 applied)`, with
  the measured reason in one line. Nothing else in `PRyM/` changes. The Julia branches are not
  patched. Do not run `black` on `PRyM/`.
- **(b) The version.** `PRYM_VERSION = "bf24c3d+ri02+sr01+bt02"`, with a dated comment.
  `VERSION_LABEL` is not touched (P5).
- **(c) The warning (P6).** Add a pure function in `pipeline_selection.py`, for example
  `stale_prym_versions(bbn_objects, current) -> dict[str, int]`. It counts the **successful**
  `BBNData` objects whose `PRyM_version` differs from `current`, by version. Add a printer, in the
  style of `warn_super_planckian`, that prints one warning naming each version and count and
  returns nothing. `main.py` calls it on the BBN rows its lookup returns; `plot_by_beta.py` calls
  it on the rows it reads. **Nothing is skipped, filtered or recomputed because of it.** Check
  that `PRyM_version` is readable under `_do_not_populate`. If it is not, that is a
  `STRUCTURALLY REQUIRED` deviation: stop, since a factory change is outside this prompt.
- **(d) The pinned constants (P7).** Re-derive each constant log 01 lists from its stated
  provenance at the new setting, using log 01's method. Replace the value. Keep the old value in a
  comment with its commit, and add a dated line naming this prompt. **No bound changes.** Update
  `test_bbn_solver_failures`'s `PRYM_VERSION` assertion.

## 2. Tests

- **(a) The low-T calls receive the setting.** Intercept `solve_ivp` around one small-network and
  one full-network SM-baseline solve. Both low-T calls carry the ruled `rtol` (and `atol`), and the
  other six calls' arguments are unchanged from their literals. **Fails on `HEAD~1`**, where the
  low-T calls carry no `rtol`. The docstring says it runs two solves.
- **(b) The warning.** `stale_prym_versions` on stub objects: a mix of current, foreign and
  failed rows gives the right counts, and failure rows (`PRyM_version` `None`) are not counted.
  The printer prints once per foreign version and returns `None`. An `ast` check confirms
  `main.py` and `plot_by_beta.py` call it. That check fails on `HEAD~1`.
- **(c) The re-pinned tests** pass at their unchanged bounds.

## 3. Acceptance (README §6.2)

- The tool, with **no override**, on the 17-input roster × 3 variants (one variant for the SM
  baseline), must equal log 01's override results at the ruled setting **to every printed digit**.
  Any difference is a stop: it means the patch and the override are not the same change.
- All 11 failures complete.
- The serial cost of the control is within 1.2× of log 01's figure (idle machine, three repeats).
- All three suites pass; the counts are not lower; `black --check` is clean on the non-`PRyM/`
  files you changed.

## 4. What this prompt does not do

No `VERSION_LABEL` bump. No change to the `BBNData` lookup or schema. No other PRyMordial stage.
No `main.py` run. No write to the science store. No documents beyond the board, the index and the
log (prompt 03 writes them).

## 5. Stop conditions — stop and ask the user

- The patched tree does not reproduce log 01 to every printed digit.
- A re-pinned test fails its unchanged bound.
- `PRyM_version` is not available on the objects the drivers hold.
- Any failure of the 11 does not complete.

## 6. The log, the board and the index

- `logs/02-low-T-tolerance-patch.md`, in the README §5.1 template.
  - List every `PRyM/` hunk with its marker comment, for re-application on an upgrade.
  - List every re-pinned constant: old value, new value, bound, how it was re-derived.
- The board: S, N and K done. Move the two owned issues to §4 with **Resolved** lines. Close
  `[03-…]` by README §5 rule 4. The **Resolved** line for `[03-…]` quotes the D/H spread before
  and after, and says that the residual is a measurement (P9).
- The index: delete the three rows; correct the count and date.

**Allowed files:**
- `PRyM/PRyM_main.py` (the two low-T calls and their comments only);
- `ComputeTargets/BBNData.py` (`PRYM_VERSION` and its comment only);
- `pipeline_selection.py`, `main.py`, `plot_by_beta.py` (the warning only);
- `ComputeTargets/tests/`;
- `tools/bbn_from_store.py` (only if a field is needed);
- the log; this campaign's board; the `review-remediation` board (a Resolved line only);
  `.documents/OPEN_ISSUES.md`.
