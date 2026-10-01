# Prompt 02 — A failure reason on `ScalarModel` rows

**Campaign:** [`README.md`](README.md) · **Board item:** **F** · **Board:**
`IMPLEMENTATION_STATE.md`. Update your row and F.
**Closes:** `[00-scalarmodel-failure-rows-carry-no-reason]`, assigned from the
`integrator-remediation` board (README §5 rule 4).
**Recommended model:** **Sonnet.** One column, mirroring a pattern `BBNData` already has.

**Read first:**

1. [`README.md`](README.md) §0.2 (P9), §2 (f), §5, §6.3.
2. `logs/01-hubble-only-bbn-route.md`, "State handed to the next prompt".
3. `ComputeTargets/ScalarModel.py`: `compute_scalar_model`'s two `return {"failure": True}` sites
   and `ScalarModel.__init__`, `failure`, `store()`.
4. `ComputeTargets/BBNData.py`: `_failure_payload` and `BBNData.failure_reason`, the pattern to
   copy.
5. `Datastore/SQL/ObjectFactories/ScalarModel.py`: the table's columns, the `build` lookup (the
   `select` and the payload it hands back), and the `store` method's row dict.
   `Datastore/SQL/ObjectFactories/BBNData.py` for how `failure_reason` is written and read there.
6. `main.py`: the BBN stage's stored-failure summary (`stored_failure_summary`, near `:780–795`)
   and the `ScalarModel` stage's counts. `plot_by_beta.py:590–660`, the drop report.
7. `Datastore/tests/` for how a test builds a temporary SQLite datastore through
   `Datastore.__ray_actor_class__` (`test_bbn_failure_lookup.py` writes `BBNData` failure rows).

---

## 1. The changes

- **`compute_scalar_model`** returns `{"failure": True, "failure_reason": <reason>}` from both
  failure exits, truncated to `DEFAULT_STRING_LENGTH`:
  - the `ComputationFailureError` exit uses `e.message`;
  - the sampling `OverflowError` exit uses `"sampling: overflow when assembling sample values:
    <e>"`.

  The prints stay.
- **`ScalarModel`**: `_failure_reason`, set from the payload in `__init__` and from the result in
  `store()`. A `failure_reason` property, readable on a failure row and `None` on a success. It
  raises if the object has not been populated, as `BBNData.failure_reason` does.
- **The factory:** a nullable `failure_reason String(DEFAULT_STRING_LENGTH)` column, written by
  `store` and read back by `build`.
- **`main.py`:** after the `ScalarModel` stage, the failures are counted, grouped by the reason up
  to its first `:`, and printed in the BBN summary's style.
- **`plot_by_beta.py`:** where the drop report names a model dropped because its `ScalarModel`
  failed, it quotes the reason.

## 2. Tests

- **(a) `Datastore/tests/`: the round trip.** A temporary SQLite datastore. A `ScalarModel`
  failure row with a 300-character reason is stored and looked up (`failure=True`). Its
  `failure_reason` is the first 256 characters. A success row reads back `None`.
  **On `HEAD~1`:** the object has no `failure_reason`, and the reason the history printed is not
  in the row. The log shows that, with the row's columns on `HEAD~1`, as the stand-in measurement.
- **(b) `ComputeTargets/tests/`: the payload.** `compute_scalar_model._function` with
  `main.py`'s initial data, β = 2, M = 0.5, and the step budget reduced to 50. This needs the
  budget to be passed in: if `compute_scalar_model` cannot take a `StepControl` today, patch
  `StepControl`'s default in the test with `mock`, and say so. It returns a payload whose
  `failure_reason` begins `"step budget exhausted"`. Under a second.
- **(c) The summary.** The grouping function `main.py` uses, factored as a pure function and
  tested on a list of reasons.

## 3. What this prompt does not do

No other column; the first bounce is prompt 03. No change to the step loop, to the exception
messages themselves, or to `VERSION_LABEL`.

## 4. Acceptance

README §6.3, every row. All three suites pass; `Datastore/tests` and `ComputeTargets/tests` rise
by your methods. `black --check` clean on changed files. The board and the index: F done, the
assigned issue closed per README §5 rule 4.

## 5. Stop conditions — stop and ask the user

- The factory's lookup cannot return a failure row's reason without changing what a lookup
  matches on.
- `compute_scalar_model` has a failure exit other than the two named.

## 6. The log and the board

`logs/02-scalarmodel-failure-reasons.md`, in the README §5.1 template. Name the column and its
type. In "State handed to the next prompt", give the factory's column list as it now stands.
