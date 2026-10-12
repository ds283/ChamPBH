# Log 03 — pin `raypoolkit` `v0.2.0`

**Subject:** Pin raypoolkit v0.2.0 and install it into the venv
**Commit:** `2f61943`, on branch `dk-and-rpk-migration`, on top of `e287a40`
**Date:** 2026-10-12, by Claude Sonnet 5.5
**Result:** **COMPLETE.**
- K1: one line added to `requirements.txt`, in sorted position.
- K2: the venv installed from it. Pip added one distribution, `raypoolkit 0.2.0`, and moved none.
- K3: the install is the tag's commit `eaef0ce6…`, not editable, with the tag's four modules
  byte for byte and no `tests/`.
- The three roots are at 18 / 114 / 66, unchanged. No §4 stop condition was met. No §3 issue opened.

## What shipped

- `requirements.txt`: one added line (below).
- This log, and the board (§1 row, K1–K3, §5.4).

No code and no test changed. Nothing was changed in RayPoolKit (read only through
`git ls-remote`, `rev-parse` and `archive`). No Ray was started and no store was opened.

```diff
@@ -76,6 +76,7 @@ pytokens==0.4.1
 pytz==2025.2
 PyYAML==6.0.3
 ray==2.53.0
+raypoolkit @ git+https://github.com/ds283/RayPoolKit@v0.2.0
 referencing==0.37.0
 requests==2.32.5
 rpds-py==0.30.0
```

## Deviations from the prompt

- **D1 — IMPLEMENTATION CHOICE.** ComputeTargets was run in its two pattern groups (61 + 53) and
  then whole (114), serially. The prompt allows the split; the whole run was added to give the
  figure the baseline states directly. Nothing differed.

No other deviation.

## Verification performed

Each command was run from the repository root.

**Before (§2.1).** Saved in the agent's scratch directory.
- `./venv/bin/pip freeze` equals `measure/out/52142d7-pip-freeze.txt` (`diff` empty; 99 lines).
- `pip check`: `No broken requirements found.`
- `pip show raypoolkit`: `WARNING: Package(s) not found: raypoolkit` (exit 1).
- No Ray process (`pgrep -lf 'gcs_server|raylet|ray::'` empty). Load 4.79 / 6.70 / 29.27 at 01:57.

**The tag (§2.2).** `git ls-remote --tags https://github.com/ds283/RayPoolKit v0.2.0 'v0.2.0^{}'`:

```text
4e7eb153ca9837c6aa6ec51d702b5fa6ccf08fef	refs/tags/v0.2.0
eaef0ce60e2fc354bb32f0ad576010e6a83c1976	refs/tags/v0.2.0^{}
```

`git -C /Users/ds283/Documents/Code/RayPoolKit rev-parse 'v0.2.0^{commit}'` gives
`eaef0ce60e2fc354bb32f0ad576010e6a83c1976`, in agreement.

**The dry run** (`./venv/bin/pip install --dry-run -r requirements.txt`, exit 0), without the
99 "Requirement already satisfied" lines:

```text
Collecting raypoolkit @ git+https://github.com/ds283/RayPoolKit@v0.2.0 (from -r requirements.txt (line 79))
  Cloning https://github.com/ds283/RayPoolKit (to revision v0.2.0) to <tmp>
  Running command git clone --filter=blob:none --quiet https://github.com/ds283/RayPoolKit <tmp>
  Running command git checkout -q 4e7eb153ca9837c6aa6ec51d702b5fa6ccf08fef
  Resolved https://github.com/ds283/RayPoolKit to commit 4e7eb153ca9837c6aa6ec51d702b5fa6ccf08fef
  Installing build dependencies: started
  Installing build dependencies: finished with status 'done'
  Getting requirements to build wheel: started
  Getting requirements to build wheel: finished with status 'done'
  Preparing metadata (pyproject.toml): started
  Preparing metadata (pyproject.toml): finished with status 'done'
Would install raypoolkit-0.2.0
```

Only `raypoolkit` is reported.

**Pip's `Resolved … to commit 4e7eb153…`.** That is the *tag object*: pip prints the unpeeled ref
here. It is not the commit, and it is not the prompt's "installed commit is not `eaef0ce6`" stop,
which is judged on `pip freeze` and `direct_url.json` (K3.1 and K3.3 below). The "Installing build
dependencies" step fetches `setuptools>=64` into pip's isolated build environment, not the venv.

**K2, the install** (`./venv/bin/pip install -r requirements.txt`, exit 0), without the
"already satisfied" lines:

```text
Collecting raypoolkit @ git+https://github.com/ds283/RayPoolKit@v0.2.0 (from -r requirements.txt (line 79))
  Cloning https://github.com/ds283/RayPoolKit (to revision v0.2.0) to <tmp>
  Running command git clone --filter=blob:none --quiet https://github.com/ds283/RayPoolKit <tmp>
  Running command git checkout -q 4e7eb153ca9837c6aa6ec51d702b5fa6ccf08fef
  Resolved https://github.com/ds283/RayPoolKit to commit 4e7eb153ca9837c6aa6ec51d702b5fa6ccf08fef
  Installing build dependencies: started
  Installing build dependencies: finished with status 'done'
  Getting requirements to build wheel: started
  Getting requirements to build wheel: finished with status 'done'
  Preparing metadata (pyproject.toml): started
  Preparing metadata (pyproject.toml): finished with status 'done'
Building wheels for collected packages: raypoolkit
  Building wheel for raypoolkit (pyproject.toml): started
  Building wheel for raypoolkit (pyproject.toml): finished with status 'done'
  Created wheel for raypoolkit: filename=raypoolkit-0.2.0-py3-none-any.whl size=13565 sha256=8075fcd9f7e5b3c4c4d2db9392566dbc5ee340bd5a981850720ccbbd9775ad46
  Stored in directory: <tmp>/wheels/80/77/a3/ffb624468d503c9d448ab31dc36bc5e5207e20153b1e1fada4
Successfully built raypoolkit
Installing collected packages: raypoolkit
Successfully installed raypoolkit-0.2.0
```

**K3.1 — `pip freeze`** against the saved one: exactly one added line.

```text
78a79
> raypoolkit @ git+https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976
```

**K3.2 — `pip check`:** `No broken requirements found.`

**K3.3 — the provenance helper**, with `utilities.installed_package_provenance()` (no argument):
- `["raypoolkit"]` = `{'version': '0.2.0', 'url': 'https://github.com/ds283/RayPoolKit', 'commit':
  'eaef0ce60e2fc354bb32f0ad576010e6a83c1976', 'requested_revision': 'v0.2.0', 'editable': False}`
- `["datastorekit"]` = `None`
- `format_package_provenance` prints:

  ```text
  >> packages: datastorekit not installed; raypoolkit 0.2.0 (https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976, requested v0.2.0)
  ```

  There is no `git+` before the URL; the helper prints the `direct_url.json` URL as recorded.

The installed `raypoolkit-0.2.0.dist-info/direct_url.json`, verbatim:

```json
{"url": "https://github.com/ds283/RayPoolKit", "vcs_info": {"commit_id": "eaef0ce60e2fc354bb32f0ad576010e6a83c1976", "requested_revision": "v0.2.0", "vcs": "git"}}
```

A VCS record, with no `dir_info`: not editable, not a local path.

**K3.4 — the installed files are the tag's.** `git -C /Users/ds283/Documents/Code/RayPoolKit
archive v0.2.0 raypoolkit`, extracted into a new scratch directory, against
`venv/lib/python3.13/site-packages/raypoolkit/` (from `find_spec("raypoolkit")`).

| | `.py` files |
|---|---|
| archive, outside `raypoolkit/tests/` | 4: `__init__.py`, `RayWorkPool.py`, `_timing.py`, `contract.py` |
| archive, under `raypoolkit/tests/` | 9 |
| installed, anywhere (ignoring `__pycache__`) | 4: the same names |

- `cmp` of each of the four: identical, byte for byte.
- `raypoolkit/tests/` does not exist in the venv.
- No installed `.py` file is absent from the archive's non-test set.

Result: **the installed files are the tag's**.

**K3.5 — the import.** `./venv/bin/python -c "import ray; import raypoolkit; from raypoolkit import RayWorkPool; print(ray.is_initialized())"` prints `False`.

**The suites (§2.5)**, after the install, foreground and serially, from the repository root.

| Root | Baseline | Result | Load at start |
|---|---|---|---|
| `CosmologyModels/tests` | 18 | `Ran 18 tests`, `OK` | 4.53 / 6.46 / 28.40 |
| `Datastore/tests` | 66 | `Ran 66 tests`, `OK` | 4.53 / 6.46 / 28.40 |
| `ComputeTargets/tests`, `test_[a-f]*.py` | 61 | `Ran 61 tests`, `OK` (114 s) | 8.25 / 7.04 / 26.52 |
| `ComputeTargets/tests`, `test_[g-z]*.py` | 53 | `Ran 53 tests`, `OK` (176 s) | |
| `ComputeTargets/tests`, whole | 114 | `Ran 114 tests`, `OK` (254 s) | 5.77 / 8.57 / 21.80 |

No count moved. The load fell from 4.8 / 6.7 / 29 at the first check to 4.8 / 6.4 / 17 at the end.

## The deliberate-breakage record

None. 03 adds no code and no test (prompt §3.4). §2.4's checks are its proof, and each is one a
wrong install would fail: a version-only install lacks the VCS record, an editable install carries
`dir_info` and a different freeze line, a local-path install would show a file URL, and a stale or
partial install would fail the byte comparison.

## Observations not acted on

1. The helper's line has no `git+` prefix (`https://…@<commit>`), where a hand-written expectation
   might have `git+https://…`. It is the format log 01 chose; the prompt requires only URL, commit
   and requested revision, which it gives. Not changed.
2. Pip's `Resolved … to commit 4e7eb153…` names the tag object (see above). A reader comparing it
   with `eaef0ce6…` may take it for a mismatch.

## State handed to the next prompt

- The venv holds `raypoolkit 0.2.0`, installed from the GitHub tag `v0.2.0` (commit
  `eaef0ce60e2fc354bb32f0ad576010e6a83c1976`, `direct_url.json` a VCS record, not editable). Its
  `pip freeze` is `measure/out/52142d7-pip-freeze.txt` plus the one line
  `raypoolkit @ git+https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976`;
  `pip check` is clean. `datastorekit` is not installed.
- `requirements.txt` carries the pin on line 79. `ray==2.53.0` is unchanged.
- Nothing in ChamPBH imports `raypoolkit` yet. `RayTools/` is intact, so prompt 04's swap starts
  from the tree at `d7791c5`'s code plus this pin.
- The provenance line now reads
  `>> packages: datastorekit not installed; raypoolkit 0.2.0 (https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976, requested v0.2.0)`.
- Suites: 18 / 114 / 66, unchanged.
- **Rollback is `git revert` of `2f61943` plus `./venv/bin/pip uninstall -y raypoolkit`**: the
  venv is outside git (README §4.4).
