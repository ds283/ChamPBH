# rrk-adoption campaign — implementation state

**Last updated:** 2026-10-19 · **Status: LIVE — 5 of 5 prompts written, 0 landed. R1–R7, U1–U5
and P2–P13 ruled by the user on 2026-10-19, as proposed; P1 amended (the wrapper in `RunDrivers/`)
(README §6).**
The plan is [`README.md`](README.md), written on 2026-10-19 on branch `rrk-adoption`, cut from
`main` at `3cd2be4`.

The campaign adopts RunRegistryKit `v0.2.0`, with its store layer:
- the pin, `runregistrykit.toml`, `var/` ignored, and the rules in `CLAUDE.md`;
- a fingerprinter at format 1, with a guard and a golden fingerprint;
- a wrapper driver, `RunDrivers/registered_run.py`, which registers a `main.py` run and leaves `main.py`
  unchanged and launchable without it;
- a rehearsal of registered, interrupted and hard-killed builds on the isolated cluster.

It comes before `jordan-normalization`, so that the science rebuild is a registered run.

**Campaign:** [`README.md`](README.md) ·
**Code:** `requirements.txt`, `runregistrykit.toml`, `.gitignore`, `config/fingerprint.py`, a new
`RunDrivers/registered_run.py` and its test root `RunDrivers/tests/`; `main.py` unchanged ·
**Verification:** `.documents/rrk-adoption-verification.md` (created by 04) ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) (no section: none opened)

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, or an issue assigned to this campaign closes, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md)
> is updated **in the same commit**. See `CLAUDE.md`.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [The pin, the config and the rules](01-pin-config-and-rules.md) | `runregistrykit[stores]@v0.2.0`; `runregistrykit.toml`; `var/` ignored; `CLAUDE.md`'s rules | Sonnet | ✍️ 2026-10-19 | ⬜ | — | — |
| 02 | [The fingerprinter](02-the-fingerprinter.md) | `config/fingerprint.py` at format 1; `[stores]`; the format guard; a golden fingerprint | Sonnet | ✍️ 2026-10-19 | ⬜ | — | — |
| 03 | [The wrapper](03-the-wrapper.md) | `RunDrivers/registered_run.py`: register, beat from stdout, signals after `ray.init`, `main.py` by `runpy`, finish with the fingerprint | Opus | ✍️ 2026-10-19 | ⬜ | — | — |
| 04 | [Rehearse registered runs](04-rehearse-registered-runs.md) | a registered build; `SIGTERM` and resume; `SIGKILL`, `end` and resume; `store create`; stores compared; `histories.csv` = R0's | Opus | ✍️ 2026-10-19 | ⬜ | — | — |
| 05 | [Close](05-close.md) | documents, handover, RunRegistryKit's gate drafts, indexes, board | Sonnet | ✍️ 2026-10-19 | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

**Held, by design (README §2.1).** 01–05 are written once the user has ruled on README §6.1 and §6.2.
*Written 2026-10-19 (§5.1).*

Orchestrator notes are written one prompt at a time, in [`orchestrator/`](orchestrator/), just
before each dispatch.

### 1.1 Items

| Item | Prompt | What | State |
|---|---|---|---|
| **C1** | 01 | `runregistrykit[stores] @ …@v0.2.0` in `requirements.txt`; the venv installed from it, not editable | ⬜ |
| **C2** | 01 | `runregistrykit.toml` (roots, distributions; no `[stores]`); `var/` in `.gitignore` | ⬜ |
| **C3** | 01 | `CLAUDE.md`: the section "Long-running jobs — the run registry", with the rules and `git clean -x` forbidden | ⬜ |
| **C4** | 01 | `tools/tests/test_registry_config.py`: the config, the ignore rule, the pin | ⬜ |
| **F1** | 02 | `config/fingerprint.py`: `FINGERPRINT_FORMAT = 1`, `SPEC_DIGESTS`, `fingerprinter` | ⬜ |
| **F2** | 02 | `[stores] fingerprinter` in `runregistrykit.toml` | ⬜ |
| **F3** | 02 | `ObjectFactories/tests/test_fingerprint_format_guard.py`: the Q5 guard, keys kept by type | ⬜ |
| **F4** | 02 | `ObjectFactories/tests/test_store_fingerprint_golden.py` and its golden file; the loader and the command line | ⬜ |
| **W1** | 03 | the package `RunDrivers/` and its test root; `RunDrivers/registered_run.py` (P1 as amended) | ⬜ |
| **W2** | 03 | the wrapper's flags and refusals before `begin()`, exit 2 (P2, P3, U2) | ⬜ |
| **W3** | 03 | registration: `begin` with `script` = the program (U3), `scope`, `heartbeat_means`; the stream proxies | ⬜ |
| **W4** | 03 | `ray.init` wrapped; the handler installed after it (P5) | ⬜ |
| **W5** | 03 | `main.py` by `runpy.run_path` (P7); `finish_after_exec` with the fingerprint (P6); `** driver exit status N` | ⬜ |
| **W6** | 03 | `RunDrivers/tests/test_registered_run*.py`, with no Ray; `CLAUDE.md`'s test command and `RunDrivers/` sentence | ⬜ |
| **H1** | 04 | `reference`, `done`; `store create` and the fingerprint | ⬜ |
| **H2** | 04 | `resumed-a`, `SIGTERM`ed, `killed`/143, with the signal's timing (P5) | ⬜ |
| **H3** | 04 | `resumed-b`, `SIGKILL`ed: `stale`, the resume refused, `end`, `lost` (U5) | ⬜ |
| **H4** | 04 | `resumed-c`, `done`, its fingerprint written to the sidecar | ⬜ |
| **H5** | 04 | the stores compared; `histories.csv` equal to R0's | ⬜ |
| **H6** | 04 | `.documents/rrk-adoption-verification.md` §1 | ⬜ |
| **Z1** | 05 | `architecture-summary.md` | ⬜ |
| **Z2** | 05 | `CLAUDE.md` | ⬜ |
| **Z3** | 05 | the handover, `.documents/review-remediation-verification.md` §4.15 | ⬜ |
| **Z4** | 05 | RunRegistryKit's G3 (`extraction`) and G2 (`interrupted-runs`) drafted, in the verification document §2 | ⬜ |
| **Z5** | 05 | the board closed; `prompts/INDEX.md`; `.documents/OPEN_ISSUES.md` | ⬜ |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Pin | the venv holds `runregistrykit` 0.2.0 from the tag, not editable; `list` runs on the empty runs root; `var/` is ignored | 01 | ⬜ |
| Fingerprint | the config's fingerprinter loads; the format guard and the golden fingerprint pass, and each deliberate breakage fails | 02 | ⬜ |
| Wrapper | with no Ray, a run through the wrapper is registered before its first line, beats, ends `done`/`failed`/`killed` with `main.py`'s exit code, takes its fingerprint after the program returns, and refuses a store an unfinished run names; `main.py` is unchanged | 03 | ⬜ |
| R | on the isolated cluster, the four builds are recorded as README §2.2 (04) says, the resumed store equals the reference, and `histories.csv` has R0's SHA-256 | 04 | ⬜ |
| RunRegistryKit G3 / G2 | recorded in RunRegistryKit, on its `extraction` and `interrupted-runs` boards, by the user from 05's drafts (P12) | 05 | ⬜ |

## 3. Active and unresolved issues

None opened by this campaign.

### 3.1 Assigned to this campaign

None.

## 4. Resolved issues

None.

## 5. Dispatch record

### 5.0 Planning (2026-10-19)

Planned at the user's request. The user asked whether RunRegistryKit could manage "our own
long-running and persistent jobs that have archival value", and the evaluation found it feasible.
- **The package.** `v0.2.0`'s suite passed (410 tests, one skipped), and its code is unchanged since
  the tag. A dry-run install into this venv adds only `runregistrykit`.
- **SGK's adoption is the pattern.** It uses a wrapper driver; beats from the pipeline's stdout;
  installs its signal handler after `ray.init`; finishes with the fingerprint after the pool
  closes; and leaves `store create` to a person after the first close (README §0.2).
- **R1–R7** were ruled by the user on 2026-10-19 (README §6.0):
  - the store layer;
  - a wrapper, so that `main.py` stays launchable without the package;
  - naming, heartbeat and sidecar as SGK does;
  - stores in `var/`;
  - the old stores dropped, not adopted;
  - the dirty flag cleared by cleaning up the root;
  - this campaign before `jordan-normalization`.
- **Measured** at `3cd2be4`, read-only (README §0.3); suites 18 / 176 (61 + 115) / 69 / 79.
- **No issue** opened or assigned (P13).

### 5.1 Ruled, and prompts 01–05 written (2026-10-19)

The user's rulings:
- **U1–U5 accepted as proposed:**
  - U1: `utilities.installed_package_provenance` kept, so that no driver imports the package;
  - U2: `--inventory` refused by the wrapper;
  - U3: the manifest's `script` is `main.py`;
  - U4: the run scripts get a handover only;
  - U5: the hard-kill route is rehearsed.
- **P1 amended**: "SecondaryGWKit placed the scripts in `RunDrivers/` and kept `tools/` for
  repo-level tooling. Let's do the same for consistency." The wrapper is
  `RunDrivers/registered_run.py`, with a new test root, `RunDrivers/tests/`. 03 adds the root to
  `CLAUDE.md`'s commands.
- **P2–P13 accepted as proposed.**

An earlier reply had been sent with the text "U1–U5 and P1–P13 accepted as proposed". The user had
not agreed to it, and said so. No ruling was recorded from it, and these are the user's own words.

Prompts 01–05 were written the same day, from the measurement of README §0.3 and a read of
SecondaryGWKit at `031bcb4`. Its `RunDrivers/scoped_pipeline_run.py` (moved there from
`docs/gktk-remedial/` by `031bcb4`) is the model for 03, and the prompts cite it at that commit. Three
choices made while writing them, each within its charter:
- **03 refuses an address that `ray_address_problem` refuses**, as well as no database and
  `--inventory` (README §2.2). Otherwise `main.py` would refuse it after registration, and a typing
  error would be recorded as a `failed` run.
- **03's refusals exit 2**, as ChamPBH's drivers do, where SecondaryGWKit's wrapper exits 1.
- **04 launches every build through `sh`**, which reaps the driver when it is killed. RunRegistryKit's
  `[04-a-killed-process-not-yet-reaped-answers-kill-0]` would otherwise make `end` refuse the
  `SIGKILL`ed run. Its launch logs and pid files are under `var/rrk-adoption-04/`, beside the stores
  (R4).
