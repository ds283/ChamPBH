# rrk-adoption campaign — implementation state

**Last updated:** 2026-10-19 · **Status: PLANNED — 0 of 5 prompts written; R1–R7 ruled by the
user 2026-10-19 (README §6.0); awaiting the rulings on README §6.1 (U1–U5) and §6.2 (P1–P13).**
The plan is [`README.md`](README.md), written on 2026-10-19 on branch `rrk-adoption`, cut from
`main` at `3cd2be4`.

The campaign adopts RunRegistryKit `v0.2.0`, with its store layer:
- the pin, `runregistrykit.toml`, `var/` ignored, and the rules in `CLAUDE.md`;
- a fingerprinter at format 1, with a guard and a golden fingerprint;
- a wrapper driver, `tools/registered_run.py`, which registers a `main.py` run and leaves `main.py`
  unchanged and launchable without it;
- a rehearsal of registered, interrupted and hard-killed builds on the isolated cluster.

It comes before `jordan-normalization`, so that the science rebuild is a registered run.

**Campaign:** [`README.md`](README.md) ·
**Code:** `requirements.txt`, `runregistrykit.toml`, `.gitignore`, `config/fingerprint.py`, a new
`tools/registered_run.py`; `main.py` unchanged ·
**Verification:** `.documents/rrk-adoption-verification.md` (created by 04) ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) (no section: none opened)

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, or an issue assigned to this campaign closes, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md)
> is updated **in the same commit**. See `CLAUDE.md`.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | The pin, the config and the rules | `runregistrykit[stores]@v0.2.0`; `runregistrykit.toml`; `var/` ignored; `CLAUDE.md`'s rules | Sonnet | ⬜ | ⬜ | — | — |
| 02 | The fingerprinter | `config/fingerprint.py` at format 1; `[stores]`; the format guard; a golden fingerprint | Sonnet | ⬜ | ⬜ | — | — |
| 03 | The wrapper | `tools/registered_run.py`: register, beat from stdout, signals after `ray.init`, `main.py` by `runpy`, finish with the fingerprint | Opus | ⬜ | ⬜ | — | — |
| 04 | Rehearse registered runs | a registered build; `SIGTERM` and resume; `SIGKILL`, `end` and resume; `store create`; stores compared; `histories.csv` = R0's | Opus | ⬜ | ⬜ | — | — |
| 05 | Close | documents, handover, RunRegistryKit's gate drafts, indexes, board | Sonnet | ⬜ | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

**Held, by design (README §2.1).** 01–05 are written once the user has ruled on README §6.1 and §6.2.

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
