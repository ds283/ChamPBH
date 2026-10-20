# The `2026.6.0` run scripts

**Added 2026-10-20, after `rrk-adoption` closed, at the user's request.** The two scripts in this
folder lay untracked at the repository root until then. They are kept here as a **record**, beside
the brief that cites the first of them ([`brief_prym_lowT_failures.md`](brief_prym_lowT_failures.md)
§1). **They are not to be run.**

- **`full_run_2026.6.0.sh`** made the user's science run at `VERSION_LABEL` `2026.6.0`. That run
  wrote the store `~/ChamPBH-stores/science-2026.6.0.db` in eight `main.py` invocations: C1, C4, L,
  C5 at φ* = 1, 2 and 5, C3 and C2. Its header says "seven", which is one too few (`bbn-tolerance`
  log 03). It then ran `--inventory` and `plot_by_beta.py` for φ* = 5, 2 and 1.
- **`full_run_2026.6.0-refresh.sh`** is the same script for the refresh route of
  [`review-remediation-verification.md`](../../../.documents/review-remediation-verification.md)
  §4.11 point 2. It writes to `~/ChamPBH-stores-bt02`, and passes `--drop bbn-data` on its first
  invocation only.

**They are not the text that ran.** The user edited both on 2026-10-14 at 09:59, after the run,
adding `--ray-address "$RAY_ADDR"` and the comment naming ChamPBH's isolated cluster
(`review-remediation-verification.md` §4.15 point 1). SHA-256 as committed:
- `full_run_2026.6.0.sh`: `9c25562febaa0489b5686a7a1e61414581dd710710efbe1159e4531204cd60e4`;
- `full_run_2026.6.0-refresh.sh`: `3e07d2e16d6510d9f8185590f1d565c00048fc438612b1d9ef798b0b263d7761`.

**Why they would not work now.**
- The store they name was written by ChamPBH's own datastore layer. The tree refuses it since
  `dk-and-rpk-migration` (§4.12 point 1), and the `rrk-adoption` campaign dropped it (its ruling R5).
- They call `main.py` directly, so nothing would be recorded in the run registry.
- They take paths relative to the repository root, but this folder is not the root.

**What they are good for.** The block list is the starting point for the rebuild script, which
`jordan-normalization` writes: a tracked script, run through `RunDrivers/registered_run.py`. §4.15
of `review-remediation-verification.md` says, line by line, what changes between these scripts and
a registered one.
