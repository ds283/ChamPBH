# 2026-10-03 — the `2026.6.0` science run

**Collected here 2026-10-20**, after `rrk-adoption` closed, at the user's request. Until then the two
run scripts lay untracked at the repository root, and briefly in `prompts/bbn-tolerance/source/`
(`1fc5d34`). The figure script was only in a Claude Science bundle in `~/Downloads`. The logs and the
figure's inputs were only beside the stores.

**Status: superseded.** These stores were written by ChamPBH's own datastore layer, and the tree has
refused them since `dk-and-rpk-migration` (`857ec73`; `.documents/review-remediation-verification.md`
§4.12 point 1). `rrk-adoption` dropped them rather than adopting them (its ruling R5), and
`jordan-normalization`'s rebuild replaces them. **Nothing here is to be run on today's tree**, and
the scripts would not work as they stand: their paths are relative to the repository root, and the
stores they name are refused.

## The run, and its refresh

| | The science run | The BBN refresh |
|---|---|---|
| When | 2026-10-03, 01:25–04:24 BST (`logs/science/`) | 2026-10-03 23:04 – 2026-10-04 00:17 BST (`logs/refresh-bt02/`) |
| Tree | `4ae25b4` most likely: the script's header says "tree 4ae25b4 or later", and nothing landed on `main` between `4ae25b4` (2026-10-02 18:19) and the run. The logs record no commit | `9b3db51` (2026-10-03 23:04:19), committed 24 s before the first block's log starts |
| Labels | `VERSION_LABEL` `2026.6.0`; `PRYM_VERSION` `bf24c3d+ri02+sr01` | the same label; `PRYM_VERSION` `bf24c3d+ri02+sr01+bt02` (PRyMordial's small network, low-T `rtol` 1e-6) |
| Store | `~/ChamPBH-stores/science-2026.6.0.db`, 16 shards, 710 histories | `~/ChamPBH-stores-bt02/science-2026.6.0.db`: a copy of the science store, with BBN dropped and recomputed |
| Script | `full_run_2026.6.0.sh` | `full_run_2026.6.0-refresh.sh` |
| Plots | `~/ChamPBH-stores/science-out-phi{5,2,1}/` | `~/ChamPBH-stores-bt02/science-out-phi{5,2,1}/` |

The run made eight `main.py` invocations: C1, C4, L, C5 at φ* = 1, 2 and 5, C3 and C2. The script's
header says "seven", which is one too few. It then ran `--inventory`, and `plot_by_beta.py` for
φ* = 5, 2 and 1. In the science run, 11 of the 684 φ* = 5 histories failed in PRyMordial's
low-temperature network. That is the subject of the `bbn-tolerance` campaign
(`prompts/bbn-tolerance/source/brief_prym_lowT_failures.md`). The refresh is that campaign's handover
route (`review-remediation-verification.md` §4.11 point 2): BBN recomputed on a copy of the store,
with no history recomputed.

## Where each file came from, and how that is known

- **`full_run_2026.6.0.sh`.**
  - **Its run plan is Claude Science's.** Blocks C1–C5 are the "Phase C — production roster" of
    `prompts/science-readiness/source/campaign_reevaluation_2026-10-01.md` (`:146-158`), a Claude
    Science artifact of 2026-10-01. The script adds block L, the C5 loop over φ* and the plot loop.
  - **Who wrote it is inferred, not recorded.** No Claude Code session in this repository wrote it or
    mentions it before 2026-10-03 09:22. Its last save before the run was 2026-10-03 00:15:18. A
    Claude Science session, "Evaluate readiness for science run", began at 22:58 BST on 2026-10-02
    (listed in the fig6 bundle's provenance record). That session is the likely author, but its
    workspace no longer exists, and the user may have written or adapted the script.
- **`full_run_2026.6.0-refresh.sh`.** Made by the user, as a copy of the first with the
  `--drop bbn-data` lines added. The user told a Claude Code session at 23:01 BST on 2026-10-03, "I have
  set up the refresh script", for §4.11 point 2. The two scripts share a birth time to the second, as
  a `cp -p` gives. Its first attempt failed because `--drop` was broken, fixed in `9b3db51`; the logs
  here are of the rerun.
- **Both scripts were edited on 2026-10-14** at 09:59 by a Claude Code session, the `read-only-plots`
  orchestrator, at the user's instruction: `RAY_ADDR`, `--ray-address` on `main.py` and on the plot
  loop, and the header comment naming `ray_cluster.sh` (`prompts/read-only-plots/IMPLEMENTATION_STATE.md:428-433`).
  **They are not the text that ran.** SHA-256 as kept here:
  - `full_run_2026.6.0.sh`: `9c25562febaa0489b5686a7a1e61414581dd710710efbe1159e4531204cd60e4`;
  - `full_run_2026.6.0-refresh.sh`: `3e07d2e16d6510d9f8185590f1d565c00048fc438612b1d9ef798b0b263d7761`.
- **`logs/science/` and `logs/refresh-bt02/`** are copied unchanged from `~/ChamPBH-stores/logs/`
  and `~/ChamPBH-stores-bt02/logs/`: each block's `main.py` output (`tee`'d by the script), the
  inventory, and the three plot runs.
- **`fig6_draft/`** is the draft BBN-constraint figure of the paper (`Paper1.tex`, and
  `BriefingNotes/bbn_constraints_explainer.tex`, in the Chameleon PBHs repository). It was made by
  Claude Science on 2026-10-04 from the refresh's `histories.csv`. Its notes are
  [`fig6_draft/NOTES.md`](fig6_draft/NOTES.md).

## For the rebuild

The block list is the starting point for `jordan-normalization`'s rebuild, which writes a tracked
script of its own and runs it through `RunDrivers/registered_run.py`. §4.15 of
`review-remediation-verification.md` says, line by line, what changes between these scripts and a
registered one. That rebuild's record belongs in a folder of its own beside this one.
