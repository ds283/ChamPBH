# Orchestrator — prompt 02, the patch

Read [`README.md`](README.md) and [`../README.md`](../README.md) §0.2 (P4–P9), §2 (b), (f), §4,
§6.2 first. **You do not write code.**

**The prompt:** [`../02-low-T-tolerance-patch.md`](../02-low-T-tolerance-patch.md)
**Board items:** S, N, K · **Closes:**
`[00-the-low-T-network-fails-near-1-keV-on-ulp-level-input]`,
`[00-a-store-serves-bbn-rows-from-another-prym-version-silently]`, and the assigned
`[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]`.

## 1. Before you dispatch

1. Prompts 01 and 01b landed with no unresolved miss. **The board's Decisions record the user's ruling on
   the setting (P3) and on P4–P7, dated after log 01.** If they do not, stop.
2. On branch `bbn-tolerance`; `git status` clean apart from the user's untracked run files; record
   `HEAD`.
3. The three suite counts and their wall-clocks.
4. The store's mtimes.
5. `venv/bin/black --check ComputeTargets/BBNData.py pipeline_selection.py main.py plot_by_beta.py ComputeTargets/tests/`.
   (`main.py` and `plot_by_beta.py` may not be clean already; record it.)
6. **Your own baseline:** the tool on the control, `prod`, with
   `--lowT-rtol <ruled> [--lowT-atol <ruled>]` on `HEAD`. Keep the abundances.

## 2. Dispatch

One fresh-context subagent, **Opus**, using the template in `README.md`. Add:

> *The ruled setting is the one on the board's Decisions, dated <date>: rtol = <…>, atol = <…>.*
>
> *Your diff may touch:*
> - *`PRyM/PRyM_main.py` (the two low-T calls and their marker comments only);*
> - *`ComputeTargets/BBNData.py` (`PRYM_VERSION` and its comment only);*
> - *`pipeline_selection.py`, `main.py` and `plot_by_beta.py` (the warning only);*
> - *`ComputeTargets/tests/`;*
> - *`tools/bbn_from_store.py` (only if a field is needed);*
> - *the log, this campaign's board, the `review-remediation` board (a Resolved line only), and
>   `.documents/OPEN_ISSUES.md`.*

## 3. The review — seven checks

1. **Allowed files.** The `PRyM/PRyM_main.py` hunk is the two calls and their comments. The
   `BBNData.py` hunk is the version and its comment.
2. **The breakage, run by you.** Revert `PRyM/PRyM_main.py` to `HEAD~1`; test (a) must fail
   because the low-T calls carry no `rtol`. Then revert the three driver and helper files; the
   `ast` part of test (b) must fail. Restore both and confirm `git status` is clean.
3. **The digit-for-digit check, run by you.** The tool on the control, `prod`, **no override**,
   must equal your §1.6 baseline to every printed digit. Then spot-check one failure, β = 1.345 at
   M = 10⁻³, against log 01's override figure.
4. **The bounds.** `git diff HEAD~1 -- ComputeTargets/tests/` changes constants and comments
   only; no tolerance literal in an assertion changed. Each re-pinned constant keeps its old value
   in a comment.
5. **The warning warns only.** Read the calls in `main.py` and `plot_by_beta.py`: nothing after
   the call depends on its result.
6. **Suites.** All three pass; the counts are not lower; `black --check` is clean on the
   non-`PRyM/` files the prompt changed.
7. **The store.** The mtimes are unchanged.

Then the board: S, N and K done; three issues closed by README §5 rule 4; the
`review-remediation` board carries the `[03-…]` Resolved line; the index corrected.

## 4. Report, then stop

As `README.md` says.
