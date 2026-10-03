# Orchestrator — prompt 02, the small network and the patch

> **Rewritten 2026-10-03 with prompt 02, after the user's ruling U4 on log 01c.** The first
> version is in `git log` (this file at `f3fa41a`).

Read [`README.md`](README.md) and [`../README.md`](../README.md) §0.2 (P4–P9, U4, P14–P16), §2
(b), (c′), (f), §4, §6.2 first. **You do not write code.**

**The prompt:** [`../02-low-T-tolerance-patch.md`](../02-low-T-tolerance-patch.md)
**Board items:** S, N, K · **Closes:**
- `[00-the-low-T-network-fails-near-1-keV-on-ulp-level-input]`;
- `[00-a-store-serves-bbn-rows-from-another-prym-version-silently]`;
- the assigned `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]`.

## 1. Before you dispatch

1. Prompt 01c landed, and its ruling is complete. **The board's Decisions must record:**
   - U4;
   - the user's confirmation that `plot_by_beta.py` follows `main.py`;
   - the user's acceptance of P14–P16.

   If any is missing, stop. Prompt 01b need not have landed.
2. On branch `bbn-tolerance`; `git status` clean apart from the user's untracked run files; record
   `HEAD`.
3. The three suite counts and their wall-clocks.
4. The store's mtimes (`README.md` rule 10, with `/usr/bin/stat`).
5. `venv/bin/black --check` on `ComputeTargets/BBNData.py config/version.py pipeline_selection.py
   main.py plot_by_beta.py tools/bbn_baseline.py tools/bbn_from_store.py ComputeTargets/tests/`.
   Record any file that is not clean already.
6. **Your own baseline, on `HEAD` before dispatch.** Run the tool on the control β = 1.6,
   M = 10⁻³, `prod`, twice:
   - `--small-network --lowT-rtol 1e-6`;
   - the default network with `--lowT-rtol 1e-5`.

   Keep the abundances.

## 2. Dispatch

One fresh-context subagent, **Opus**, using the template in `README.md` with `NN-<name>` =
`02-low-T-tolerance-patch`. Add:

> *The ruled settings are U4's, on the board's Decisions: production runs the small network; its
> low-T call gets rtol 1e-6 (atol 1e-11 unchanged); the full network's low-T call gets rtol 1e-5
> (atol 1e-15 unchanged). P14–P16 are accepted.*
>
> *Your diff may touch only the files in your prompt's "Allowed files" list, and in each only
> what that list names. Your acceptance runs may go 8–10 solves at a time (P14); the cost
> measurement runs alone, after them. You may read logs 01 and 01c and their probes; do not read
> the other prompts.*

## 3. The review — eight checks

1. **Allowed files.** `git diff --stat HEAD~1` names nothing outside the list.
   - The `PRyM/PRyM_main.py` hunk is the two low-T calls and their comments.
   - The `ComputeTargets/BBNData.py` hunk is the version, its comment, one default and two
     fallbacks.
   - `config/version.py` gains lines and loses none.
   - `tools/bbn_from_store.py` changes in help text only.
2. **The breakage, run by you.**
   - Revert `PRyM/PRyM_main.py` to `HEAD~1`. Test (a) must fail, because the low-T calls carry no
     `rtol`.
   - Restore it. Then revert `main.py`, `plot_by_beta.py` and `pipeline_selection.py`. The `ast`
     part of test (b) and `test_network_flag` (c) must fail on the values (`False`, or a missing
     call), not only on an import.
   - Restore both, and confirm that `git status` is clean.
3. **The digit-for-digit check, run by you.** The tool on the control, `prod`, **no override**,
   small network and full, must equal your §1.6 baselines to every printed digit. Then spot-check
   one failure, β = 1.345 at M = 10⁻³, small network, against log 01c's T1 row at 1e-6.
4. **The reproduction counts.** The log reports 49 of 49 small-network rows identical to log 01c
   and 17 of 17 full-network rows identical to log 01's S1 at 1e-5. Check two of each in its
   CSV against the logs yourself.
5. **The bounds.** `git diff HEAD~1 -- ComputeTargets/tests/` changes constants, comments, one
   test's network (P16), `test_network_flag` (c)'s expected values, and `OVERRIDE_RTOL`. No
   tolerance literal in an assertion changed. Each re-pinned constant keeps its old value in a
   comment, and equals §1 (e)'s expected value.
6. **The flag and the warning.**
   - `main.py` has one name for the network, assigned `True` once. Its comment carries the Li8
     fragility as prompt 02 §1 (c) lists it. The payload and the warning use that name.
   - `plot_by_beta.py` does the same for its baseline.
   - No command-line flag was added.
   - Nothing after either warning call depends on its result.
7. **Suites.** All three pass. The counts are not lower than §1.3 (ComputeTargets grows by the new
   tests). `black --check` is clean on the non-`PRyM/` files the prompt changed.
8. **The store.** The mtimes are unchanged.

Then the board:
- S, N and K done;
- three issues closed by README §5 rule 4;
- the Li8 issue narrowed;
- the `review-remediation` board carries the `[03-…]` Resolved line, with the spread before and
  after;
- the index corrected.

## 4. Report, then stop

As `README.md` says. In addition, quote the ⁷Li/H shift `test_network_flag` (b) prints, and the
text of `main.py`'s comment. Tell the user that prompt 03 needs prompt 01b landed first, if it has
not.
