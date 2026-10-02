# Orchestrator — prompt 07, extraction and figures

Read [`README.md`](README.md) and [`../README.md`](../README.md) §2 (k), §6.8 first. **You do not
write code.**

**The prompt:** [`../07-extraction-and-figures.md`](../07-extraction-and-figures.md)
**Board item:** G · **Closes:** `[00-no-extraction-for-the-science-figures]`

> **Amended 2026-10-02 (the user's ruling U6; `../README.md` §0.2).** A first dispatch landed
> `a2deb00`. It was reverted in `8fcb295` because it removed `_do_not_populate` to read the
> fixed-`T` values from the samples, which is the prompt's first stop condition. The review
> passed that over as a deviation. Changes to this orchestrator:
> - **§1.1:** prompt **06b** has landed, not just 06.
> - **§2:** the dispatch adds: *"If the prompt's stop conditions apply, stop and ask. Recording
>   a deviation is not a substitute for stopping."*
> - **§3 check 2:** read *three* functions; `value_at_T_Jordan` is withdrawn.
> - **§3 check 3:** (a), (b), (d) and (e) pass; (c) is withdrawn.
> - **New check 6, `_do_not_populate`.** `grep -n _do_not_populate plot_by_beta.py` on `HEAD`
>   and on `HEAD~1` shows the same lookups. Figure 4 and the CSV read
>   `ScalarModel.fixed_T_values`. A removal is a stop, whatever the log calls it.
> - **New check 7, the stop conditions.** For each of the prompt's §5 stop conditions, say
>   whether it applies.

## 1. Before you dispatch

1. Prompt 06 landed with no unresolved miss; on branch `science-readiness`; `git status` clean;
   record `HEAD`.
2. Baselines: the three suite counts and wall-clocks. Record the list of figure names
   `plot_by_beta.py` writes (`grep -n "fig_path\|savefig" plot_by_beta.py`).
3. `venv/bin/black --check extract_common.py plot_by_beta.py config/argument_parser.py`.

## 2. Dispatch

One fresh-context subagent, **Sonnet**, using the template in `README.md`. Add:

> *Your diff may touch:*
> - *`extract_common.py`;*
> - *`plot_by_beta.py`;*
> - *`config/argument_parser.py` (one option);*
> - *the test package for them;*
> - *`prompts/science-readiness/logs/07-figures/`;*
> - *the log; this campaign's board; and `.documents/OPEN_ISSUES.md`.*
>
> *Not any compute target, factory, or `.documents/`.*

## 3. The review — five checks

1. **Allowed files.** `git diff --stat HEAD~1 HEAD`.
2. **The functions are pure.** None of the four takes a pool, a datastore or a Ray handle (read
   their signatures).
3. **The tests, run by you.** (a)–(e) pass. Open the four PNGs in `logs/07-figures/`. They are
   labelled as synthetic, and each shows what README §2 (k) says it shows.
4. **Existing figures unchanged.** The figure names from your §1.2 record are all still written.
   The only change to an existing figure is the max |Q| caption.
5. **Suites.** All three pass and rise.

Then the board: G done; the issue closed; the index corrected.

## 4. Report, then stop

As `README.md` says.
