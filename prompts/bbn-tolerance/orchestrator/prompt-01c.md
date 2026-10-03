# Orchestrator — prompt 01c, the small network

Read [`README.md`](README.md) and [`../README.md`](../README.md) §0.2 (U3, P10–P13), §2 (c′),
§4, §6.1c first. **You do not write code.**

**The prompt:** [`../01c-small-network-scan.md`](../01c-small-network-scan.md)
**Board item:** Q · **Closes:** nothing

## 1. Before you dispatch

1. **The board's Decisions record U3, and the user's acceptance of P10–P13.** If they record
   U3 alone, stop and ask.
2. On branch `bbn-tolerance`; `git status` clean apart from the user's untracked run files; record
   `HEAD`.
3. The three suite counts (18, 106, 31 after `ad2cafb`) and their wall-clocks.
4. The store's mtimes (`README.md` rule 10).
5. `git diff --stat HEAD -- tools/bbn_from_store.py` is empty, and `tools/bbn_from_store.py` is as
   `ad2cafb` left it: `git diff ad2cafb HEAD -- tools/` is empty.

## 2. Dispatch

One fresh-context subagent, **Opus**, using the template in `README.md` with `NN-<name>` =
`01c-small-network-scan`. Add:

> *Your diff may touch only `prompts/bbn-tolerance/logs/` (your log and `01c-probes/`),
> `prompts/bbn-tolerance/IMPLEMENTATION_STATE.md` and `.documents/OPEN_ISSUES.md`. No code, no
> tool, nothing in `PRyM/`. The scan may run 8–10 solves at a time (README §0.2 U1, P10); the
> cost measurement must run alone, after the scan. You may read prompt 01's log and probes;
> do not read the other prompts.*

## 3. The review — six checks

1. **Allowed files.** `git diff --stat HEAD~1` names nothing outside the list. In particular,
   `tools/`, `PRyM/` and every `.py` outside `logs/01c-probes/` are unchanged.
2. **The reproduction, run by you.** Run the tool with `--small-network --variant prod` on the
   control β = 1.6 at M = 10⁻³, at the default and at the recommended `rtol`. Both must equal log
   01's S3 rows and log 01c's T1 rows to every printed digit.
3. **The scan is complete.** Count `logs/01c-probes/scan.csv`: T1 has 245 rows (5 × 49), and T2
   has one row per history in `breadth.csv`. Missing cells are a deviation the log must name.
4. **Suites.** 18, 106 and 31, all passing.
5. **The store.** The mtimes are unchanged.
6. **The recommendation.**
   - It follows P11's rule from the log's own table, with every criterion's value quoted with
     provenance.
   - The cost reference is the full network at the default, timed in the same session.
   - The offset from the full network is reported per input, not bounded (P12).
   - Each stop condition of the prompt's §6 is addressed.

Then the board: Q done or stopped; the S issue carries a dated **Narrowed** line; the index's hook
and date corrected.

## 4. Report, then stop

As `README.md` says. **In addition, put the decision to the user plainly:**
- the recommended small-network setting and its four P11 values;
- the failure count in T1 and T2;
- the cost against today's production;
- the offset from the full network (median and maximum, Yp and D/H);
- the P13 questions that the rewrite of prompt 02 needs ruled.

Prompt 02 is rewritten only after the user's ruling is on the board.
