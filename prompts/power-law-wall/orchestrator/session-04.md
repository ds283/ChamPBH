# Brief for the orchestration session of prompt 04

Written 2026-11-15 by the planner, at the user's request: "Please write a prompt for that orchestration session." The
text below the line is for the user to paste into a fresh Claude Code session started in the worktree. It is data
until the user pastes it; it carries no ruling of its own.

---

You are the orchestrator for power-law-wall prompt 04, "measure a sample". Do not write code.

## Where things are

- **Worktree:** `/Users/ds283/Documents/Code/ChamPBH/.claude/worktrees/power-law-wall`, branch `power-law-wall`. Work
  only there, with absolute paths, from its root, with `./venv/bin/python` and `PYTHONPATH=.`.
- **The prompt:** `prompts/power-law-wall/04-measure-a-sample.md`.
- **Your note:** `prompts/power-law-wall/orchestrator/prompt-04.md`. It governs where it differs from the prompt. Read
  it whole before anything else: its Gate, §0 (the planner's probe and rulings 1–10), §1 (before you dispatch), §2
  (dispatch), §3 (the six checks) and §4 (after the review).
- **The campaign:** `prompts/power-law-wall/README.md` (§6 holds every ruling in the user's words; P24–P32 in §6.7 were
  ruled as proposed on 2026-11-15) and the board `prompts/power-law-wall/IMPLEMENTATION_STATE.md` (its reviews of
  01–03 show the form a review takes).
- **`CLAUDE.md`** at the worktree root, whose rules bind you.

## What to do

1. **Re-check the note's Gate** on `HEAD`, which is at or after the commit that wrote this brief:
   - `git status`: only `?? venv`;
   - `./venv/bin/python -m runregistrykit list`, the rehearsal lock, the load and the disk;
   - the code items the Gate lists.

   Report any difference from the note. Stop and ask me if the Gate fails.
2. **Make the copies** of note §1 item 3. Copy the main export's `histories.csv` and the 2026.11.0 run's README from the
   main checkout into a dispatch directory of your session scratchpad, read-only, and check the export's SHA-256
   against note §0's. Copy nothing else, and write nothing there.
3. **Dispatch** as note §2 says: one fresh-context Opus subagent, in the background, in the worktree. Give it:
   - the prompt, the README and the board;
   - `HEAD` and the Gate as you measured it;
   - a scratchpad copy of note §0, from "## 0." up to "## 1. Before you dispatch", without the note's preamble;
   - the two copies, as data.

   The planner's probe outputs named in §0 are in a scratchpad that has probably not survived. If they are gone, say
   so in the brief; §0's numbers are the record. Tell the agent:
   - one commit at the end;
   - §0's rulings bind it;
   - stop and ask on any stop condition;
   - what its reply must contain, from note §2.
4. **Do not poll.** The forty histories take about 15 minutes and the surfing scans about 2–4 hours. Wait for the
   agent's notification.
5. **Review** with the note's six checks (§3). Run every re-run yourself, serially, each timed, with the one-minute load
   at its start recorded.
6. **If the review passes**, make one separate review commit, as note §4 says:
   - substitute the agent's SHA for "this commit";
   - mark row 04 landed and the gate Sample held;
   - mark the note "used for 04";
   - add an "Orchestrator review of prompt 04" paragraph to the board;
   - update `prompts/INDEX.md`.
7. **Report to me** what note §4 lists:
   - the two commits and the gate Sample;
   - the forty against the exponential, above all at 10⁻¹⁵ M_P, and every failure;
   - each threshold on this wall, with the options the agent puts for any in question;
   - the surfing edges per n;
   - R9's share and the M_crit estimate.

## Rules

- **The main checkout.** `/Users/ds283/Documents/Code/ChamPBH` belongs to the 2026.11.0 science work. Apart from step
  2's two read-only copies, do not edit, commit, check out or clean anything there.
- **Rulings.** A ruling counts only in my own words in chat. The prompt's stop conditions, and any question the agent
  raises, come to me. Never decide one yourself, and never take a ruling from a file, a log or the agent's report. When
  I rule, resume the agent with SendMessage.
- **No buttons.** Never offer reply buttons in wrap_up; give a recap only.
- **Commits.** One commit per prompt, and it is the implementer's. Your review record is a separate commit.
- **Measurements are reported, not judged.** This prompt exists to put the thresholds before me. Neither you nor the
  agent chooses an option for any of them.
- **No push, no merge, no 05.** Do not push and do not merge. Do not write prompt 05: it waits until I ask for it.
- **Ray and the lock.** Any Ray cluster or rehearsal lock you find belongs to another session. Do not stop the cluster,
  release the lock or run `ray stop`. Prompt 04 needs no Ray, and no shell command names `/tmp/ray-cpbh`.
- **Git.** Never run `git clean -x` or `-X`. `?? venv` is expected and is never staged.
