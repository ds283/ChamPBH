# Orchestrator prompts — the bbn-tolerance campaign

Three prompts, one per campaign prompt. Each dispatches one fresh-context subagent, reviews its
commit against fixed criteria, and either continues or stops and reports to the user.

| Orchestrator | Campaign prompt | Character |
|---|---|---|
| [`prompt-01.md`](prompt-01.md) | 01, the mechanism and the scan | Measurement and a tool. The review: the reproduction (run by you), the override touches one call, no production file changed, no store written, a recommendation by P3's rule |
| [`prompt-02.md`](prompt-02.md) | 02, the patch | Two vendored lines, a version, re-pinned constants, a warning. The review: the test fails on `HEAD~1`, the patched tree reproduces log 01 digit for digit, bounds unchanged |
| [`prompt-03.md`](prompt-03.md) | 03, documents and close-out | Additive only. The review is the `--numstat` and the roster |

## Running one

Start a fresh context with, for example:

> Read `prompts/bbn-tolerance/orchestrator/prompt-01.md` and follow it.

Run 01 → 02 → 03 in order. **Between 01 and 02 the user rules** on log 01's recommendation (README
§0.2 P3) and on P4–P7. Orchestrator 02 checks that the board's Decisions record that ruling. Do not
start one with an earlier miss unresolved.

**Take the baselines each orchestrator names before dispatching anything.** They cannot be
reconstructed after the fact. **Wall times are taken serially on an idle machine** (README §2 (d)).
Prompt 01's parallel scan (U1) gives outcomes, not costs.

## The rules that bind the orchestrator

1. **You do not write code.** Not a fix, not a test, not a docstring, not a document.
2. **One prompt, one subagent, one commit.**
3. **Do not re-derive the work.** Check four things:
   - that the prompt's own tests pass when *you* run them;
   - that the log classifies every deviation;
   - that the board and `.documents/OPEN_ISSUES.md` were updated in the same commit;
   - that the diff stayed inside its allowed files.
4. **Give each subagent only its own prompt.** Do not let it read the other prompts in the
   campaign; a prompt that knows what comes next starts optimising for it.
5. **Relay every subagent question verbatim.** Do not answer it yourself.
6. **Stop rather than repair.** Do not fix a failed check, revert it, or dispatch a follow-up agent
   to patch it. Report the specific check and what the log says about it.
7. **An agent must never assume `HEAD` is its own.** Planning and orchestration commits land on
   the same branch. Every dispatch says so.
8. **A test that passes both before and after proves nothing** (campaign README §0.4). Run the
   breakage check, or its stand-in, yourself.
9. **Every number in a log carries its provenance** (README §5 rule 8). A bare number is a failed
   check.
10. **The science store is read-only.** Before dispatch, record
    `stat -f '%m %N' ~/ChamPBH-stores/science-2026.6.0*.db`; after the landing, compare. Any
    change is a stop.

## The dispatch template

For prompt NN, launch a subagent with **exactly** this context, with the model the campaign
README §3 names:

> You are the implementation agent for one prompt in a campaign. Read, in this order:
> `CLAUDE.md`, `prompts/bbn-tolerance/README.md`,
> `prompts/bbn-tolerance/IMPLEMENTATION_STATE.md`, then your prompt
> `prompts/bbn-tolerance/NN-<name>.md` and everything its "Read first" list names.
> Execute the prompt exactly. **Do not read the other prompts in this campaign.** Follow README §5
> for the commit, the log, the board and `.documents/OPEN_ISSUES.md`. Run everything from the
> repository root with `venv/bin/python`. Open the science store only read-only, through the
> tool. Other commits may land on this branch while you work: make exactly one commit, and do
> not amend, reset or rebase anything you did not create. If you need to change a commit you
> already made and it is no longer `HEAD`, stop and say so rather than rewriting. When you
> finish, reply with:
> - the commit SHA;
> - the **Result** line from your log;
> - the "State handed to the next prompt" section, verbatim;
> - the per-package suite counts before and after;
> - every deviation, with its classification tag.

## Before every dispatch

1. `git status` clean apart from the user's untracked run files (`pilot-*`, `full_run_*.sh`);
   on branch `bbn-tolerance`; record `HEAD`.
2. Suite counts, recorded (18, 103 and 31 at `8efc50f`, unchanged through `4ae25b4`):
   ```bash
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t . 2>&1 | grep -E "^Ran|^OK|FAILED"
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . 2>&1 | grep -E "^Ran|^OK|FAILED"
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s Datastore/tests -t . 2>&1 | grep -E "^Ran|^OK|FAILED"
   ```
3. `venv/bin/black --check` on the non-`PRyM/` Python files the prompt may touch, so that a
   pre-existing formatting difference is not attributed to the agent.
4. The store's mtimes (rule 10).

## The breakage check, generically

For a new test module `T` and a changed production file `F`:

```bash
git checkout HEAD~1 -- F
PYTHONPATH=. ./venv/bin/python -m unittest T 2>&1 | tail -20      # must FAIL, for the stated reason
git checkout HEAD -- F
PYTHONPATH=. ./venv/bin/python -m unittest T 2>&1 | tail -5       # must pass
git status                                                         # must be clean
```

A failure that is only an `ImportError` of a new symbol is **not** the check. The test must fail
because the old code gives the wrong answer. Where the prompt names a stand-in measurement
instead, run it yourself and compare with the log.

## After every landing, report to the user

Report:
- the commit;
- the Result line;
- each review check, with pass or fail;
- the numbers against the acceptance table;
- the issues opened, narrowed or closed;
- the suite counts.

Then **stop**. The user starts the next orchestrator.
