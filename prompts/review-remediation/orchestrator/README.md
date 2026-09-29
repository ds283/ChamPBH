# Orchestrator prompts — the review-remediation campaign

Six prompts, one per campaign prompt. Each dispatches one fresh-context subagent, reviews its
commit against fixed criteria, and either continues or stops and reports to the user.

| Orchestrator | Campaign prompt | Character |
|---|---|---|
| [`prompt-01.md`](prompt-01.md) | 01, the temperature-law harness | No production code. The review is about whether the reference is independent and whether the characterisation numbers are the audit's |
| [`prompt-02.md`](prompt-02.md) | 02, the entropy-derivative fix | Tiny diff. The review is the deliberate-breakage check against `HEAD~1`, run by you |
| [`prompt-03.md`](prompt-03.md) | 03, PRyMordial's passenger | A vendored file is patched. The review is that the patch is inert and the stall is gone |
| [`prompt-04.md`](prompt-04.md) | 04, ratio splines and a baseline | A refactor of the interface. The review is the conservation identity and the synthetic bounds |
| [`prompt-05.md`](prompt-05.md) | 05, kicking function and EOS hygiene | Tests and a document. The review is provenance on every number |
| [`prompt-06.md`](prompt-06.md) | 06, close-out | Verification only. The rule that it may not touch production code is the review |

## Running one

Start a fresh context with, for example:

> Read `prompts/review-remediation/orchestrator/prompt-01.md` and follow it.

Run 01 → 06 in order. Each orchestrator's preconditions include the previous prompt's landing;
do not start 02 with a prompt-01 miss unresolved.

**Take the baselines each orchestrator names before dispatching anything.** They cannot be
reconstructed after the fact.

## The rules that bind the orchestrator

1. **You do not write code.** Not a fix, not a test, not a docstring, not a document.
2. **One prompt, one subagent, one commit.**
3. **Do not re-derive the work.** Check that the prompt's own tests pass when *you* run them,
   that the log classifies every deviation, that the board and `.documents/OPEN_ISSUES.md` were
   updated in the same commit, and that the diff stayed inside its allowed files.
4. **Give each subagent only its own prompt.** Do not let it read the other prompts in the
   campaign; a prompt that knows what comes next starts optimising for it.
5. **Relay every subagent question verbatim.** Do not answer it yourself.
6. **Stop rather than repair.** Do not fix a failed check, revert it, or dispatch a follow-up
   agent to patch it. Report the specific check and what the log says about it.
7. **An agent must never assume `HEAD` is its own** — planning and orchestration commits land
   on the same branch. Every dispatch says so.
8. **A test that passes both before and after proves nothing** (campaign README §0.2). Where a
   prompt says "show it fails on the unfixed tree", **run that check yourself**.
9. **Every number in a log carries its provenance** (README §5 rule 8). A bare number is a
   failed check.

## The dispatch template

For prompt NN, launch a subagent with **exactly** this context, with the model the campaign
README §3 names:

> You are the implementation agent for one prompt in a campaign. Read, in this order:
> `CLAUDE.md`, `prompts/review-remediation/README.md`,
> `prompts/review-remediation/IMPLEMENTATION_STATE.md`, then your prompt
> `prompts/review-remediation/NN-<name>.md` and everything its "Read first" list names. Execute
> the prompt exactly. **Do not read the other prompts in this campaign.** Follow README §5 for
> the commit, the log, the board and `.documents/OPEN_ISSUES.md`. Run everything from the
> repository root with `venv/bin/python`. Other commits may land on this branch while you work:
> make exactly one commit, and do not amend, reset or rebase anything you did not create — if
> you need to change a commit you already made and it is no longer `HEAD`, stop and say so
> rather than rewriting. When you finish, reply with: the commit SHA, the **Result** line from
> your log, the "State handed to the next prompt" section verbatim, the per-package suite counts
> before and after, and every deviation with its classification tag.

## Before every dispatch

1. `git status` clean; on branch `review-remediation`; record `HEAD`.
2. Suite counts, recorded (0 and 0 before prompt 01; `CosmologyModels/tests` from 01;
   `ComputeTargets/tests` from 03):
   ```bash
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t . 2>&1 | tail -3
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . 2>&1 | tail -3
   ```
3. `venv/bin/black --check` on the files the prompt is allowed to touch, so a pre-existing
   formatting difference is not attributed to the agent.

## After every landing, report to the user

The commit; the Result line; each review check with pass/fail; the numbers against the
acceptance table; the issues opened or closed; the suite counts; and then **stop** — the user
starts the next orchestrator.
