# Orchestrator prompts — the integrator-remediation campaign

Four prompts, one per campaign prompt. Each dispatches one fresh-context subagent, reviews its
commit against fixed criteria, and either continues or stops and reports to the user.

| Orchestrator | Campaign prompt | Character |
|---|---|---|
| [`prompt-01.md`](prompt-01.md) | 01, the step loop | The integrator. The review is that the audit's pointwise figures are reproduced, that the nine histories complete at the audit's cost, that nothing about the RHS or the sampling changed, and that the label bumped once |
| [`prompt-02.md`](prompt-02.md) | 02, fallback and exceptions | Deletions. The review is that no physical value moved (to `1e-10`), that every remaining raise is in the table, and that the budget stops a physical-`M` history cleanly |
| [`prompt-03.md`](prompt-03.md) | 03, documents | Additive only. The review is the `--numstat` |
| [`prompt-04.md`](prompt-04.md) | 04, close-out | Verification only. The rule that it may not touch production code is the review |

## Running one

Start a fresh context with, for example:

> Read `prompts/integrator-remediation/orchestrator/prompt-01.md` and follow it.

Run 01 → 04 in order. Each orchestrator's preconditions include the previous prompt's landing;
do not start 02 with a prompt-01 miss unresolved. The user's decisions on README §0.2 and the two
reflection guards were recorded on the board on 2026-10-01; prompt 01's orchestrator checks that
they are there before dispatching.

**Take the baselines each orchestrator names before dispatching anything.** They cannot be
reconstructed after the fact.

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
6. **Stop rather than repair.** Do not fix a failed check, revert it, or dispatch a follow-up
   agent to patch it. Report the specific check and what the log says about it.
7. **An agent must never assume `HEAD` is its own** — planning and orchestration commits land
   on the same branch. Every dispatch says so.
8. **A test that passes both before and after proves nothing** (campaign README §0.3). Prompts
   01 and 02 name tests that must fail on `HEAD~1`, or a measurement on `HEAD~1` that stands in
   for one where the new symbol does not exist there. **Run that check yourself.**
9. **Every number in a log carries its provenance** (README §5 rule 8). A bare number is a
   failed check.

## The dispatch template

For prompt NN, launch a subagent with **exactly** this context, with the model the campaign
README §3 names:

> You are the implementation agent for one prompt in a campaign. Read, in this order:
> `CLAUDE.md`, `prompts/integrator-remediation/README.md`,
> `prompts/integrator-remediation/IMPLEMENTATION_STATE.md`, then your prompt
> `prompts/integrator-remediation/NN-<name>.md` and everything its "Read first" list names.
> Execute the prompt exactly. **Do not read the other prompts in this campaign.** Follow README §5
> for the commit, the log, the board and `.documents/OPEN_ISSUES.md`. Run everything from the
> repository root with `venv/bin/python`. Other commits may land on this branch while you work:
> make exactly one commit, and do not amend, reset or rebase anything you did not create — if you
> need to change a commit you already made and it is no longer `HEAD`, stop and say so rather than
> rewriting. When you finish, reply with: the commit SHA, the **Result** line from your log, the
> "State handed to the next prompt" section verbatim, the per-package suite counts before and
> after, and every deviation with its classification tag.

## Before every dispatch

1. `git status` clean; on branch `integrator-remediation`; record `HEAD`.
2. Suite counts, recorded (18, 41 and 17 at `2b89022`):
   ```bash
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t . 2>&1 | grep -E "^Ran|^OK|FAILED"
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . 2>&1 | grep -E "^Ran|^OK|FAILED"
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s Datastore/tests -t . 2>&1 | grep -E "^Ran|^OK|FAILED"
   ```
3. `venv/bin/black --check` on the Python files the prompt is allowed to touch, so a pre-existing
   formatting difference is not attributed to the agent.

## The breakage check, generically

For a new test module `T` and a changed production file `F`:

```bash
git checkout HEAD~1 -- F
PYTHONPATH=. ./venv/bin/python -m unittest T 2>&1 | tail -20      # must FAIL, for the stated reason
git checkout HEAD -- F
PYTHONPATH=. ./venv/bin/python -m unittest T 2>&1 | tail -5       # must pass
git status                                                         # must be clean
```

A failure that is only an `ImportError` of a new symbol is **not** the check. The test must
fail because the old code gives the wrong answer. Where the new tests call a function that does
not exist on `HEAD~1` (prompt 01's loop), the check is instead a measurement of the old code by
the audit's scripts on `HEAD~1`, which drive their own copy of the old loop; each orchestrator
names the script, the arguments and the figure it must print.

## After every landing, report to the user

The commit; the Result line; each review check with pass/fail; the numbers against the
acceptance table; the issues opened or closed; the suite counts; and then **stop** — the user
starts the next orchestrator.
