# Orchestrator prompts — the science-readiness campaign

Nine prompts, one per campaign prompt. Each dispatches one fresh-context subagent, reviews its
commit against fixed criteria, and either continues or stops and reports to the user.

| Orchestrator | Campaign prompt | Character |
|---|---|---|
| [`prompt-01.md`](prompt-01.md) | 01, the BBN route | Vendored patch and a route removal. The review: the patched route reproduces the reference, the limit and the checks fire, nothing else in PRyMordial moved, one bump |
| [`prompt-02.md`](prompt-02.md) | 02, failure reasons | One column. The review: the round trip, and both exits |
| [`prompt-03.md`](prompt-03.md) | 03, the first bounce | A pure function and four columns. The review: §4.8's bounces, and no trajectory moved |
| [`prompt-04.md`](prompt-04.md) | 04, φ\* | One option and a guard. The review: no literal left, and the arithmetic |
| [`prompt-05.md`](prompt-05.md) | 05, bounce averages | Quadrature in the sampling. The review: the β = 1.6, M = 10⁻⁵ witness, the cost, and no point value or trajectory moved |
| [`prompt-06.md`](prompt-06.md) | 06, the spline floor | A default. The review: the pre-check and the abundances |
| [`prompt-07.md`](prompt-07.md) | 07, extraction and figures | Pure functions and plotting. The review: the tests and the synthetic figures |
| [`prompt-08.md`](prompt-08.md) | 08, documents | Additive only. The review is the `--numstat` |
| [`prompt-09.md`](prompt-09.md) | 09, close-out | Verification only. The rule that it may not touch production code is the review |

## Running one

Start a fresh context with, for example:

> Read `prompts/science-readiness/orchestrator/prompt-01.md` and follow it.

Run 01 → 09 in order. Each orchestrator's preconditions include the previous prompt's landing.
Do not start one with an earlier miss unresolved. Prompt 01's orchestrator checks that the board's
Decisions record the user's ruling on README §0.2 P1–P9 before dispatching.

**Take the baselines each orchestrator names before dispatching anything.** They cannot be
reconstructed after the fact. **Run the driver unloaded**: one history at a time, and no suite
running beside it. Wall-time acceptance rows are meaningless under load.

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
8. **A test that passes both before and after proves nothing** (campaign README §0.4). Each prompt
   names a test that must fail on `HEAD~1`, or a stand-in measurement on `HEAD~1` where the
   feature is new. **Run that check yourself.**
9. **Every number in a log carries its provenance** (README §5 rule 8). A bare number is a failed
   check.

## The dispatch template

For prompt NN, launch a subagent with **exactly** this context, with the model the campaign
README §3 names:

> You are the implementation agent for one prompt in a campaign. Read, in this order:
> `CLAUDE.md`, `prompts/science-readiness/README.md`,
> `prompts/science-readiness/IMPLEMENTATION_STATE.md`, then your prompt
> `prompts/science-readiness/NN-<name>.md` and everything its "Read first" list names.
> Execute the prompt exactly. **Do not read the other prompts in this campaign.** Follow README §5
> for the commit, the log, the board and `.documents/OPEN_ISSUES.md`. Run everything from the
> repository root with `venv/bin/python`. Run full histories one at a time, with nothing else
> running. Other commits may land on this branch while you work: make exactly one commit, and do
> not amend, reset or rebase anything you did not create. If you need to change a commit you
> already made and it is no longer `HEAD`, stop and say so rather than rewriting. When you
> finish, reply with:
> - the commit SHA;
> - the **Result** line from your log;
> - the "State handed to the next prompt" section, verbatim;
> - the per-package suite counts before and after;
> - every deviation, with its classification tag.

## Before every dispatch

1. `git status` clean; on branch `science-readiness`; record `HEAD`.
2. Suite counts, recorded (18, 67 and 17 at `6aaa706`):
   ```bash
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t . 2>&1 | grep -E "^Ran|^OK|FAILED"
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . 2>&1 | grep -E "^Ran|^OK|FAILED"
   PYTHONPATH=. ./venv/bin/python -m unittest discover -s Datastore/tests -t . 2>&1 | grep -E "^Ran|^OK|FAILED"
   ```
3. `venv/bin/black --check` on the non-`PRyM/` Python files the prompt may touch, so that a
   pre-existing formatting difference is not attributed to the agent.

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
instead, run it on `HEAD~1` yourself and compare with the log.

## After every landing, report to the user

Report:
- the commit;
- the Result line;
- each review check, with pass or fail;
- the numbers against the acceptance table;
- the issues opened or closed;
- the suite counts.

Then **stop**. The user starts the next orchestrator.
