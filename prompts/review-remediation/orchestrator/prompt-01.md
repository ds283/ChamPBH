# Orchestrator — prompt 01, the temperature-law harness

Read [`README.md`](README.md) (this directory) and [`../README.md`](../README.md) §0, §2 (a)–(c),
§6.1 first. **You do not write code.**

**The prompt:** [`../01-temperature-law-harness.md`](../01-temperature-law-harness.md)
**Board items:** R1 (guard), R5 (characterised) · **Closes:** nothing

*Amended 2026-09-29.* The first dispatch stopped on case 2: the corrected law was off by
+1.465e-4 at 10 keV and T_CMB. That is R5, the 10 keV join (campaign README §2 (j), audit §11).
Case 2 now asserts 1e-5 above the join and characterises R5 at and below it.

## 0. What makes this prompt unusual

It is the only prompt that must land **before** the defect is fixed, and its tests are written to
*pass on the broken tree* by asserting the measured defect. The review is therefore not "do the
tests pass" but "do the tests assert the audit's numbers, with constants and comments that make
the next prompt's flip a one-line edit, and is the reference truly independent of the shipped
derivative".

## 1. Before you dispatch

1. If branch `review-remediation` does not exist, create it from `f5896bb`
   (`git switch -c review-remediation f5896bb`). If it exists, switch to it. Record `HEAD`.
2. If the planning files (`CLAUDE.md`, `prompts/`, `.documents/OPEN_ISSUES.md`) are uncommitted,
   commit them first as one planning commit (subject *"Plan the review-remediation campaign"*,
   body citing the audit; `Co-Authored-By` naming the planning model, Claude Fable 5.1). Tell the
   agent the tree is not its own.
3. Reproduce the audit yourself, from the root, and keep the output:
   ```bash
   venv/bin/python .documents/audit-2026-09-29/tlaw_check.py
   venv/bin/python .documents/audit-2026-09-29/eos_consistency.py
   ```
   Expected: N to T_CMB **41.4969** (code) vs **40.0746** (exact); ρ_R ratio **0.0041** at 10 keV;
   with ÷ln 10, **1.0026**. Also run `venv/bin/python .documents/audit-2026-09-29/low_t_join_probe.py ship`:
   **+1.465e-04** at 10 keV and **+1.464e-04** at T_CMB, and **|residual| < 5e-8** above the join.
4. Confirm `CosmologyModels/tests/` and `ComputeTargets/tests/` do not exist. Baseline counts: 0, 0.
5. `git status` clean.

## 2. Dispatch

One fresh-context subagent, Opus, with the template in `README.md`. Add: *"The tree at `HEAD` is
not yours; the campaign's planning commit is on this branch. Your diff must contain no production
file."*

## 3. The review — eight checks

1. **No production file in the diff.** `git diff --stat HEAD~1 HEAD` shows only
   `CosmologyModels/tests/`, `prompts/review-remediation/logs/01-…`, the board, and
   `.documents/OPEN_ISSUES.md` if an issue was opened.
2. **The reference is independent.** Read `eos_reference.py`: `exact_efolds` calls `G_s` only;
   nothing in the module calls `dG_s_dlogT` except `integrate_temperature_law` (which *is* the
   thing being scored) and `derivative_convention_ratio` (which compares it). No import of the
   jax class outside the `skipUnless` test.
3. **The numbers are the audit's.** Run the suite yourself. Then read the constants: the eight
   expected offsets are +0.180, +0.779, +0.987, +0.994, +1.336, +1.403, +1.422, +1.422 (±2e-3);
   the derivative ratio ln 10 (±1e-3); the ρ_R ratios 0.182 and 1.005 from 5 MeV (or 0.0041 and
   1.0026 from 2×10⁴ GeV — the test says which). A test that asserts
   different numbers and passes is a stop — either the audit or the test is wrong, and you do
   not adjudicate.
4. **Case 2 passes, in two halves.**
   - **Above the join** (1 GeV to 70 keV): the corrected convention agrees with exact entropy
     conservation to 1e-5. This proves R1 is a pure factor.
   - **At 10 keV and T_CMB:** the residual is asserted as R5's +1.465e-4 ± 5e-6, under a named
     constant whose comment says prompt 02 sets it to 0.
   If the log says otherwise, stop.
5. **Every threshold has a comment naming the prompt that tightens it.** `grep -n "prompt 02\|prompt 05" CosmologyModels/tests/test_temperature_law.py`.
6. **No Ray, no datastore.** `grep -n "ray\.\|Datastore\|ShardedPool" CosmologyModels/tests/*.py` is empty.
7. **Suite: 0 → 6** (or 5 with jax skipped — jax 0.9.0 is installed, so expect 6). Wall-clock
   under a minute; the log says how long.
8. **Housekeeping.** `black --check` clean on added files; the board's §1 row for 01 and the R1
   row in §2 updated in the same commit; the log in the §5.1 template with every deviation tagged.

## 4. Stop and ask the user

Relay verbatim; do not adjudicate.

- Any of the prompt's §5.
- Any check in §3 fails.
- The agent edited a production file "to make the test importable".

## 5. After it lands

Report: the commit; the eight offsets as measured; whether case 2 passed and to what precision;
the suite count and wall-clock; the issues opened. Then stop; the user starts prompt 02.
