# Orchestrator — prompt 05, the kicking function and EOS hygiene

Read [`README.md`](README.md) and [`../README.md`](../README.md) §2 (c), §6.4 first. **You do not
write code.**

**The prompt:** [`../05-kicking-function-and-eos-hygiene.md`](../05-kicking-function-and-eos-hygiene.md)
**Board item:** R4 (pins) · **Closes:** the pinning half of R4

## 0. What makes this prompt unusual

The deliverable includes a document the authors will rewrite the paper from. The review is
provenance: every number in it must point at a test or a script and a commit, and nothing in it
may describe how the CSV was built, because nobody in the repository knows.

## 1. Before you dispatch

1. Prompt 04 landed and reviewed; branch; `HEAD`; `git status` clean.
2. Baselines: both suite counts.
3. Reproduce the peaks yourself from the CSV (a five-line pandas read, as the audit did):
   e⁺e⁻ 0.1007 @ 0.1585 MeV; QCD 0.3138 @ 0.1778 GeV; EW 0.03733 @ 56.23 GeV; ∫Σ d ln T =
   0.1617.

## 2. Dispatch

One fresh-context subagent, Opus, template in `README.md`. Add: *"`Paper1.tex` is read-only
context. Do not guess the CSV's construction. Do not delete the base-class `w()` methods."*

## 3. The review — eight checks

1. **Allowed files.** `CosmologyModels/tests/`, the two Saikawa–Shirai EOS files and
   `Xav_EOS_spline.py` (docstrings only — `git diff` shows no executable line changed),
   `.documents/numerical-methods-for-paper.md` (new), log, board, index.
2. **Docstrings only in the EOS files.** Read the diff line by line.
3. **The pins are the audit's** (§1.3 above) at the prompt's tolerances, and the tests go through
   `Xav_EOS_spline.w`, not `pd.read_csv`.
4. **Case 5 tightened**: 1.005 ± 5e-3 from 5 MeV, and 1.003 ± 5e-3 from 100 MeV, both through
   `eos_reference.integrate_temperature_law` with the shipped (now corrected) derivative,
   `kappa = 1`.
5. **Case 6 pins the dead freeze**: `SaikawaShirai_EOS_spline.w(1 MeV) == w(2 MeV)` and the
   production class differs.
6. **The note.** Every number carries a provenance tag; the provenance gap is stated as a gap;
   nothing in it edits or contradicts the audit; §5 of it points at the board's seeded issues by
   name.
7. **Suites** up by the cases added; nothing down.
8. **Housekeeping**: `black --check` on changed files; log; board R4 pins; index.

## 4. Stop and ask the user

- Any of the prompt's §6.
- Any number in the note has no provenance, or the note asserts how the table was built.

## 5. After it lands

Report: the commit; the pinned values as measured; the tightened consistency figure; a one-line
summary of what the note says the paper currently gets wrong. Then stop.
