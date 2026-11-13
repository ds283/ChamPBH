# Orchestrator — prompt 03, documents and handover

Read [`../README.md`](../README.md) first: §0, §1, §4, §5, §6.1 as ruled, §7, and §10. The user's words on the plan are
"U1 to U6 are fine as proposed; U7 doesn't need to be in scope here." §10's U12 and P21–P29 were written on
2026-11-13, at the user's request ("Please write prompt 03 and its orchestration note"). **U12 is the user's to rule
before dispatch.** P21–P29 stand unless the user rules otherwise. Then read the prompt. This note binds 03 only. The
conventions of earlier notes bind here as well: a number is cited with its source, timing is taken under load, and the
suites run serially.

**You do not write documents.** You may:
- run the suites, the inventory and the agent's check scripts;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`03-documents-and-handover.md`](../03-documents-and-handover.md) · **Gate:** "Close" · **Closes:**
the campaign, and no issue · **Opens:** under U12 (a), P26's issue; otherwise only what the work finds · **Model:**
Sonnet, as the prompt recommends · **Network:** none. Nothing is pushed.

**When:** written 2026-11-13. It is dispatched on the user's word, after U12 is ruled, from this context or a fresh
one. The dispatching context re-checks the Gate before dispatch (§1).

**Gate**, as the planner measured it at `4fa0aa7` on 2026-11-13 at 18:04 GMT, before this note, the prompt and README
§10 were committed:
- `electroweak-eos` is checked out in the main checkout, and `be81f8b` is an ancestor of `HEAD`. `git worktree list`
  shows the main checkout only.
- `git status --porcelain` shows the user's own uncommitted work: ` M CLAUDE.md`, ` M science-runs/README.md`,
  ` M science-runs/2026-11-08-science-2026.10.0/README.md` and `?? science-runs/2026-11-08-science-2026.10.0/relativistic/`.
  Nothing else is listed, apart from this note, the prompt and README §10 before their commit.
- `./venv/bin/python -m runregistrykit list`: 54 runs, all finished.
- `tools/ray_cluster.sh status`: no process of `/tmp/ray-cpbh`, and the lock is free.
- `var/datastores/electroweak-eos-02-old` (184 MB, `store_id` `821adc85…`) and `-new` (179 MB, `5fd9d146…`) each exist,
  with 16 shards and a manifest. `var/electroweak-eos/` is 278 MB.
- `df -h .`: 92 % used, 81 GB free. 03 writes almost nothing to disk.
- Load averages 4.34 / 28.15 / 38.97.
- `git merge-base --is-ancestor main electroweak-eos`: true. `main` is at `ec6bca8`, 14 commits behind this branch
  once this note lands.

## 0. What the planner found

- **The user's files are three of 03's targets.** `CLAUDE.md` (the suite counts, log 01 §6), the run record (README §4's
  dated note) and `science-runs/README.md` (the standard analyses, P12) all hold the user's uncommitted edits. The
  `CLAUDE.md` edit adds invariant 8 and a sentence on science runs. The `science-runs/README.md` edit adds the section
  "Standard analyses", listing Figure 6 and the relativistic quanta. The run-record edit adds `relativistic/` and its
  own "Standard analyses" section. That section says a β-axis defect of `tools/plot_quanta_grid.py` is "to be opened
  on the next campaign's board" (`relativistic/PLAN.md` §4.3, Q4, whose ruling is "retire for figures; keep as a
  diagnostic"). Hence U12, and P26.
- **The working-directory hazard was already documented.** `architecture-summary.md` §14 (note of 2026-11-07, from
  `one-loop-drain` 06's probe) says that Ray workers run in the cluster's working directory, and that
  `Xav_EOS_data.csv` and `PRyMrates/` are then the start directory's. 02's plan did not cite it, 02's agent did not
  find it, and the orchestrator did not find it in review. The issue 02 opened says that 06's probe "did not see"
  it. That is true of the effect, which needs two different tables to show, but not of the mechanism. P25 corrects
  the issue by a dated line, and D1 adds a line to §14's note.
- **The documents that describe the table.** `architecture-summary.md` describes the EOS only in §8.5 ("a lattice QCD
  equation of state for g*(T)"), and §14's note names the CSV. `numerical-strategies.md` names `Xav_EOS_spline` at
  "Σ_T's source" ("0 beyond the table") and in dated notes of earlier campaigns. The planner's grep was not
  exhaustive; the prompt asks for a search.
- **The surfing tests** (`tools/tests/test_surfing_scan.py`, `test_plot_surfing_histories.py`) read
  `.documents/electroweak-eos/table/out/surfing_reference_rows.csv`, the untuned rows that 01 regenerated on the new
  table. S1-new is tuned. P27 leaves the tests as they are.
- **The audit** is `report.tex` with a committed `report.pdf`. `pdflatex` and `latexmk` are in `/opt/local/bin`.
- **The counts after 02:** 26, 684 (219 + 465), 163, 292 and 76. `CLAUDE.md` still says 665 (219 + 446) and 277.

**Rulings.** Each is recorded in the log, with its class.
1. **U12.** The dispatching context tells the agent how the user ruled, in the user's words.
   - Under (a), the Gate asks for a clean tree, and the agent writes D3, D4 and P26 on top of the user's commit.
   - Under (b), the agent never stages, edits, stashes or reverts the user's files. D3 and D4 go into §4.21 as left
     for the user, P26 is not opened, and the prompt's (U12 b) stop applies.

   *STRUCTURALLY REQUIRED.*
2. **The `CLAUDE.md` parenthesis** (D4) is replaced by one sentence, as `one-loop-drain` 08's prompt allowed. If the
   user's commit has already changed it, the agent leaves the user's text and changes only the numbers. *IMPLEMENTATION
   CHOICE*, ruled here.
3. **The audit's PDF.** It is rebuilt in a scratch copy, and only `report.pdf` is copied back. A failed build is
   recorded and is not a stop. An unchanged PDF beside a changed `.tex` is named in the log's §5. *IMPLEMENTATION
   CHOICE*, ruled here.
4. **The inventory** runs on a copy, never on a store. *STRUCTURALLY REQUIRED.*
5. **Nothing is measured but the suites and the inventory.** A number the handover needs and no record holds is
   reported as not measured (prompt §5). *As the prompt says.*
6. **No file but the prompt's §4 list.** *STRUCTURALLY REQUIRED.*

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before
  committing. Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit". The orchestrator substitutes the SHA in its review.
- **Waiting.** The suites run in the background, and the agent waits for each one's notification. It never polls,
  and never sleeps in the foreground.
- **Scratch.** Copies, builds and suite outputs go in the agent's own subdirectory of the session scratchpad. Nothing
  goes in `/tmp`.
- **No Ray.** The agent starts no cluster.
- **Stop conditions are real:** the prompt's §5. A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Repeat these on the commit that adds this note.
1. **U12 is ruled**, in the user's own words. Without a ruling, do not dispatch.
2. **The Gate**, as above and as the prompt states it under the ruled option. Under (a), check that the user's commit
   holds the three files and `relativistic/`, and that `git status` is clean.
3. **`runregistrykit list`** and the load. A load above 30 is not a stop, but tell the user before dispatching.

## 2. Dispatch

Start one fresh-context subagent, on **Sonnet**, in the main checkout, in the background. Give it:
- the prompt, the README and the board;
- `HEAD`, the Gate as measured, and U12 as ruled, quoted;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the prompt's §4 files for the ruled option. Its log is
  `logs/03-documents-and-handover.md`, in `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **Stop and ask** on any of the prompt's §5 conditions.
- **At the end it replies with:**
  - the log's path and the message as committed; `git show --stat` of its commit;
  - each document changed, and where, with each dated note's first line;
  - §4.21's points, one line each, and its "This supersedes" list;
  - the inventory's output and hashes;
  - the suites, with counts, times and `uptime`;
  - §2 item 2's table and §2 item 4's ten numbers with their sources;
  - the issues changed or opened, and the index's count;
  - the PDF's build;
  - every deviation with its class;
  - "Observations not acted on" and "State handed on", verbatim.

## 3. The review — six checks

1. **Scope.**
   - `git show --stat <agent>` lists only the prompt's §4 files for the ruled option. Its parent is this note's
     commit, or a later commit that is not the agent's.
   - No Python file and no test changed. Nothing is under `var/`.
   - Under (b), the user's files are still modified and unstaged.
   - Nothing is pushed.
2. **The records are additive.** For each record file, `git diff` adds lines and removes none, apart from the
   `CLAUDE.md` counts, the parenthesis of ruling 2, the board's status line moved under its heading, and the index's
   header and rows. The files are the verification document §1–§2, `review-remediation-verification.md` before §4.21,
   the surfing-temperature verification §1–§3, the run record, and the audit's text.
3. **The numbers.** Choose ten numbers of §4.21 and the run record's note, other than the agent's ten, and check each
   against its source. Re-run the inventory on a fresh copy of `electroweak-eos-02-old`, and see the same output and
   unchanged hashes.
4. **The statements corrected.**
   - Grep both documents for the table's old values (25.1 TeV, 0.037436, 3.020, 3.0195, "vacuum") and see each present-tense
     use carry a dated note.
   - See §8.5's note and §14's line.
   - See the issue's correction, and that its index row agrees.
5. **The handover.** It covers P22's points, and README §7's four items are each answered. Every path it names exists.
   Every commit it names is on this branch.
6. **The records.**
   - The log is in §5.1's form, with every deviation classed.
   - On the board: row 03, the Close gate, §5's baseline, and the status line with its history.
   - `prompts/INDEX.md` and `OPEN_ISSUES.md` agree on the count.
   - The suites' counts equal 02's.
   - Under (a), `CLAUDE.md` gains no rule: every added or changed sentence describes.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears (the log, the board, §4.21, the index);
- this note marked "used for 03";
- an *Orchestrator review of prompt 03* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s row, if the SHA appears there.

**Report to the user:**
- the agent's commit and the review commit;
- that the campaign is closed, with its four prompts' commits;
- where the handover is, and its points in a line each;
- what stays open;
- that the merge into `main` is a fast-forward, and that it and the push are the user's.
