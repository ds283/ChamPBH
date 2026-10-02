# Log 08 — Documents

**Prompt:** prompts/science-readiness/08-documents.md
**Commit:** the commit that adds this file ("Document the Hubble-only BBN route and the stored observables"); its SHA is in `git log`
**Model:** Sonnet 5.5
**Date:** 2026-10-02
**Result:** COMPLETE WITH DEVIATIONS

Worked on top of `541c048` (prompt 07's commit; `HEAD` at dispatch). No code and no test: the suites
are unchanged (Verification). Every quoted number in the new document text carries its provenance
in the text itself (the log, the test or the README row it comes from). `VERSION_LABEL`
(`"2026.6.0"`) and `PRYM_VERSION` (`"bf24c3d+ri02+sr01"`) are unchanged.

## What shipped

All additions; no line above a dated heading is edited, and no `.documents/` document has a
deleted line (`--numstat` below).

- **`.documents/numerical-strategies.md`** (+263).
  - **§3.6** (new, after §3.5.7), "Added 2026-10-02": the amended prompt asks for a note in §3 on
    the sampling, and log 05's measurement replaces the cell means.
    - §3.6.1: the three additions on the `ScalarModel` row (failure reason, first bounce,
      fixed-T values): definition, columns, units, accessor, why the dense output (the
      sample-based detector's 742.79 MeV and 0.39 MeV against 746.686 and 420.758), and the values on
      the three roster histories (logs 03 and 06b).
    - §3.6.2: log 05's half-periods-per-cell table with its provenance, the resolved sawtooth, that
      aliasing begins below about 100 eV, and that the cell means were built, measured (0.71–0.84×
      rms step; +5.65e-5 bias; D/H +1.57e-3 at β = 2, M = 0.5; β = 2, M = 10⁻⁵ failing in
      PRyMordial) and withdrawn by the user's ruling. The adiabatic stage's late samples are the
      aliased ones (`[post-adiabatic-Q-reads-aliased-late-samples]`).
  - **§7.6** (new, after §7.5), "Added 2026-10-02": states that §7.2–§7.4 were superseded in
    `review-remediation` prompt 04 (`eba4473`) and that §7.1, §7.3–§7.5 and §9 item 2 were superseded
    by this campaign's prompts 01 and 06.
    - 7.6.1: what PRyMordial is given. One ρ_NP callable from a cubic ratio spline times ρ_SM, the
      point `H_J`, the guards, the window [0.2 keV, 100 MeV], PRyMordial's query range
      (0.3628 keV–10 MeV), and the 20 eV pre-check.
    - 7.6.2: the Hubble-only patch, and why `p_NP` and `T_NP` are gone. It has a roster table and
      the PRyMordial-ulp sensitivity (log 01, Deviation 3).
    - 7.6.3: the wall-clock limit and the output checks.
    - 7.6.4: **every `PRyM/` hunk**, from log 01, as a table with file and lines, for a PRyMordial
      upgrade, with the note that `cham03` is reverted and must not be re-applied.
    - 7.6.5: other things recorded.
- **`.documents/numerical-methods-for-paper.md`** (+108).
  - **§4.1**: replaces "Why the plasma is unaffected" by statement (unaffected by construction);
    replaces "What is handed over" and "The passenger patch"; lists the checks; gives the measured
    abundances of the new route (four histories and the baseline); states the sample grid in
    PRyMordial's window; and states what a history now stores.
  - **An addendum to §5**, at the end of the file, on the initial condition (review H8): the
    option, the warning, log 04's selections.
- **`.documents/architecture-summary.md`** (+111).
  - A note after the file tree (`pipeline_selection.py`, `tools/`, the new role of
    `extract_common.py`).
  - A note after the CLI options list (`--phi-init-Mp`, `--bbn-wall-clock-limit`,
    `--band-half-width`, and the `--T-stop-GeV` interaction).
  - A note on the `ScalarModel` schema: nine columns with units and the prompt that added each, the
    three properties, the payload keys, and that `SampleValues` is unchanged.
  - A note at §7.4 (`compute_BBN_data` as it now is; `BBNDataValue` loses `pressure_NP_MeV4`).
  - A note on the pipeline stages (the warning step, the `ScalarModel` failure summary, the stage 3
    payload).
- **`.documents/paper-corrections-numerical-section.md`** (+93). A new **§5**, rows 10–13: the
  new-physics pressure (paper 3132–3134 and 3672–3674), the asinh splines (3135–3146), the
  derivative (3147–3151) and the one-decade requirement against the spline range (3152–3155). Each
  has Then, Now, the measurement and the log it comes from, and a drafted replacement. It also has
  the statements left correct (the scalar "changes the expansion rate at fixed plasma
  temperature" is now literally true; φ\* = 5 is the option's default), the sampling
  measurements to check against, and three things the text may want to add.
  `Paper1.tex` was read at the path in the header (it exists, modified 2026-10-01 12:04) and is not
  edited.
- **Boards and index.**
  - `prompts/science-readiness/IMPLEMENTATION_STATE.md`: header (9 of 10 landed; suites after 08),
    row 08, item D done, §3 (the `[00-documents-…]` entry removed and the intro paragraph's counts),
    §3.1 (the `[04-numerical-strategies-…]` row marked resolved), §4 (both entries, with Resolved
    lines).
  - `prompts/review-remediation/IMPLEMENTATION_STATE.md`: a Resolved line under
    `[04-numerical-strategies-describes-the-removed-asinh-bbn-interface]`, and nothing else.
  - `.documents/OPEN_ISSUES.md`: both rows deleted, the count **24 → 22** (review-remediation 6 → 5,
    science-readiness 3 → 2), §1.1, §1.7 and §1.8 text corrected, date 2026-10-02.

## Deviations from the prompt

### 1. The prompt expects a "temperature-dependent new-physics fluid" sentence in the paper; there is none — STRUCTURALLY REQUIRED

The prompt (§1, `paper-corrections-numerical-section.md`) lists "the new-physics pressure, the
temperature-dependent new-physics fluid, the asinh splines, the BBN spline range". The pressure,
the asinh splines and the spline range have sentences in `Paper1.tex` (rows 10, 11 and 13; the
derivative of row 12 is a fourth). The "fluid" does not: the paper has no sentence about
$T_{\rm NP}$, `NP_thermo_flag` or a
fictitious temperature. I searched `T_{\rm NP}`, `thermo_flag`, `fictitious`, "new-physics fluid",
"wall clock" and "timeout", and read the 13 lines matching "fluid". All of them are about the
cosmological, radiation or a perfect fluid. §5.1 of the new section says so under "Searched for and
not found", rather than inventing a row.

### 2. The sampling note is §3.6, a new subsection, and it also holds the stored-per-history additions — IMPLEMENTATION CHOICE

The prompt says "in §3 (the sampling), a dated note". `numerical-strategies.md` §3 is the
step-control section; the sampling itself is described in §2.3 and §3.5.1/§3.5.7, and nothing
after them can be extended without editing text above a dated heading. So the note is a new dated
§3.6 at the end of §3. The alternative was to put it under §7.6 only; §3.6 sits beside §3.5.7,
which lists what is stored per history, and the sampling grid belongs there. §3.6.1 (the three
columns on the parent row) was not asked for by name, but "what is stored per history" is exactly
what §3.5.7 says and is silent about those columns, and the prompt asks architecture-summary to
carry the schema; the definitions needed one home with their values, and §3.6.1 is it.

### 3. The §5 addendum to `numerical-methods-for-paper.md` and the architecture notes beyond the three requested — IMPLEMENTATION CHOICE

The prompt names, for `numerical-methods-for-paper.md`, "a dated addendum after §4", and for
`architecture-summary.md`, notes at §7.4, the schema descriptions and the CLI list. I added, in
addition:

- an addendum to §5 at the end of `numerical-methods-for-paper.md`: its bullet on review H8 says
  φ\* is hard-coded and unchecked, which prompt 04 made false, and the prompt's own closing
  statement ("every sentence that now disagrees with the code") covers it;
- a note after the file tree and one on the pipeline stages in `architecture-summary.md`: the
  stage 3 payload there had been annotated as `{"small_network": False}`, and the tree omitted
  `pipeline_selection.py` and `tools/`, both of which this campaign made load-bearing.

The alternative was to leave those two statements as they stood, which would have left a reader of
`architecture-summary.md` with a wrong payload and no pointer to the driver.

### 4. Rows 10–13 are a new §5 of the corrections document, not further rows of §1's table — IMPLEMENTATION CHOICE

§1's table has a header that fixes "Then" as `b1f64d8` and "Now" as `614c41a`, and its rows are
about the integrator paragraphs. The BBN rows have a different "then" (`eba4473`, the tree whose
asinh sentences were already wrong) and a different "now" (`541c048`). A new section with its own
header says which, and numbering continues from 9, so a row has one number across the document.

### 5. The orchestrator's measurement is quoted from log 05, and its script is not in the repository — no deviation

The half-period table is the orchestrator's (`orch_bounce_density.py`, in the session scratchpad,
not committed). The new text says so wherever it quotes the table, and cites log 05, Verification,
as the place that holds it.

## Verification performed

I ran each of these.

- **`git diff --numstat`** on the staged changes, before the commit: the `.documents/` documents
  have 0 deletions each (the "Numstat" block below).

- **Suites** (the three commands of README §5 rule 6, from the repository root; no code changed).

  | package | before (`541c048`, the orchestrator's) | after (this tree) |
  |---|---|---|
  | CosmologyModels | 18 OK | 18 OK (74.7 s) |
  | ComputeTargets | 103 OK | **103** OK (91.0 s) |
  | Datastore | 31 OK | 31 OK (2.8 s) |

- **Quoted numbers against their sources.** A script (scratchpad, not committed) extracted every
  decimal with four or more digits, and every integer of five or more digits, from the added
  lines of the four `.documents/` files (51 numbers). It looked each up in the campaign logs, the
  campaign README and the existing `numerical-methods-for-paper.md`. 50 were found verbatim. The
  one not found, −0.0737, is −0.07367 (log 05, β = 2, `M = 10⁻⁵`, [10, 100) keV) rounded for the
  corrections document's range statement. That is a check that a number was copied, not that it was
  copied from the right row: the rows I rely on were read against the logs by eye, and two slips
  that way were caught and fixed during the work (a decade count of 1.8 where
  log₁₀(362.8/20) = 1.26, and an invented count of "28" lines for a grep that found 13).
- **Reasoned, not run.** That `Paper1.tex`'s line numbers are those of the 2026-10-02 read. That
  the prose of §3.6 and §7.6 describes the code, which I read at the places the logs name
  (`ComputeTargets/BBNData.py`, `config/argument_parser.py`, `PRyM/` markers by `grep`:
  every `science-readiness prompt 01` marker is at the line the §7.6.4 table gives) and did not
  re-run. The corrections document's drafted replacement sentences are drafts for the authors.
- **Black.** Not applicable: no Python file is changed.

### Numstat

`git diff --cached --numstat` (added, deleted, file), taken before the commit; the log's own line is absent from its own copy and is a new file:

```
6	10	.documents/OPEN_ISSUES.md
111	0	.documents/architecture-summary.md
108	0	.documents/numerical-methods-for-paper.md
263	0	.documents/numerical-strategies.md
93	0	.documents/paper-corrections-numerical-section.md
4	0	prompts/review-remediation/IMPLEMENTATION_STATE.md
32	14	prompts/science-readiness/IMPLEMENTATION_STATE.md
202	0	prompts/science-readiness/logs/08-documents.md
```

Every `.documents/` document has 0 deletions except `OPEN_ISSUES.md`, whose two deleted index rows (and rewritten header lines) are what CLAUDE.md requires of a closed issue; the two board files carry the moves and Resolved lines. The acceptance "only additions in `.documents/`" holds for the four documents the prompt names.

## Observations not acted on

- `numerical-strategies.md` §9 (the "additional categories" list) still has item 2 on the
  frame-conversion of Ḣ_J/H_J² "used for P_NP". §7.6 says it has no subject now; the item itself is
  above a dated heading and is left as it stands.
- `paper-corrections-numerical-section.md` §4 ("Open dependencies") says rows 1–9 do not change the
  BBN interface. That is still true of rows 1–9; rows 10–13 are about it. Not edited.
- `review-remediation-verification.md` §4 still describes the old route. It is prompt 09's
  document, and prompt 09 adds §4.10 to it (README §7).

No §3 issue is opened by this prompt.

## State handed to the next prompt

- **For prompt 09 (README §7).** The documents it may cite, with their new anchors:
  - `numerical-strategies.md` §3.6 (what the row carries; the sample grid) and §7.6 (the route;
    7.6.4 is the `PRyM/` table);
  - `numerical-methods-for-paper.md` §4.1 and the §5 addendum;
  - `architecture-summary.md`: the notes at the file tree, the CLI options, the `ScalarModel`
    schema, §7.4 and the pipeline stages;
  - `paper-corrections-numerical-section.md` §5 (rows 10–13, §5.2–§5.4).
- **Handover items README §7 asks for that these documents already state:** the options
  (`--phi-init-Mp`, `--bbn-wall-clock-limit`, `--band-half-width`, and the warning that only warns);
  the route and `PRYM_VERSION`; the window and the 20 eV pre-check; the stored row contents
  including the four fixed-T columns; that there are no cell means, and log 05's finding. Prompt 09
  still owes the roster's figures against the source's and §4.10.
- **Open issues named in the new text**, so that the handover can name them too:
  `[post-adiabatic-Q-reads-aliased-late-samples]`, `[00-stored-samples-alias-the-rebounds]`
  (narrowed), `[05-the-ratio-spline-may-ring-at-resolved-bounce-jumps]`,
  `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]`, and the parked-tracking model
  (`[00-settling-at-physical-M-needs-a-parked-tracking-model]`).
- **Index count** after this prompt: 22 open (review-remediation 5, production-readiness 2,
  run-integrity 4, integrator-remediation 9, science-readiness 2).
- **Suite counts after this prompt:** CosmologyModels 18, ComputeTargets 103, Datastore 31 (unchanged
  from prompt 07).
