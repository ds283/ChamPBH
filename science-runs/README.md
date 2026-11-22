# Science runs

This folder holds what produced science output for publication: the run scripts, the scripts that
made figures from a run, their inputs, and the run's logs. One folder per run. It is a record, not
a code campaign; campaigns that change the code are in [`prompts/`](../prompts/INDEX.md).

**Naming.** Each run's folder is `YYYY-MM-DD-<slug>`. The date is the day the run started, and it
is in the name because file dates do not survive a copy or a move. The slug names the store the
run wrote.

**What a run's folder holds.** A `README.md` saying:
- the tree and `VERSION_LABEL`;
- the stores the run wrote;
- whether its results still stand;
- who or what made each file, and how that is known.

Then its scripts, its logs, and one folder per figure made from it, with that figure's inputs. Files
are kept as they were found. If one is edited, its README says so, with the date. `.gitattributes`
here turns off line-ending conversion, so a checkout gives the same bytes. A Python file here is a
record, not part of the code: it is not run through `black`, and no test root imports it.

**Registered runs.** Since `rrk-adoption` (2026-10-20), a production run is launched through
`RunDrivers/registered_run.py`, and the registry records it under `var/runs/`, which git ignores.
That record does not replace this folder: the registry says that a run happened and how it ended,
and this folder says what it was for and what came of it.

## Standard analyses

A run of record makes each of these from its stores, or says in its README why it does not. Each names the folder of
its latest instance, whose script is the template and whose NOTES.md says how to repeat it: copy the folder to the new
run, copy the new inputs, re-run, and write a new NOTES.md. A run's PLAN.md lists them, its run script makes every
input they read, and its README has a section "Standard analyses" saying, for each, made (and where) or not made (and
why). A campaign that changes what one of them reads says so in its README (CLAUDE.md, campaign conventions).

| Analysis | Question | Inputs it needs from the run | Latest |
|---|---|---|---|
| Figure 6 | Which β are excluded by Y_p, D/H and ΔN_eff at laboratory M? | each wall's `histories.csv` (`plot_by_beta.py`, one export root per potential and n) | [`2026-11-21-science-2026.14.0/fig6/`](2026-11-21-science-2026.14.0/fig6/NOTES.md), one figure per wall (`--wall exp`, `pl1`, `pl2`) |
| Relativistic quanta | Do the quanta produced at reflections stay relativistic, and how much of the dark radiation is not covered by the test? | each wall's `histories.csv` and, in the same export root, each one-loop history's `fields.csv` and `bounces.csv` (`plot_ScalarHistory.py`); then `tools/quanta_measures.py` on that root at c_a = 1, 0 and 10 | [`2026-11-21-science-2026.14.0/relativistic/`](2026-11-21-science-2026.14.0/relativistic/NOTES.md), one set per wall (`--wall exp`, `pl1`, `pl2`) |

Added 2026-11-09, when the relativistic-quanta output of the `2026.10.0` run was found unexamined (that run's
`relativistic/PLAN.md` §4). Figure 6 around M_crit and the trend in M are not on the list: they answer questions of
one run's grid.

Since `2026.14.0` (2026-11-22) each is made once per wall, a wall being one potential at one n (`exp`, `pl1`, `pl2`),
from that wall's own export root.

## Runs

| Folder | What | Status |
|---|---|---|
| [`2026-10-03-science-2026.6.0/`](2026-10-03-science-2026.6.0/README.md) | The first full science run, at `VERSION_LABEL` `2026.6.0`, with its BBN refresh (`+bt02`) and the draft BBN-constraint figure (fig6) | Superseded: the tree cannot read its stores, and `jordan-normalization`'s rebuild replaces them |
| [`2026-11-01-science-2026.8.0/`](2026-11-01-science-2026.8.0/README.md) | The run at `VERSION_LABEL` `2026.8.0` that replaces it: Figure 6 at laboratory M with α bands, comparisons near M_crit and in natural mode at large M, and a reflection store; registered, with its plan and probe | Superseded by `2026.10.0`, whose tree cannot read its stores; Figure 6, 6a and the reflection figure were made from it |
| [`2026-11-08-science-2026.10.0/`](2026-11-08-science-2026.10.0/README.md) | The rebuild of record after `one-loop-drain`, at `VERSION_LABEL` `2026.10.0`: the one-loop drain law at ten strengths A beside the fixed α, at laboratory M, down to 10⁻⁴² M_P, around M_crit and in natural mode at large M, and a reflection store; registered, with its plan, probe and post-build script | Superseded by `2026.11.0`, the same grid on the electroweak-eos w table; Figure 6, the trend in M, Figure 6b and the relativistic-quanta analysis were made from it |
| [`2026-11-13-science-2026.11.0/`](2026-11-13-science-2026.11.0/README.md) | The rebuild of `2026.10.0` at `VERSION_LABEL` `2026.11.0`, on the w table that follows Laine & Meyer through the electroweak crossover: the same grid, with the dense β grid's fine segment moved to 2.950–3.040 to resolve the new QCD-capture edge; registered, with its plan, its exports and measures in one script | Superseded by `2026.14.0`, whose tree cannot read its stores (format 7 against 8); Figure 6, the relativistic-quanta analysis, Figure 6b, the trend in M and the comparison with 2026.10.0 were made from it (2026-11-14) |
| [`2026-11-21-science-2026.14.0/`](2026-11-21-science-2026.14.0/README.md) | The first run of both potentials, at `VERSION_LABEL` `2026.14.0`: the exponential at n = 1 on 2026.11.0's grid under `one-loop-exponential-v2`, and the inverse power law at n = 1 and 2 on that grid without C and D under `one-loop-inverse-power-v1`; three stores; registered, run on the Mac Studio over SSH, with its plan, probe and run script | Current; the exponential's 6 surfing-region failures as in 2026.11.0, and the power law fails at β ≥ 10 (n = 1) and β = 20 below 10⁻³⁶ M_P (n = 2) by overflow; Figure 6, the relativistic-quanta analysis and the trend in M made per wall, Figure 6b and the comparison with 2026.11.0 for the exponential (2026-11-22, notes reviewed by the user); the power walls' ln R extrapolation accepted for now by the user, then tested to ln R = 700 (n = 1) and 500 (n = 2): the law keeps the offset it had where it was fitted |
