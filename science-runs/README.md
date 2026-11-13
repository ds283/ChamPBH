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
| Figure 6 | Which β are excluded by Y_p, D/H and ΔN_eff at laboratory M? | the main store's `histories.csv` (`plot_by_beta.py`) | [`2026-11-08-science-2026.10.0/fig6/`](2026-11-08-science-2026.10.0/fig6/NOTES.md) |
| Relativistic quanta | Do the quanta produced at reflections stay relativistic, and how much of the dark radiation is not covered by the test? | the main store's `histories.csv` and, in the same export root, each one-loop history's `fields.csv` and `bounces.csv` (`plot_ScalarHistory.py`); then `tools/quanta_measures.py` on that root at c_a = 1, 0 and 10 | [`2026-11-08-science-2026.10.0/relativistic/`](2026-11-08-science-2026.10.0/relativistic/NOTES.md) |

Added 2026-11-09, when the relativistic-quanta output of the `2026.10.0` run was found unexamined (that run's
`relativistic/PLAN.md` §4). Figure 6 around M_crit and the trend in M are not on the list: they answer questions of
one run's grid.

## Runs

| Folder | What | Status |
|---|---|---|
| [`2026-10-03-science-2026.6.0/`](2026-10-03-science-2026.6.0/README.md) | The first full science run, at `VERSION_LABEL` `2026.6.0`, with its BBN refresh (`+bt02`) and the draft BBN-constraint figure (fig6) | Superseded: the tree cannot read its stores, and `jordan-normalization`'s rebuild replaces them |
| [`2026-11-01-science-2026.8.0/`](2026-11-01-science-2026.8.0/README.md) | The run at `VERSION_LABEL` `2026.8.0` that replaces it: Figure 6 at laboratory M with α bands, comparisons near M_crit and in natural mode at large M, and a reflection store; registered, with its plan and probe | Superseded by `2026.10.0`, whose tree cannot read its stores; Figure 6, 6a and the reflection figure were made from it |
| [`2026-11-08-science-2026.10.0/`](2026-11-08-science-2026.10.0/README.md) | The rebuild of record after `one-loop-drain`, at `VERSION_LABEL` `2026.10.0`: the one-loop drain law at ten strengths A beside the fixed α, at laboratory M, down to 10⁻⁴² M_P, around M_crit and in natural mode at large M, and a reflection store; registered, with its plan, probe and post-build script | Current; 6 surfing-region failures, all at the tuning cap or as in 2026.8.0; Figure 6, the trend in M, Figure 6b and the relativistic-quanta analysis made |
