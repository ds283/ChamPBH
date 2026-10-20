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

## Runs

| Folder | What | Status |
|---|---|---|
| [`2026-10-03-science-2026.6.0/`](2026-10-03-science-2026.6.0/README.md) | The first full science run, at `VERSION_LABEL` `2026.6.0`, with its BBN refresh (`+bt02`) and the draft BBN-constraint figure (fig6) | Superseded: the tree cannot read its stores, and `jordan-normalization`'s rebuild replaces them |
