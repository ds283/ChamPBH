# Prompt 01 — the pin, the config and the rules

**Campaign:** [`README.md`](README.md) · **Board items:** **C1**–**C4** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-19, after the user's rulings (README §6). Measured at `3cd2be4`; your own
measurement governs.
**Gate:**
- on branch `rrk-adoption`, no tracked change in `git status`. The untracked entries are the
  user's, and none is yours: the 17 `pilot-2026.6.0*.db` files, `pilot-main.log`, `pilot-plot.log`,
  `pilot-out/`, and the two `full_run_2026.6.0*.sh` scripts;
- there is no `var/` at the repository root;
- no ChamPBH Ray process is running (`.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh
  status`). You start none.

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Sonnet**. One requirement line, a config file, an ignore rule, a section of
prose, one test module.

**Read first:**

1. [`README.md`](README.md): §0.1, §0.3, §2.2 (01's charter), §5, §6 (R1–R7, and P9).
2. RunRegistryKit's `README.md` at `v0.2.0`, read in its checkout
   (`/Users/ds283/Documents/Code/RunRegistryKit`; read, never edit): "Installing", "The client's
   root", "The rules", and "Ending a run whose end was never recorded".
3. SecondaryGWKit's `CLAUDE.md` at `031bcb4`, the section "Long-running jobs — the run registry"
   (`git -C /Users/ds283/Documents/Code/SecondaryGWKit show 031bcb4:CLAUDE.md`). It is the model for
   C3; adapt it to this repository, never copy its paths or its campaign history.
4. `CLAUDE.md` here, "Repository mechanics", in particular "Long-running jobs" (`:115-117`), "Ray on
   this machine" and the test commands.
5. `requirements.txt` (`datastorekit` at `:17`; `rsa==4.9.1`, then `scipy`, at `:84-85`), and
   `.gitignore`.

---

## 1. What is wanted

This repository depends on RunRegistryKit `v0.2.0` with its store layer, says where its records go,
keeps them out of git, and binds its agents to the registry's rules. Nothing yet launches a
registered run. 03's wrapper does that, and 02's fingerprinter is what the store layer reads.

### 1.1 The items

- **C1 — the pin.**
  - Add `runregistrykit[stores] @ git+https://github.com/ds283/RunRegistryKit@v0.2.0` to
    `requirements.txt`, in its alphabetical place, after `rsa==4.9.1`. No other line changes:
    `datastorekit`'s line at `:17` already satisfies the extra's floor (`>=0.4.0`).
  - Install it into the venv from the file: `./venv/bin/pip install -r requirements.txt`. **Never
    editable.**
  - Record `pip freeze` before and after. The only difference must be the one `runregistrykit` line.
    The README's §0.3 dry run found nothing else; anything more is a stop.
- **C2 — the config and the ignore rule.**
  - A new `runregistrykit.toml` at the repository root, with a two-line comment naming this
    campaign and RunRegistryKit's README, and:
    ```toml
    runs_root = "var/runs"
    stores_root = "var/datastores"
    distributions = ["datastorekit", "raypoolkit"]
    ```
  - **No `[stores]` table**: 02 adds it with the module it names. Until then every store operation
    refuses for want of a fingerprinter, which is the package's behaviour, not a defect.
  - `.gitignore` gains `var/`, with a one-line comment saying what is under it (the registry's run
    directories and this repository's datastores; README §6.0, R4).
- **C3 — `CLAUDE.md`.** The bullet "Long-running jobs" (`:115-117`) becomes a section of its own,
  `## Long-running jobs — the run registry`, placed after "Repository mechanics". It says, in this
  repository's terms:
  1. **What the registry is.** RunRegistryKit, pinned at `v0.2.0` with its store layer, records each
     run in `var/runs/<campaign>-<prompt>-<slug>-<timestamp>/`. `runregistrykit.toml` at the root
     configures it. It records; it does not schedule, supervise, restart or lock, and it deletes
     nothing but a store's own files, through `store retire`, which a person runs.
  2. **How a registered run is launched.** Through the wrapper `RunDrivers/registered_run.py`, which
     prompt 03 of `rrk-adoption` adds. Write the sentence so that it is true now, and 05 makes it
     definite. `main.py` stays launchable on its own, without the registry (R2). An unregistered run
     is recorded nowhere.
  3. **The six rules**, in SecondaryGWKit's numbering and form, each one or two sentences, with
     RunRegistryKit's README cited for the failures they answer:
     - discovery, with `./venv/bin/python -m runregistrykit list` as the command, at the start of a
       session and before anything long;
     - liveness: `kill -0` and the heartbeat, judged on the run's own host, and never a
       command-line match;
     - launch: detached, manifest first, pid recorded;
     - do not babysit: one check after the launch, then end the turn;
     - the route after a hard kill: `list` shows it stale; a person checks;
       `./venv/bin/python -m runregistrykit end RUN_ID --reason TEXT` on its host; then the resume.
       `Run.finish` is never called by hand on a run that is not the caller's;
     - durability and provenance.
  4. **Stores live under `var/datastores/`** (R4). Since `var/` is ignored, **never run `git clean`
     with `-x` or `-X`**, which deletes ignored files, the stores among them.
  5. **What stays.** Production runs are the user's, and no campaign prompt starts one except a
     rehearsal the campaign's plan names. And "Nothing a prompt runs should take longer than a few
     PRyMordial solves", carried over from the old bullet. Delete the old sentence "There is no run
     registry in this repository".

  Keep it as short as SecondaryGWKit's section or shorter. Nothing else in `CLAUDE.md` changes.
- **C4 — the test.** A new module, `tools/tests/test_registry_config.py`, with no Ray, no store and
  nothing written outside a `tempfile` directory:
  1. `runregistrykit.find_client_root(client_root=<repository root>)` gives `runs_root` and
     `stores_root` equal to `<root>/var/runs` and `<root>/var/datastores`, and `distributions` equal
     to `("datastorekit", "raypoolkit")`;
  2. the config holds exactly the keys above, so that a misspelt key (which the package refuses as
     a `ConfigError`) cannot be added unnoticed;
  3. `git check-ignore -q var/runs/x var/datastores/x.db`, run in the repository root by
     subprocess, exits 0: both are ignored;
  4. `runregistrykit.package_provenance(("runregistrykit",))` says 0.2.0, installed from
     `https://github.com/ds283/RunRegistryKit` at a commit, with `requested_revision` `v0.2.0` and
     `editable` false. This is the pin's check, in the form of `ObjectFactories/tests/
     test_package_provenance.py`'s for the other two packages.

  The module's docstring says what it pins and why.

---

## 2. Verification

1. **Before any edit:** the four roots at README §0.4's counts (18 / 176 (61 + 115) / 69 / 79),
   serially, in the foreground, ComputeTargets in its two halves (`CLAUDE.md`), with the load
   (`uptime`).
2. **After:**
   - `./venv/bin/pip show runregistrykit`, and its `direct_url.json`: the tag's commit, not
     editable;
   - `./venv/bin/python -m runregistrykit list`, from the root: "No runs under …/var/runs", exit 0.
     That creates nothing; check that no `var/` appeared;
   - `./venv/bin/python -m runregistrykit store show pilot-2026.6.0.db` is **not** run. No store is
     opened or named by this prompt;
   - the four roots: tools 79 + the new tests, the others unchanged;
   - `black --check` on the new module.
3. **Deliberate breakages** (README §5: each recorded as a diff, never committed). Each must fail at
   least one new test, named in the log:
   - `var/` removed from `.gitignore`;
   - a misspelt key (`run_root`) in the config;
   - `distributions` reduced to `["datastorekit"]`.

## 3. Acceptance

1. C1–C4 as §1.1 says.
2. §2's checks, each with its command and its output in the log.
3. Suites: CosmologyModels 18, ComputeTargets 176, ObjectFactories 69, tools 79 + the new tests.
4. **One commit**, with the log `logs/01-pin-config-and-rules.md` in the template of
   `dk-and-rpk-migration` README §5.1. The board: row 01, C1–C4, the gate "Pin", and a
   `### 5.2 After prompt 01 (<date>)`. Write "this commit" where the SHA goes.

## 4. Stop conditions: stop and ask

- The install changes any distribution other than adding `runregistrykit`, or resolves a different
  `datastorekit`.
- The installed package is not `v0.2.0`'s commit (`0bae3a3`), or is editable.
- An existing test fails.
- `list` or `find_client_root` behaves otherwise than RunRegistryKit's README says. That would be
  a package defect, reported there (README §5), never worked around here.
- Anything would need an edit outside §5's list.

## 5. What this prompt does not do

- **Files it changes:**
  - `requirements.txt` (one line);
  - `runregistrykit.toml` (new);
  - `.gitignore` (one rule and its comment);
  - `CLAUDE.md` (the section of C3, replacing the bullet);
  - `tools/tests/test_registry_config.py` (new);
  - this board, and the log.
- It writes no fingerprinter, no wrapper and no driver code. `main.py`, the plot scripts and
  `utilities.py` are untouched (README §5).
- It opens no store, starts no Ray, and creates no `var/`.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
