# Prompt 02c — the measured law

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-05, on branch `one-loop-drain` in the main checkout, after the side-campaign `crossover` closed
and the user adopted its measured drain law (README §8.10). It is written from:
- README §8.10 whole (the rulings in the user's words, the law, its constants and sources, U15);
- `source/crossover/README.md`, from "Stage 2" to "Closed";
- `source/crossover/measured_law_map.py`, whose `fraction_adopted` is the law's reference implementation, and its
  outputs `source/crossover/out/adopted-A0.05/` and `out/adopted-floor/`;
- `source/crossover/fit_law.py` and `out/fit_law.txt` (the fits), `out/stage1a_S_of_L.txt` (the S(L) table);
- prompt 01 and its log, which built the code this prompt changes;
- the tree's code at the commit that adds this prompt.

The user dispatches it in a fresh context. If an orchestrator note is written for this prompt at dispatch, it governs
where it differs from this file.

**Where it runs.** In the main checkout `/Users/ds283/Documents/Code/ChamPBH`, on branch `one-loop-drain`, from the
repository root, with `PYTHONPATH=.` and `./venv/bin/python`. Outputs go under `var/one-loop-drain/02c/`.

**Gate:**
- on branch `one-loop-drain`, at or after the commit that adds this prompt, with no tracked change in `git status`;
- **U15 is ruled:** option (b), 2026-11-05 (README §8.10). L = ln(1 + E/V₀) at every n;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- the suites stand at the counts of `CLAUDE.md` (26, 566 = 163 + 403, 130, 156, 68), all passing.

If any of these fails, stop and ask.

**Closes:** nothing (`[02b-the-one-loop-fraction-is-unreliable-at-small-ell]` was closed by the ruling). **Narrows:**
nothing. **Opens:** only what the work finds.

**Recommended model:** **Opus.** The law is a small change, but it removes a feature, changes a payload record that 03
will store, and must reproduce a research script candidate by candidate.

**Read first:**
1. [`README.md`](README.md) §8.10 whole; §8.2 and §8.6 (U8–U14, P17–P26) for what is superseded.
2. The board's §3 and §4: the small-A failures entry and the closed small-ℓ entry, with their 2026-11-05 paragraphs.
3. `source/crossover/measured_law_map.py`: `fraction_adopted`, `S_of`, `drain_regime`, `drain_alpha_effective` (the
   patch), and how `cascade_map.py` reads A and the normalization from a `result.json`.
4. `ComputeTargets/ScalarHistory.py`:
   - the law's block of constants and comments (:150–:195): `DRAIN_*`, the regimes, `GAP_PRESCRIPTION_*`,
     `_GAP_PRESCRIPTION`;
   - `DrainCandidate` and `DrainRecord` (:627–:658) and their comments;
   - `drain_regime` (:1674), `drain_alpha_effective` (:1704), and `candidate` in `integrate_scalar_history`, where
     both are called (:2185–:2200), with the assessment `(phi, ln E, ClassicalTurningPoint)`;
   - where the `"drain"` record is built (:1939–:1950).
5. `CosmologyConcepts/Models/AbstractModel.py`: `ClassicalTurningPoint` (:71; `phi_t`, `log_R`) and `RealizedModel`;
   `CosmologyConcepts/Models/ExponentialModel.py`: how M and n are held (`_M_float`, `_n`).
6. `tools/history_and_bbn.py` (`--drain-law`, `--gap-prescription`, its history and `drain` lines) and its tests,
   `tools/tests/test_history_and_bbn_drain_law.py`.
7. `ComputeTargets/tests/test_drain_law.py`, every class.

---

## 0. Where this prompt sits

The one-loop law today drains min(α_cap, A·2.7/ℓ)·w_gate at a one-loop candidate, and in the gap by a prescription
(`zero`, the default, or `edge`). The user replaced both by the measured law of README §8.10, removed the
prescriptions, and ruled one versioned drain-model key, which 03 stores. This prompt puts the law in the code. 02d
then re-measures the sample, and 03 stores the key. Nothing is stored here.

## 1. What is wanted

### 1.1 The law (README §8.10)

Under `DRAIN_LAW_ONE_LOOP`, `drain_alpha_effective` returns min(`DRAIN_ALPHA_CAP`, A·f)·w_gate, with f by regime:

| regime | f |
|---|---|
| `one_loop`, ℓ ≥ 1 | d = ℓ + b₀ − c ln L; min(f_cap, 1 − exp(−a/d)) if d > 0, else f_cap |
| `one_loop`, ℓ < 1 | f_plateau |
| `gap` | 0 if ln R ≤ r₀; else 1 − exp(−g (ln R − r₀)^p (1 + q ln L)) |
| `classical` | exp(ln R)·S(L) |
| `unassessed` | 0, and the caller takes the α_eff = 0 branch, as today |

- **Inputs.** ln R is `ClassicalTurningPoint.log_R`. ℓ is the turnaround's, as today. L depends on U15:
  - under option (a), L = M/φ_t, with φ_t the candidate's `ClassicalTurningPoint.phi_t` and M the model's;
  - under option (b), which the n = 2 measurement supports (README §8.10), L = ln(1 + E/V₀) at every n. Compute it
    from the candidate's ln E and the model's ln V_floor, stably, through `log1p` of an exponential, not by
    subtracting. At n = 1 this equals M/φ_t to the turning point's accuracy, and §2.3's reproduction checks it at
    the maps' printed precision.
  - `drain_alpha_effective` gains the arguments it needs: L and ln R, or the turning point and M. Choose the cleaner
    signature, and say which in the log.
  - Under (a), if the realized model has no public way to give M, add a read-only one, and say so in the log.
- **The constants**, each a module constant beside today's, with its source quoted (P17):
  - a = 2.69, the Born coefficient held fixed (`wall_backreaction.tex`, "The inelastic fraction");
  - b₀ = 0.9691 and c = 0.41;
  - f_cap = 0.95 and f_plateau = 0.9, which are the user's adopted judgement (README §8.10);
  - g = 0.0198, r₀ = 1.5601, p = 1.5472 and q = 0.841.

  The fitted constants are `source/crossover/out/fit_law.txt`'s, rounded as `fraction_adopted` rounds them. Their
  names are yours; say what each is in its comment.
- **S(L).** The 21 values of `source/crossover/out/stage1a_S_of_L.txt` (L = 0.3–134), as a module table with its
  source quoted. Interpolate linearly in (ln L, ln S). Below L = 0.3 hold S(0.3). Above L = 134 use
  S(134)·(134/L)², since L²S tends to a constant (`production_energy_ratio.tex`). A pure function, tested alone.
- **The order of operations** follows `fraction_adopted` term for term, so that the values equal the reference's
  (§2.2). Where the production form must differ (for example, an interpolation that is not `np.interp`), the
  difference is bounded in §2.2.
- **U15 (n ≠ 1)**, ruled (b): no check on n; the law applies at every n with L = ln(1 + E/V₀) (README §8.10).
- `DRAIN_C1` goes. 2.7/ℓ is no longer computed anywhere.

### 1.2 The prescriptions removed (decision 2)

Remove these, with their comments, tests and documentation:
- `_GAP_PRESCRIPTION`, `GAP_PRESCRIPTION_ZERO`, `GAP_PRESCRIPTION_EDGE`, `GAP_PRESCRIPTIONS` and `DRAIN_ELL_EDGE`;
- `tools/history_and_bbn.py --gap-prescription` and the attribute's save and restore;
- the `gap_prescription` field of `DrainRecord`.

02's and 02b's records stay as they are. Their scripts (`.documents/one-loop-drain/measure/`, `measure-02b/`) name
the removed option and will no longer run on this tree. Say so in a dated note in each folder's README or driver
docstring; do not change the scripts.

### 1.3 The `"drain"` record (decision 1)

- **`DrainRecord`** replaces `gap_prescription` with `model`, a module constant naming the drain model and its version:
  - `DRAIN_MODEL_ONE_LOOP = "one-loop-measured-v1"` under the one-loop law;
  - `DRAIN_MODEL_FIXED = "fixed"` under the fixed law.

  03 stores the key from these; their exact strings may change there. Say in the comment that the version moves
  whenever f or its constants change.
- **`DrainCandidate`** gains `L` and `log_R`, the law's inputs, None where the candidate is unassessed. Every other
  field is as today.
- The fixed law's record is as today but for the new field.

### 1.4 The tool route

`tools/history_and_bbn.py` under `--drain-law one_loop`:
- its history line names `drain_law=one_loop A=… model=one-loop-measured-v1` in place of `gap=…`;
- its `drain` line is as today. If you add the K share drained in each regime, as it gives now, say so.

Under the fixed law, every line it prints is as today.

### 1.5 The crossover scripts

`source/crossover/cascade_map.py` and `measured_law_map.py` set `SM._GAP_PRESCRIPTION`. Make the smallest change that
lets them run on the new tree: skip the attribute when it is absent. They are §2.3's instruments.

- `measured_law_map.py` patches `drain_alpha_effective` by its call signature. If §1.1 changes that signature, adapt
  the patch to it, without changing the law it applies.
- Each change is named in the log. Nothing else in `source/crossover/` changes.

### 1.6 Tests

- **The law alone:** each row of §1.1's table at synthetic inputs, including:
  - ℓ = 1 exactly, the cap, and d ≤ 0;
  - ln R = r₀ exactly, ln R ≤ 0, and the unassessed case;
  - A·f ≥ α_cap, and w_gate's two cases.
- **S(L):** at every table node; between nodes against an independent computation of the log-linear interpolation;
  below 0.3; above 134.
- **The reference:** at a grid of (regime, ℓ, L, ln R) covering every branch, f equals `fraction_adopted` (imported
  from `source/crossover/`, or a verbatim copy kept in the test module with its source named), to the bound of §2.2.
- **U15:** the law at an n = 2 model takes L = ln(1 + E/V₀), and at n = 1 it equals M/φ_t to the turning point's
  accuracy.
- **The record:** `DrainRecord.model` under both laws, and the new `DrainCandidate` fields.
- **The removed feature:** no `_GAP_PRESCRIPTION`, and the tool refuses `--gap-prescription` as an unknown argument.
- **`TestTheOneLoopLawOn2197`** and any other test pinning values of the old law: re-pinned to the new law.
  - Each re-pinned value is cross-checked against `measured_law_map.py --law=adopted` on the same inputs, in the log.
  - Each re-pinned test is named in the log.

**The counts.** Tests of the removed behaviour (`zero`, `edge`, `DRAIN_C1`, `--gap-prescription`) are deleted or
rewritten. A suite's count may fall only by those, each named in the log. Every other fall is a stop.

## 2. The Measured gate (README §8.10)

### 2.1 The fixed law is unchanged

- The goldens pass bitwise, unchanged: `test_golden_history`, `test_golden_reflecting_history`, `test_tuning`, and
  `ObjectFactories/tests/test_store_fingerprint_golden.py`.
- The fixed law's payload differs only in `DrainRecord`'s field (§1.3).

### 2.2 The law equals its reference

- f equals `fraction_adopted` at every grid point of §1.6, bitwise wherever the expressions are the same.
- Where an interpolation or an operation order must differ, they agree to 10⁻¹⁴ relative, and the log says where and
  why.

### 2.3 Crossover's adopted-law maps reproduced

Run `source/crossover/cascade_map.py` **unpatched** on the new tree. The production law now does what
`measured_law_map.py --law=adopted` patched in. Run these four, with the same `result.json` and normalization as
crossover used (its README, "The adopted law at small A"):

| history | A | crossover's map |
|---|---|---|
| P-2197 | 0.05 | `out/adopted-A0.05/P-2197-A0.05-zero.txt` |
| W-2187 | 0.05 | `out/adopted-A0.05/W-2187-A0.05-zero.txt` |
| R-632 | 0.5 (A = 0.5's tuning) | `out/adopted-floor/R-632-A0.5.txt` |
| L-M5e-20 | 0.2 (A = 0.5's tuning) | `out/adopted-floor/L-M5e-20-A0.2.txt` |

For R-632 and the ladder, use the same `result.json` copies with A changed. Rebuild them as crossover did:
`--alpha` in `argv` changed, and the rest A = 0.5's.

- **Each candidate** must equal crossover's at the map's printed precision: N, regime, ℓ, α_eff, ln(E/V0), ln R and
  L.
- **The header** must match too: the parking N, N_final and the candidate count.
- **A difference is a stop**, with the first differing candidate and its cause. The one exception is a cause in the
  last bits of an operation order that §2.2 bounds; then state it and continue.

### 2.4 The tool route

`tools/history_and_bbn.py --drain-law one_loop --alpha 0.05` on P-2197, at its 02b normalization
(`--fixed-normalization`). Its history line names the model version, and its `drain` line agrees with §2.3's map on
the counts by regime and ℓ_min.

## 3. What is not wanted

- **No stored key, column, table or label.** 03 stores the drain-model key. That means no change to
  `build_extra_data`, the factories, `config/`, `main.py` or the plot scripts. `VERSION_LABEL`, `FINGERPRINT_FORMAT`
  and the stepper label stay as they are.
- **No change to the classification** (`drain_regime`, `one_loop_turnaround`, `classical_turning_point`), the gate,
  parking, the release, the floor rule or G1/G2. Only α_eff changes, and the record.
- **No re-measurement** of the sample; that is 02d. No tuning beyond §2.4's single run, and none of 02's or 02b's
  scripts run or changed (§1.2).
- No store written but a test's temporary one. No Ray, no cluster, no merge, no push.
- `PRyM/`, `thirdparty/` and the packages are not touched.

## 4. Verification

- **The suites**, before the change and after, per `CLAUDE.md`: ComputeTargets in two halves, `tools/tests` alone.
  Record counts, times and `uptime`. A count may move only as §1.6 says.
- **The runs.** §2.3's four maps take minutes each (crossover: R-632 and the ladder are the longest). Run them
  detached, check once that they started, and wait on them by a background command, not by polling. At most six
  workers at once; the machine is shared.
- **Where things go.** Outputs go under `var/one-loop-drain/02c/`. Scratch goes in the agent's subdirectory of the
  session scratchpad, never `/tmp`.
- **Reproducibility.** Every number of the verification section comes from a script in
  `.documents/one-loop-drain/law-02c/`, with its output in `law-02c/out/` (text only), or from a file under
  `var/one-loop-drain/02c/` that the section names.

## 5. The commit, the log and the board

One commit (invariant 1), with:
- **the code:** `ComputeTargets/ScalarHistory.py`, any model file §1.1 touches, `tools/history_and_bbn.py`; the new
  and changed tests; §1.5's two scripts; §1.2's dated notes;
- **the scripts:** `.documents/one-loop-drain/law-02c/` and its `out/`;
- **the verification document:** `.documents/one-loop-drain-verification.md`, a new §2c holding §2's gate item by
  item. It is additive (invariant 6): §1, §2 and §2b stay as they are;
- **dated notes** in `.documents/numerical-strategies.md` (the law, its constants, S(L), the removed prescriptions) and
  `.documents/architecture-summary.md` (`DrainRecord.model`, the new `DrainCandidate` fields, the tool's option);
- **`CLAUDE.md`'s test counts;**
- **the log** `logs/02c-the-measured-law.md` (template: `dk-and-rpk-migration` README §5.1).
  - Every deviation from this prompt is classified.
  - It has "Observations not acted on".
  - Its "State handed to the next prompt" says what 02d needs: the tool's options, the record, the costs, and
    anything of §2.3 that bears on the sample;
- **the board:** row 02c, its items, the Measured gate, §3/§4 if an issue moves, and **Last updated**;
  `.documents/OPEN_ISSUES.md` in the same commit.

Format the Python files you change with `black`. Stage by explicit path, and show `git diff --cached --name-status`
before committing. The commit message follows `CLAUDE.md` invariant 5. Write "this commit" where the SHA goes; the
orchestrator substitutes it.
