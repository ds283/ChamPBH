# Log 03 — Store the first bounce

**Prompt:** prompts/science-readiness/03-first-bounce.md
**Commit:** the commit that adds this file ("Store the first bounce of every ScalarModel"); its SHA is in `git log`
**Model:** Opus 5.5
**Date:** 2026-10-02
**Result:** COMPLETE WITH DEVIATIONS

The work was done on top of `521636e`. "HEAD~1" below means the parent of this prompt's commit,
which has `521636e`'s tree. HEAD~1 measurements were made on `521636e` itself before any edit
(the stand-in measurement, the "before" suite counts), or on an export of it
(`git archive 521636e | tar -x`, with the two new test files copied in), run with this
repository's `venv/bin/python`.

## What shipped

`VERSION_LABEL` stays `"2026.6.0"` and `PRYM_VERSION` stays `"bf24c3d+ri02+sr01"`; neither is
touched.

- **`ComputeTargets/ScalarModel.py`**
  - `:16` imports `bisect_left`.
  - `:766–851` (new, after `integrate_scalar_history`, before `_failure_payload`):
    - `FirstBounce = namedtuple("FirstBounce", ["N", "phi_Einstein", "log_T_Jordan", "reflected"])`.
      `phi_Einstein` and `log_T_Jordan` are in the cosmology's units, as in `SampleValues`.
    - `first_bounce(result: IntegrationResult) -> Optional[FirstBounce]`. It walks
      `result.solution.ts` and `.interpolants` in order. It stops at the first step `k` whose own
      interpolant has `π(t_k) < 0 < π(t_{k+1})`, and takes the root of π on that interpolant by
      `brentq(..., xtol=1e-15)`. It reads `φ` and `ln T_J` from the interpolant at the root and
      returns `reflected=False`. Before testing step `k`, it checks whether the first reflection
      has `N ≤ t_k`. If so, the reflection comes first. The function then returns
      `FirstBounce(N=reflections[0].N, φ, ln T_J, reflected=True)`, with `φ` and `ln T_J` read
      from the interpolant of the step that ends at the reflection (found by `bisect_left` on
      `ts`), or from the first step if the reflection is at the history's first `N`. It returns
      `None` if there is neither. No `φ < 1.5 M` filter is applied (P5).
  - `compute_scalar_model`, `:1061–1062`: after the `try` that holds the loop and the sampling,
    `bounce = first_bounce(result)`; the success payload gains `"first_bounce": bounce` (`:1077`).
    The failure payloads are unchanged; `_failure_payload` does not carry the key.
  - `ScalarModel.__init__`: `self._first_bounce = None` with no payload (`:1195`), and
    `payload["first_bounce"]` from one (`:1205`).
  - `ScalarModel.first_bounce` (new property, `:1298–1312`): returns the `FirstBounce` or `None`;
    raises `RuntimeError` on a failure row, with `metadata`'s message, and on an object that has
    not been populated (`_failure is None`).
  - `ScalarModel.store()`: `_first_bounce = None` on a failure (`:1475`), and
    `data["first_bounce"]` on a success (`:1481`).
- **`Datastore/SQL/ObjectFactories/ScalarModel.py`**: four schema columns are added to the
  `ScalarModel` table. They are all nullable, and none is removed:
  - `first_bounce_N Float(64)`;
  - `first_bounce_log_T_Jordan Float(64)`;
  - `first_bounce_phi_Einstein Float(64)`;
  - `first_bounce_reflected Boolean`.

  They sit after `min_RHS_time` (`:189–196`) and are stored in the units of the
  `ScalarModelValue` columns (Deviation 2). The other edits:
  - `:28` imports `FirstBounce`.
  - `build` selects the four columns (`:252–255`), and its payload gains `"first_bounce"`
    (`:470–481`). That is a `FirstBounce` converted back to the cosmology's units. It is `None`
    on a failure row or when `first_bounce_N` is NULL.
  - `store` writes the four values (`:542–562`). All four are NULL on a failure row and when
    there is no bounce.
- **`tools/history_and_bbn.py`**: a `bounce …` line after the `history …` line. It prints `N`,
  `T_J` in MeV, `φ` in M_P and `reflected`, or `bounce …: none`. The module docstring lists the
  line.
- **Tests:**
  - `ComputeTargets/tests/test_first_bounce.py` (new, 7 tests): (a), (b), (c), and (d) on four
    windows.
  - `Datastore/tests/test_first_bounce_round_trip.py` (new, 5 tests): (e) with a turning point,
    with a reflection, with no bounce, and on a failure row, plus an unpopulated object.

Nothing touches `integrate_scalar_history`, `IntegrationResult`, the sampling loop, `extra_data`,
BBN or `.documents/`.

## Deviations from the prompt

### 1. Acceptance 3 is compared against two references, because log 01's driver output has neither wall bounces nor β = 1.6 — STRUCTURALLY REQUIRED

The prompt assumed log 01's driver output held RHS, accepted steps and wall bounces for all
three histories. It holds RHS, accepted steps and reflections, and only for β = 2 at
`M = 0.5` and `10⁻³`. The driver prints no wall-bounce count. It receives only
`compute_scalar_model`'s payload, which has no such count, and the prompt forbids storing one.

What was done instead:

- RHS, accepted steps and reflections were compared with log 01 for the two β = 2 histories.
- For β = 1.6, `M = 10⁻⁵` they were compared with README §6.1 (the planner's probe on `6aaa706`).
- The wall bounces were counted by a scratch probe outside the repository
  (`scratchpad/bounce_probe.py`). It runs the same `compute_scalar_model._function` call as the
  driver and wraps `integrate_scalar_history` to capture the `IntegrationResult`. It counts with
  `test_kinematic_cap_loop.wall_bounces`. The counts were compared with verification §4.8 (26 and
  803), and the count for β = 1.6 was recorded.

All match (Verification).

### 2. The columns store φ in M_P and ln T_J with T_J in GeV — IMPLEMENTATION CHOICE

README §2 (g) fixes the column names and gives them no unit suffix. The alternatives:

- **The raw values in the cosmology's `UnitsLike`.** This is simpler, but a row would then mean
  different things under different unit systems.
- **The `ScalarModelValue` convention (chosen).** `phi_Einstein_Mp = φ/M_P` and
  `log_T_Jordan_GeV = ln T − ln GeV`. `store` converts and `build` converts back, as the value
  table does. A comment on the columns states the units.

The round trip is exact to the last bit for any physical bounce:

- `N` is not converted.
- `PlanckMass = 1.0` in `Planck_units`, so φ is not changed.
- For `ln T_J`, the subtraction `ln T − ln GeV` is exact by Sterbenz's lemma when `ln T` is
  within a factor 2 of `ln GeV` = −42.33. That covers `4×10⁻¹⁹ GeV < T_J < 1.5×10⁹ GeV`, all of
  them below 2×10⁴ GeV and above `T_CMB` = 2.3×10⁻¹³ GeV. The addition back is then exact,
  because its result is representable.

The Datastore test checks this with `Planck_units()` on the stand-in cosmology.

### 3. A reflection's φ is read from the interpolant, not from `Reflection.phi_Einstein` — IMPLEMENTATION CHOICE

README §2 (g) says the reflection's φ and `ln T_J` come "from the step that ends at it", and
`Reflection` records no `ln T_J`. So both are read from that step's interpolant at the
reflection's `N`, for consistency.

- **The alternative.** Take `Reflection.phi_Einstein` for φ, which is the exact accepted state,
  and the interpolant for `ln T_J` only.
- **The difference.** One ulp at P1, `M = 10⁻¹⁰`: 4.702273820088335e-11 against
  4.7022738200883345e-11. Test (b) bounds it at 1e-9 relative.

One case is not covered by "the step that ends at it": a reflection at the history's first `N`,
where no step ends. There `φ` and `ln T_J` are read from the step that starts at it, at its left
end. That gives the accepted state exactly, since a Radau dense output at `x = 0` returns `y_old`.

### 4. Where the call sits, and the property on an unpopulated object — IMPLEMENTATION CHOICE

- **Where the call sits.** `first_bounce(result)` is called after the `try` that holds the loop
  and the sampling, not inside it. The function raises nothing that should become a failure row:
  `brentq` is only called on a strict sign change. Any exception there is a bug and should not
  be stored as a history's failure.
- **The property on an unpopulated object.** `ScalarModel.first_bounce` also raises when
  `_failure is None` (the object was never populated). `None` is a legal value for a history
  with no bounce, so returning `_first_bounce` there would be indistinguishable from "no bounce".
  This mirrors `failure_reason`'s check.

### 5. Two tests beyond the prompt's list — IMPLEMENTATION CHOICE

- **(e) has five methods, not three.** The extra two are a reflection row, so that
  `first_bounce_reflected = True` is exercised, and an unpopulated object.
- **(a) also checks `ln T_J`.** It must equal the solution's at the bounce `N`.

## Verification performed

**A note on load.** From about 00:01 on 2026-10-02 a job that is not part of this prompt ran on
this machine throughout the driver runs and the "after" suites. It was a SecondaryGWKit
`extract_GkWKB_data.py` with a ten-worker local Ray cluster. The load average was 100–216 during
the driver runs and 126 → 28 over the after-suites. The histories were still run one at a time,
with nothing else of this prompt's running. **The wall times quoted below are loaded.** RHS
counts, step counts, bounces and abundances do not depend on load. The "before" suites ran before
that job started.

**Suites** (the three commands of README §5 rule 6, from the repository root; I ran them):

| package | before (`521636e`) | after |
|---|---|---|
| CosmologyModels | 18 OK (71.7 s) | 18 OK (73.1 s) |
| ComputeTargets | 75 OK (80.2 s) | **82** OK (82.7 s) |
| Datastore | 21 OK (3.5 s) | **26** OK (1.9 s) |

ComputeTargets rises by the 7 methods of `test_first_bounce.py`, Datastore by the 5 of
`test_first_bounce_round_trip.py`. No test was deleted. `venv/bin/black --check` on the five
changed or new Python files: "5 files would be left unchanged".

**The new tests fail on HEAD~1** (I ran them on an export of `521636e` with the two test files
copied in):

- `ComputeTargets.tests.test_first_bounce`: "Ran 7 tests … FAILED (errors=7)". Each is an
  `AttributeError` on `SM.first_bounce`.
- `Datastore.tests.test_first_bounce_round_trip` fails at import, with
  `AttributeError: module 'ComputeTargets.ScalarModel' has no attribute 'FirstBounce'`, at the
  module-level `TURNING_POINT`. That tree has no `first_bounce_*` column and no
  `ScalarModel.first_bounce` either.

**README §6.4**, row by row (`test_first_bounce`, `test_first_bounce_round_trip`, and a scratch
print of the same calls, on this tree):

| row | target | measured |
|---|---|---|
| P1 window to `N = 21`, `M = 0.5` | `N = 20.343028 ± 1e-5`, `φ = 4.57371e-3 ± 1e-4` rel., not reflected, equal to `interpolated_minima(...)[0]` to 1e-12 | `N = 20.343026850496464` (−1.15e-6), `φ = 4.573704679806311e-3` (1.15e-6 rel.), `reflected = False`; equal to `interpolated_minima[0]` exactly (ΔN = 0, Δφ = 0); 1 945 RHS |
| P1 at `M = 1e-10` | `reflected = True`, `φ ∈ [1e-11, 1e-10]` | `reflected = True`, `N = 20.352100380348762 = reflections[0].N`, `φ = 4.702273820088335e-11`, `ln T_J = −42.62899904487363`; one reflection; 1 693 RHS |
| P2 from `N₀` to `N₀ + 0.01` | `None` | `None` (23 RHS, no reflection) |
| (d) agreement with `wall_bounces` | the same step | the same step on all four windows: P1 at 0.5, 0.01 and 0.001 (`N` = 20.343026850, 20.351918851, 20.352082227; `wall_bounces[0]` at the end of that step, 20.343034652, 20.351918855, 20.352082228), and P3 (`N` = 33.199041106, `φ` = 2.79886e-4, `wall_bounces[0]` at 33.199102583); `φ < 1.5 M` on each, and equal to `interpolated_minima[0]` to the bit |
| full histories (driver) | §4.8 to 1e-5 in `N` and to the printed digits in `T_J`; β = 1.6 recorded | below |
| round trip | the four columns back as written; `None` back as `None` | `assertEqual` on all four values for a turning point and for a reflection (raw columns also checked in stored units); four NULLs and `None` with no bounce; four NULLs and `RuntimeError` on a failure row |

The (d) stop condition (P5's premise) is not met. On every window, the first negative-to-positive
turning point with no filter is the first `φ < 1.5 M` wall bounce, on the same step. The
reflection restart did not make any interpolant unusable: the reflection is at a step boundary,
and the step before and the step after are each read only on their own interval.

**The driver** (I ran each, one after another, from the repository root, `./venv/bin/python
tools/history_and_bbn.py β M`, full network; cosmology banner lines omitted; loaded, see above):

```
history beta=2 M=0.5: RHS=40580 accepted_steps=4469 reflections=0 samples=5392 wall=1.6 s
bounce beta=2 M=0.5: N=20.343026853 T_J=746.634744 MeV phi=4.573705e-03 reflected=False
bbn beta=2 M=0.5: Yp=0.249229266 DoH=2.560889654 He3oH=1.054673338 Li7oH=5.241925487 network=full PRyM_time=20.8 s wall=20.8 s PRyM_version=bf24c3d+ri02+sr01

history beta=2 M=0.001: RHS=271783 accepted_steps=27979 reflections=0 samples=5435 wall=21.5 s
bounce beta=2 M=0.001: N=20.352082230 T_J=746.686275 MeV phi=9.150507e-06 reflected=False
bbn beta=2 M=0.001: Yp=0.2467606164 DoH=2.463862263 He3oH=1.042634494 Li7oH=5.409240365 network=full PRyM_time=15.7 s wall=15.7 s PRyM_version=bf24c3d+ri02+sr01

history beta=1.6 M=1e-05: RHS=1445132 accepted_steps=137137 reflections=0 samples=5219 wall=54.5 s
bounce beta=1.6 M=1e-05: N=18.974433718 T_J=420.758153 MeV phi=9.316270e-08 reflected=False
bbn beta=1.6 M=1e-05: Yp=0.2468788501 DoH=2.4647705 He3oH=1.042121506 Li7oH=5.419865323 network=full PRyM_time=9.7 s wall=9.7 s PRyM_version=bf24c3d+ri02+sr01
```

(The `ratio` lines are identical to log 01's for the two β = 2 histories, to every printed
digit.)

- **The first bounce against verification §4.8.**
  - β = 2, `M = 0.5`: `N = 20.34302685` against `20.34303` (3.1e-6), and `T_J` 746.63 MeV
    against 746.63.
  - β = 2, `M = 10⁻³`: `20.35208223` against `20.35208` (2.2e-6), and 746.69 MeV against
    746.69.

  Both are within 1e-5 in `N` and agree to the printed digits in `T_J`. β = 1.6 at `10⁻⁵`, now
  recorded: `N = 18.974433718`, `T_J = 420.758153 MeV`, `φ = 9.316270e-8 M_P`, not reflected.
- **Acceptance 3: no trajectory moved.** The two β = 2 histories match log 01 (40 580 / 4 469 / 0
  and 271 783 / 27 979 / 0). β = 1.6 matches README §6.1 (1 445 132 / 137 137, 0 reflections).
  The scratch probe's wall-bounce counts are 26, 803 and 4 337. The first two equal §4.8's. The
  probe's `nfev` equals the supervisor's RHS count on all three. On all three full histories the
  probe also finds `first_bounce` on the step that ends at `wall_bounces[0]`, with `φ < 1.5 M`,
  and equal to `interpolated_minima[0]` (ΔN = 0). That is P5's premise on full histories too.
- The BBN abundances are those of log 01 (β = 2) and README §6.1 (β = 1.6, "honly": Yp 0.2468788501,
  D/H 2.4647705), digit for digit. BBN is not touched by this prompt.

**The stand-in measurement for HEAD~1.** On HEAD~1 nothing stores the bounce. The source's
detector is the first π sign change from − to + among the *stored samples* with `φ < 1.5 M`.
The scratch probe applies it to `compute_scalar_model._function`'s samples:

| history | tree | sample detector: `N` / `T_J` / `φ` | dense output (`first_bounce`) |
|---|---|---|---|
| β = 2, `M = 10⁻³` (the prompt's) | `521636e`, before any edit | 20.354886 / **742.79 MeV** / 1.4006e-3 | 20.352082 / 746.686 MeV / 9.1505e-6 |
| β = 2, `M = 0.5` | this tree (the samples are unchanged) | 20.349666 / 737.54 MeV / 7.8001e-3 | 20.343027 / 746.635 MeV / 4.5737e-3 |
| β = 1.6, `M = 10⁻⁵` | this tree | 26.547158 / **0.38993 MeV** / 1.9223e-6 | 18.974434 / 420.758 MeV / 9.3163e-8 |

The source reported 0.138 GeV for 0.747 GeV "at small M". Here, at `M = 10⁻³`, the sample
detector lands 2.8×10⁻³ e-folds late, with `φ` 150 times the dense-output minimum. At
`M = 10⁻⁵` it misses the first 7.6 e-folds of bounces altogether and reports 0.39 MeV for
421 MeV. Either way, the bounce cannot be recovered from the samples.

## Observations not acted on

- The load from the other job during this prompt (Verification) inflated the driver's wall
  times 1.6–2.2× for the β = 2 PRyMordial solves (20.8 s and 15.7 s against log 01's 9.6 s
  and 10.0 s). Nothing in this prompt is bounded by wall time. It is noted so that a later
  prompt does not compare against these walls. No issue opened.

No §3 issue is opened by this prompt.

## State handed to the next prompt

- **`first_bounce`'s signature:** `first_bounce(result: IntegrationResult) -> Optional[FirstBounce]`
  in `ComputeTargets/ScalarModel.py`, with
  `FirstBounce = namedtuple("FirstBounce", ["N", "phi_Einstein", "log_T_Jordan", "reflected"])`.
  The fields are in the cosmology's units, as `SampleValues`; `N` is the history's forward
  e-fold number (`SampleValues.raw_N`'s). `compute_scalar_model` returns it as
  `data["first_bounce"]` (a `FirstBounce` or `None`) on a success, after the loop and the
  sampling; `ScalarModel.first_bounce` reads it back (raises on a failure row and on an
  unpopulated object). Import it as `from ComputeTargets.ScalarModel import FirstBounce,
  first_bounce` (the package does not re-export it).
- **The four column names** (`ScalarModel` table, after `min_RHS_time`, all nullable):
  `first_bounce_N Float(64)`, `first_bounce_log_T_Jordan Float(64)` (ln of T_J in GeV),
  `first_bounce_phi_Einstein Float(64)` (φ in M_P), `first_bounce_reflected Boolean`. All four
  NULL with no bounce and on a failure row. With prompt 02's `failure_reason`, the table now has
  29 columns of its own (plus `serial`, `version`, `timestamp`).
- **Where the payload carries it** (the pattern prompt 05 repeats for its sample fields):
  `compute_scalar_model`'s return dict -> `ScalarModel.store()` -> factory `store` row dict
  (converted to stored units) -> factory `build` (`select` list, and the payload handed to
  `ScalarModel.__init__` as `payload["first_bounce"]`) -> property.
- **The driver's output on the three histories** on this tree (loaded machine; the `ratio` lines,
  unchanged from log 01, omitted):

  ```
  $ ./venv/bin/python tools/history_and_bbn.py 2 0.5
  history beta=2 M=0.5: RHS=40580 accepted_steps=4469 reflections=0 samples=5392 wall=1.6 s
  bounce beta=2 M=0.5: N=20.343026853 T_J=746.634744 MeV phi=4.573705e-03 reflected=False
  bbn beta=2 M=0.5: Yp=0.249229266 DoH=2.560889654 He3oH=1.054673338 Li7oH=5.241925487 network=full PRyM_time=20.8 s wall=20.8 s PRyM_version=bf24c3d+ri02+sr01

  $ ./venv/bin/python tools/history_and_bbn.py 2 1e-3
  history beta=2 M=0.001: RHS=271783 accepted_steps=27979 reflections=0 samples=5435 wall=21.5 s
  bounce beta=2 M=0.001: N=20.352082230 T_J=746.686275 MeV phi=9.150507e-06 reflected=False
  bbn beta=2 M=0.001: Yp=0.2467606164 DoH=2.463862263 He3oH=1.042634494 Li7oH=5.409240365 network=full PRyM_time=15.7 s wall=15.7 s PRyM_version=bf24c3d+ri02+sr01

  $ ./venv/bin/python tools/history_and_bbn.py 1.6 1e-5
  history beta=1.6 M=1e-05: RHS=1445132 accepted_steps=137137 reflections=0 samples=5219 wall=54.5 s
  bounce beta=1.6 M=1e-05: N=18.974433718 T_J=420.758153 MeV phi=9.316270e-08 reflected=False
  bbn beta=1.6 M=1e-05: Yp=0.2468788501 DoH=2.4647705 He3oH=1.042121506 Li7oH=5.419865323 network=full PRyM_time=9.7 s wall=9.7 s PRyM_version=bf24c3d+ri02+sr01
  ```

  Wall bounces (`test_kinematic_cap_loop.wall_bounces`, scratch probe): 26, 803 and 4 337.
- **Suite counts after this prompt:** CosmologyModels 18, ComputeTargets 82, Datastore 26.
