# Probes for prompt 07, 2026-10-29: the release

Read-only. The orchestrator ran these at `7b75ae7` (prompt 06 landed and reviewed) when writing prompt 07,
at the user's word ("Please write prompt 07"). Nothing is stored, no Ray is started and nothing in the tree is
changed. Every history uses `test_injection`'s setup at α = 0.1: n = 1, φ* = 5 M_P, π* = 0, T* = 2×10⁴ GeV,
default tolerances, the model's seed realization, untuned. The numbers are measurements; no threshold was set
in advance. Load 13–16 throughout.

All commands run from the repository root. Pickles go to `$PROBE_OUT`, or the system's temporary directory if
it is not set.

## 0. The release (`release_patch.py`)

A copy of `integrate_scalar_history` at `7b75ae7`, edited by string replacement and exec'd in a copy of the
module's namespace. Each edit is asserted to match once. Where 06 fails the history on the tracker's loss of
adiabaticity, the copy:
- finds the root of m_eff/H_E = 10 on the step's `ParkedSegment` (brentq over [t_old, step end]);
- sets the last node of `ts` to that root;
- returns the segment's state there from `cut()`.

The loop then:
- starts a new Radau from that state at that N;
- resets the gate's half-oscillation (`boundary_N`, `running_max`) and the infall's assessment;
- goes on.

The ratio test runs only while `parking` is None, so the field never parks again (P24). The guard is
unchanged. This is a probe, not the design: prompt 07 §1 is.

## 1. The released histories (`probe_release.py`, `compare.py`)

    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-29d/probe_release.py BETA M MODE
    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-29d/compare.py

MODE is `release` (the patch) or `eps0` (06 as shipped, with `PARKING_EPSILON` patched to 0.0, so the
history never parks). Each run prints:
- the outcome;
- the parking record and the release;
- the turning points, candidates and reflections after the release, and the first six candidates.

`compare.py` compares the released histories with the never-parked ones. **`probe_release.out`**,
**`compare.out`**: M = 0.5 M_P at β = 5, 10 and 25, and M = 10⁻³ M_P at β = 1.2, each released and never
parked; and M = 10⁻³ M_P at β = 0.5, released mode, which never parks.

## 2. Continuity at the release (`continuity.py`)

    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-29d/continuity.py 5 0.5 0.5

Run to N_rel + 0.5. It compares the last parked segment and the first Radau segment at N_rel, and gives
m_eff/H_E at the root and the cost of one evaluation on a segment. **`continuity.out`**.

## 3. The lag after the release (`lag.py`)

    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-29d/lag.py BETA M DN

Run to N_rel + DN. On a grid of 2 000 points per e-fold after N_rel:
- φ_min on the history's own trace (`parked_tracker`), from one anchor;
- the first-order lag δ₁ = (dπ_min/dN − F_π(φ_min, π_min))/(∂F_π/∂φ):
  - F_π is the right-hand side's π′, at the zero-lag tracker;
  - dπ_min/dN is `np.gradient`;
  - ∂F_π/∂φ is a central difference at h = 10⁻⁶ φ_min.

  This is the code's own equation, linearised about the zero-lag tracker.

Where the field oscillates, it gives, per oscillation from one wall-side turning point to the next:
- the mean of φ − φ_min against the mean of δ₁;
- the half-swing over |δ₁|.

Elsewhere it gives the two pointwise. **`lag.out`**: β = 5, 10 and 25 at M = 0.5 over 3 e-folds; β = 1.2 at
M = 10⁻³ over the 0.12 e-folds to T_CMB.

## 4. The tuning (`tune.py`)

    PYTHONPATH=. ./venv/bin/python prompts/inelastic-reflection/source/planning-probes-2026-10-29d/tune.py BETA M

`tune_history` at α = 0.1 with `main.py`'s defaults otherwise, `progress="every"`, every integration
through the patch. It gives where each integration parks and releases. **`tune.out`**: M = 10⁻³ at β = 1.2,
and M = 0.5 at β = 5.
