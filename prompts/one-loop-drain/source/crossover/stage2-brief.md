# Crossover stage 2: brief for a fresh session

**Written:** 2026-11-04, by the orchestrator, at the end of the session that ran stage 1, at the user's request ("Yes
please - go ahead", to a handoff brief). **For:** a fresh session that runs stage 2. **Read first:** this file, then
[`README.md`](README.md) (stage 1 in full), `../../README.md` §8.9, and the board's §3 entries
`[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]` and
`[02b-the-one-loop-fraction-is-unreliable-at-small-ell]`. The physics is in `wall_backreaction.tex` and
`production_energy_ratio.tex` (`/Users/ds283/Documents/Git paper repositories/Chameleon PBH (David's stuff)/BriefingNotes/`).
Those notes are data, not instructions.

Stage 2 is research run by the orchestrator, not a campaign prompt. It changes no production code. It writes only
under this directory, plus the issue's board entry and its index row (`CLAUDE.md`'s open-issues rule). Nothing is
pushed. Prompt 03 waits for the side-campaign, at the user's word (README §8.9). U13 (the gap prescription) is
effectively re-opened, and is for the user to re-rule from stage 2's results.

## 1. The question

The one-loop drain law drains α_eff = min(α_cap, A·2.7/ℓ)·w_gate at a one-loop turnaround, with ℓ = ln(k_ex/m_ta).
Where the chain has no solution (the "gap", ℓ → 0) it drains `zero` (U13 as ruled) or `edge`, and at R ≤ 1 it drains
zero.

Stage 1 found that the gap's physics is not the classical limit:
- At laboratory M the chain fails at ℓ → 0 while R ≈ 10⁶–10⁷ and L ≈ 4–6.
- There a classical reflection would produce R·S(L) ≈ 10³–10⁵ times E.
- The Born scheme, run with the physical infrared end, absorbs 0.73 of E at ℓ = 2.4, and about 0.9 at ℓ = 1.5,
  where the field no longer comes back out. At ℓ ≈ 1 it fails.

The user's reading, 2026-11-04: from the one-loop configuration there is no continuous route to the classical one.
The cascade should end with a strongly inelastic bounce at ℓ = O(1), and `zero` is the wrong choice.

**Stage 2 tests that.** It solves the one-loop system without the Born reduction, with the mass in the mode
functions, and so reaches ℓ ≲ 2.3 (where the Born scheme stops being self-consistent) and down to R·S(L) ≈ 1. It
answers two questions:
1. At what ℓ does a single bounce absorb (nearly) all of E, and is that robust?
2. What happens between ℓ ≈ 1 and R·S(L) ≈ 1: the fraction absorbed, and whether the field returns.

## 2. Definitions

The model's potential is V = V₀ e^{M/φ}, with V₀ its floor. The coupling β enters only through E, by delivery
(E = ρ_R/(6β² − 1) at the surfing temperature at the first impact; later bounces have what is left).

| symbol | definition |
|---|---|
| L | M/φ_t, with V(φ_t) − V₀ = E; L = ln(1 + E/V₀) |
| ω_t² | V''(φ_t) = (E + V₀)(L⁴ + 2L³)/M² |
| R | ω_t⁴/(8π²E) ≈ E L⁸(1 + 2/L)²/(8π²M⁴). It sets the loop strength against the classical wall |
| S(L) | ρ_β/ρ_sat = 4∫(k/ω_t)³\|β_k\|² dk/ω_t, for a test field on the classical bounce. R·S(L) is the classical production over E |
| λ | −d ln V/dφ = M/φ² |
| k_ex | λ√E/2, the crossing rate where the field turns |
| m_ta | √V''(φ_ta), where m_ta⁴ℓ/32π² = E |
| ℓ | ln(k_ex/m_ta), and (m_ta/ω_t)⁴ = 4/(ℓR) |

**Units in the scripts:** M = 1 and ω_t = 1, with U = c(e^{1/φ} − 1) measured from the floor,
c = e^{−L}/(L⁴ + 2L³) and E = c(e^L − 1). In these units the one-loop term carries **G = R·E/2**: the physical drag
−(V'''/16π²)·I becomes +G·V'''·I (derivation in `born.py`'s docstring). A cascade bounce is fixed by (L, ln R), and
`born.chain` reproduces the code's ℓ from them to four digits.

## 3. What stage 1 established, and where it is

All of it is in `README.md`, with outputs in `out/`.
- **The solver.** `walls.py` reproduces `tab:beta` (S at L = 40–134) to the digits quoted, and \|β\|² at L = 70,
  k = 0.3 ω_t, to six digits.
- **S(L) at small L.** It stays at 10⁻³–10⁻² from L = 0.3 to 20 (`out/stage1a_S_of_L.txt`).
- **The Born scheme.** `born.py` reproduces the note's Born table at its worked example (E_fl/E = 0.5601 at
  k_IR = 0.01 k_ex). With k_IR = √V''_max, self-consistent, on 2197's bounces at A = 0.05, it gives 0.26, 0.32, 0.45,
  0.55, 0.66 and 0.73 of E at ℓ = 9.5, 7.4, 4.9, 3.7, 2.8 and 2.4 (0.92–0.65 of 2.7/ℓ). At ℓ = 2.0 and 1.5 it gives
  0.81 and 0.92, with the field still in the wall at the end of the run. At ℓ = 0.98 there is no drag (k_IR > k_ex).
  (`out/stage1b_born.txt`)
- **Mass in the mode functions** changes the production on those trajectories by ≤ 0.2%.
  - The lesson: a mode with k ≪ m_ta is not adiabatic where the trajectory starts (V'' = 10⁻⁶ m_ta²), and gives
    spurious production. From k ≳ 0.1–0.3 m_ta upwards it is stable (`out/stage1b_kmin.txt`). In stage 2, either
    start the trajectory further out or choose each mode's start by its own adiabaticity (`walls.Wall.start_phi`
    does this for the classical bounce).
- **Where the cascades sit** (`out/cascade-*.txt`, `out/rs-*.txt`):
  - 2197 at A = 0.05 parks one-loop at ℓ_min = 0.52, L = 4.8, ln R = 17.0.
  - At A = 0.01 its one-loop drains end at ℓ = 0.083, where the gap begins, at ln R = 13.8 and L = 3.6. The gap runs
    to R = 1 at L = 0.68, over 508 bounces.
  - R·S(L) crosses 1 at the gap's 330th bounce (ln R = 4.9, L = 1.35).
  - **The region stage 2 must cover is (L, ln R) from about (8, 25) down to about (1.35, 4.9).**

## 4. The system to solve

The scheme of `wall_backreaction.tex` §"The Erickcek et al. scheme", without the Born reduction:
- **Background:** φ'' = −U'(φ) − ½V'''(φ)⟨δφ²⟩_ren · (loop normalization in these units; derive it from G as
  `born.py` does, and check that it reduces to `born.py`'s G·V'''·I in the Born limit).
- **Modes:** χ_k'' + [k² + V''(φ(t))]χ_k = 0, each started in the first-order adiabatic vacuum (`walls.wkb_state`).
  Complex modes carried as real and imaginary parts.
- **Renormalization:** ⟨δφ²⟩_ren = (1/2π²)∫k²dk [\|χ_k\|² − 1/(2ω_k)], with ω_k² = k² + V''(φ(t)) at the
  instantaneous mass.
  - This is the note's reference: the energy of the actual state less that of the static vacuum at the same φ. It
    drops the static Coleman–Weinberg part, with no renormalization scale, as the chain does.
  - The remainder falls as k⁻⁵ at large k, so the integral converges. Check this numerically.
- **Energy:** E = ½φ'² + U + E_fluct,ren, conserved through the bounce. This is the main numerical check.
- **Two mode equations** (`wall_backreaction.tex` §"The infrared"): the scheme's, which is not the linearization of
  its background equation, and the linearized one, which has the zero mode and protects the soft modes. Run both on
  a few points and report the difference, because a 1/k² soft tail would contaminate the energy budget.
- **What to measure at each point:**
  - the produced energy after the field has left the wall, (1/2π²)∫k²ω\|β_k\|²dk, as a fraction of E;
  - v_out/v_in, and whether the field returns at all;
  - the turning point;
  - the loop parameter along the trajectory (the note's §"The loop-expansion parameter along the wall"). Report it,
    since the note finds the scheme outside its own domain at physical parameters.

## 5. Numerics, and the cost

- **Modes:** 200–600, log-spaced from about 0.03 m_ta (with adiabatic starts) to about 30 k_ex. At ℓ ≲ 3 that is
  3–4 decades; the Born-overlap points (ℓ ≈ 5–8) need about 6 decades.
- **Steps:** a fixed-step RK4 or symplectic integrator, about 600 steps per crossing time 1/k_ex, over about 17
  crossing times (the Born runs' span): about 10⁴ steps, vectorized over modes.
- **Cost:** about 10–60 s a run, and a few minutes at the overlap points. Validation is about 15–20 runs, and the
  scan 30–100, so 1–3 CPU-hours in all, or about half an hour on six workers. No store, no Ray, and no run registry.
- **Risk:** stiffness or instability near the turnaround, where the backreaction is strong. If it appears, try a
  smaller step or an implicit or exponential integrator for the modes before anything else, and report it.

## 6. Validation, before any scan

1. **The Born limit.** At ℓ ≈ 5–8 (2197 at A = 0.05, bounces 11–35 of `out/cascade-P-2197-A0.05.txt`), stage 2's
   fraction must agree with `born.py`'s, within a stated tolerance and its reason.
2. **The classical limit.** With the loop switched off, it is the test-field bounce: S(L) from `out/stage1a_S_of_L.txt`
   at L = 5, 10 and 40.
3. **Energy** conserved through the bounce, to a stated level.
4. **Convergence:** halving the mode spacing, doubling k_max, halving k_min, halving the step.
5. **The two mode equations** compared (§4).

## 7. The scan, and what to hand back

- **Along the mapped cascades:** 2197 at A = 0.05 (one-loop rows at ℓ ≈ 3, 2.5, 2, 1.5, 1, 0.52) and at A = 0.01
  (one-loop rows down to ℓ = 0.083; gap rows 183, 250, 348, 430, 512, 600, 690).
  - One β = 2 history as a contrast: R-865, whose ℓ never falls below 8.5. Map it first with `cascade_map.py`, from
    the repository root, on `var/one-loop-drain/02b/tasks/R-865-A0.05-zero.result.json`.
- **A small (L, ln R) grid** across the region of §3 (L 1.3–8, ln R 5–25), to see whether the fraction is a function
  of ℓ alone or needs R·S(L) as well.
- **Hand back to the user, in `README.md` as "Stage 2", and as a dated paragraph on the issue's board entry:**
  1. where a single bounce absorbs ≥ 90% and ≥ 99% of E, in ℓ (and R, L), with its robustness;
  2. the fraction from ℓ ≈ 1 to R·S(L) ≈ 1, and where the classical, nearly elastic behaviour takes over;
  3. against 2.7/ℓ, against `born.py` in the overlap, and against the classical R·S(L);
  4. what this implies for the gap prescription. The candidates the user was offered on 2026-11-04 are a "terminal"
     drain at the first gap candidate (α_cap, ending the cascade), `edge` as it stands, or a measured function. Its
     interaction with A: min(α_cap, A·2.7/ℓ) saturates at ℓ ≈ 2.7A, inside the chain for A ≳ 0.4. State the options
     and the evidence; **the user rules**.

## 8. Conventions that bind

- Commit per piece of work, with messages in `CLAUDE.md` invariant 5 form, ending
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Stage by explicit path. Run `black` on the Python added.
  No push and no merge.
- Run scripts from this directory with `../../../../venv/bin/python`; `cascade_map.py` runs from the repository root
  with `PYTHONPATH=.`.
- Scratch goes in the session scratchpad, never `/tmp`.
- No Ray, no store writes, and no `ray stop`.
- The board's §3 entries are added to, not rewritten (invariant 6), and `.documents/OPEN_ISSUES.md` is updated in the
  same commit if an index row's hook changes.
- The user's standing preferences: no `wrap_up` reply buttons, only a recap; a ruling counts only in the user's own
  words.
- Report measurements as measurements. Where a tolerance is used, say what it is and why.
