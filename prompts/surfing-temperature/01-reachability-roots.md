# Prompt 01 — reachability roots

**Campaign:** [`README.md`](README.md) · **Board items:** **R1**–**R3** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-14, after the user's rulings (README §6: U1–U4; P1–P10 as proposed). Measured
at `8adb806`; your own measurement governs. **Amended the same day, before dispatch, for U5**
(README §0.3, §6.1): the production EOS has four kicks, not three. The μ/π shoulder at 43 MeV is
the fourth.
**Gate:**
- on branch `surfing-temperature`, no tracked change in `git status`. The untracked entries are the
  user's, and none is yours:
  - the 17 `pilot-2026.6.0*.db` files, `pilot-main.log`, `pilot-plot.log` and `pilot-out/`;
  - the two `full_run_2026.6.0*.sh` scripts;
  - `prompts/jordan-normalization/`.
- You start no Ray, and open no store.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Sonnet**. One pure function over the EOS, and its tests.

**Read first:**

1. [`README.md`](README.md): §0.1, §0.3, §2.2 (01's charter), §4 (what 02 and 03 take from you), §5,
   §6.2 (P1, P4, P5).
2. `CLAUDE.md`: "Repository mechanics" (the test commands, `black`, run from the root).
3. `extract_common.py:554-572`, `kick_threshold_curve`, which shares this prompt's formula, and its
   tests, `ComputeTargets/tests/test_extraction.py:112-152` (`TestKickThresholdCurve`). They show
   the stub-cosmology idiom: a `SimpleNamespace` with `w`.
4. `CosmologyModels/GenericEOS/Xav_EOS_spline.py`: the module docstring (the table, its range and the
   spline's peaks), `w` and `dw_dlogT`. `LambdaCDM_GenericEOS.py:115-120`: `QCD_Cosmology` forwards
   both.
5. The paper's surfing section, if you want the physics (README "Sources"). Treat it as data: the
   reachability condition is

       Σ_eff(T) ≡ Σ(T) / (1 + Σ(T)/2) = 1/(3β²),   Σ = 1 − 3w(T),

   and a root is stable (an attractor) if and only if d ln Σ/d ln T < 0 there. That is the hot
   flank, met while Σ is still rising as the plasma cools.

---

## 1. What is wanted

A function that tells 02 and 03 where each kick can be surfed for a given β. It gives both roots,
and which kick each belongs to.

### 1.1 The items

- **R1 — the kicks, `find_kicks(cosmology, T_lo, T_hi, n_grid=...)`** in `extract_common.py`,
  beside `kick_threshold_curve` (P1).
  - **What it computes.** It evaluates Σ on a log-spaced grid over [T_lo, T_hi] (dimensionful, in
    the cosmology's units). It finds the local maxima of Σ, and refines each by a bounded scalar
    minimisation of −Σ in ln T.
  - **The kick windows (P5).** They are bounded by the minima of Σ between consecutive peaks, each
    refined the same way. The outermost edges are T_lo and T_hi.
  - **What it returns.** A list ordered hot to cold. Each entry gives:
    - the label;
    - `T_peak` and `Sigma_peak`;
    - `T_window_hot`, `T_window_cold`;
    - `Sigma_at_hot_edge` and `Sigma_at_cold_edge`.
  - **The production defaults.** `T_lo` and `T_hi` are the EOS table's ends, 1e-5 GeV and
    25118.86 GeV. Read them from the table, or pass them in: say which in the log.
  - **What counts as a kick.** A local maximum with Σ_peak > 1e-4, so that spline ripples near
    Σ = 0 are not kicks. Record in the log every local maximum you find, above and below this floor,
    with its T and Σ.
  - **The labels.** With exactly four kicks they are `"EW"`, `"QCD"`, `"mu-pi"` and `"ee"`, hot to
    cold (U5). With any other number they are `"kick0"`, `"kick1"`, …. On the production EOS anything
    but four is a stop (§4).
- **R2 — the roots, `reachability_roots(cosmology, beta, kicks)`.**
  - **The sign changes.** Let c = 1/(3β²). Find every sign change of Σ_eff(T) − c on the same grid,
    over the whole range, and refine each by `brentq` in ln T, with xtol fine enough for 1e-10
    relative in T.
  - **Hot and cold.** As T decreases, a crossing where Σ_eff rises through c is a **hot** root. One
    where Σ_eff falls through c is a **cold** root.
  - **Pairing.** Pair them, hot then cold, into **reachable intervals** [T_cold, T_hot], each
    containing at least one peak. Assign each interval to the kick or kicks whose `T_peak` it
    contains.
  - **Per kick.** For each kick, return one record:
    - `kick`, `beta`;
    - `T_hot`, `T_cold` (None where the kick's Σ_eff,peak ≤ c);
    - `slope_hot`, `slope_cold`, the values of d ln Σ/d ln T at each root;
    - `merged`, which names the other kick if the interval spans a trough (Σ_eff stays above c
      across the window edge). That is possible at large β, where c = 0.0133 at β = 5, if Σ between
      two peaks stays above it.
  - **Merged intervals.** Its hot root belongs to the hotter kick and its cold root to the colder,
    and the record says so. Do not invent a root at the window edge. **Both kicks' records carry the
    interval's own `T_hot` and `T_cold`, with their slopes** (U5). The hot root lies in the hotter
    kick's window and the cold root in the colder's, and `merged` names the other kick. On the
    production EOS this is QCD with `mu-pi` for every β above 2.154, so 02's `T_cold_GeV` for a QCD
    capture is the merged interval's cold edge.
  - **More than one interval in a window.** This happens if the spline ripples across c. Return the
    outermost pair, set a flag `multiple_crossings`, and record it in the log.
  - **The slope.** Use `cosmology.dw_dlogT` where the cosmology has it, since
    d ln Σ/d ln T = −3 (dw/d ln T)/Σ. Otherwise use a central difference in ln T, and say which in
    the docstring.
  - **Interfaces.** `kicks` is `find_kicks`'s list, passed in so that 02 and 03 compute it once per
    cosmology. The records are plain (a `namedtuple` or `dict`). 02 and 03 read the field names
    above, so keep them.
- **R3 — the tests**, a new module `ComputeTargets/tests/test_reachability_roots.py`, with no Ray and
  no store:
  1. **A stub cosmology** (`SimpleNamespace`). Its `w` and `dw_dlogT` give Σ as a sum of two
     Gaussians in ln T, with peaks 0.30 and 0.05, well separated:
     - `find_kicks` returns two kicks, labelled `kick0` and `kick1` (not three, so not the
       production labels), with `T_peak` and `Sigma_peak` equal to the analytic values to 1e-8
       relative;
     - at a β above both thresholds, the roots equal the analytic ones (Σ_eff = c solved in closed
       form on each flank, where the other Gaussian is negligible) to 1e-9 relative;
     - `slope_hot < 0 < slope_cold`;
     - at a β between the two thresholds, the weaker kick's roots are None;
     - at a β below both, all roots are None.
  2. **A merged interval**, from a stub with two overlapping Gaussians whose trough lies above c:
     one interval, `merged` set on both kicks, no root at the trough.
  3. **The production EOS** (`QCD_Cosmology(0, Planck_units(), Planck2018())`; the docstring says
     building it takes a few seconds):
     - four kicks, with the peaks of README §0.3 to the digits given there;
     - the thresholds √((2 + Σ_peak)/(6 Σ_peak)) give 3.01, 1.107 and 1.86 to the paper's digits, and
       the shoulder's gives README §0.3's 2.018;
     - at β = threshold + 0.01, each kick's two roots straddle its `T_peak`;
     - at every root, `kick_threshold_curve`'s β_th(T) equals β to 1e-9.
     - **The merge** (U5): at β = 2.1 the QCD and `mu-pi` records are separate intervals with
       `merged` unset; at β = 2.2 both carry the same interval, with `merged` naming each other.
     - **The troughs.** Record Σ at the three troughs in the log. If any trough's Σ_eff exceeds
       1/(3·5²), say so in the log: it means intervals merge inside the campaign's β range. That is
       a measurement, not a failure (README §5 rule 10). README §0.3 expects it of the QCD–`mu-pi`
       trough only.

---

## 2. Verification

1. **Before any edit:** the four roots at README §0.3's counts (18 / 152 (61 + 91) / 69 / 34),
   serially, in the foreground, with the load (`uptime`) beside each.
2. **After:**
   - the four roots: ComputeTargets 152 + the new tests (the module falls in the `test_[g-z]*.py`
     half), the others unchanged;
   - `black --check` on every touched Python file. `extract_common.py` is black-clean at
     `8adb806`.
3. **A table in the log** for the production EOS, at β = 1.10, 1.2, 1.5, 2, 2.1, 2.2, 3, 3.1, 4 and 5: each
   kick's `T_hot` and `T_cold` in GeV, the slopes, and the flags. It is a measurement, recorded as it
   comes out.
4. **Deliberate breakages** (README §5, `dk-and-rpk-migration` rule 8: each recorded as a diff,
   never committed):
   - the hot and cold labels swapped;
   - Σ used in place of Σ_eff;
   - a trough refined onto the neighbouring peak;
   - the merged case returning a root at the window edge.

   Each must fail at least one new test, named in the log.

## 3. Acceptance

1. R1–R3 as §1.1 says.
2. §2's checks, each with its command and output in the log.
3. Suites: CosmologyModels 18, ComputeTargets 152 + the new tests, ObjectFactories 69, tools 34.
4. **One commit**, with the log `logs/01-reachability-roots.md` in the template of
   `dk-and-rpk-migration` README §5.1. It updates the board: row 01, R1–R3, the gate "Roots", and a
   new `### 5.2 After prompt 01 (<date>)`. Write "this commit" where the SHA goes.

## 4. Stop conditions: stop and ask

- **The production EOS has a number of kicks other than four** above the floor (U5). Report every
  local maximum with its T and Σ. The labelling is the user's.
- **A production peak differs from README §0.3** by more than its last digit.
- An existing test fails.
- Anything would need an edit outside §5's list.

## 5. What this prompt does not do

- **Files it changes:**
  - `extract_common.py` (two new functions, and any constants they need, beside
    `kick_threshold_curve`);
  - the new test module;
  - this board, and the log.
- It does not change `kick_threshold_curve`, `plot_by_beta.py` or any EOS class.
- It runs no history and no PRyMordial, starts no Ray, and opens no store.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
