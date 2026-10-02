# Prompt 06 — Narrow the BBN spline to PRyMordial's range

**Campaign:** [`README.md`](README.md) · **Board item:** **L** · **Board:**
`IMPLEMENTATION_STATE.md`. Update your row and L.
**Closes:** `[00-bbn-spline-domain-is-far-wider-than-prymordial-uses]`, assigned from the
`review-remediation` board (README §5 rule 4).
**Recommended model:** **Sonnet.** One default, one pre-check, and measurements.

> **Amended 2026-10-02 (the user's ruling on prompt 05; README §0.2 amendment).** Prompt 05
> changed no code: BBN reads the point `H_J`, and no sample is averaged. "Prompt 05's tree" and
> "log 05's figures" below mean point-input BBN on `a522005`'s code. Log 05's "State handed to the
> next prompt" lists them. Section 3's "no change to which samples are averaged" is moot.

**Read first:**

1. [`README.md`](README.md) §0.2 (P8), §2 (j), §5, §6.0, §6.7.
2. `logs/05-bounce-averages.md`, "State handed to the next prompt".
3. `ComputeTargets/BBNData.py`: `compute_BBN_data`'s signature, the pre-check, and the sample
   window.
4. `prompts/science-readiness/planning-probes/prym_callback_domain.py` and its recorded output.

---

## 1. The changes

- `T_BBN_keV_spline_min` defaults to `0.2`. Its comment says why: PRyMordial's lowest query is
  0.363 keV, measured, and the pre-check makes a history reach 20 eV.
- The pre-check is unchanged in form (`T_Jordan_stop > 0.1 × T_BBN_spline_min` fails). Its failure
  reason names both temperatures, as now.
- Nothing else. In particular, not the 100 MeV top and not the domain guard.

## 2. Tests

- **(a) The pre-check, with no solve.** A stand-in model whose `T_Jordan_stop` is `1e-8` GeV passes
  the pre-check: patch `_run_PRyMordial` to record the call. One at `1e-7` GeV returns the
  pre-check failure payload. **On `HEAD~1`, `1e-8` GeV fails the pre-check**, because the old
  floor needs 0.01 eV.
- **(b) The domain on a solve.** Re-run `prym_callback_domain.py`'s measurement inside a test, on
  the small network: the lowest positive `T` PRyMordial queries is above 0.2 keV. One solve; the
  docstring says so.

## 3. What this prompt does not do

No physical-`M` run (README §0.5). No change to which samples are averaged, or to the route.

## 4. Acceptance

README §6.7, every row: the driver on β = 2 at `M = 0.5` and `10⁻³` against log 05's figures. All
three suites pass and rise. `black --check` clean. The board and the index: L done, the assigned
issue closed.

## 5. Stop conditions — stop and ask the user

- Any driver history trips the domain guard.
- An abundance moves by more than `1e-5` relative.

## 6. The log and the board

`logs/06-bbn-spline-floor.md`, in the README §5.1 template.
