# The orchestrator's probe after 07b (2026-11-18)

Made in scratch by the orchestrator at `4f9b1bd`, after 07b landed, when the user asked why ℓ moved on the exponential
and whether 07b's candidate laws had the wrong form. The probe led to 07b2 (README §6.13). Kept at the user's request,
2026-11-18, as part of the record. It reads 07b's records only, and changes nothing.

| file | what it is |
|---|---|
| `dump.py` | every exponential stage-2 point of 07b's `fits.points()` (185), with ℓ and the regime in both forms of k_ex and 07b's selections, as JSON |
| `plot_laws.py` | the three figures below, and the like-for-like residuals in `residuals.txt` |
| `f_vs_ell_old_new.png` | f against ℓ in the old and the new k_ex for every chain point (back out, ln R > 0.3; at n = 2 ln(V₀/M⁴) ≤ 10), coloured by ln L, with v1's curves at four L in the old ℓ; and ℓ_new − ℓ_old against L. Nothing held fixed. Shows the collapse of f onto one curve in the new ℓ above its peak |
| `laws_vs_lnR_n1.png`, `laws_vs_lnR_n2.png` | the `grid` and `n2-grid` points against ln R at each fixed L, with v1 (old chain) and 07b's candidates (i), (iii)/(iv) (new chain) evaluated along ln R at that L; dotted lines where ℓ = 1 in each chain |
| `residuals.txt` | the largest miss and rms of v1 and 07b's candidates scored on one identical point set per row |

**How to run**, from the repository root (the scripts import 07b's `fits`, `rechain` and `kexchain` from `SRC`):

    T=<a scratch directory>
    ./venv/bin/python -B prompts/power-law-wall/source/kex/probe-07b/dump.py $PWD/prompts/power-law-wall/source/kex $T/points.json
    ./venv/bin/python -B prompts/power-law-wall/source/kex/probe-07b/plot_laws.py $PWD/prompts/power-law-wall/source/kex $T > $T/residuals.txt

Re-run so at `0f1b240`: `points.json` and the three PNG files byte-identical to the scratch originals, and `residuals.txt`
as committed.
