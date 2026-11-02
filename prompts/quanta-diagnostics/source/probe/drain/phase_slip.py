"""
drain-probe (2026-11-02): the phase slip per bounce, from drain_probe.py's results (the "cands" dump).

For bounce j with comoving scale Q_j = k_ex,j a_j, the free phase of mode Q_j over the interval to the
next bounce is taken massless, theta = (Q_j/a) dt with dt = dN/H at the interval's start; the slip is
|theta_{j,j+1} - theta_{j-1,j}|. The mass only adds phase, so this understates the slip. Parametric
resonance needs a slip <~ |beta|^2 (<~ 1) over many consecutive bounces.

    python3 phase_slip.py RESULT.json [RESULT.json ...]
"""

import json, math, sys

for path in sys.argv[1:]:
    d = json.load(open(path))
    if "cands" not in d:
        print(path, "has no bounce dump")
        continue
    c = [dict(zip(d["cand_fields"], x)) for x in d["cands"]]
    c = [x for x in c if x["log_k_ex"] is not None]
    slips, kH = [], []
    for j in range(1, len(c) - 1):
        a, x, b = c[j - 1], c[j], c[j + 1]
        Q = math.exp(x["log_k_ex"] + x["N"])

        def theta(p, q):
            return Q * math.exp(-0.5 * (p["N"] + q["N"])) * (q["N"] - p["N"]) / p["H_before"]

        slips.append(abs(theta(x, b) - theta(a, x)))
        kH.append(math.exp(x["log_k_ex"]) / x["H_before"])
    if not slips:
        print(path, "too few bounces")
        continue
    s, k = sorted(slips), sorted(kH)
    print(
        f"{d['serial']} {d['label']}: bounces {len(c)}; k_ex/H min {k[0]:.3g} median {k[len(k)//2]:.3g}; "
        f"slip min {s[0]:.3g} median {s[len(s)//2]:.3g} rad; slips < 1 rad: {sum(1 for v in slips if v < 1)}; "
        f"bounces with k_ex < H: {sum(1 for v in kH if v < 1)}"
    )
