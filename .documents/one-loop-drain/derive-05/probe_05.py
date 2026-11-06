"""
one-loop-drain prompt 05, Derive gate §2.2 (orchestrator note 05, ruling 5): E with the spectrum collapsed to its peak
against the scoping probe, on P-2197 at A = 1.

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/derive-05/probe_05.py > .documents/one-loop-drain/derive-05/out/probe.txt

The case: P-2197 (M = 4e-31 M_P, beta = 1.185, A = 1) by test_drain_store's one_loop_payload (test_injection's setup, the
seed realization, one integration under the one-loop law), written on a temporary store by _DrainStoreCase (the stand-in
cluster, never Ray), read back, and exported by 04's writers (tools/tests/quanta_exports.export_history: histories.csv
by build_history_record and write_histories_csv, fields.csv by plot_ScalarHistory.py's own write_csv_file extracted by
ast, bounces.csv by write_bounces_csv) to var/one-loop-drain/05/probe/QCD_Cosmology/.

The tool's side reads the exported files with the tool's own readers (Bounce, Sample, KHistory, Generations) and calls
its matter_excess with ComputeTargets.quanta_spectrum.PEAK_SPECTRUM, so that G is called through the spectrum object as
on every other route (ruling 4). The probe's side is `excess` of prompts/one-loop-drain/source/probe/all_generations.py,
extracted by ast and run unedited, with its names bound to the payload's values (ruling 5): Ns the samples' raw_N
sorted, gN and gq the payload's generations' N_turn and log_q, gdK their share x ledger.K(N_final), ledger the payload's
"dark_radiation", np and isfinite; lm the payload's log_m_eff (then log_m_tracker) in the samples' order, None as NaN.

The tolerance (ruling 5): |E_tool - E_probe| <= a eps sum_born Delta K_g / K(N) + b |E_probe|, eps = 2^-52, with
  a = 2: the probe forms sqrt(1 + x^2) - 1, whose rounding (half an ulp in 1 + x^2, half in the square root, and the
     subtraction exact) is an absolute error of at most 1.5 u sqrt(1 + x^2) per term, u = eps/2, which is 1.06 eps for
     x <= 1; above x = 1 the same error is within 2.6 eps relative of the term and falls under b;
  b = 8 eps (number born): each term's relative rounding on either side (exp of 2 ln x or of ln y, the square, the
     square root, the division, the product with Delta K: about 6 u on each side) and gdK = share x K_final against
     Delta K (2 u), with the sum of n positive terms, whose relative error is at most (n - 1) u; both sides form ln y
     = (N + ln m) - ln Q in the same order from the same floats. 8 eps n bounds it for every n >= 1.
Where the probe gives no number (ruling 5): a sample whose mass is None (the probe 0.0, the tool a blank), and a sample
with K(N) = 0 (the probe 0.0 or non-finite, the tool a blank); each is matched only in its own case, and counted.

K(N): the tool's rebuilt K at every sample against the payload ledger's K(N) converted by to_column (ruling 6): the count
equal bit for bit (a measurement), and the largest relative difference before and after the parking leftover, which no
row brackets here.
"""

import csv
import sys
import unittest
from math import exp, log
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

import extract_common as ec  # noqa: E402
from ComputeTargets import quanta_spectrum as qs  # noqa: E402
from ObjectFactories.ScalarHistory import LINEAR_MP4, to_column  # noqa: E402
from ObjectFactories.tests.test_drain_store import (
    ONE_LOOP,
    P2197,
    _DrainStoreCase,
    one_loop_payload,
)  # noqa: E402
from tools import quanta_measures as qm  # noqa: E402
from tools.tests import quanta_exports as qe  # noqa: E402
from tools.tests.test_quanta_measures import (
    EPS,
    PROBE_A,
    PROBE_B,
    run_probe,
)  # noqa: E402

OUT = ROOT / "var" / "one-loop-drain" / "05" / "probe" / qe.LABEL


def rows(path):
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


class Probe(_DrainStoreCase):
    def test_probe(self):
        import shutil

        payload = one_loop_payload(*P2197)
        self.write_one_loop(P2197)
        full = self.read_one_loop(P2197)
        lean = self.read_one_loop(P2197, _do_not_populate=True)
        beta, M, A = P2197
        if OUT.exists():
            shutil.rmtree(OUT)
        record, d = qe.export_history(
            OUT, self.units, lean, full, beta, M, 1, A, ONE_LOOP
        )
        ec.write_histories_csv([record], OUT / "histories.csv")
        print(f"P-2197, A = 1: exported to {d.relative_to(ROOT)}")
        print(
            f"tolerance: |E_tool - E_probe| <= {PROBE_A:g} eps sum_born dK/K + {PROBE_B:g} eps n_born |E_probe|, eps = {EPS!r}"
        )

        hrow = rows(OUT / "histories.csv")[0]
        bounces = [qm.Bounce(r) for r in rows(d / "bounces.csv")]
        samples = [qm.Sample(r) for r in rows(d / "fields.csv")]
        cands = [b for b in bounces if b.source == "candidate"]
        K = qm.KHistory(cands, float(hrow["parking_share"]))
        gens = qm.Generations(cands)
        Ns = [s.raw_N for s in samples]
        Ks = [K.K(N) for N in Ns]
        print(
            f"samples {len(Ns)}; candidate rows {len(cands)}; generations {len(gens)} (payload {len(payload['generations'])}); "
            f"leftover {K.placement} at N = {K.leftover_N!r}"
        )

        ok = True
        for which in ("log_m_eff", "log_m_tracker"):
            masses = [getattr(s, which) for s in samples]
            tool = qm.matter_excess(qs.PEAK_SPECTRUM, gens, Ns, masses, Ks)
            pN, probe = run_probe(payload, which)
            assert np.array_equal(pN, np.array(Ns)), "the samples differ"
            counts = dict(
                compared=0,
                mass_none=0,
                K_zero=0,
                K_zero_born=0,
                beyond=0,
                bitwise=0,
                cancel_band=0,
            )
            worst_total = worst_a = worst_b = worst_rel = 0.0
            for E_t, E_p, N, k, m in zip(tool, probe, Ns, Ks, masses):
                born = gens.N_turn <= N
                if k == 0.0:
                    if E_t is None and (E_p == 0.0 or not np.isfinite(E_p)):
                        counts["K_zero"] += 1
                        counts["K_zero_born"] += bool(born.any())
                    else:
                        counts["beyond"] += 1
                    continue
                if m is None:
                    if E_t is None and E_p == 0.0:
                        counts["mass_none"] += 1
                    else:
                        counts["beyond"] += 1
                    continue
                n_born = int(born.sum())
                sum_dK = float(np.sum(gens.delta_K[born]))
                a_part = PROBE_A * EPS * sum_dK / k
                b_part = PROBE_B * EPS * n_born * abs(E_p)
                diff = abs(E_t - E_p)
                counts["compared"] += 1
                counts["bitwise"] += E_t == E_p
                lx = (N + m) - gens.log_q[born]
                counts["cancel_band"] += bool(np.any((lx > -20) & (lx < -8)))
                if diff > a_part + b_part:
                    counts["beyond"] += 1
                if a_part + b_part > 0:
                    worst_total = max(worst_total, diff / (a_part + b_part))
                if a_part > 0:
                    worst_a = max(worst_a, diff / a_part)
                if b_part > 0:
                    worst_b = max(worst_b, diff / b_part)
                if E_p != 0.0:
                    worst_rel = max(worst_rel, diff / abs(E_p))
            ok &= counts["beyond"] == 0 and sum(
                counts[c] for c in ("compared", "mass_none", "K_zero")
            ) == len(Ns)
            print(
                f"{which}: compared {counts['compared']} (equal bit for bit {counts['bitwise']}; with a term in the probe's "
                f"cancellation band -20 < ln x < -8: {counts['cancel_band']}); matched blanks: mass None {counts['mass_none']}, "
                f"K(N) = 0 {counts['K_zero']} (of which with a generation born {counts['K_zero_born']}); beyond the tolerance "
                f"{counts['beyond']}"
            )
            print(
                f"  largest |diff| / tolerance {worst_total:.3g}; largest |diff| / (a part) {worst_a:.3g}; largest |diff| / "
                f"(b part) {worst_b:.3g}; largest |diff| / |E_probe| {worst_rel:.3g} ({worst_rel / EPS:.1f} eps)"
            )

        ledger = payload["dark_radiation"]
        parking_N = payload["parking"].N
        equal = before = after = 0
        worst_before = worst_after = 0.0
        for N, k in zip(Ns, Ks):
            expected = to_column(ledger.K(N), LINEAR_MP4, self.units)
            r = 0.0 if expected == k else abs(k / expected - 1.0)
            equal += k == expected
            if N < parking_N:
                before += 1
                worst_before = max(worst_before, r)
            else:
                after += 1
                worst_after = max(worst_after, r)
        ps = float(hrow["parking_share"])
        print(
            f"K(N) against the payload ledger's: equal bit for bit at {equal} of {len(Ns)} samples; before the leftover "
            f"({before} samples) largest rel diff {worst_before:.3g}; after it ({after}) {worst_after:.3g} (tolerance "
            f"8 eps (1 + ps/(1 - ps)) = {8 * EPS * (1 + ps / (1 - ps)):.3g}, parking_share {ps!r})"
        )
        ok &= worst_before <= 8 * EPS and worst_after <= 8 * EPS * (1 + ps / (1 - ps))
        print("RESULT:", "holds" if ok else "FAILS")
        assert ok


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
