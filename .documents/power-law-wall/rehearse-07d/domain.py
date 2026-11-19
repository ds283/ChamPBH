"""
power-law-wall prompt 07d §3 (P120; P109, and the open issue [07c-the-third-hook-leaves-float-range-below-phi-1e-103-mp]):
the fitted domain of the exponential's law v2 and the third hook's float range, over a store's stored candidates. New
in 07d.

    ./venv/bin/python .documents/power-law-wall/rehearse-07d/domain.py STORE_STEM [STORE_STEM ...]

Each STORE_STEM is a store's path without `.db`. Stdlib only. Every shard (`<stem>-shard*.db`) is read through sqlite3
with `file:...?mode=ro` URIs only; no primary is opened, neither datastorekit nor Ray is used, and nothing is written but
stdout (prompt 07d sends it to a file under var/power-law-wall/07d-r2/).

The candidates are the ScalarHistoryBounce rows of source `candidate` (written under the one-loop law only); a row with
log_E_Mp4 NULL (unassessed) is counted and left out. Every quantity is in Planck units (M_P = 1), as the stored columns
are; M is the history's M_value over the reduced Planck mass in eV, and ln V_0 = ScalarHistory.normalization, which is
ln(V_0/M_P^4) (checked on the exponential against V_0 = E/(e^L - 1), and printed).

- **The domain (P109):** for every exponential one-loop candidate, ln(V_0/M^4) with V_0 = E/(e^L - 1) from its stored
  log_E_Mp4 and L, and M from the history's key: the largest, where, and how many exceed 10 (the fitted domain of
  `one-loop-exponential-v2`, README §6.14 P100). The largest |ln V_0 (from L) - normalization| is printed beside it.
- **The third hook (P120), against the range 07c2 measured** (README §6.16, P133; verification §10.5): for each
  potential and n, the least phi_t over the candidates, from the stored E with the model's closed form: on the
  exponential V = V_0 e^{(M/phi)^n}, so (M/phi_t)^n = L and phi_t = M L^{-1/n}; on the power wall V = V_0 [1 + (M/phi)^n],
  so (M/phi_t)^n = E/V_0 and phi_t = M (V_0/E)^{1/n}. The end of the range is where V''/V = (ln V)'' + (ln V)'^2, as the
  turning points form it, leaves float range: on the power wall V''/V = s n(n+1)/phi^2, s = u/(1+u), u = (M/phi)^n, whose
  end 07c2 measured by bisection on the hooks (1.054769e-154 M_P at n = 1, 1.826913e-154 at n = 2, equal to
  sqrt(n(n+1)/DBL_MAX) to the printed digits, at M = 1e-33 and 4e-31 M_P alike); on the exponential V''/V =
  n u (n u + n + 1)/phi^2, whose end at the history's M is found here by bisection in ln phi, in log space (07c2 did not
  measure it). Printed: log10 V''/V at the least phi_t against log10 of the largest float, and the margin in decades of
  phi between the least phi_t and the end. Before the amendment of 2026-11-19 the prompt asked for the third
  log-derivative of V there; the amendment replaces it with 07c2's range.
"""

import glob
import math
import sqlite3
import sys
from collections import defaultdict
from urllib.parse import quote

M_P_EV = 2.4353235955263046e27
MODEL_TABLES = {
    1: ("ExponentialModel", "exponential"),
    2: ("InversePowerModel", "inverse-power"),
}
LOG10_MAX = math.log10(sys.float_info.max)
LN10 = math.log(10.0)


def ro(path):
    c = sqlite3.connect("file:" + quote(path, safe="/") + "?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    return c


def log_expm1(x):
    """ln(e^x - 1) for x > 0, without overflow."""
    return x + math.log(-math.expm1(-x)) if x > 0 else math.log(math.expm1(x))


# 07c2's measured end of the range on the power wall (verification §10.5), in M_P
POWER_END_07C2 = {1: 1.054769e-154, 2: 1.826913e-154}
LN_MAX = math.log(sys.float_info.max)


def ln_vpp_over_v(potential, n, log_M, log_phi):
    """ln(V''/V) at phi = e^{log_phi}, u = (M/phi)^n, in log space (nothing overflows)."""
    log_u = n * (log_M - log_phi)
    if potential == "exponential":
        # n u (n u + n + 1) / phi^2
        if log_u > 0:
            inner = log_u + math.log(n) + math.log1p((n + 1) / n * math.exp(-log_u))
        else:
            inner = math.log(n * math.exp(log_u) + n + 1)
        return math.log(n) + log_u + inner - 2.0 * log_phi
    # s n (n + 1) / phi^2, s = u / (1 + u)
    log_s = -math.log1p(math.exp(-log_u)) if log_u > -700 else log_u
    return log_s + math.log(n * (n + 1)) - 2.0 * log_phi


def ln_phi_end(potential, n, log_M):
    """ln phi below which V''/V leaves float range, at this M, by bisection in ln phi."""
    lo, hi = -2000.0, math.log(10.0)
    if ln_vpp_over_v(potential, n, log_M, hi) >= LN_MAX:
        return hi
    for _ in range(200):
        mid = 0.5 * (lo + hi)
        if ln_vpp_over_v(potential, n, log_M, mid) >= LN_MAX:
            lo = mid
        else:
            hi = mid
    return hi


def read(stem):
    out = []
    unassessed = 0
    for path in sorted(glob.glob(glob.escape(stem) + "-shard*.db")):
        conn = ro(path)
        tabs = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if "ScalarHistoryBounce" not in tabs:
            conn.close()
            continue
        q = (
            "SELECT h.serial AS hs, h.model_type, h.model_serial, h.normalization, a.drain_model, a.value AS A, "
            'b.ordinal, b.log_E_Mp4, b.L, b.regime FROM "ScalarHistoryBounce" b '
            'JOIN "ScalarHistory" h ON h.serial = b.history_serial '
            'JOIN "alpha_value" a ON a.serial = h.alpha_serial '
            "WHERE b.source = 'candidate'"
        )
        for r in conn.execute(q):
            r = dict(r)
            table, potential = MODEL_TABLES[r["model_type"]]
            m = dict(
                conn.execute(
                    f'SELECT * FROM "{table}" WHERE serial = ?', (r["model_serial"],)
                ).fetchone()
            )
            M = conn.execute(
                'SELECT value_eV FROM "M_value" WHERE serial = ?', (m["M_serial"],)
            ).fetchone()[0]
            beta = conn.execute(
                'SELECT value FROM "beta_value" WHERE serial = ?', (m["beta_serial"],)
            ).fetchone()[0]
            if r["log_E_Mp4"] is None:
                unassessed += 1
                continue
            r.update(
                potential=potential,
                n=m["n_value"],
                M_Mp=M / M_P_EV,
                beta=beta,
                shard=path.rsplit("-", 1)[-1],
            )
            out.append(r)
        conn.close()
    return out, unassessed


def main(argv):
    for stem in argv:
        rows, unassessed = read(stem)
        print(
            f"=== {stem}: {len(rows)} assessed candidates, {unassessed} with log_E NULL (left out)"
        )
        # the domain, exponential one-loop candidates
        exp_rows = [r for r in rows if r["potential"] == "exponential"]
        worst = None
        over = 0
        check = 0.0
        by_hist = defaultdict(lambda: -math.inf)
        for r in exp_rows:
            log_V0 = r["log_E_Mp4"] - log_expm1(r["L"])
            check = max(check, abs(log_V0 - r["normalization"]))
            x = log_V0 - 4.0 * math.log(r["M_Mp"])
            r["log_V0_M4"] = x
            key = (r["M_Mp"], r["beta"], r["A"], r["n"])
            by_hist[key] = max(by_hist[key], x)
            if x > 10.0:
                over += 1
            if worst is None or x > worst[0]:
                worst = (x, r)
        if exp_rows:
            x, r = worst
            print(
                f"  domain (exponential one-loop candidates, {len(exp_rows)}): largest ln(V_0/M^4) = {x:.6g} at "
                f"M={r['M_Mp']:.3g}Mp n={r['n']} beta={r['beta']:g} A={r['A']:g} ({r['drain_model']}) ordinal {r['ordinal']}; "
                f"{over} exceed 10; largest |ln V_0 (from L) - normalization| {check:.3g}"
            )
            for (M, beta, A, n), v in sorted(by_hist.items()):
                print(
                    f"    M={M:.3g}Mp n={n} beta={beta:g} A={A:g}: largest ln(V_0/M^4) {v:.6g}"
                )
        else:
            print("  domain: no exponential one-loop candidate in this store")
        # the third hook, per potential and n
        groups = defaultdict(list)
        for r in rows:
            groups[(r["potential"], r["n"])].append(r)
        for (potential, n), rs in sorted(groups.items()):
            least = None
            for r in rs:
                if potential == "exponential":
                    log_u = math.log(r["L"])
                else:
                    log_u = r["log_E_Mp4"] - r["normalization"]
                log_phi = math.log(r["M_Mp"]) - log_u / n
                if least is None or log_phi < least[0]:
                    least = (log_phi, log_u, r)
            log_phi, log_u, r = least
            log_M = math.log(r["M_Mp"])
            vpp = ln_vpp_over_v(potential, n, log_M, log_phi)
            end = ln_phi_end(potential, n, log_M)
            line = (
                f"  third hook [{potential} n={n}, {len(rs)} candidates]: least phi_t = 10^{log_phi / LN10:.4f} Mp at "
                f"M={r['M_Mp']:.3g}Mp beta={r['beta']:g} A={r['A']:g} ({r['drain_model']}) ordinal {r['ordinal']} "
                f"(ln(M/phi_t)^n = {log_u:.6g}); V''/V there = 10^{vpp / LN10:.4f}, the largest float "
                f"10^{LOG10_MAX:.4f} ({(LN_MAX - vpp) / LN10:.2f} decades); the end of the range at this M, "
                f"phi = 10^{end / LN10:.4f} Mp: least phi_t {(log_phi - end) / LN10:.2f} decades above it"
            )
            if potential == "inverse-power" and n in POWER_END_07C2:
                e07 = math.log(POWER_END_07C2[n])
                line += f" ({(log_phi - e07) / LN10:.2f} decades above 07c2's measured {POWER_END_07C2[n]:.6e} Mp)"
            print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
