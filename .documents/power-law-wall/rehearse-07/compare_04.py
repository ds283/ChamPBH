"""
power-law-wall prompt 07 §2.3 (P66; orchestrator note 07, ruling 4): each history of a build's store against 04's
tool-route output for it.

    PYTHONPATH=. ./venv/bin/python .documents/power-law-wall/rehearse-07/compare_04.py STORE_STEM [--exp]

STORE_STEM is the store's path without `.db`. Without --exp, the outputs are 04's forty,
.documents/power-law-wall/sample-04/runs/ip-n<N>-M<M>-b<beta>-<law>.txt (prompts/power-law-wall/source/sample/
sample_run.py's: the tool's lines, then `sample` and `cand` lines), each matched to the store's inverse-power history
of the same (n, M, beta, drain model, alpha). With --exp, they are the three plain-tool outputs
.documents/power-law-wall/sample-04/tool-route/exp-*.txt, each matched to the store's exponential history at n = 1.
Every output's `# command:` line is parsed for beta, M, --model, --n, --drain-law and --alpha, and must name no other
option (the tool then runs at main.py's defaults: phi* = 5 M_P, the config files' tolerances, T*, the tuning's tolerance
and cap, the small network); the map is printed.

Run from this worktree's root. The store's shards (`<stem>-shard*.db`) are read through sqlite3 with `file:...?mode=ro`
URIs only; no primary is opened, neither datastorekit nor Ray is used, and nothing is written but stdout (prompt 07 sends
it to a file under var/power-law-wall/07/). It imports ObjectFactories.ScalarHistory (BOUNCE_COLUMNS, to_column) and
Units only for the conversions below.

For each pair, by line kind (the formats are the tool's, tools/history_and_bbn.py, and sample_run.py's, read from their
source and named in FORMATS below):
- **outcome:** success or failure as 04's `sample outcome` line (for a plain-tool output, its `history` line: `FAILURE`
  or not) against the stored `failure`; a failure's stored `failure_reason` equal to the tool's once the history's name
  in each is masked (the parenthesised name after the reason's first word, up to the first "): ", which the tool gives as
  `history_and_bbn-beta…` and main.py as its own label), every other character, every number included, as printed.
  Production stores a reason cut to config.defaults.DEFAULT_STRING_LENGTH (256) characters, the column's width, so the
  comparison is exact on what the store holds: the tool's reason with the stored history's name put in place of the
  tool's, cut to that width, equals the stored reason character for character; the characters beyond the cut are not
  held, and their count is printed.
- **bit for bit** (a difference is a stop): every value of the `tuning` line (TUNING_OUTPUT_KEYS, printed by repr or as
  an int or string) against the column of the same name, a float by float.hex of the printed repr; and the counts of
  the `history` line that the store holds: RHS = RHS_evaluations, accepted_steps = extra_data["accepted_steps"],
  reflections = extra_data["number_reflections"] (absent when 0, as build_extra_data stores it), samples = z_samples,
  integrations = tuning_iterations.
- **at the printed digits** (a difference is a stop): the stored value formatted with the tool's format for the field
  must equal the printed text, for every value of the `bounce`, `dissipation`, `drain` and `fixed_T` lines that the store
  holds, and every `cand` row against the stored ScalarHistoryBounce row of source = `candidate` with the same
  candidate_index, their number equal to the `drain` line's `candidates` and to the cand rows' (a plain-tool output prints none). Each compared cand field
  goes through ObjectFactories.ScalarHistory.to_column(value, kind, Planck_units()) with its BOUNCE_COLUMNS kind, which
  for every compared field is None (the identity; printed). Under the fixed law the store writes no bounce row and no
  drain record (one-loop-drain prompt 03), so a fixed-law history's cand rows are listed as not held, after checking
  that it has no bounce row.
- **Y_p and D/H** (`bbn` line, `.10g`), and He3/H and Li7/H beside them: the stored value rounded to ten significant
  digits against the printed text; where they differ, the relative difference of the stored value from the printed one,
  up to 5e-10 reported and not a stop (one-loop-drain 06, ruling 3), above it a stop. The network and PRyM_version
  exactly.
For each line kind the fields compared and the fields the store does not hold are printed, with the count of values
compared per line kind, per history and in total.

Exit 0 if every bit-for-bit and printed-digit item is equal and Y_p and D/H are within 5e-10; 1 otherwise.
"""

import argparse
import glob
import json
import re
import shlex
import sqlite3
import sys
from collections import Counter, defaultdict
from math import exp, log
from pathlib import Path
from urllib.parse import quote

SAMPLE = Path(".documents/power-law-wall/sample-04")
M_P_EV = (
    2.4353235955263046e27  # REDUCED_PLANCK_MASS_EV of Units/base.py, to match M only
)
MODEL_TABLES = {
    1: ("ExponentialModel", "exponential"),
    2: ("InversePowerModel", "inverse-power"),
}
ONE_LOOP_MODEL = {
    "exponential": "one-loop-measured-v1",
    "inverse-power": "one-loop-measured-pl-v1",
}
ALLOWED_OPTIONS = {"--model", "--n", "--drain-law", "--alpha"}

# the formats, read from the source (tools/history_and_bbn.py, sample_run.py)
FORMATS = {
    "bounce N": ".9f (history_and_bbn.py main: bounce.N:.9f)",
    "bounce T_J": ".6f MeV (exp(bounce.log_T_Jordan) / units.MeV)",
    "bounce phi": ".6e (bounce.phi_Einstein / units.PlanckMass)",
    "dissipation": ".6g (dissipation_line), None as None",
    "drain": ".6g (drain_line), None as None",
    "fixed_T phi, ratio": ".9e (_fmt)",
    "bbn": ".10g",
    "cand N": ".6f (sample_run.py)",
    "cand log_x, log_R": ".8g (sample_run.py g(..., '.8g'))",
    "cand ell, alpha_eff, w_gate": ".6g (sample_run.py g())",
}

TUNING_KEYS = (
    "normalization",
    "V_ref",
    "phi_ref",
    "phi_0",
    "pi_0",
    "tracking",
    "residual_H",
    "residual_phi",
    "tuning_iterations",
    "plain_steps",
    "secant_steps",
    "guard_steps",
    "seed_kind",
    "seed_normalization",
    "seed_phi_ref",
)
DISSIPATION_COLUMN = {
    "parking_log_T_Jordan_GeV": "parking_log_T_Jordan",
    "release_log_T_Jordan_GeV": "release_log_T_Jordan",
}
REGIMES = ("one_loop", "gap", "classical", "unassessed")
DRAIN_COLUMN = dict(
    [(r, f"candidates_{r}") for r in REGIMES]
    + [(f"K_share_{r}", f"drained_share_{r}") for r in REGIMES]
    + [
        ("K_share_parking", "parking_share"),
        ("ell_min", "ell_min"),
        ("unresolvable_assessments", "unresolvable_assessments"),
        ("unresolved_declined", "unresolved_declined"),
    ]
)
BBN_COLUMN = {
    "Yp": "Yp_BBN",
    "DoH": "DOverH",
    "He3oH": "He3OverH",
    "Li7oH": "Li7OverH",
}
CAND_FIELDS = (
    "idx",
    "N",
    "kind",
    "log_x",
    "phi_t_Mp",
    "log_R",
    "ell",
    "regime",
    "f",
    "alpha_eff",
    "w_gate",
    "dK_over_K",
    "resolved",
    "rho_kin_over_rho_rad",
)
# cand field -> (bounce column, format); idx is matched as candidate_index
CAND_COMPARED = {
    "N": ("N", ".6f"),
    "kind": ("kind", None),
    "log_R": ("log_R", ".8g"),
    "ell": ("ell", ".6g"),
    "regime": ("regime", None),
    "alpha_eff": ("alpha_eff", ".6g"),
    "w_gate": ("w_gate", ".6g"),
    "resolved": ("resolved", "bool"),
}
CAND_NOT_HELD = {
    "log_x": "ln(E/V0): the store holds log_E_Mp4 and the normalization, not ln x (reported below as a derived check, not compared)",
    "phi_t_Mp": "the classical turning point is not stored",
    "f": "the law's fraction is not stored (alpha_eff and A are)",
    "dK_over_K": "delta_K over K(N_final): K(N_final) is not a column",
    "rho_kin_over_rho_rad": "rho_R,E at the candidate is not stored",
}


def ro(path):
    c = sqlite3.connect("file:" + quote(path, safe="/") + "?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    return c


def tables_of(conn):
    return {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def value(conn, table, serial, col=None):
    """A key table's value: col, or its first column whose name starts with `value` (value, value_eV, ...)."""
    if col is None:
        col = next(
            r[1]
            for r in conn.execute(f'PRAGMA table_info("{table}")')
            if r[1].startswith("value")
        )
    r = conn.execute(
        f'SELECT "{col}" FROM "{table}" WHERE serial = ?', (serial,)
    ).fetchone()
    return None if r is None else r[0]


def fmt(v, spec):
    if v is None:
        return "None"
    if spec == "bool":
        return str(bool(v))
    if spec is None:
        return str(v)
    return format(v, spec)


def store_histories(stem):
    out = []
    for path in sorted(glob.glob(glob.escape(stem) + "-shard*.db")):
        conn = ro(path)
        if "ScalarHistory" not in tables_of(conn):
            conn.close()
            continue
        for r in conn.execute('SELECT * FROM "ScalarHistory"'):
            r = dict(r)
            mtable, potential = MODEL_TABLES[r["model_type"]]
            m = dict(
                conn.execute(
                    f'SELECT * FROM "{mtable}" WHERE serial = ?', (r["model_serial"],)
                ).fetchone()
            )
            rec = dict(
                path=path,
                row=r,
                potential=potential,
                n=m["n_value"],
                M_Mp=value(conn, "M_value", m["M_serial"]) / M_P_EV,
                beta=value(conn, "beta_value", m["beta_serial"]),
                phi=value(conn, "phi_value", r["phi_Einstein_init_serial"]),
                alpha=value(conn, "alpha_value", r["alpha_serial"]),
                drain_model=value(
                    conn, "alpha_value", r["alpha_serial"], "drain_model"
                ),
            )
            b = conn.execute(
                'SELECT * FROM "BBNData" WHERE scalar_history_serial = ?',
                (r["serial"],),
            ).fetchall()
            rec["bbn"] = [dict(x) for x in b]
            rec["bounce"] = [
                dict(x)
                for x in conn.execute(
                    'SELECT * FROM "ScalarHistoryBounce" WHERE history_serial = ? ORDER BY ordinal',
                    (r["serial"],),
                )
            ]
            out.append(rec)
        conn.close()
    return out


def parse_output(path):
    lines = path.read_text().splitlines()
    out = {"cand": [], "lines": {}}
    for line in lines:
        if line.startswith("# command: "):
            out["command"] = line[len("# command: ") :]
        elif line.startswith("sample outcome: "):
            out["outcome"] = line[len("sample outcome: ") :]
        elif line.startswith("cand ") and not line.startswith("cand idx "):
            out["cand"].append(dict(zip(CAND_FIELDS, line.split()[1:])))
        else:
            for kind in (
                "history",
                "tuning",
                "bounce",
                "dissipation",
                "drain",
                "fixed_T",
                "bbn",
            ):
                if line.startswith(kind + " ") and ": " in line:
                    out["lines"][kind] = line.split(": ", 1)[1]
    return out


def kv(text):
    """key=value pairs of a line's body, in order (a value has no space)."""
    d = {}
    for word in text.split():
        if "=" in word:
            k, v = word.split("=", 1)
            d[k] = v
    return d


def parse_command(cmd):
    words = shlex.split(cmd)
    k = next(i for i, w in enumerate(words) if w.endswith(".py"))
    beta, M = float(words[k + 1]), float(words[k + 2])
    opts = {}
    rest = words[k + 3 :]
    for i in range(0, len(rest), 2):
        opts[rest[i]] = rest[i + 1]
    return beta, M, opts


def mask(reason):
    if reason is None:
        return None
    # the name ends at the first "): ", since main.py's label holds the model's own parentheses
    m = re.match(r"^\S+ \((.*?)\): ", reason)
    if not m:
        return reason
    return reason.replace(m.group(1), "<history>")


class Count:
    def __init__(self):
        self.by_kind = Counter()
        self.diffs = []
        self.reported = []

    def eq(self, kind, tag, field, ok, printed, stored):
        self.by_kind[kind] += 1
        if not ok:
            self.diffs.append(
                f"{tag} {kind}.{field}: printed {printed!r} stored {stored!r}"
            )


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("stem")
    p.add_argument("--exp", action="store_true")
    args = p.parse_args(argv)

    global DEFAULT_STRING_LENGTH
    from config.defaults import DEFAULT_STRING_LENGTH
    from ObjectFactories.ScalarHistory import BOUNCE_COLUMNS, to_column
    from Units import Planck_units

    units = Planck_units()
    kinds = {col: kind for _field, col, kind in BOUNCE_COLUMNS}
    log_GeV = log(units.GeV)

    if args.exp:
        files = sorted((SAMPLE / "tool-route").glob("exp-*.txt"))
    else:
        files = sorted((SAMPLE / "runs").glob("ip-*.txt"))
    hs = store_histories(args.stem)
    print(f"store {args.stem}: {len(hs)} ScalarHistory rows; outputs: {len(files)}")
    print(
        "formats (read from the source): "
        + "; ".join(f"{k} {v}" for k, v in FORMATS.items())
    )
    print(
        "cand fields compared, with their BOUNCE_COLUMNS kinds (to_column in Planck_units): "
        + ", ".join(f"{f}->{c} kind {kinds[c]}" for f, (c, _s) in CAND_COMPARED.items())
    )
    print(
        "cand fields not held: "
        + "; ".join(f"{k}: {v}" for k, v in CAND_NOT_HELD.items())
    )
    print(
        "not held, by line: history `wall`, `drain_law`/`A`/`model` (keys, matched); tuning none; bounce none; "
        "dissipation none; drain `candidates` (compared as the sum of candidates_<regime> and as the count of candidate "
        "bounce rows); fixed_T `crossings`, `stand_in_phi`, `stand_in_ratio`; bbn `PRyM_time`, `wall`"
    )

    c = Count()
    per_history = {}
    yp_reported = []
    used = Counter()
    ok = True
    print()
    print("THE MAP")
    pairs = []
    for f in files:
        o = parse_output(f)
        beta, M, opts = parse_command(o["command"])
        extra = sorted(set(opts) - ALLOWED_OPTIONS)
        potential = opts.get("--model", "exponential")
        n = int(opts.get("--n", "1"))
        law = opts.get("--drain-law", "fixed")
        alpha = float(opts.get("--alpha", "0"))
        dm = "fixed" if law == "fixed" else ONE_LOOP_MODEL[potential]
        cands = [
            h
            for h in hs
            if h["potential"] == potential
            and h["n"] == n
            and abs(h["M_Mp"] / M - 1.0) < 1e-9
            and h["beta"] == beta
            and h["drain_model"] == dm
            and h["alpha"] == alpha
            and h["phi"] == 5.0
        ]
        print(
            f"  {f.name}: {potential} n={n} M={M:g} beta={beta:g} {dm} alpha={alpha:g}; options {sorted(opts)}"
            f"{'; OTHER OPTIONS ' + str(extra) if extra else ''}; store rows {len(cands)}"
            + (
                f" ({cands[0]['path'].rsplit('-', 1)[-1]} #{cands[0]['row']['serial']})"
                if len(cands) == 1
                else ""
            )
        )
        print(f"      # command: {o['command']}")
        if extra or len(cands) != 1:
            ok = False
            continue
        used[id(cands[0])] += 1
        pairs.append((f, o, cands[0], potential, dm))
    selected = [h for h in hs if used[id(h)]]
    print(
        f"  outputs matched: {len(pairs)} of {len(files)}; store rows matched once: {sum(1 for h in hs if used[id(h)] == 1)}"
    )

    for f, o, h, potential, dm in pairs:
        tag = f.name
        before = sum(c.by_kind.values())
        r = h["row"]
        L = o["lines"]
        print()
        print(f"=== {tag}")

        # outcome
        if "outcome" in o:
            status = kv(o["outcome"])["status"]
            reason = o["outcome"].split(" reason=", 1)[1]
            tool_fail = status != "ok"
        else:
            tool_fail = L["history"].startswith("FAILURE")
            reason = L["history"].split(" reason=", 1)[1] if tool_fail else "None"
        c.eq(
            "outcome",
            tag,
            "failure",
            bool(r["failure"]) == tool_fail,
            tool_fail,
            r["failure"],
        )
        if tool_fail:
            # production stores the reason cut to DEFAULT_STRING_LENGTH characters (the column's width;
            # ComputeTargets/ScalarHistory.py, ScalarHistory's construction), so the tool's reason, with
            # the stored history's name put where the tool's stands, is cut the same way and compared
            # character for character; what lies beyond the cut is a field the store does not hold
            stored = r["failure_reason"]
            mt_full = mask(reason)
            m_s = re.match(r"^\S+ \((.*?)\): ", stored or "")
            m_t = re.match(r"^\S+ \((.*?)\): ", reason)
            expected = (
                reason.replace(m_t.group(1), m_s.group(1))[:DEFAULT_STRING_LENGTH]
                if m_s and m_t
                else None
            )
            same = expected == stored
            c.eq(
                "outcome",
                tag,
                "failure_reason (to the column's width)",
                same,
                expected,
                stored,
            )
            cut = (
                len(reason.replace(m_t.group(1), m_s.group(1))) - len(stored)
                if m_s and m_t
                else None
            )
            print(
                f"    failure at both: {bool(r['failure']) and tool_fail}; the tool's reason with the stored name, cut to "
                f"{DEFAULT_STRING_LENGTH} characters, equal to the stored reason: {same}; characters beyond the cut "
                f"(not held): {cut}"
            )
            print(f"      tool (masked, whole): {mt_full}")
            print(f"      store (masked):       {mask(stored)}")
            per_history[tag] = sum(c.by_kind.values()) - before
            continue

        # history counts
        xd = json.loads(r["extra_data"]) if r.get("extra_data") else {}
        hv = kv(L["history"])
        for field, stored in (
            ("RHS", r["RHS_evaluations"]),
            ("accepted_steps", xd.get("accepted_steps")),
            ("reflections", xd.get("number_reflections", 0)),
            ("samples", r["z_samples"]),
            ("integrations", r["tuning_iterations"]),
        ):
            c.eq(
                "history",
                tag,
                field,
                stored is not None and int(hv[field]) == stored,
                hv[field],
                stored,
            )

        # tuning, bit for bit
        tv = kv(L["tuning"])
        for k in TUNING_KEYS:
            stored = r[k]
            if isinstance(stored, float):
                good = float(tv[k]).hex() == stored.hex()
            elif isinstance(stored, int):
                good = int(tv[k]) == stored
            else:
                good = tv[k] == str(stored)
            c.eq("tuning", tag, k, good, tv[k], stored)

        # bounce
        b = L["bounce"]
        if b.strip() == "none":
            for col in (
                "first_bounce_N",
                "first_bounce_log_T_Jordan",
                "first_bounce_phi_Einstein",
                "first_bounce_reflected",
            ):
                c.eq("bounce", tag, col, r[col] is None, "none", r[col])
        else:
            bv = kv(b)
            T = (
                None
                if r["first_bounce_log_T_Jordan"] is None
                else exp(r["first_bounce_log_T_Jordan"] + log_GeV) / units.MeV
            )
            for field, stored, spec in (
                ("N", r["first_bounce_N"], ".9f"),
                ("T_J", T, ".6f"),
                ("phi", r["first_bounce_phi_Einstein"], ".6e"),
                ("reflected", r["first_bounce_reflected"], "bool"),
            ):
                s = fmt(stored, spec)
                c.eq("bounce", tag, field, s == bv[field], bv[field], s)

        # dissipation
        if "dissipation" in L:
            for field, text in kv(L["dissipation"]).items():
                col = DISSIPATION_COLUMN.get(field, field)
                s = fmt(r[col], ".6g")
                c.eq("dissipation", tag, field, s == text, text, s)
        else:
            print(
                f"    no dissipation line (alpha = 0); the store's injections, Delta_N_eff_today, log_R_first_impact: {[r[k] for k in ('injections', 'Delta_N_eff_today', 'log_R_first_impact')]}"
            )

        # drain and the cand rows
        cand_rows = [x for x in h["bounce"] if x["source"] == "candidate"]
        if "drain" in L:
            dv = kv(L["drain"])
            for field, text in dv.items():
                if field == "candidates":
                    total = sum(r[f"candidates_{x}"] or 0 for x in REGIMES)
                    c.eq(
                        "drain",
                        tag,
                        "candidates (sum of candidates_<regime>)",
                        str(total) == text,
                        text,
                        total,
                    )
                    c.eq(
                        "drain",
                        tag,
                        "candidates (candidate bounce rows)",
                        str(len(cand_rows)) == text,
                        text,
                        len(cand_rows),
                    )
                    continue
                s = fmt(r[DRAIN_COLUMN[field]], ".6g")
                c.eq("drain", tag, field, s == text, text, s)
            # a plain-tool output (--exp) prints no cand rows: there is nothing to count
            if "outcome" in o:
                c.eq(
                    "cand",
                    tag,
                    "(rows)",
                    len(o["cand"]) == len(cand_rows),
                    len(o["cand"]),
                    len(cand_rows),
                )
            by_idx = {x["candidate_index"]: x for x in cand_rows}
            derived_ok = derived_n = 0
            for row in o["cand"]:
                idx = int(row["idx"])
                sr = by_idx.get(idx)
                if sr is None:
                    c.eq("cand", tag, f"idx {idx}", False, idx, None)
                    continue
                for field, (col, spec) in CAND_COMPARED.items():
                    stored = sr[col]
                    if spec not in (None, "bool"):
                        # the printed value through the store's own conversion, then at the printed digits
                        conv = to_column(float(row[field]), kinds[col], units)
                        good = (
                            fmt(stored, spec) == row[field]
                            and fmt(conv, spec) == row[field]
                        )
                    else:
                        good = fmt(stored, spec) == row[field]
                    c.eq(
                        "cand",
                        tag,
                        f"{field}[{idx}]",
                        good,
                        row[field],
                        fmt(stored, spec),
                    )
                if row["log_x"] != "None" and sr["log_E_Mp4"] is not None:
                    derived_n += 1
                    lx = sr["log_E_Mp4"] - r["normalization"]
                    derived_ok += format(lx, ".8g") == row["log_x"]
            print(
                f"    cand rows: {len(o['cand'])} printed, {len(cand_rows)} stored candidate rows, "
                f"{len(h['bounce']) - len(cand_rows)} other bounce rows "
                f"({dict(Counter(x['source'] for x in h['bounce']))}); "
                f"derived ln x = log_E_Mp4 - normalization equal at .8g: {derived_ok} of {derived_n} (a measurement)"
            )
        else:
            c.eq(
                "cand",
                tag,
                "(no bounce rows under the fixed law)",
                len(h["bounce"]) == 0,
                0,
                len(h["bounce"]),
            )
            print(
                f"    fixed law: no drain line; {len(o['cand'])} cand rows not held; the store's bounce rows {len(h['bounce'])}, "
                f"drain record {'all NULL' if all(r[col] is None for col in DRAIN_COLUMN.values()) else 'NOT NULL'}"
            )

        # fixed_T
        halves = L["fixed_T"].split(" | ")
        for half in halves:
            name = half.split()[0]
            fv = kv(half)
            for field, col in (
                ("phi", f"phi_Einstein_{name}"),
                ("ratio", f"density_NP_ratio_{name}"),
            ):
                s = fmt(r[col], ".9e")
                c.eq("fixed_T", tag, f"{name} {field}", s == fv[field], fv[field], s)

        # bbn
        bb = kv(L["bbn"])
        if len(h["bbn"]) != 1:
            c.eq("bbn", tag, "(rows)", False, 1, len(h["bbn"]))
        else:
            row = h["bbn"][0]
            for field, col in BBN_COLUMN.items():
                stored = row[col]
                printed = bb[field]
                c.by_kind["bbn"] += 1
                if format(stored, ".10g") == printed:
                    continue
                rel = abs(stored - float(printed)) / abs(float(printed))
                yp_reported.append(
                    f"{tag} {field}: stored {stored!r}, printed {printed}, relative {rel:.3e}"
                )
                if rel > 5e-10 and field in ("Yp", "DoH"):
                    c.diffs.append(
                        f"{tag} bbn.{field}: relative difference {rel:.3e} > 5e-10"
                    )
            c.eq(
                "bbn",
                tag,
                "network",
                (bb["network"] == "small") == bool(row["small_network"]),
                bb["network"],
                row["small_network"],
            )
            c.eq(
                "bbn",
                tag,
                "PRyM_version",
                bb["PRyM_version"] == row["PRyM_version"],
                bb["PRyM_version"],
                row["PRyM_version"],
            )
            c.eq("bbn", tag, "failure", not row["failure"], "ok", row["failure"])
        per_history[tag] = sum(c.by_kind.values()) - before
        print(f"    values compared: {per_history[tag]}")

    print()
    print("Values compared, per history:")
    for tag, n in per_history.items():
        print(f"  {tag}: {n}")
    print("Values compared, per line kind:")
    for k, n in sorted(c.by_kind.items()):
        print(f"  {k}: {n}")
    print(f"  total: {sum(c.by_kind.values())}")
    print()
    print(
        "bbn values not equal at ten digits (reported):"
        + (" none" if not yp_reported else "")
    )
    for line in yp_reported:
        print(f"  {line}")
    print()
    if c.diffs:
        ok = False
        print(f"DIFFERENCES ({len(c.diffs)}):")
        for d in c.diffs[:200]:
            print(f"  {d}")
    else:
        print("DIFFERENCES: none")
    print(
        f"RESULT: {'every output matched once; every bit-for-bit and printed-digit item equal; Yp and D/H within 5e-10' if ok else 'NOT EQUAL (see above)'}"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
