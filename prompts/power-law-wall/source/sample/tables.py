"""
power-law-wall prompt 04 §1.1–§1.4, §3 (P25–P30): the checks and the tables, built from the run files and the
extract, never typed. Run from the repository root:

    ./venv/bin/python prompts/power-law-wall/source/sample/tables.py extract HISTORIES_CSV
    ./venv/bin/python prompts/power-law-wall/source/sample/tables.py compare FILE_A FILE_B
    ./venv/bin/python prompts/power-law-wall/source/sample/tables.py overlaps
    ./venv/bin/python prompts/power-law-wall/source/sample/tables.py export-check FILE ...
    ./venv/bin/python prompts/power-law-wall/source/sample/tables.py tables
    ./venv/bin/python prompts/power-law-wall/source/sample/tables.py markdown

- `extract` reads a copy of the 2026.11.0 main export's histories.csv, checks its SHA-256 against P25's, and writes
  the twenty exponential counterparts (prompt §1.2) verbatim, the source's header line first, to
  .documents/power-law-wall/sample-04/exponential_counterparts.csv, after one comment line naming the source and its
  SHA-256.
- `compare` prints whether two outputs' tool lines are identical once the timings are masked (orchestrator note 04,
  ruling 3): every line but the `#` header and footer (the command and `uptime`) and the runner's `sample` and `cand`
  lines, with `wall=<number>` and `PRyM_time=<number>` masked. Exit 0 if identical, 1 if not (with a diff).
- `overlaps` compares, as `compare` does, each of the fourteen histories of the forty that 01 or 03 ran before (01's
  twelve in model-01/out/, 03's two in law-03/) with its run file (P26).
- `export-check` reads tool-route outputs of the exponential and compares each with its export row (from the
  extract) at the digits the tool prints: the tuning iterations, reflections, injections, ln R at the first impact,
  parking (its T_J), the drained shares, Delta N_eff today and at 1 MeV, Y_p and D/H (prompt §1.2). Exit 1 on any
  difference.
- `markdown` prints the readable tables of verification §4.1 and §4.2 from summary.csv and comparison.csv.
- `tables` writes summary.csv (one row per history of runs/, prompt §1.3), comparison.csv (per (M, beta, law), n = 1,
  n = 2 and the exponential side by side, prompt §1.2) and thresholds.txt (prompt §1.4, with the M_crit estimate of
  item 5) under .documents/power-law-wall/sample-04/.
"""

import csv
import difflib
import hashlib
import re
import sys
from math import exp, log
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
OUT = ROOT / ".documents" / "power-law-wall" / "sample-04"
RUNS = OUT / "runs"
EXTRACT = OUT / "exponential_counterparts.csv"
SOURCE_NAME = "var/science-2026.11.0/out/main/QCD_Cosmology/histories.csv (2026.11.0 main export, on main)"
SOURCE_SHA256 = "fb6a8dbb41fa7bbd7a0dd6b6076e25c3f9623dec9328b3e07b41b5e4fdb04253"

# the sample (prompt §1.1): (M token, beta token, law, value); law "fixed" (value alpha), "oneloop" (value A) or
# "natural" (value None)
LAB = "4e-31"
SAMPLE = (
    [
        (LAB, b, law, v)
        for b in ("1.2", "2.0", "2.9")
        for law, v in (
            ("fixed", "0.01"),
            ("fixed", "0.1"),
            ("oneloop", "0.05"),
            ("oneloop", "1"),
            ("oneloop", "10"),
        )
    ]
    + [("1e-33", "2.0", "oneloop", "1"), ("1e-15", "2.0", "oneloop", "1")]
    + [("1e-3", b, "natural", None) for b in ("1.2", "2.0", "2.9")]
)

# the export's key for a sample entry: M_Mp, beta, drain_model, alpha as the export writes them
EXPORT_M = {"4e-31": "4e-31", "1e-33": "1e-33", "1e-15": "1e-15", "1e-3": "0.001"}


def export_key(M, b, law, v):
    if law == "natural":
        return (EXPORT_M[M], b, "fixed", "0.0")
    if law == "fixed":
        return (EXPORT_M[M], b, "fixed", v)
    return (EXPORT_M[M], b, "one-loop-measured-v1", str(float(v)))


def run_name(n, M, b, law, v):
    return f"ip-n{n}-M{M}-b{b}-" + ("natural" if law == "natural" else f"{law}{v}")


# ---- extract ----


def extract(path):
    data = Path(path).read_bytes()
    sha = hashlib.sha256(data).hexdigest()
    if sha != SOURCE_SHA256:
        print(f"!! extract: SHA-256 {sha} is not P25's {SOURCE_SHA256}")
        return 1
    # the source's lines end in CRLF (the csv module's default); they are written with LF, as git (core.autocrlf
    # input) would commit them, and every field is verbatim
    lines = [line + "\n" for line in data.decode().splitlines()]
    header = lines[0]
    cols = next(csv.reader([header]))
    wanted = {export_key(*s): s for s in SAMPLE}
    found = {}
    for line in lines[1:]:
        row = dict(zip(cols, next(csv.reader([line]))))
        key = (row["M_Mp"], row["beta"], row["drain_model"], row["alpha"])
        if key in wanted:
            if key in found:
                print(f"!! extract: two rows for {key}")
                return 1
            found[key] = line
    missing = [k for k in wanted if k not in found]
    if missing:
        print(f"!! extract: no row for {missing}")
        return 1
    with open(EXTRACT, "w", newline="") as fh:
        fh.write(
            f"# source: {SOURCE_NAME}; {len(data)} bytes; SHA-256 {sha}; the source's header and the twenty rows of "
            f"power-law-wall prompt 04 §1.2, verbatim, in the sample's order\n"
        )
        fh.write(header)
        for s in SAMPLE:
            fh.write(found[export_key(*s)])
    print(f"wrote {EXTRACT.relative_to(ROOT)}: {len(found)} rows, source SHA-256 {sha}")
    return 0


def read_extract():
    with open(EXTRACT, newline="") as fh:
        rows = list(csv.DictReader(line for line in fh if not line.startswith("#")))
    return {(r["M_Mp"], r["beta"], r["drain_model"], r["alpha"]): r for r in rows}


# ---- compare ----

MASK = re.compile(r"\b(wall|PRyM_time)=[0-9.]+")


def tool_lines(path):
    out = []
    for line in Path(path).read_text().splitlines():
        if (
            line.startswith("#")
            or line.startswith("sample ")
            or line.startswith("cand")
        ):
            continue
        out.append(MASK.sub(r"\1=<t>", line))
    return out


def compare(a, b):
    la, lb = tool_lines(a), tool_lines(b)
    if la == lb:
        print(f"identical ({len(la)} tool lines, timings masked): {a} | {b}")
        return 0
    print(f"DIFFERENT: {a} | {b}")
    for line in difflib.unified_diff(la, lb, lineterm="", n=0):
        print(line)
    return 1


# ---- parsing a run file ----


def kv(text):
    """key=value pairs of one line, a value running to the next ' key=' (reasons contain spaces)."""
    out = {}
    for m in re.finditer(r"(\w[\w\-]*)=(.*?)(?= \w[\w\-]*=|$)", text):
        out[m.group(1)] = m.group(2).strip()
    return out


def parse_run(path):
    r = {"file": Path(path).name, "lines": {}, "cands": []}
    for line in Path(path).read_text().splitlines():
        if line.startswith("# start:"):
            r["start"] = line[len("# start: ") :]
        elif line.startswith("# end:"):
            r["end"] = kv(line)
            r["end_uptime"] = line
        elif line.startswith("cand idx"):
            r["cand_cols"] = line.split()[1:]
        elif line.startswith("cand "):
            r["cands"].append(dict(zip(r["cand_cols"], line.split()[1:])))
        elif line.startswith("sample "):
            head, _, rest = line.partition(": ")
            r["lines"][head] = kv(rest)
            if head == "sample outcome":
                # the reason runs to the end of the line and holds "key=value" text of its own
                r["lines"][head]["reason"] = rest.partition(" reason=")[2]
        else:
            for tag in ("history", "tuning", "bounce", "dissipation", "drain", "bbn"):
                if line.startswith(tag + " "):
                    head, _, rest = line.partition(": ")
                    r["lines"][tag] = (
                        rest if tag == "history" and "FAILURE" in rest else rest
                    )
                    r["lines"][tag + "_kv"] = kv(rest)
    return r


def fnum(s):
    if s is None or s in ("None", "", "-"):
        return None
    return float(s)


# ---- the overlaps (P26) ----


def overlap_pairs():
    """The fourteen of the forty run before: 01's twelve and 03's two, (committed file, run file)."""
    m01 = ROOT / ".documents" / "power-law-wall" / "model-01" / "out"
    l03 = ROOT / ".documents" / "power-law-wall" / "law-03"
    pairs = []
    for n in (1, 2):
        for b in ("1.2", "2.0", "2.9"):
            pairs.append(
                (
                    m01 / f"ip-n{n}-M4e-31-beta{b}-fixed0.1.txt",
                    RUNS / f"{run_name(n, LAB, b, 'fixed', '0.1')}.txt",
                )
            )
            pairs.append(
                (
                    m01 / f"ip-n{n}-M1e-3-beta{b}-natural.txt",
                    RUNS / f"{run_name(n, '1e-3', b, 'natural', None)}.txt",
                )
            )
        pairs.append(
            (
                l03 / f"tool_route_ip_n{n}.txt",
                RUNS / f"{run_name(n, LAB, '2.0', 'oneloop', '1')}.txt",
            )
        )
    return pairs


def overlaps():
    bad = 0
    for a, b in overlap_pairs():
        bad += compare(a.relative_to(ROOT), b.relative_to(ROOT))
    print(
        f"overlaps: {len(overlap_pairs())} compared, "
        + ("all identical" if bad == 0 else f"{bad} DIFFERENT")
    )
    return 0 if bad == 0 else 1


# ---- export-check ----

# (the tool line, its key, the export column, a conversion of the export's value to the tool's quantity)
CHECKS = [
    ("tuning_kv", "tuning_iterations", "tuning_iterations", None),
    ("history_kv", "reflections", "reflections", None),
    ("dissipation_kv", "injections", "injections", None),
    ("dissipation_kv", "log_R_first_impact", "log_R_first_impact", None),
    ("dissipation_kv", "parking_log_T_Jordan_GeV", "parking_T_GeV", log),
    ("dissipation_kv", "Delta_N_eff_today", "Delta_N_eff_today", None),
    ("dissipation_kv", "Delta_N_eff_1MeV", "Delta_N_eff_1MeV", None),
    ("drain_kv", "K_share_one_loop", "drained_share_one_loop", None),
    ("drain_kv", "K_share_gap", "drained_share_gap", None),
    ("drain_kv", "K_share_classical", "drained_share_classical", None),
    ("drain_kv", "K_share_unassessed", "drained_share_unassessed", None),
    ("drain_kv", "K_share_parking", "parking_share", None),
    ("drain_kv", "ell_min", "ell_min", None),
    ("bbn_kv", "Yp", "Yp_BBN", None),
    ("bbn_kv", "DoH", "D_over_H", None),
]


def tool_format(tag, x):
    """An export value as the tool prints its quantity: bbn's :.10g, every other float :.6g."""
    return format(x, ".10g") if tag == "bbn_kv" else format(x, ".6g")


def export_check(paths):
    """
    Each tool-route output against its export row, at the digits the tool prints: the export's value formatted as
    the tool formats that quantity (an int as an int), compared as strings. A quantity the tool does not print for
    the history (natural mode prints no dissipation or drain line) is listed with the export's value and not compared.
    """
    extract_rows = read_extract()
    bad = 0
    for path in paths:
        r = parse_run(path)
        text = Path(path).read_text()
        m = re.search(r"^history beta=(\S+) M=(\S+):", text, re.M)
        beta, M = m.group(1), m.group(2)
        hist = r["lines"]["history"]
        one_loop = "drain_law=one_loop" in hist
        m = re.search(r"\b(?:A|alpha)=(\S+)", hist)
        value = str(float(m.group(1))) if m else "0.0"
        b = f"{float(beta):.1f}"
        key = (M, b, "one-loop-measured-v1" if one_loop else "fixed", value)
        row = extract_rows[key]
        print(
            f"## {Path(path).name} against the export row (M_Mp, beta, drain_model, alpha) = {key}"
        )
        for tag, k, col, conv in CHECKS:
            line = r["lines"].get(tag)
            ev = row[col]
            if line is None or k not in line:
                print(
                    f"   {k}: not printed by the tool for this history; export {col}={ev!r}"
                )
                continue
            printed = line[k]
            if printed == "None":
                shown = "None" if ev == "" else ev
            elif ev == "":
                shown = "(blank)"
            elif re.fullmatch(r"-?\d+", printed):
                shown = str(int(float(ev)))
            else:
                x = float(ev)
                shown = tool_format(tag, conv(x) if conv is not None else x)
            same = shown == printed
            bad += 0 if same else 1
            print(
                f"   {k}: tool {printed}; export {col}={ev} -> {shown}  {'equal' if same else 'DIFFERENT'}"
            )
        # the first bounce: the bounce line's T_J (MeV, :.6f) against the export's T_deliver_GeV
        bounce = r["lines"].get("bounce_kv", {})
        if "T_J" in bounce:
            printed = bounce["T_J"].replace(" MeV", "")
            shown = format(float(row["T_deliver_GeV"]) * 1000.0, ".6f")
            same = shown == printed
            bad += 0 if same else 1
            print(
                f"   first bounce T_J: tool {printed} MeV; export T_deliver_GeV={row['T_deliver_GeV']} -> {shown} MeV  "
                f"{'equal' if same else 'DIFFERENT'}"
            )
    print("export-check: " + ("all equal" if bad == 0 else f"{bad} DIFFERENT"))
    return 0 if bad == 0 else 1


# ---- tables ----

SUMMARY_COLUMNS = [
    "name", "n", "M_Mp", "beta", "law", "value", "status", "exit", "reason",
    "integrations", "seed_kind", "residual_H", "residual_phi", "tuning_wall_s", "run_wall_s", "load_start",
    "load_end", "RHS", "accepted_steps", "reflections", "candidates", "injections", "resolved", "unresolved",
    "first_bounce_N", "first_bounce_T_J_MeV", "first_bounce_reflected", "first_impact_log_R", "first_impact_log_x",
    "n_one_loop", "n_gap", "n_classical", "n_unassessed",
    "K_share_one_loop", "K_share_gap", "K_share_classical", "K_share_unassessed", "K_share_parking",
    "ell_min", "ell_max", "log_R_min", "log_R_max", "log_R_min_drain", "f_min", "f_max", "alpha_eff_min",
    "alpha_eff_max", "at_cap", "consistency_mismatches",
    "parked", "parking_N", "parking_T_J_GeV", "parking_candidate_index", "parking_rho_kin_over_rho_rad",
    "parking_m_eff_over_H", "parking_min_m_eff_over_H", "parked_steps", "released", "release_N",
    "release_T_J_GeV", "release_m_eff_over_H", "release_m_eff_over_H_check", "min_ratio_before_cut",
    "min_ratio_before_cut_index",
    "Delta_N_eff_today", "Delta_N_eff_1MeV", "injected_fraction", "Yp", "DoH", "He3oH", "Li7oH",
    "K_final", "share_x_lt_100", "share_x_lt_1000", "log_x_min", "log_x_max", "inv_s_t_minus_1_at_min",
    "phi_t_max_Mp", "phi_t_ge_1e-8", "log_R_le_0", "unresolvable_assessments", "unresolved_declined",
    "log1p_x_min_over_16ulp", "max_wall_to_kinetic_ratio",
]  # fmt: skip


def load1(uptime_text):
    m = re.search(r"load averages?: ([0-9.]+)", uptime_text or "")
    return m.group(1) if m else ""


def summary_row(n, M, b, law, v):
    name = run_name(n, M, b, law, v)
    r = parse_run(RUNS / f"{name}.txt")
    L = r["lines"]
    o = L.get("sample outcome", {})
    row = dict.fromkeys(SUMMARY_COLUMNS, "")
    row.update(name=name, n=n, M_Mp=M, beta=b, law=law, value=v or "")
    row["status"] = o.get("status", "")
    row["exit"] = o.get("exit", "")
    row["reason"] = "" if o.get("reason") in (None, "None") else o["reason"]
    m = re.search(r"wall=([0-9.]+) s", r.get("end_uptime", ""))
    row["run_wall_s"] = m.group(1) if m else ""
    row["load_start"] = load1(r.get("start"))
    row["load_end"] = load1(r.get("end_uptime"))
    hist = L.get("history_kv", {})
    row["tuning_wall_s"] = hist.get("wall", "").replace(" s", "")
    if row["status"] != "ok":
        return row, r
    t = L["sample tuning"]
    row.update(
        integrations=t["integrations"],
        seed_kind=t["seed_kind"],
        residual_H=t["residual_H"],
        residual_phi=t["residual_phi"],
        RHS=hist["RHS"],
        accepted_steps=hist["accepted_steps"],
    )
    c = L["sample cascade"]
    for k in ("reflections", "candidates", "injections", "resolved", "unresolved"):
        row[k] = c[k]
    row["first_impact_log_R"] = c["first_impact_log_R"]
    row["first_impact_log_x"] = c["first_impact_log_x"]
    bounce = L.get("bounce_kv", {})
    row["first_bounce_N"] = bounce.get("N", "")
    row["first_bounce_T_J_MeV"] = bounce.get("T_J", "").replace(" MeV", "")
    row["first_bounce_reflected"] = bounce.get("reflected", "")
    for reg in ("one_loop", "gap", "classical", "unassessed"):
        row[f"n_{reg}"] = sum(1 for x in r["cands"] if x["regime"] == reg)
    dr = L.get("drain_kv")
    if dr is not None:
        for k in (
            "K_share_one_loop",
            "K_share_gap",
            "K_share_classical",
            "K_share_unassessed",
        ):
            row[k] = dr[k]
    d = L["sample drain"]
    for k in ("ell_min", "ell_max", "log_R_min", "log_R_max", "f_min", "f_max", "alpha_eff_min",
              "alpha_eff_max", "at_cap", "consistency_mismatches"):  # fmt: skip
        row[k] = d[k]
    drained = [
        fnum(x["log_R"])
        for x in r["cands"]
        if (fnum(x["dK_over_K"]) or 0.0) > 0.0 and x["log_R"] != "None"
    ]
    row["log_R_min_drain"] = repr(min(drained)) if drained else ""
    p = L["sample parking"]
    row["parked"] = p["parked"]
    row["parking_N"] = p["N"]
    row["parking_T_J_GeV"] = p["T_J_GeV"]
    row["parking_candidate_index"] = p["candidate_index"]
    row["parking_rho_kin_over_rho_rad"] = p["rho_kin_over_rho_rad"]
    row["parking_m_eff_over_H"] = p["m_eff_over_H"]
    row["parking_min_m_eff_over_H"] = p["min_m_eff_over_H"]
    row["parked_steps"] = p["parked_steps"]
    row["K_share_parking"] = p["K_share_parking"]
    row["released"] = p["released"]
    row["release_N"] = p["release_N"]
    row["release_T_J_GeV"] = p["release_T_J_GeV"]
    row["release_m_eff_over_H"] = p["release_m_eff_over_H"]
    row["release_m_eff_over_H_check"] = p["release_m_eff_over_H_check"]
    row["min_ratio_before_cut"] = p["min_ratio_before_cut"]
    row["min_ratio_before_cut_index"] = p["min_ratio_before_cut_index"]
    dis = L.get("dissipation_kv", {})
    row["Delta_N_eff_today"] = dis.get(
        "Delta_N_eff_today", "0" if law == "natural" else ""
    )
    row["Delta_N_eff_1MeV"] = dis.get(
        "Delta_N_eff_1MeV", "0" if law == "natural" else ""
    )
    row["injected_fraction"] = dis.get("injected_fraction", "")
    bbn = L.get("bbn_kv", {})
    for k in ("Yp", "DoH", "He3oH", "Li7oH"):
        row[k] = bbn.get(k, "")
    q = L["sample r9"]
    for k in (
        "K_final",
        "share_x_lt_100",
        "share_x_lt_1000",
        "log_x_min",
        "log_x_max",
        "inv_s_t_minus_1_at_min",
    ):
        row[k] = q[k]
    th = L["sample thresholds"]
    for k in ("phi_t_max_Mp", "phi_t_ge_1e-8", "log_R_le_0", "unresolvable_assessments", "unresolved_declined",
              "log1p_x_min_over_16ulp", "max_wall_to_kinetic_ratio"):  # fmt: skip
        row[k] = th[k]
    for k, x in row.items():
        if x == "None":
            row[k] = ""
    return row, r


COMPARE_FIELDS = [
    # (name, summary column, export column)
    ("success", "status", None),
    ("integrations", "integrations", "tuning_iterations"),
    ("tuning_wall_s", "tuning_wall_s", None),
    ("reflections", "reflections", "reflections"),
    ("injections", "injections", "injections"),
    ("first_impact_log_R", "first_impact_log_R", "log_R_first_impact"),
    ("first_bounce_N", "first_bounce_N", None),
    ("first_bounce_T_J_GeV", None, "T_deliver_GeV"),
    ("parking_N", "parking_N", None),
    ("parking_T_J_GeV", "parking_T_J_GeV", "parking_T_GeV"),
    ("release_T_J_GeV", "release_T_J_GeV", "release_T_GeV"),
    ("share_one_loop", "K_share_one_loop", "drained_share_one_loop"),
    ("share_gap", "K_share_gap", "drained_share_gap"),
    ("share_classical", "K_share_classical", "drained_share_classical"),
    ("share_unassessed", "K_share_unassessed", "drained_share_unassessed"),
    ("share_parking", "K_share_parking", "parking_share"),
    ("ell_min", "ell_min", "ell_min"),
    ("log_R_min_drain", "log_R_min_drain", "log_R_min_drain"),
    ("Delta_N_eff_today", "Delta_N_eff_today", "Delta_N_eff_today"),
    ("Delta_N_eff_1MeV", "Delta_N_eff_1MeV", "Delta_N_eff_1MeV"),
    ("Yp", "Yp", "Yp_BBN"),
    ("DoH", "DoH", "D_over_H"),
]


def comparison_rows(summ):
    ex = read_extract()
    out = []
    for s in SAMPLE:
        M, b, law, v = s
        e = ex[export_key(*s)]
        row = {"M_Mp": M, "beta": b, "law": law, "value": v or ""}
        for name, scol, ecol in COMPARE_FIELDS:
            for n in (1, 2):
                sr = summ[run_name(n, M, b, law, v)]
                if name == "success":
                    val = sr["status"]
                elif name == "first_bounce_T_J_GeV":
                    t = fnum(sr["first_bounce_T_J_MeV"])
                    val = "" if t is None else format(t / 1000.0, ".6g")
                else:
                    val = sr.get(scol, "")
                row[f"n{n}_{name}"] = val
            if name == "success":
                row[f"exp_{name}"] = "ok" if e["failure_reasons"] == "" else "failure"
            else:
                row[f"exp_{name}"] = "" if ecol is None else e[ecol]
        for n in (1, 2):
            for k, ecol in (
                ("Delta_N_eff_1MeV", "Delta_N_eff_1MeV"),
                ("Delta_N_eff_today", "Delta_N_eff_today"),
            ):
                a, x = fnum(row[f"n{n}_{k}"]), fnum(e[ecol])
                row[f"n{n}_{k}_ratio"] = (
                    "" if a is None or not x else format(a / x, ".6g")
                )
            for k, ecol in (("Yp", "Yp_BBN"), ("DoH", "D_over_H")):
                a, x = fnum(row[f"n{n}_{k}"]), fnum(e[ecol])
                row[f"n{n}_{k}_minus_exp"] = (
                    "" if a is None or x is None else format(a - x, ".4g")
                )
        out.append(row)
    return out


def mcrit_lines(summ):
    ex = read_extract()
    lines = [
        "## 5. M_crit on this wall (prompt §1.4 item 5; P30) — an estimate from the path formula, not a measurement",
        "",
        "ln R at a candidate is ln(V0/M^4) + (terms in ln x alone): on the power wall 2 ln(n(n + 1)) - ln 8 pi^2 +",
        "(1 + 4/n) ln x (02's path formula, verification §2.2), and 02's maps show ln x along a path independent of M.",
        "V0 is tuned to H0 and does not move with M. So at fixed x ln R moves as -4 ln M, and the M at which a",
        "candidate's ln R would reach 0 is M exp(ln R/4). It is read here per history, from the smallest ln R over its",
        "candidates (column log_R_min; over the drained candidates, log_R_min_drain, for the exponential's export) and",
        "from its first impact's ln R. The exponential's is read the same way from its export row (log_R_first_impact,",
        "log_R_min_drain); its ln x along a path is taken as independent of M as well, which this sample does not test.",
        "",
        "| history | n = 1: M at min ln R | n = 1: M at first impact | n = 2: M at min ln R | n = 2: M at first "
        "impact | exponential: M at min ln R (drained) | exponential: M at first impact |",
        "|---|---|---|---|---|---|---|",
    ]

    def m_at(M, lr):
        return "" if lr is None else format(float(M) * exp(lr / 4.0), ".3g")

    for s in SAMPLE:
        M, b, law, v = s
        if law == "natural":
            continue
        cells = []
        for n in (1, 2):
            sr = summ[run_name(n, M, b, law, v)]
            cells += [
                m_at(M, fnum(sr["log_R_min"])),
                m_at(M, fnum(sr["first_impact_log_R"])),
            ]
        e = ex[export_key(*s)]
        cells += [
            m_at(M, fnum(e["log_R_min_drain"])),
            m_at(M, fnum(e["log_R_first_impact"])),
        ]
        lines.append(
            f"| {M}, beta {b}, {law} {v} | "
            + " | ".join(c or "—" for c in cells)
            + " |"
        )
    return lines


def threshold_lines(summ):
    rows = [summ[run_name(n, *s)] for n in (1, 2) for s in SAMPLE]
    ok = [r for r in rows if r["status"] == "ok"]
    drained = [r for r in ok if r["law"] != "natural"]
    L = [
        "thresholds.txt — power-law-wall prompt 04 §1.4 (P10, P28): each threshold of README §0.3 read off the forty.",
        "Built by prompts/power-law-wall/source/sample/tables.py from runs/*.txt; reported, not judged.",
        "",
        f"histories: {len(rows)}; succeeded {len(ok)}; failed {len(rows) - len(ok)}; at alpha > 0 (fixed or one-loop) "
        f"and succeeded {len(drained)}",
        "",
        "## 1. The unresolved reflection (UNRESOLVED_REFLECTION_FACTOR K = 3; phi_t < 1e-8 M_P or ln R > 0), alpha > 0",
        "",
        "| history | candidates | resolved | unresolved | largest phi_t / M_P | candidates with phi_t >= 1e-8 M_P | "
        "smallest ln R | candidates with ln R <= 0 |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for r in drained:
        L.append(
            f"| {r['name']} | {r['candidates']} | {r['resolved']} | {r['unresolved']} | {r['phi_t_max_Mp']} | "
            f"{r['phi_t_ge_1e-8']} | {r['log_R_min']} | {r['log_R_le_0']} |"
        )
    L += [
        "",
        "## 2. The parking cut (PARKING_EPSILON 1e-6 on rho_kin,E/rho_R,E; m_eff/H_E >= 10; the pi guard 1e-9 M_P), "
        "alpha > 0",
        "",
        "| history | parked | candidate | N | T_J (GeV) | rho_kin/rho_R at the cut | m_eff/H at the cut | smallest "
        "rho_kin/rho_R before the cut (index) | min m_eff/H parked | released | release T_J (GeV) | m_eff/H at the "
        "release | leftover's share of K |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in drained:
        L.append(
            f"| {r['name']} | {r['parked']} | {r['parking_candidate_index'] or '—'} | {r['parking_N'] or '—'} | "
            f"{r['parking_T_J_GeV'] or '—'} | {r['parking_rho_kin_over_rho_rad'] or '—'} | "
            f"{r['parking_m_eff_over_H'] or '—'} | {r['min_ratio_before_cut'] or '—'} "
            f"({r['min_ratio_before_cut_index'] or '—'}) | {r['parking_min_m_eff_over_H'] or '—'} | {r['released']} | "
            f"{r['release_T_J_GeV'] or '—'} | {r['release_m_eff_over_H'] or '—'} | {r['K_share_parking'] or '—'} |"
        )
    guard = [
        r for r in rows if "pi_in" in r["reason"] or "guard" in r["reason"].lower()
    ]
    L += [
        "",
        f"histories failed by the pi guard (P21): {len(guard)}"
        + "".join(f"; {r['name']}" for r in guard),
    ]
    L += [
        "",
        "## 3. The wall's rounding (WALL_ROUNDING_QUANTA = 16)",
        "",
        "ln(1 + x) at the smallest x of a history's candidates is ln V - ln V_floor at that turning point; the column "
        "gives it in units of 16 ulp(|ln V0|), the width _log_V_minus_floor compares it with.",
        "",
        "| history | unresolvable assessments | smallest ln(E/V0) | ln(1 + x_min) / (16 ulp ln V0) |",
        "|---|---|---|---|",
    ]
    for r in ok:
        L.append(
            f"| {r['name']} | {r['unresolvable_assessments']} | {r['log_x_min'] or '—'} | "
            f"{r['log1p_x_min_over_16ulp'] or '—'} |"
        )
    L += [
        "",
        "## R9's share (P27): the share of K(N_final) from candidates with E/V0 < 100 and < 1e3",
        "",
        "| history | share x < 100 | share x < 1e3 | smallest ln(E/V0) | 1/s_t - 1 there |",
        "|---|---|---|---|---|",
    ]
    for r in drained:
        L.append(
            f"| {r['name']} | {r['share_x_lt_100']} | {r['share_x_lt_1000']} | {r['log_x_min']} | "
            f"{r['inv_s_t_minus_1_at_min']} |"
        )
    L += ["", "## 4. The surfing thresholds: see surfing/edges.txt", ""]
    L += mcrit_lines(summ)
    return L


def write_csv(path, rows, cols):
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in cols})


def tables():
    summ = {}
    order = []
    for n in (1, 2):
        for s in SAMPLE:
            row, _ = summary_row(n, *s)
            summ[row["name"]] = row
            order.append(row)
    write_csv(OUT / "summary.csv", order, SUMMARY_COLUMNS)
    comp = comparison_rows(summ)
    cols = list(comp[0].keys())
    write_csv(OUT / "comparison.csv", comp, cols)
    (OUT / "thresholds.txt").write_text("\n".join(threshold_lines(summ)) + "\n")
    print(
        f"wrote summary.csv ({len(order)} rows), comparison.csv ({len(comp)} rows), thresholds.txt"
    )
    return 0


def markdown():
    """The readable tables of verification §4.1 and §4.2, from summary.csv and comparison.csv, printed."""
    with open(OUT / "summary.csv", newline="") as fh:
        summ = list(csv.DictReader(fh))
    with open(OUT / "comparison.csv", newline="") as fh:
        comp = list(csv.DictReader(fh))

    def c(x):
        return x if x not in ("", None) else "—"

    print(
        "| history | outcome | integrations (seed) | tuning wall s (load) | RHS / steps | reflections | candidates "
        "(one-loop) | first bounce N, T_J MeV | ln R first impact | ℓ min | f range | parked at (T_J GeV) | "
        "ΔN_eff 1 MeV | Y_p | D/H |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r in summ:
        if r["status"] != "ok":
            m = re.search(
                r"W/\(pi\^2/2\) = (\S+) at N=(\S+), phi_E=(\S+),", r["reason"]
            )
            why = (
                f"G2, W/(π²/2) = {m.group(1)} at N = {m.group(2)}, φ_E = {m.group(3)} M_P"
                if m
                else r["reason"]
            )
            print(
                f"| {r['name']} | **failure**: {why} | — | {r['tuning_wall_s']} ({r['load_start']}) "
                f"| — | — | — | — | — | — | — | — | — | — | — |"
            )
            continue
        f = f"{r['f_min']}–{r['f_max']}" if r["f_min"] else "—"
        park = (
            f"{r['parking_candidate_index']} ({r['parking_T_J_GeV']})"
            if r["parked"] == "True"
            else "—"
        )
        print(
            f"| {r['name']} | ok | {r['integrations']} ({r['seed_kind']}) | {r['tuning_wall_s']} ({r['load_start']}) | "
            f"{r['RHS']} / {r['accepted_steps']} | {r['reflections']} | {r['candidates']} ({r['n_one_loop']}) | "
            f"{float(r['first_bounce_N']):.4f}, {float(r['first_bounce_T_J_MeV']):.3f} | {c(r['first_impact_log_R'])} | "
            f"{c(r['ell_min'])} | {f} | {park} | {c(r['Delta_N_eff_1MeV'])} | {c(r['Yp'])} | {c(r['DoH'])} |"
        )
    print()
    print(
        "| (M, β, law) | injections n1 / n2 / exp | ln R first impact n1 / n2 / exp | parking T_J GeV n1 / n2 / exp | "
        "ℓ min n1 / n2 / exp | ΔN_eff 1 MeV n1 / n2 / exp | ratio to exp n1 / n2 | Y_p − exp n1 / n2 | "
        "D/H − exp n1 / n2 |"
    )
    print("|---|---|---|---|---|---|---|---|---|")
    for r in comp:

        def t(k, fmt=None):
            out = []
            for w in ("n1", "n2", "exp"):
                v = r.get(f"{w}_{k}", "")
                if v not in ("", None) and fmt:
                    v = format(float(v), fmt)
                out.append(c(v))
            return " / ".join(out)

        law = f"{r['law']} {r['value']}".strip()
        print(
            f"| {r['M_Mp']}, {r['beta']}, {law} | {t('injections')} | {t('first_impact_log_R', '.6g')} | "
            f"{t('parking_T_J_GeV', '.4g')} | {t('ell_min', '.4g')} | {t('Delta_N_eff_1MeV', '.4g')} | "
            f"{c(r['n1_Delta_N_eff_1MeV_ratio'])} / {c(r['n2_Delta_N_eff_1MeV_ratio'])} | "
            f"{c(r['n1_Yp_minus_exp'])} / {c(r['n2_Yp_minus_exp'])} | {c(r['n1_DoH_minus_exp'])} / "
            f"{c(r['n2_DoH_minus_exp'])} |"
        )
    return 0


def main(argv):
    if not argv:
        print(__doc__)
        return 2
    cmd = argv[0]
    if cmd == "extract":
        return extract(argv[1])
    if cmd == "compare":
        return compare(argv[1], argv[2])
    if cmd == "overlaps":
        return overlaps()
    if cmd == "export-check":
        return export_check(argv[1:])
    if cmd == "tables":
        return tables()
    if cmd == "markdown":
        return markdown()
    print(f"unknown command {cmd}")
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
