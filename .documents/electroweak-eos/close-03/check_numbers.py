"""electroweak-eos prompt 03, section 2 item 4: ten numbers of the handover (review-remediation-verification.md 4.21)
and the run record's note, each re-read from the record it is cited to. Reads files only; no store, no Ray.
"""

import csv, hashlib, json, pathlib, subprocess, sys

ROOT = pathlib.Path(__file__).resolve().parents[3]
E = ROOT / ".documents/electroweak-eos"


def rows(p):
    return list(csv.DictReader(open(p)))


out = []


def line(n, claim, value, source):
    out.append(f"{n:2d}. {claim}: {value}   [{source}]")


# 1, 2, 3: the edges of F-new and F-old
for name, n in (("f_new", 1), ("f_old", 2)):
    r = rows(E / f"threshold/out/{name}.csv")
    qcd = max(float(x["beta"]) for x in r if x["capture_kick"] == "QCD")
    ew = min(float(x["beta"]) for x in r if x["capture_kick"] == "EW")
    unc = [float(x["beta"]) for x in r if x["captured"] == "false"]
    line(
        n,
        f"{name}: largest QCD, smallest EW, uncaptured range and count",
        f"{qcd}, {ew}, {min(unc)}-{max(unc)}, {len(unc)}",
        f"threshold/out/{name}.csv",
    )
# 3: the 13 histories across 10 GeV
r = rows(E / "effect/out/effect.csv")
c = [
    x
    for x in r
    if (float(x["T_deliver_GeV_old"]) - 10) * (float(x["T_deliver_GeV_new"]) - 10) < 0
]
line(3, "histories with T_deliver across 10 GeV", len(c), "effect/out/effect.csv")
# 4: a-edge beta = 3.0
x = [y for y in r if y["run"] == "a-edge" and float(y["beta"]) == 3.0][0]
line(
    4,
    "a-edge beta=3.0 T_deliver old -> new (GeV)",
    f"{float(x['T_deliver_GeV_old']):.6g} -> {float(x['T_deliver_GeV_new']):.6g}",
    "effect/out/effect.csv",
)
# 5: largest relative Y_p change of a-edge, and two edge parking temperatures
a = [abs(float(y["Yp_BBN_rel"])) for y in r if y["run"] == "a-edge"]
line(
    5,
    "a-edge largest |relative change| of Y_p",
    f"{max(a):.3g}",
    "effect/out/effect.csv",
)
pk = (
    {
        float(y["beta"]): (
            float(y["parking_T_GeV_old"]) * 1e3,
            float(y["parking_T_GeV_new"]) * 1e3,
        )
        for y in r
        if y["run"] == "a-edge"
    }
    if "parking_T_GeV_old" in r[0]
    else {}
)
if pk:
    line(
        6,
        "parking T (MeV) old -> new at beta 2.99 and 2.995 (a-edge)",
        f"{pk[2.99][0]:.3g} -> {pk[2.99][1]:.3g}; {pk[2.995][0]:.3g} -> {pk[2.995][1]:.3g}",
        "effect/out/effect.csv",
    )
else:
    line(6, "parking column", "not found", "effect/out/effect.csv")
# 7: bath drift
b = rows(E / "bath/out/bath_tables.csv")
g = {(y["table"], y["T_quoted_GeV"]): float(y["percent_100_Delta"]) for y in b}
line(
    7,
    "bath drift 100 Delta at 1 MeV, old and new",
    f"{g.get(('old','0.001'), 'n/a'):.4f}, {g.get(('new','0.001'), 'n/a'):.4f}",
    "bath/out/bath_tables.csv",
)
# 8: the table
csvp = ROOT / "CosmologyModels/GenericEOS/Xav_EOS_data.csv"
lines = csvp.read_text().splitlines()
line(
    8,
    "Xav_EOS_data.csv rows and SHA-256",
    f"{len(lines) - 1}, {hashlib.sha256(csvp.read_bytes()).hexdigest()}",
    "CosmologyModels/GenericEOS/Xav_EOS_data.csv",
)
# 9: store ids and file counts
for s in ("old", "new"):
    m = json.load(open(ROOT / f"var/datastores/electroweak-eos-02-{s}.manifest.json"))
    n = len(list((ROOT / "var/datastores").glob(f"electroweak-eos-02-{s}*")))
    line(
        9,
        f"store {s}: store_id and files",
        f"{m['store_id']}, {n}",
        f"var/datastores/electroweak-eos-02-{s}.manifest.json",
    )
# 10: the index count
t = (ROOT / ".documents/OPEN_ISSUES.md").read_text()
n = sum(1 for l in t.splitlines() if l.startswith("- `["))
line(
    10,
    "rows of the index and its stated count",
    (
        f"{n}; header says " + t.split("**")[1].split("·")[-1].strip()
        if False
        else f"{n}; " + [w for w in t.splitlines()[2].split("**") if "open" in w][0]
    ),
    ".documents/OPEN_ISSUES.md",
)
print("\n".join(out))
