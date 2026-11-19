import json, os, sys

SRC = sys.argv[1]
OUT = sys.argv[2]
os.chdir(SRC)
sys.path.insert(0, SRC)
import fits

ps = fits.points()
keep = []
for p in ps:
    q = {
        k: p[k]
        for k in (
            "set",
            "n",
            "L",
            "lnR",
            "frac",
            "back",
            "ell_old",
            "ell_new",
            "reg_old",
            "reg_new",
            "lv",
        )
    }
    q["sel_one_new"] = bool(fits.sel_one(p, "new"))
    q["sel_pla_new"] = bool(fits.sel_plateau(p, "new"))
    q["sel_gap_new"] = bool(fits.sel_gap(p, "new"))
    q["sel_one_old"] = bool(fits.sel_one(p, "old"))
    q["sel_pla_old"] = bool(fits.sel_plateau(p, "old"))
    q["sel_gap_old"] = bool(fits.sel_gap(p, "old"))
    q["planner"] = bool(fits.planner_ok(p))
    q["gap_ok"] = bool(fits.gap_ok(p))
    q["n2_ok"] = bool(fits.n2_ok(p))
    keep.append(q)
json.dump(keep, open(OUT, "w"), indent=0)
print(len(keep))
