"""
The inputs of the reflection-structure figure: the columns it draws from each of the reflection
store's 42 fields.csv, written by the run's plot_ScalarHistory.py step, and (since 2026-11-02) the
diagnostic store's 12, written by run_diagnostic_large_M.sh, gathered into one compressed file. The
files are too large to keep in git together; this keeps the columns the figure reads, unchanged,
the store each came from, and the SHA-256 of every source.

    ./venv/bin/python science-runs/2026-11-01-science-2026.8.0/reflections/extract_inputs.py

From the repository root. Reads var/science-2026.8.0/out/reflections/QCD_Cosmology/ and
var/science-2026.8.0/diagnostic/out/large-M-dissipation/QCD_Cosmology/; writes
inputs/fields.csv.gz and inputs/sources.json beside this file.
"""

import hashlib
import json
import re
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
SOURCES = {  # store: the plot_ScalarHistory.py output written from it
    "reflections": Path("var/science-2026.8.0/out/reflections/QCD_Cosmology"),
    "large-M-dissipation": Path("var/science-2026.8.0/diagnostic/out/large-M-dissipation/QCD_Cosmology"),
}
COLUMNS = ["T_Jordan_GeV", "phi_Einstein_Mp", "pi_Einstein_Mp", "rho_chi_over_rho_rad"]
M_EV_TO_MP = {  # the folder names give M in eV; these are the run's M in M_P
    "0.00097413eV": 4e-31,
    "2.4353e+12eV": 1e-15,
    "2.4353e+15eV": 1e-12,
    "2.4353e+18eV": 1e-9,
    "2.4353e+21eV": 1e-6,
    "2.4353e+24eV": 1e-3,
    "1.2177e+27eV": 0.5,
}


def main():
    frames, sources = [], []
    paths = [(store, p) for store, source in SOURCES.items()
             for p in sorted(source.glob("alpha=*/csv/beta=*/M=*_n=1/fields.csv"))]
    for store, path in paths:
        alpha = float(re.search(r"alpha=([^/]+)", str(path)).group(1))
        beta = float(re.search(r"beta=([^/]+)", str(path)).group(1))
        M_eV = re.search(r"M=([^_]+)_n=1", str(path)).group(1)
        d = pd.read_csv(path, usecols=COLUMNS)
        d.insert(0, "M_Mp", M_EV_TO_MP[M_eV])
        d.insert(0, "beta", beta)
        d.insert(0, "alpha", alpha)
        d.insert(0, "store", store)
        d.insert(4, "sample", range(len(d)))
        frames.append(d)
        sources.append(
            {
                "store": store,
                "path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "rows": len(d),
            }
        )
    (HERE / "inputs").mkdir(exist_ok=True)
    out = HERE / "inputs" / "fields.csv.gz"
    pd.concat(frames).to_csv(out, index=False, float_format="%.17g", compression={"method": "gzip", "mtime": 0})
    (HERE / "inputs" / "sources.json").write_text(json.dumps(sources, indent=2) + "\n")
    print(f"{len(sources)} histories, {sum(s['rows'] for s in sources)} rows -> {out} "
          f"({out.stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
