"""
Copies the PNGs of 05's export (var/one-loop-drain/07/exports-05/QCD_Cosmology) that prompt 07 §3 commits into this directory,
named by their row and history (orchestrator note 07 ruling 7): `A=<A:.5g>_quanta_grid.png` for each A at c_a = 1, and
`A=<A:.5g>_` + the history directory with `/` replaced by `_` + `_quanta_history.png`. The c_a = 0 and 10 grids stay under var/.
Run from the repository root.
"""

import shutil
from pathlib import Path

SRC = Path("var/one-loop-drain/07/exports-05/QCD_Cosmology/one-loop-measured-v1")
DST = Path(__file__).resolve().parent
n = 0
for a_dir in sorted(SRC.glob("A=*")):
    grid = a_dir / "plots/quanta_grid.png"
    if grid.is_file():
        shutil.copy(grid, DST / f"{a_dir.name}_quanta_grid.png")
        n += 1
    for png in sorted(a_dir.glob("plots/beta=*/M=*/quanta_history.png")):
        hd = "_".join(png.parent.relative_to(a_dir / "plots").parts)
        shutil.copy(png, DST / f"{a_dir.name}_{hd}_quanta_history.png")
        n += 1
print(f"copied {n} PNGs")
