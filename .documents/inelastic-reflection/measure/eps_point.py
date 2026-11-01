"""
Run `tools/history_and_bbn.py` with the parking epsilon patched (inelastic-reflection prompt 11;
README §8.1 R30, §8.2 P42).

    PYTHONPATH=. ./venv/bin/python .documents/inelastic-reflection/measure/eps_point.py EPS -- <tool arguments>

for example `eps_point.py 1e-8 -- 2 4e-31 --alpha 0.1`. From the repository root, as the tool
requires (PRyMordial reads PRyMrates/ from the working directory).

It sets `PARKING_EPSILON = EPS` on the module `ComputeTargets.ScalarHistory` (taken from
`sys.modules` through importlib, not by `import ... as`, which binds the class of that name) before
importing the tool, prints `epsilon=<EPS:g>` as its first line, calls
`tools.history_and_bbn.main(<tool arguments>)` in the same process, asserts that the module's
attribute still holds EPS after the call, and exits with the tool's status. `PARKING_EPSILON` is read at call time by `_parking_cut`, so the patch changes the cut and
nothing else. No source file is changed.

It writes nothing but its stdout and stderr; `run_grid.py` sends those to a file under
var/inelastic-reflection-11/.
"""

import importlib
import sys
import types

MODULE = "ComputeTargets.ScalarHistory"


def main(argv) -> int:
    if len(argv) < 2 or argv[1] != "--":
        print(
            "usage: eps_point.py EPS -- <tool arguments>",
            file=sys.stderr,
        )
        return 2
    eps = float(argv[0])
    tool_argv = list(argv[2:])

    # the module, by importlib: `import ComputeTargets.ScalarHistory as SH` would bind the class
    # ScalarHistory, which ComputeTargets/__init__.py exports under the module's name, and a patch
    # of the class changes nothing the cut reads (prompt 11's log, deviation D3)
    SH = importlib.import_module("ComputeTargets.ScalarHistory")
    assert isinstance(SH, types.ModuleType) and SH is sys.modules[MODULE]

    SH.PARKING_EPSILON = eps
    print(f"epsilon={eps:g}", flush=True)

    import tools.history_and_bbn as HB

    status = HB.main(tool_argv)
    held = sys.modules[MODULE].PARKING_EPSILON
    assert held == eps, f"PARKING_EPSILON is {held!r} after the call, not {eps!r}"
    sys.stdout.flush()
    return status


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
