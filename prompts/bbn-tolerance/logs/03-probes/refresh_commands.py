"""
bbn-tolerance prompt 03: the printer of prompt 02's warning, called on stub
objects (no store, no solve), to quote what it prints on a store that was not
refreshed. The stub mix is what the 2026.6.0 science store would show: every
successful BBN row made by PRyM_version bf24c3d+ri02+sr01 on the full network,
plus failure rows (which store no provenance). The numbers are illustrative of
the format; the real counts are the store's (663 successes and 21 failures on
the phi* = 5 histories at the time of the brief).

    PYTHONPATH=. ./venv/bin/python prompts/bbn-tolerance/logs/03-probes/refresh_commands.py
"""

from types import SimpleNamespace

from ComputeTargets.BBNData import PRYM_VERSION
from pipeline_selection import warn_foreign_bbn_provenance


def stub(version, small, failure=False):
    return SimpleNamespace(
        available=True,
        failure=failure,
        PRyM_version=None if failure else version,
        small_network=None if failure else small,
    )


rows = [stub("bf24c3d+ri02+sr01", False) for _ in range(663)] + [
    stub(None, None, failure=True) for _ in range(21)
]
print(f"PRYM_VERSION = {PRYM_VERSION}; production network: small")
warn_foreign_bbn_provenance(rows, PRYM_VERSION, True)
print("-- a refreshed store prints nothing:")
warn_foreign_bbn_provenance(
    [stub(PRYM_VERSION, True) for _ in range(5)], PRYM_VERSION, True
)
