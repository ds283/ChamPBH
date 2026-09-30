"""
Planning probe for the run-integrity campaign: how the datastore's lookups treat
the version label and failed rows. No Ray, no cluster: it builds the undecorated
Datastore class on a temporary SQLite file and uses stand-ins for the objects a
lookup keys on. About 1 s, from the repository root:

    PYTHONPATH=. ./venv/bin/python prompts/run-integrity/planning-probes/datastore_version_probe.py

Rows are inserted through the datastore's own inserter, so the version column is
filled exactly as Datastore._insert fills it in production. Without a serial
broker the inserter needs an explicit serial, which the probe supplies.
"""

import tempfile
from pathlib import Path
from types import SimpleNamespace as NS

from Datastore.SQL.Datastore import Datastore

DS = Datastore.__ray_actor_class__


def stand_in(store_id, type_id=0):
    return NS(
        store_id=store_id,
        type_id=type_id,
        units=NS(PlanckMass=1.0, GeV=1.0, MeV=1.0e-3),
    )


cosmology, potential, coupling = stand_in(1, 7), stand_in(2, 3), stand_in(4, 5)
T_init, T_stop, phi, pi = stand_in(10), stand_in(11), stand_in(12), stand_in(13)
atol, rtol = stand_in(14), stand_in(15)

SCALAR_MODEL_QUERY = dict(
    solver_labels={},
    cosmology=cosmology,
    T_Jordan_init=T_init,
    T_Jordan_stop=T_stop,
    phi_Einstein_init=phi,
    pi_Einstein_init=pi,
    potential=potential,
    coupling=coupling,
    atol=atol,
    rtol=rtol,
    z_grid=None,
    tags=[],
    _do_not_populate=True,
)


def sid(obj):
    return obj.store_id if obj.available else None


def reason(obj):
    return obj.failure_reason if obj.available else None


def insert(store, cls_name, row):
    with store._engine.begin() as conn:
        serial = store._schema[cls_name]["insert"](conn, row)
        conn.commit()
    return serial


def insert_failed_scalar_model(store):
    for serial, log10_tol in ((14, -10.0), (15, -8.0)):
        insert(store, "tolerance", dict(serial=serial, log10_tol=log10_tol))
    return insert(
        store,
        "ScalarModel",
        dict(
            serial=1,
            label="m",
            cosmology_type=7,
            cosmology_serial=1,
            potential_type=3,
            potential_serial=2,
            coupling_type=5,
            coupling_serial=4,
            atol_serial=14,
            rtol_serial=15,
            phi_Einstein_init_serial=12,
            pi_Einstein_init_serial=13,
            T_Jordan_init_serial=10,
            T_Jordan_stop_serial=11,
            failure=True,
            validated=True,
        ),
    )


def insert_failed_bbn(store, model_serial, reason, serial):
    return insert(
        store,
        "BBNData",
        dict(
            serial=serial,
            model_serial=model_serial,
            failure=True,
            failure_reason=reason,
            validated=True,
        ),
    )


db = Path(tempfile.mkdtemp()) / "probe.db"

old = DS(version_label="2026.3.0", db_name=db)
model_serial = insert_failed_scalar_model(old)
m = old.object_get("ScalarModel", **SCALAR_MODEL_QUERY)
print(
    f"[1] ScalarModel under the label that stored it: available={m.available}, "
    f"store_id={sid(m)}"
)

new = DS(version_label="2026.4.0", db_name=db)
print(
    f"[2] version serials: 2026.3.0 -> {old._version.store_id}, "
    f"2026.4.0 -> {new._version.store_id}"
)
m2 = new.object_get("ScalarModel", **SCALAR_MODEL_QUERY)
print(
    f"[3] ScalarModel under a NEW label: available={m2.available}, "
    f"store_id={sid(m2)}   <- on 27a32bc the old row is returned"
)

proxy = NS(
    store_id=model_serial,
    get=lambda: NS(cosmology=cosmology, coupling=coupling, potential=potential),
)
insert_failed_bbn(old, model_serial, "first failure", serial=1)

d = new.object_get("BBNData", model_proxy=proxy, tags=[], _do_not_populate=True)
print(
    f"[4] BBNData, default lookup (failure=False, what main.py uses): "
    f"available={d.available}   <- a stored failure is invisible, so it is recomputed"
)

d = new.object_get(
    "BBNData", model_proxy=proxy, tags=[], failure=True, _do_not_populate=True
)
print(
    f"[5] BBNData, failure=True under the NEW label: available={d.available}, "
    f"reason={reason(d)!r}   <- on 27a32bc, the row stored under 2026.3.0"
)

insert_failed_bbn(old, model_serial, "second failure", serial=2)
try:
    d = new.object_get(
        "BBNData", model_proxy=proxy, tags=[], failure=None, _do_not_populate=True
    )
    print(f"[6] BBNData, failure=None, two failed rows: available={d.available}")
except Exception as e:
    print(f"[6] BBNData, failure=None, two failed rows: raises {type(e).__name__}")
