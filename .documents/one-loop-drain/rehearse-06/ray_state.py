"""Summarise the isolated cluster's jobs, workers and actors (rrk-adoption 04, ruling 9).
Read-only: it runs `ray list` with --address 127.0.0.1:6380 and prints one line per item.

one-loop-drain prompt 06: copied from inelastic-reflection 11's (.documents/inelastic-reflection/out/11-ray_state.py.txt),
with this paragraph added. Run from the main checkout's root, with the cluster up, as
`RAY_AUTH_MODE=token ./venv/bin/python .documents/one-loop-drain/rehearse-06/ray_state.py`; it starts nothing, opens no
store and writes nothing but stdout."""
import json, subprocess, sys
A = "127.0.0.1:6380"
RAY = "./venv/bin/ray"
def lst(kind, *extra):
    cmd = [RAY, "list", kind, "--address", A, "--limit", "1000", "--format", "json", *extra]
    print("$ " + " ".join(cmd))
    out = subprocess.run(cmd, capture_output=True, text=True)
    try:
        return json.loads(out.stdout)
    except Exception:
        print(out.stdout, out.stderr); return []
jobs = lst("jobs")
for j in jobs:
    print(f"  job {j['job_id']} {j['status']} driver pid {(j.get('driver_info') or {}).get('pid')}: {j['entrypoint'][-110:]}")
w = lst("workers")
alive = [x for x in w if x.get("is_alive")]
print(f"  workers: {len(w)} recorded, {len(alive)} alive")
for x in alive:
    print(f"    alive {x['worker_type']} pid {x['pid']} worker {x['worker_id'][:12]}")
a = lst("actors")
al = [x for x in a if x.get("state") == "ALIVE"]
from collections import Counter
print(f"  actors: {len(a)} recorded, {len(al)} ALIVE; by (job, class, state): {dict(Counter((x.get('job_id'), x.get('class_name'), x.get('state')) for x in a))}")
for x in al:
    print(f"    ALIVE {x.get('class_name')} job {x.get('job_id')} pid {x.get('pid')}")
