#!/usr/bin/env bash
# The probe's Part 1 reference for the exponential, rebuilt on the laptop (PLAN.md section 3, amended 2026-11-20).
#
# power-law-wall-07d-r2-exp was retired on 2026-11-19, so this rebuilds it from 07d's three commands
# (.documents/power-law-wall/rehearse-07d/out/07d-commands.txt, runs r2-exp-a, -b, -c), with only --database and
# --job-name changed, into science-2026.14.0-probe-ref-exp. Each run is registered as --prompt Q. The store's
# fingerprint is then compared, class by class, with the one the retired store's sidecar keeps.
#
# Run from the repository root, with no Ray cluster of ChamPBH's running:
#   bash science-runs/plan-science-2026.14.0/probe/ref_exp_laptop.sh
set -euo pipefail

RUN=science-2026.14.0
HERE=science-runs/plan-science-2026.14.0/probe
LOG=$HERE/logs
DB=$PWD/var/datastores/$RUN-probe-ref-exp.db
RETIRED=var/datastores/power-law-wall-07d-r2-exp.manifest.json
RAY_ADDR=127.0.0.1:6380

[ -e CLAUDE.md ] && [ -d RunDrivers ] || { echo "run from the repository root" >&2; exit 2; }
[ -e "$DB" ] && { echo "$DB exists; this script builds it from empty" >&2; exit 2; }
mkdir -p "$LOG"

run() {  # run <job> <purpose> <main.py arguments...>
    local job="$1" purpose="$2"
    shift 2
    echo ">> $(date '+%F %T') $job" | tee -a "$LOG/$job.log"
    ./venv/bin/python -u RunDrivers/registered_run.py --campaign "$RUN" --prompt Q \
        --register "$job" --purpose "$purpose" -- --config exponential.yaml --database "$DB" \
        --shards 16 --phi-init-Mp 5 --ray-address "$RAY_ADDR" --job-name "$job" "$@" 2>&1 | tee -a "$LOG/$job.log"
}

tools/ray_cluster.sh start "$RUN probe: laptop reference for the exponential (Part 1)"
trap 'tools/ray_cluster.sh stop' EXIT

run probe-ref-exp-a \
    "$RUN probe Part 1: laptop rebuild of 07d r2-exp-a (exponential n 1; beta 1.2 2.0 2.9 at M 4e-31 Mp, phi* 5, alpha 0.01 0.1, A 0.05 1 10)" \
    --M-values-Mp 4e-31 --beta-values 1.2 2.0 2.9 --alpha-values 0.01 0.1 --A-values 0.05 1 10
[ -e "${DB%.db}.manifest.json" ] || ./venv/bin/python -m runregistrykit store create \
    --purpose "$RUN probe Part 1: the exponential's reference, rebuilt on the laptop from 07d's r2-exp commands (power-law-wall-07d-r2-exp was retired)" "$DB"

run probe-ref-exp-b \
    "$RUN probe Part 1: laptop rebuild of 07d r2-exp-b (exponential n 1; beta 1.2 2.0 2.9 at M 1e-3 Mp, phi* 5, alpha 0)" \
    --M-values-Mp 1e-3 --beta-values 1.2 2.0 2.9 --alpha-values 0

run probe-ref-exp-c \
    "$RUN probe Part 1: laptop rebuild of 07d r2-exp-c (exponential n 1; beta 2.0 at M 1e-33 1e-15 Mp, phi* 5, A 1, no fixed alpha)" \
    --M-values-Mp 1e-33 1e-15 --beta-values 2.0 --A-values 1 --no-alpha-values

echo ">> $(date '+%F %T') fingerprint against the retired sidecar" | tee -a "$LOG/probe-ref-exp-compare.log"
./venv/bin/python - "${DB%.db}.manifest.json" "$RETIRED" <<'EOF' 2>&1 | tee -a "$LOG/probe-ref-exp-compare.log"
import json, sys
new = json.load(open(sys.argv[1]))["fingerprint"]
old = json.load(open(sys.argv[2]))["fingerprint"]
print(f"rebuild  {new['digest']}  format {new['fingerprint_format']}  taken {new['taken']}")
print(f"retired  {old['digest']}  format {old['fingerprint_format']}  taken {old['taken']}")
for name in sorted(set(new["classes"]) | set(old["classes"])):
    a, b = new["classes"].get(name), old["classes"].get(name)
    same = a is not None and b is not None and a["digest"] == b["digest"] and a["count"] == b["count"]
    print(f"  {'same' if same else 'DIFFERS':8} {name:20} rebuild {a and a['count']}  retired {b and b['count']}")
print("EQUAL" if new["digest"] == old["digest"] else "NOT EQUAL")
EOF
