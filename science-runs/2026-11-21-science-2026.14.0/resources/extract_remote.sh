#!/usr/bin/env bash
# Read-only extraction of the 2026.14.0 run's resource figures from the Mac Studio, where its stores and output live.
#
#   ssh macstudio-tunnel bash -s -- histories < science-runs/2026-11-21-science-2026.14.0/resources/extract_remote.sh > .../inputs/histories.csv
#   ssh macstudio-tunnel bash -s -- tables    < ... > .../inputs/tables.csv
#   ssh macstudio-tunnel bash -s -- files     < ... > .../inputs/files.csv
#   ssh macstudio-tunnel bash -s -- outputs   < ... > .../inputs/outputs.csv
#
# Every database is opened with ?mode=ro&immutable=1, so sqlite takes no lock and writes nothing (no -wal or -shm).
# Nothing is written on the Mac Studio. Written 2026-11-23 at the user's request ("generate some figures for the
# resource requirements of the 2026.14.0 run").
set -euo pipefail
ROOT="$HOME/Documents/Code/ChamPBH"
DS="$ROOT/var/datastores"
OUT="$ROOT/var/science-2026.14.0/out"
STORES="exponential inverse-power reflections"
SQLITE=/usr/bin/sqlite3

ro() { echo "file:$1?mode=ro&immutable=1"; }

case "${1:-}" in
histories)
    # one row per stored ScalarHistory, with its model, alpha row, timings and its BBN and adiabatic rows' timings.
    # The label ends with the time the history's compute task began (local time on the Mac Studio, UTC in November);
    # timestamp is when its row was written. The replicated tables (models, alpha, M, beta) are in every shard.
    first=1
    for s in $STORES; do
        for f in "$DS"/science-2026.14.0-$s-shard*.db; do
            shard=$(basename "$f" .db | sed 's/.*-shard//')
            hdr=""
            [ $first = 1 ] && hdr="-header" && first=0
            $SQLITE -csv $hdr "$(ro "$f")" "
SELECT '$s' AS store, '$shard' AS shard, sh.serial, sh.label, sh.timestamp, sh.model_type,
       em.n_value AS em_n, bem.value AS em_beta, mem.value_eV AS em_M_eV,
       ip.n_value AS ip_n, bip.value AS ip_beta, mip.value_eV AS ip_M_eV,
       a.value AS alpha, a.drain_model,
       sh.failure, sh.failure_reason, sh.compute_time, sh.compute_steps, sh.RHS_evaluations, sh.mean_RHS_time,
       sh.tuning_iterations, sh.plain_steps, sh.secant_steps, sh.guard_steps, sh.injections, sh.bounce_rows,
       sh.z_samples,
       (SELECT count(*) FROM ScalarHistoryValue v WHERE v.history_serial = sh.serial) AS sample_rows,
       (SELECT b.timestamp FROM BBNData b WHERE b.scalar_history_serial = sh.serial ORDER BY b.serial DESC LIMIT 1) AS bbn_timestamp,
       (SELECT b.failure FROM BBNData b WHERE b.scalar_history_serial = sh.serial ORDER BY b.serial DESC LIMIT 1) AS bbn_failure,
       (SELECT b.NP_compute_time FROM BBNData b WHERE b.scalar_history_serial = sh.serial ORDER BY b.serial DESC LIMIT 1) AS bbn_NP_time,
       (SELECT b.BBN_compute_time FROM BBNData b WHERE b.scalar_history_serial = sh.serial ORDER BY b.serial DESC LIMIT 1) AS bbn_PRyM_time,
       (SELECT d.compute_time FROM AdiabaticHistory d WHERE d.scalar_history_serial = sh.serial ORDER BY d.serial DESC LIMIT 1) AS adiabatic_time
FROM ScalarHistory sh
LEFT JOIN alpha_value a ON a.serial = sh.alpha_serial
LEFT JOIN ExponentialModel em ON em.serial = sh.model_serial
LEFT JOIN beta_value bem ON bem.serial = em.beta_serial
LEFT JOIN M_value mem ON mem.serial = em.M_serial
LEFT JOIN InversePowerModel ip ON ip.serial = sh.model_serial
LEFT JOIN beta_value bip ON bip.serial = ip.beta_serial
LEFT JOIN M_value mip ON mip.serial = ip.M_serial;"
        done
    done
    ;;
tables)
    # bytes per table or index, per shard and the primary, from sqlite's dbstat
    echo "store,file,name,bytes"
    for s in $STORES; do
        for f in "$DS"/science-2026.14.0-$s.db "$DS"/science-2026.14.0-$s-shard*.db; do
            b=$(basename "$f" .db)
            $SQLITE -csv "$(ro "$f")" "SELECT '$s', '$b', name, sum(pgsize) FROM dbstat GROUP BY name;"
        done
    done
    ;;
files)
    # every file of the three stores (primary, shards, sidecar), bytes
    echo "store,file,bytes"
    for s in $STORES; do
        for f in "$DS"/science-2026.14.0-$s*; do
            echo "$s,$(basename "$f"),$(stat -f %z "$f")"
        done
    done
    ;;
outputs)
    # the exports' files aggregated by root and kind: count and bytes
    echo "root,kind,files,bytes"
    cd "$OUT"
    find . -type f -exec stat -f '%z %N' {} + | awk '
    {
        size = $1; path = $2;
        n = split(path, p, "/"); root = p[2]; base = p[n];
        if (base ~ /^fields\.csv\.gz$/) kind = "fields.csv.gz";
        else if (base ~ /^fields\.csv$/) kind = "fields.csv";
        else if (base ~ /^bounces\.csv$/) kind = "bounces.csv";
        else if (base ~ /^quanta_samples/) kind = "quanta_samples.csv";
        else if (base ~ /^quanta_bounces/) kind = "quanta_bounces.csv";
        else if (base ~ /^quanta_histories/) kind = "quanta_histories.csv";
        else if (base ~ /^histories\.csv$/) kind = "histories.csv";
        else if (base ~ /^data\.csv$/) kind = "data.csv";
        else if (base ~ /\.pdf$/) kind = "pdf";
        else if (base ~ /\.png$/) kind = "png";
        else if (base ~ /\.csv$/) kind = "other csv";
        else kind = "other";
        key = root "," kind; files[key]++; bytes[key] += size;
    }
    END { for (k in files) print k "," files[k] "," bytes[k] }' | sort
    ;;
*)
    echo "usage: histories | tables | files | outputs" >&2
    exit 2
    ;;
esac
