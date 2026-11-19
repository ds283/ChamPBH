# power-law-wall prompt 04 — the sample's scripts

Prompt 04 ("measure a sample", [`../../04-measure-a-sample.md`](../../04-measure-a-sample.md)) runs U7's forty histories
by the tool route under both drain laws, beside the exponential's counterparts from the 2026.11.0 export, and the
surfing scans at M = 10⁻³ M_P. Nothing here changes a production module; nothing starts Ray, opens a store or registers a
run. Every script runs from the repository root, with `PYTHONPATH=.` and `./venv/bin/python`. The outputs are under
[`.documents/power-law-wall/sample-04/`](../../../../.documents/power-law-wall/sample-04/); what they say is in
`.documents/power-law-wall-verification.md` §4.

| script | what it does | what it produced |
|---|---|---|
| `sample_run.py BETA M [tool options]` | one history through `tools/history_and_bbn.py`'s own `main`, in-process, with `integrate_scalar_history` and the tool's `tune_history` name wrapped for the call to keep the final `IntegrationResult` and the payload (P24); prints the tool's lines unchanged, then `sample …` lines and the per-candidate `cand …` table (prompt §1.3; the docstring defines each field) | the body of each `runs/*.txt` |
| `runs.sh [NAME …]` | the forty, one at a time, through `sample_run.py`, each into `runs/<name>.txt` with the command and `uptime` at its start and the exit status, the wall time and `uptime` at its end; `runs/runs.log` is its own record | `runs/` (40 files and `runs.log`), 2026-11-15 19:33–19:47 |
| `tool_route.sh` | the exponential by the plain tool at three counterparts (laboratory M, β 2, A 1; 10⁻¹⁵ M_P, β 2, A 1; 10⁻³ M_P, β 2, natural) | `tool-route/exp-*.txt`, 19:31–19:33 |
| `tables.py extract HISTORIES_CSV` | checks the copy's SHA-256 against P25's and writes the twenty counterparts verbatim | `exponential_counterparts.csv` |
| `tables.py compare A B` | the tool lines of two outputs, identical or not with the timings (`wall=`, `PRyM_time=`) and the `#`, `sample` and `cand` lines masked (note 04, ruling 3) | the runner check (log §3) |
| `tables.py overlaps` | `compare` on the fourteen histories 01 and 03 ran before (P26) | `checks.txt` |
| `tables.py export-check FILE …` | each exponential tool-route output against its export row at the tool's printed digits | `tool-route/export_check.txt` |
| `tables.py markdown` | the readable tables of verification §4.1–§4.2, from `summary.csv` and `comparison.csv` | verification §4 |
| `tables.py tables` | `summary.csv` (one row per history), `comparison.csv` (n = 1, n = 2 and the exponential per (M, β, law)), `thresholds.txt` (prompt §1.4, with the M_crit estimate), all from `runs/` and the extract | those three files |
| `scans.sh NAME [B0 B1]` | one surfing scan by `tools/surfing_scan.py` at M = 10⁻³ M_P, φ* = 5 M_P, the tool's default tuning: `c_n1`, `c_n2`, `c_n1_ext` (3.55–5.00 into `c_n1.csv`), `q_exp`, `f_new_check`, and `q_n<N>`, `e_n<N>` with the ends `edges.py grids` reads from C | `surfing/*.csv`, their `.meta.json`, `*.console.txt`, 2026-11-15 19:47 to 2026-11-16 00:48 |
| `edges.py grids C_CSV` | the fine grids of prompt §1.5 read from a coarse scan's `capture_kick`, and the stops | the grids (log §3) |
| `edges.py fnew` | the three exponential rows of `surfing/f_new_check.csv` against electroweak-eos 02's `f_new.csv`, every column but `wall_s` | `checks.txt` §5 |
| `edges.py report` | `surfing/edges.txt`: every scan's rows, failures, edges and cost, beside the exponential's | `surfing/edges.txt` |

The figures of the scans are drawn by `tools/plot_surfing_temperature.py --csv surfing/<scan>.csv --out
surfing/surfing_temperature_<scan>`.
