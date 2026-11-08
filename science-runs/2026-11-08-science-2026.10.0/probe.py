"""
The probe of the 2026.10.0 science run's plan (PLAN.md §2): 31 tuned histories with BBN through
tools/history_and_bbn.py, with no Ray and no store.

    PYTHONPATH=. ./venv/bin/python -u science-runs/plan-science-2026.10.0/probe.py [--jobs N] [--only SLUG ...] [--point-timeout SECS] [--list]

From the repository root. A copy of the 2026.8.0 probe's runner, with its own points:
- each point runs as its own process, at most N at a time (default 3), in the order of POINTS (the
  dearest first: set C, then G, then L from the smallest A, then F);
- its stdout and stderr go to var/science-2026.10.0/probe/<slug>.part after a header (the command,
  `git rev-parse HEAD`, `git status --porcelain` line count, the start in UTC, `uptime`) and before
  a footer (the end in UTC, `uptime`, the exit status, the wall time); the file is renamed
  <slug>.txt when the process ends;
- a point whose <slug>.txt exists is skipped; a stale <slug>.part is deleted and the point run again;
- a point that ends non-zero is not retried: that status is its result;
- a point running longer than --point-timeout seconds (default 1800) is sent SIGTERM, then SIGKILL
  after 10 s, and recorded as `timed out`; it is not retried;
- one line per point started, skipped and ended goes to var/science-2026.10.0/probe/probe.log, and
  `** probe done` is its last line.

It writes only under var/science-2026.10.0/probe/.
"""

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

OUT = Path("var/science-2026.10.0/probe")


def _point(group, beta, M, law, value):
    tag = "A" if law == "one_loop" else "a"
    return {
        "slug": f"{group}-M{M}-b{beta}-{tag}{value}",
        "group": group,
        "beta": beta,
        "M": M,
        "law": law,
        "value": value,
    }


POINTS = (
    # C: the one-loop law at M_crit, A = 1 (C-3873's kind is dearest)
    [_point("C", "1.1", "1e-12", "one_loop", "1")]
    + [_point("C", b, "1e-15", "one_loop", "1") for b in ("1.1", "2")]
    # G: the fixed law on the ladder, R-ladder's M
    + [_point("G", "2", "1e-16", "fixed", a) for a in ("0.01", "0.1")]
    # L: the one-loop law below 1e-33, the smallest A first
    + [
        _point("L", b, M, "one_loop", A)
        for A in ("0.05", "1", "10")
        for M in ("1e-40", "1e-36")
        for b in ("1.2", "2", "10")
    ]
    # F: the fixed law below 1e-33
    + [
        _point("F", b, M, "fixed", a)
        for M in ("1e-40", "1e-36")
        for b in ("1.2", "2")
        for a in ("0.01", "0.1")
    ]
)
BY_SLUG = {p["slug"]: p for p in POINTS}


def command_line(p):
    cmd = [
        "./venv/bin/python",
        "-u",
        "tools/history_and_bbn.py",
        p["beta"],
        p["M"],
        "--alpha",
        p["value"],
    ]
    if p["law"] == "one_loop":
        cmd += ["--drain-law", "one_loop"]
    return cmd


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sh(cmd):
    return subprocess.run(cmd, capture_output=True, text=True).stdout.strip()


def _log(line):
    with open(OUT / "probe.log", "a") as f:
        f.write(f"{_now()} {line}\n")
    print(line, flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs", type=int, default=3)
    parser.add_argument("--only", nargs="+", default=None)
    parser.add_argument("--point-timeout", type=float, default=1800.0)
    parser.add_argument(
        "--list",
        action="store_true",
        help="print the points and their commands, run nothing",
    )
    args = parser.parse_args()

    if args.list:
        for p in POINTS:
            print(f"{p['slug']:28s} PYTHONPATH=. {' '.join(command_line(p))}")
        print(f"{len(POINTS)} points")
        return 0

    OUT.mkdir(parents=True, exist_ok=True)
    points = [BY_SLUG[s] for s in args.only] if args.only else POINTS

    pending = []
    for p in points:
        if (OUT / f"{p['slug']}.txt").exists():
            _log(f"-- skipped {p['slug']} (done)")
            continue
        part = OUT / f"{p['slug']}.part"
        if part.exists():
            part.unlink()
            _log(f"-- deleted stale {part.name}")
        pending.append(p)

    head = _sh(["git", "rev-parse", "HEAD"])
    dirty = len(_sh(["git", "status", "--porcelain"]).splitlines())
    running = {}

    while pending or running:
        while pending and len(running) < args.jobs:
            p = pending.pop(0)
            part = OUT / f"{p['slug']}.part"
            f = open(part, "w")
            cmd = command_line(p)
            f.write(f"## command: PYTHONPATH=. {' '.join(cmd)}\n")
            f.write(f"## git rev-parse HEAD: {head}\n")
            f.write(f"## git status --porcelain lines: {dirty}\n")
            f.write(f"## start (UTC): {_now()}\n")
            f.write(f"## uptime: {_sh(['uptime'])}\n")
            f.flush()
            proc = subprocess.Popen(
                cmd,
                stdout=f,
                stderr=subprocess.STDOUT,
                env={**os.environ, "PYTHONPATH": "."},
            )
            running[p["slug"]] = (proc, f, time.monotonic(), False)
            _log(f"-- started {p['slug']} (pid {proc.pid})")

        time.sleep(2.0)
        for slug in list(running):
            proc, f, t0, timed_out = running[slug]
            elapsed = time.monotonic() - t0
            if proc.poll() is None:
                if not timed_out and elapsed > args.point_timeout:
                    proc.send_signal(signal.SIGTERM)
                    running[slug] = (proc, f, t0, True)
                    _log(f"-- timed out {slug} after {elapsed:.0f} s; SIGTERM")
                elif timed_out and elapsed > args.point_timeout + 10.0:
                    proc.kill()
                continue
            f.write(f"## end (UTC): {_now()}\n")
            f.write(f"## uptime: {_sh(['uptime'])}\n")
            f.write(
                f"## exit status: {proc.returncode}{' (timed out)' if timed_out else ''}\n"
            )
            f.write(f"## wall: {elapsed:.1f} s\n")
            f.close()
            (OUT / f"{slug}.part").rename(OUT / f"{slug}.txt")
            del running[slug]
            _log(
                f"-- ended {slug}: exit {proc.returncode}, {elapsed:.1f} s"
                + (" (timed out)" if timed_out else "")
            )

    _log("** probe done")


if __name__ == "__main__":
    sys.exit(main())
