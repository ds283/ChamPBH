"""
Run the points of `points.py` as a detached, resumable batch (inelastic-reflection prompt 11;
README §8.1 R29, §8.2 P41).

    PYTHONPATH=. ./venv/bin/python .documents/inelastic-reflection/measure/run_grid.py [--jobs N] [--only SLUG ...] [--point-timeout SECS]

From the repository root. Detached, as prompt 11 §1.3 launches it:

    nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u .documents/inelastic-reflection/measure/run_grid.py > var/inelastic-reflection-11/grid/run_grid.out 2>&1' > /dev/null 2>&1 &

What it does:
- runs each point as its own process, from the repository root with PYTHONPATH=., at most N at a
  time (default 3), in the order of `points.POINTS` (or of --only);
- writes each point's stdout and stderr to var/inelastic-reflection-11/grid/<slug>.part, after a
  header (the command, `git rev-parse HEAD`, the start in UTC, `uptime`) and before a footer (the
  end in UTC, `uptime`, the exit status, the wall time), and renames it <slug>.txt when the process
  ends;
- skips a point whose <slug>.txt exists, and deletes a stale <slug>.part and runs the point again;
- never retries a point that ended with a non-zero status: that status is the point's row;
- stops a point that runs longer than --point-timeout seconds (default 900, prompt 11 §5's 15
  minutes) with SIGTERM, then SIGKILL after 10 s, and records it in its footer and in the log as
  `timed out`; it is not retried;
- writes its own pid to var/inelastic-reflection-11/grid/run_grid.pid, one line per point started,
  skipped and ended to var/inelastic-reflection-11/grid/run_grid.log, and `** grid done` as that
  log's last line (and its stdout's).

It writes only under var/inelastic-reflection-11/grid/.
"""

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from points import BY_SLUG, POINTS, command_line  # noqa: E402

GRID = Path("var/inelastic-reflection-11/grid")


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _uptime() -> str:
    return subprocess.run(["uptime"], capture_output=True, text=True).stdout.strip()


def _git_head() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True
    ).stdout.strip()


def _log(line: str) -> None:
    text = f"[{_utc()}] {line}"
    with open(GRID / "run_grid.log", "a") as f:
        f.write(text + "\n")
    print(text, flush=True)


class _Running:
    def __init__(self, point, proc, handle, start, part):
        self.point = point
        self.proc = proc
        self.handle = handle
        self.start = start
        self.part = part
        self.timed_out = False
        self.term_sent = None


def _start(point, head: str) -> _Running:
    part = GRID / f"{point.slug}.part"
    argv = command_line(point)
    handle = open(part, "w")
    handle.write(f"## command: PYTHONPATH=. {' '.join(argv)}\n")
    handle.write(f"## git rev-parse HEAD: {head}\n")
    handle.write(f"## start (UTC): {_utc()}\n")
    handle.write(f"## uptime: {_uptime()}\n")
    handle.flush()
    env = dict(os.environ, PYTHONPATH=".")
    start = time.monotonic()
    proc = subprocess.Popen(argv, stdout=handle, stderr=subprocess.STDOUT, env=env)
    _log(f"started {point.slug} pid={proc.pid}")
    return _Running(point, proc, handle, start, part)


def _finish(r: _Running) -> None:
    status = r.proc.returncode
    wall = time.monotonic() - r.start
    r.handle.flush()
    r.handle.write(f"## end (UTC): {_utc()}\n")
    r.handle.write(f"## uptime: {_uptime()}\n")
    if r.timed_out:
        r.handle.write("## timed out: stopped by run_grid.py after --point-timeout\n")
    r.handle.write(f"## exit status: {status}\n")
    r.handle.write(f"## wall: {wall:.1f} s\n")
    r.handle.close()
    r.part.rename(GRID / f"{r.point.slug}.txt")
    note = " timed out" if r.timed_out else ""
    _log(f"ended {r.point.slug} status={status} wall={wall:.1f} s{note}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--jobs", type=int, default=3)
    parser.add_argument("--only", nargs="+", default=None, metavar="SLUG")
    parser.add_argument("--point-timeout", type=float, default=900.0, metavar="SECS")
    args = parser.parse_args(argv)

    if not Path("PRyMrates").is_dir():
        print("!! run_grid: run from the repository root", file=sys.stderr)
        return 2
    if args.only is not None:
        unknown = [s for s in args.only if s not in BY_SLUG]
        if unknown:
            print(f"!! run_grid: unknown slug(s): {' '.join(unknown)}", file=sys.stderr)
            return 2
        points = [BY_SLUG[s] for s in args.only]
    else:
        points = list(POINTS)

    GRID.mkdir(parents=True, exist_ok=True)
    (GRID / "run_grid.pid").write_text(f"{os.getpid()}\n")
    head = _git_head()
    _log(
        f"run_grid pid={os.getpid()} jobs={args.jobs} points={len(points)} "
        f"head={head} uptime: {_uptime()}"
    )

    queue = []
    for p in points:
        txt = GRID / f"{p.slug}.txt"
        part = GRID / f"{p.slug}.part"
        if txt.exists():
            _log(f"skipped {p.slug}: {txt.name} exists")
            continue
        if part.exists():
            part.unlink()
            _log(f"deleted stale {part.name}; {p.slug} runs again")
        queue.append(p)

    running = []
    while queue or running:
        while queue and len(running) < args.jobs:
            running.append(_start(queue.pop(0), head))
        time.sleep(1.0)
        still = []
        for r in running:
            if r.proc.poll() is not None:
                _finish(r)
                continue
            elapsed = time.monotonic() - r.start
            if not r.timed_out and elapsed > args.point_timeout:
                r.timed_out = True
                r.term_sent = time.monotonic()
                r.proc.send_signal(signal.SIGTERM)
                _log(f"timeout {r.point.slug}: SIGTERM after {elapsed:.0f} s")
            elif r.timed_out and time.monotonic() - r.term_sent > 10.0:
                r.proc.kill()
                r.term_sent = time.monotonic()
                _log(f"timeout {r.point.slug}: SIGKILL")
            still.append(r)
        running = still

    _log(f"uptime: {_uptime()}")
    _log("** grid done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
