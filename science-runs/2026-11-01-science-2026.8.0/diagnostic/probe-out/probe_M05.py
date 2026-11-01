import subprocess, time, os, sys
from concurrent.futures import ThreadPoolExecutor
OUT = "var/science-2026.8.0/diagnostic/probe"
def run(beta):
    slug = f"dis-M0.5-b{beta}-a0.08"
    cmd = ["./venv/bin/python", "-u", "tools/history_and_bbn.py", beta, "0.5", "--alpha", "0.08"]
    t0 = time.time()
    with open(f"{OUT}/{slug}.txt", "w") as f:
        f.write(f"## command: {' '.join(cmd)}\n## git: {subprocess.run(['git','rev-parse','HEAD'],capture_output=True,text=True).stdout.strip()}\n"); f.flush()
        try:
            p = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, env={**os.environ, "PYTHONPATH": "."}, timeout=900)
            status = str(p.returncode)
        except subprocess.TimeoutExpired:
            status = "timed out (900 s)"
        f.write(f"\n## exit status: {status}\n## wall: {time.time()-t0:.1f} s\n")
    return slug, status, time.time() - t0
with ThreadPoolExecutor(3) as ex:
    for slug, status, wall in ex.map(run, ["1.2", "2", "2.9"]):
        print(slug, status, f"{wall:.0f} s", flush=True)
