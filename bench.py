"""
Find, for each event type, how many events per second cf-reactor can handle.

Runs main.py with more and more watchers (twice as many each time), so more
and more events per second, until cf-reactor lags behind: it is still handling
events more than --tolerance seconds after the end of the storm, i.e. the
running time is longer than the given time. Then bisects between the last load
it kept up with and the first one it lagged behind with.

sudo python3 bench.py --timeout 30
"""
import argparse
import json
import os
import subprocess
import sys

from main import EVENTS, JITTER, RESULT_PATH, event_name

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "bench-results")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--events", nargs="+", choices=EVENTS, default=list(EVENTS))
    parser.add_argument("--timeout", type=int, default=30, help="storm duration of each run")
    parser.add_argument("--interval", type=int, default=1)
    parser.add_argument("--start", type=int, default=100, help="watchers of the first run")
    parser.add_argument("--max-watchers", type=int, default=102400)
    parser.add_argument("--refine", type=int, default=3,
                        help="bisection runs between the last load kept up with and the "
                        "first lagging one")
    parser.add_argument("--tolerance", type=float, default=2,
                        help="seconds cf-reactor may still handle events after the storm "
                        "without lagging behind: a check interval (1s) and a margin")
    parser.add_argument("--max-wait", type=float,
                        help="seconds main.py waits at most for a lagging cf-reactor "
                        "(default: --timeout)")
    args = parser.parse_args()
    if args.max_wait is None:
        args.max_wait = args.timeout
    return args


def run(args, event, watchers):
    """Runs main.py and returns the summary of its result. Exits if it failed."""
    name = f"{event}-{watchers}"
    result_path = os.path.join(HERE, RESULT_PATH)
    if os.path.exists(result_path):
        os.remove(result_path)
    cmd = [sys.executable, "main.py", str(watchers), "--event", event,
           "--interval", str(args.interval), "--timeout", str(args.timeout),
           "--max-wait", str(args.max_wait)]
    log_path = os.path.join(OUT_DIR, f"{name}.log")
    with open(log_path, "w") as log:
        proc = subprocess.run(cmd, cwd=HERE, stdout=log, stderr=subprocess.STDOUT)
    if proc.returncode != 0 or not os.path.exists(result_path):
        sys.exit(f"main.py failed with {watchers} watchers for {event_name(event)}, see {log_path}")
    os.replace(result_path, os.path.join(OUT_DIR, f"{name}.json"))
    with open(os.path.join(OUT_DIR, f"{name}.json")) as f:
        return json.load(f)["summary"]


def lagging(args, s):
    return (s["lag_s"] is None or s["lag_s"] > args.tolerance
            or s["wait_end"].startswith("stopped"))


def harness_too_slow(args, event, s):
    """Whether main.py fired clearly fewer events than asked for, the limit is then
    main.py, not cf-reactor"""
    undone = EVENTS[event][1] is not None
    asked = s["watchers"] / (args.interval * (1 + JITTER / 2)) / (2 if undone else 1)
    return s["fired_per_s"] < 0.9 * asked


def fmt(x, unit=""):
    return "-" if x is None else f"{x:.1f}{unit}"


def report(event, s, lags):
    verdict = (f"LAGGING, handles at most ~{fmt(s['handled_per_s'])}/s" if lags else "ok")
    print(f"  {event:<20} {s['watchers']:>7} watchers  fired {fmt(s['fired_per_s']):>8}/s  "
          f"handled {s['handled']:>7} of {s['detections']:<7}  "
          f"running {fmt(s['running_s'])}s for {s['timeout']}s given  {verdict}", flush=True)


def bench(args, event):
    """Returns (summary of the highest load kept up with, of the lowest lagging one, note)"""
    ok = bad = None
    note = ""
    n = args.start
    while n <= args.max_watchers:
        s = run(args, event, n)
        lags = lagging(args, s)
        report(event, s, lags)
        if lags:
            bad = s
            break
        ok = s
        if harness_too_slow(args, event, s):
            return ok, bad, "main.py can't fire events faster, cf-reactor never lagged"
        n *= 2
    else:
        note = f"never lagged up to {args.max_watchers} watchers"

    for _ in range(args.refine):
        if bad is None:
            break
        lo = ok["watchers"] if ok else 0
        mid = (lo + bad["watchers"]) // 2
        if mid <= lo:
            break
        s = run(args, event, mid)
        lags = lagging(args, s)
        report(event, s, lags)
        if lags:
            bad = s
        else:
            ok = s
    return ok, bad, note


def main():
    args = parse_args()
    events = args.events
    if "owner_changed" in events and os.geteuid() != 0:
        print("Skipping owner_changed, it needs root")
        events = [e for e in events if e != "owner_changed"]
    if os.geteuid() != 0:
        events = [e for e in events if not e.startswith(("user_", "package_"))]
    os.makedirs(OUT_DIR, exist_ok=True)

    results = {}
    for event in events:
        print(f"=== {event_name(event)}")
        results[event] = bench(args, event)

    print()
    print("========================== Events per second ==========================")
    print(f"{'event':<26} {'keeps up with':>14} {'lags behind at':>15} {'handles at most':>16}")
    for event, (ok, bad, note) in results.items():
        print(f"{event_name(event):<26} {fmt(ok and ok['fired_per_s']):>14} "
              f"{fmt(bad and bad['fired_per_s']):>15} {fmt(bad and bad['handled_per_s']):>16}"
              + (f"  ({note})" if note else ""))
    print("=======================================================================")
    print(f"Lagging behind: still handling events more than {args.tolerance:g}s after a "
          f"{args.timeout}s storm.")

    with open(os.path.join(OUT_DIR, "bench.json"), "w") as f:
        json.dump({"args": vars(args), "results": {
            e: {"keeps_up": ok, "lags": bad, "note": note}
            for e, (ok, bad, note) in results.items()}}, f, indent=2)
    print(f"runs in {OUT_DIR}/, results in {OUT_DIR}/bench.json")


if __name__ == "__main__":
    main()
