import json
import argparse
import os
import subprocess
import random
import time
import re
import threading
from collections import deque, defaultdict, Counter
from pathlib import Path

DIR = "/tmp/test"
POLICY = os.path.abspath("policy.cf")
FILES_PATH = "input.json"
RESULT_PATH = "result.json"
RESULT_PATH = "result.json"

WATCHER_RE = re.compile(r"verbose: cf-reactor event watcher has (\d+) watcher\(s\)")
OBSERVED_RE = re.compile(r"verbose: Observed (.+?) of file '([^']+)'")

CLK_TCK = os.sysconf("SC_CLK_TCK")

SIZE_THRESHOLD = 10  # bytes, for the size_exceeds event

# cf-reactor reports its watchers before their first check, which only records
# the state of the file and can't fire. Changes before it would be missed.
FIRST_CHECK_S = 2

# ============ file changes ============

def touch(path):
    Path(path).touch()


def write_content(path):
    # written next to it and moved in place, so cf-reactor never hashes the
    # emptied file before the write, which would be two content changes
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        f.write(f"{time.time_ns()}\n")
    os.replace(tmp, path)


# Cycles of 3 values, not toggles between 2: two changes between two checks of
# cf-reactor would cancel out, and the second one would look like a missed event.
MODES = (0o644, 0o640, 0o600)
UIDS = (0, 1, 2)


def next_in_cycle(cycle, current):
    return cycle[(cycle.index(current) + 1) % len(cycle)] if current in cycle else cycle[0]


def change_permissions(path):
    os.chmod(path, next_in_cycle(MODES, os.lstat(path).st_mode & 0o777))


def change_owner(path):
    os.lchown(path, next_in_cycle(UIDS, os.lstat(path).st_uid), -1)


def exceed_size(path):
    with open(path, "w") as f:
        f.write("x" * (SIZE_THRESHOLD + 1))


def shrink_size(path):
    os.truncate(path, 0)


# event type -> (fire, reset). fire makes the change cf-reactor should detect.
# Some changes can't be repeated (a file can't be deleted twice), so every other
# turn of such a file runs reset instead, which undoes the change and is not an
# event (e.g. a deleted file is recreated).
EVENTS = {
    "deleted": (os.unlink, touch),
    "created": (touch, os.unlink),
    "modified": (touch, None),
    "content_modified": (write_content, None),
    "permissions_changed": (change_permissions, None),
    "owner_changed": (change_owner, None),
    "size_exceeds": (exceed_size, shrink_size),
}


def setup_file(path, event):
    """Reset the file left by a previous run to the start state of the event."""
    Path(path).unlink(missing_ok=True)
    if event != "created":
        touch(path)
        os.chmod(path, 0o644)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("num_watchers", type=int)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--interval", type=int, default=1)
    parser.add_argument("--event", choices=EVENTS, default="modified")

    return parser.parse_args()

# ============ ressources ============

def reactor_pid(root):
    """Top-most cf-reactor below our sudo (cf-reactor forks children too)."""
    children = defaultdict(list)
    for d in os.listdir("/proc"):
        if d.isdigit():
            try:
                with open(f"/proc/{d}/stat") as f:
                    children[int(f.read().rsplit(")", 1)[1].split()[1])].append(int(d))
            except (OSError, ValueError):
                pass
    queue = deque([root])
    while queue:
        pid = queue.popleft()
        try:
            with open(f"/proc/{pid}/comm") as f:
                if f.read().strip() == "cf-reactor":
                    return pid
        except OSError:
            pass
        queue.extend(children[pid])

def cpu_seconds(pid):
    """Exact on-CPU time of all threads of the process, in seconds (ns precision)."""
    total_ns = 0
    for tid in os.listdir(f"/proc/{pid}/task"):
        try:
            with open(f"/proc/{pid}/task/{tid}/schedstat") as f:
                total_ns += int(f.read().split()[0])
        except FileNotFoundError:
            pass  # thread exited in the meantime
    return total_ns / 1e9
 
 
def memory_kb(pid):
    """VmRSS = current resident memory, VmHWM = peak resident memory."""
    mem = {}
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith(("VmRSS:", "VmHWM:")):
                key, value = line.split()[:2]
                mem[key.rstrip(":")] = int(value)
    return mem

# ============ statistics ============
 
def percentile(sorted_vals, p):
    if not sorted_vals:
        return None
    k = (len(sorted_vals) - 1) * p / 100.0
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)
 
 
def mean(vals):
    return sum(vals) / len(vals) if vals else None
 
 
def fmt(x, unit="s"):
    return "-" if x is None else f"{x:.3f}{unit}"
 
 
def match(files, events, detections, undone):
    """
    One detection covers every change of that file since its previous detection
    (a poll only sees that something changed). Latency is measured from the first
    covered change; extra covered changes are 'coalesced', uncovered ones are missed.

    If the changes are undone between two events (undone=True, e.g. a deleted file
    is recreated), a detection only covers the last change: the previous ones were
    undone before a poll could see them, so they are missed.
    """
    ev_by_file = defaultdict(list)
    det_by_file = defaultdict(list)
    for t, path in events:
        ev_by_file[path].append(t)
    for t, _, path in detections:
        det_by_file[path].append(t)
 
    latencies = []  # (event time, latency)
    caught = coalesced = spurious = 0
    for path in files:
        evs, i = ev_by_file[path], 0
        for d in sorted(det_by_file[path]):
            covered = []
            while i < len(evs) and evs[i] <= d:
                covered.append(evs[i])
                i += 1
            if not covered:
                spurious += 1
                continue
            if undone:
                covered = covered[-1:]
            latencies.append((covered[0], d - covered[0]))
            caught += len(covered)
            coalesced += len(covered) - 1
    return latencies, caught, coalesced, spurious


def print_stats(args, files, events, detections, storm_s, cpu_s, mem, total_s):
    undone = EVENTS[args.event][1] is not None
    latencies, caught, coalesced, spurious = match(files, events, detections, undone)
    n = len(events)
    missed = n - caught
    lat = sorted(l for _, l in latencies)
 
    # latency trend: if the second half is clearly slower, cf-reactor is falling behind
    by_time = [l for _, l in sorted(latencies)]
    half = len(by_time) // 2
    first_half, second_half = mean(by_time[:half]), mean(by_time[half:])
 
    def pct(x):
        return f"{100.0 * x / n:.1f}%" if n else "-"
 
    kinds = ", ".join(f"{k}: {v}" for k, v in Counter(k for _, k, _ in detections).items())
 
    print()
    print("==================== Statistics ====================")
    print(f"Event type:          file_{args.event}")
    print(f"Watchers:            {len(files)}")
    print(f"Per-file interval:   {args.interval}s")
    print(f"Storm duration:      {storm_s:.1f}s")
    print(f"Total duration:      {total_s:.1f}s")
    print(f"Events fired:        {n} ({n / storm_s:.1f}/s)")
    print(f"Detections:          {len(detections)} ({kinds or 'none'})")
    print(f"Events caught:       {caught} ({pct(caught)})")
    print(f"  coalesced:         {coalesced} ({pct(coalesced)})")
    print(f"Events missed:       {missed} ({pct(missed)})")
    print(f"Spurious detections: {spurious}")
    print(f"Latency avg:         {fmt(mean(lat))}")
    print(f"Latency p50/p95/p99: {fmt(percentile(lat, 50))} / {fmt(percentile(lat, 95))} / "
          f"{fmt(percentile(lat, 99))}")
    print(f"Latency min/max:     {fmt(lat[0] if lat else None)} / {fmt(lat[-1] if lat else None)}")
    print(f"Latency trend:       1st half avg {fmt(first_half)} -> 2nd half avg {fmt(second_half)}")
    if cpu_s is not None:
        print(f"Reactor CPU:         {cpu_s:.2f}s CPU time = {100.0 * cpu_s / storm_s:.1f}% "
              f"of one core (avg)")
    if mem:
        print(f"Reactor memory:      {mem.get('VmRSS', 0) / 1024:.1f} MB now, "
              f"{mem.get('VmHWM', 0) / 1024:.1f} MB peak")
    print("====================================================")
 
    with open(RESULT_PATH, "w") as f:
        json.dump({"event": args.event, "events": events, "detections": detections}, f)
    print(f"raw events and detections in {RESULT_PATH}")

# ============ main ============

def main():

    args = parse_args()
    if args.event == "owner_changed" and os.geteuid() != 0:
        print("file_owner_changed needs root to change the owner of the files")
        return
    fire, reset = EVENTS[args.event]

    os.makedirs(DIR, exist_ok=True)
    files = [os.path.join(DIR, f"file-{i}.json") for i in range(args.num_watchers)]

    with open(FILES_PATH, "w") as f:
        json.dump({
            "event": f"file_{args.event}",
            "size_threshold": SIZE_THRESHOLD,
            "files": files,
        }, f)

    for file in files:
        setup_file(file, args.event)

    # run in the background
    proc = subprocess.Popen(
        [
            "sudo",
            "/var/cfengine/bin/cf-reactor",
            "--verbose",
            "--timestamp",
            "--no-fork",
            "-f",
            POLICY,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    ready = threading.Event()
    detections = []  # (time seen, kind, path)

    def reader():
        assert proc.stdout is not None
        for line in proc.stdout:
            if m := OBSERVED_RE.search(line):
                detections.append((time.time(), m.group(1), m.group(2)))
            elif WATCHER_RE.search(line):
                ready.set()

    threading.Thread(target=reader, daemon=True).start()

    # wait for the daemon to have read the policy
    if not ready.wait(timeout=30):
        print("cf-reactor never reported its watchers")
        proc.terminate()
        return
    time.sleep(FIRST_CHECK_S)

    pid = reactor_pid(proc.pid)
    if pid is None:
        print("cf-reactor process not found")
        proc.terminate()
        return
    cpu_start = cpu_seconds(pid)
    storm_start = time.monotonic()

    print("=============== Start the event storm ==============")
    print(f"cf-reactor pid {pid}, {len(os.listdir(f'/proc/{pid}/task'))} threads")

    events = []
    needs_reset = set()
    available = list(files)
    cooling = deque()  # (ready_time, path); FIFO because every file waits the same interval
    end = time.monotonic() + args.timeout
    while time.monotonic() < end:
        if proc.poll() is not None:
            print("cf-reactor exited during the storm!")
            break

        now = time.time()
        while cooling and cooling[0][0] <= now:
            available.append(cooling.popleft()[1])
        if not available:
            # every file is waiting: sleep until the next one is ready instead of spinning
            time.sleep(max(0.0, min(cooling[0][0] - now, end - time.monotonic())))
            continue
        j = random.randrange(len(available))
        available[j], available[-1] = available[-1], available[j]
        path = available.pop()
        now = time.time()
        if path in needs_reset:
            needs_reset.remove(path)
            reset(path)
        else:
            events.append((now, path))
            fire(path)
            if reset is not None:
                needs_reset.add(path)
        cooling.append((now + args.interval, path))

    try:
        mem = memory_kb(pid)
        cpu_s = cpu_seconds(pid) - cpu_start
    except FileNotFoundError:  # cf-reactor exited during the storm
        mem, cpu_s = None, None
    storm_s = time.monotonic() - storm_start
    proc.terminate()
    proc.wait()
    total_s = time.monotonic() - storm_start

    print_stats(args, files, events, detections, storm_s, cpu_s, mem, total_s)


if __name__ == "__main__":
    main()
