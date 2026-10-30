# Testing cf-reactor

`sudo python3 main.py 100 --interval 1 --timeout 30`

```
=============== Start the event storm ==============
cf-reactor pid 41980, 7 threads

==================== Statistics ====================
Watchers:            100
Per-file interval:   1s
Storm duration:      30.0s
Total duration:      30.0s
Events fired:        3000 (100.0/s)
Detections:          2900 (modification: 2900)
Events caught:       3000 (100.0%)
  coalesced:         134 (4.5%)
Events missed:       0 (0.0%)
Spurious detections: 34
Latency avg:         0.530s
Latency p50/p95/p99: 0.995s / 1.000s / 1.001s
Latency min/max:     0.000s / 1.002s
Latency trend:       1st half avg 0.970s -> 2nd half avg 0.090s
Reactor CPU:         13.83s CPU time = 46.1% of one core (avg)
Reactor memory:      16.5 MB now, 16.5 MB peak
====================================================
raw events and detections in result.json
```

