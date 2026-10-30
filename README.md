# Testing cf-reactor

`sudo python3 main.py 100 --interval 1 --timeout 30`

`--event` picks the file event to test (default `modified`):

| `--event`             | change made to the files                   |
|-----------------------|--------------------------------------------|
| `modified`            | touch                                      |
| `content_modified`    | write new content                          |
| `created`             | create (*)                                 |
| `deleted`             | delete (*)                                 |
| `permissions_changed` | chmod, cycling 644 -> 640 -> 600           |
| `owner_changed`       | chown, cycling uid 0 -> 1 -> 2 (needs root) |
| `size_exceeds`        | grow above 10 bytes (*)                    |

(*) can't be repeated, so every other turn of a file undoes it instead (e.g.
recreates the deleted file), which is not counted as an event. These fire half
as many events, and a change undone before cf-reactor checked the file is
missed, so use an `--interval` longer than the poll interval of cf-reactor.

After the storm, the script waits for cf-reactor to detect the last change of
every file and to run the then bundle of every detection, so a slow cf-reactor
isn't stopped with events still queued. It gives up once cf-reactor printed
nothing for `--patience` seconds (default 5), e.g. when the last changes were
missed.

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

