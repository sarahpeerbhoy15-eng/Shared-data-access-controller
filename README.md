# Shared Data Access Controller

A reader–writer simulation in C (POSIX threads + semaphores) that **records every
read and write request** and shows how shared data access can be coordinated
without conflicting operations.

**Course:** Processor Architecture & Operating Systems (PAOS) — Project 64
**Author:** Sarah Peerbhoy (Div A, Roll No 64, SRN 31252273)

## Features
- Readers share the data; writers get exclusive access (semaphores)
- Every request logged: `REQUEST → GRANTED → DONE` with millisecond timestamps (`access.log`)
- Three modes: `sync`, `fair` (starvation-free), `nosync` (shows the conflict)
- Built-in **conflict detector** and **statistics summary** with PASS/FAIL
- Configurable number of readers and writers

## Build and run (Ubuntu)
```bash
make                          # compile
./controller sync             # readers share, writers exclusive
./controller fair 6 3         # 6 readers, 3 writers, no writer starvation
./controller nosync           # no coordination -> conflicts
```
Usage: `./controller [sync|fair|nosync] [readers] [writers]`

## How it works
| Semaphore | Purpose |
|-----------|---------|
| `wrt` | Exclusive access: first reader locks it, last reader releases it; writers hold it while writing |
| `mutex` | Protects the `read_count` variable |
| `turnstile` | `fair` mode only: a waiting writer blocks *new* readers so it can't starve |

## Results
| Mode | Readers/Writers | Final data | Conflicts | Result |
|------|----------------|-----------|-----------|--------|
| nosync | 3 / 2 | 110 (expected 120) | 4 | FAIL — lost update |
| sync | 3 / 2 | 120 | 0 | PASS |
| fair | 6 / 3 | 130 | 0 | PASS |

Fairness comparison with 6 readers and 3 writers:

| Mode | Avg reader wait | Avg writer wait | Total time |
|------|----------------|-----------------|-----------|
| sync | 133 ms | 734 ms | 2304 ms |
| fair | 1049 ms | 700 ms | 3002 ms |

`fair` mode gives writers better service at the cost of reader throughput.
Full logs: `sample_output_sync.txt`, `sample_output_nosync.txt`, `sample_output_fair.txt`.

## Development history
Built in five incremental steps — see the Git commit history.
