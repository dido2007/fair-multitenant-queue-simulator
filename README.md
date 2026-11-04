# Tiny multi-tenant queue experiment

**Status: built and checked on 4 November 2026.** This is an independent,
deterministic simulation using synthetic data. It does not run Hatchet code or
measure Hatchet's production scheduler.

## Why this experiment

Hatchet's [engineering post on fair multi-tenant queues](https://hatchet.run/blog/multi-tenant-queues)
uses a bulk uploader and a small file request to explain why FIFO can be unfair.
It describes a deterministic round-robin approach in a Postgres-backed queue.
Their [September 2026 concurrency announcement](https://hatchet.run/announcement/new-concurrency-features)
also discusses per-key limits. This script asks a narrower question: if jobs
have very different durations, how does fairness by task count compare with
fairness by allocated service time?

## Run

```sh
python3 fair_queue.py
```

Python standard library only. No account, network request, or real customer data.

## Workload and policies

- 3 identical workers, non-preemptive tasks, abstract time units.
- At time 0, a bulk tenant submits 72 jobs of duration 4, 6, or 8.
- At time 3, a small tenant submits 8 jobs of duration 1.
- At time 6, a medium tenant submits 6 jobs of duration 2, 3, or 4.
- FIFO selects the oldest queued job globally.
- Round robin selects one job per active tenant in rotation.
- Service-time policy selects the tenant with the least allocated processing
  time. A newly active tenant starts at the current minimum allocation.

## Output

| Policy | Small tenant p95 queue wait | Small tenant p95 completion | Bulk max queue wait | Makespan |
|---|---:|---:|---:|---:|
| FIFO | 142 | 143 | 140 | 154 |
| Round robin | 25 | 26 | 148 | 156 |
| Service time | 10 | 11 | 147 | 155 |

The scheduler reduces the small tenant's delay by moving some bulk jobs later.
All policies finish all 86 jobs. The service-time policy is an *oracle*: it
knows each task's true duration before dispatch. Real schedulers usually do
not. The next useful experiment would replace that oracle with noisy runtime
predictions and measure p95 wait, longest wait, and idle worker time across
different tenant arrival patterns.

These numbers describe only this one hand-built scenario. The simulator does
not model database locks, polling, priorities, failures, retries, concurrency
keys, or the cost of estimating runtimes.
