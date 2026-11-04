"""Small, deterministic simulator of multi-tenant task scheduling.

This is an independent teaching experiment, not Hatchet's scheduler.
Run: python3 fair_queue.py
"""

from collections import deque
from dataclasses import dataclass
from statistics import mean


@dataclass(frozen=True)
class Task:
    id: int
    tenant: str
    arrival: int
    duration: int


def workload():
    tasks = []
    for i in range(72):
        tasks.append(Task(i, "bulk", 0, (4, 6, 8)[i % 3]))
    for i in range(8):
        tasks.append(Task(72 + i, "small", 3, 1))
    for i in range(6):
        tasks.append(Task(80 + i, "medium", 6, (2, 3, 4)[i % 3]))
    return tasks


def simulate(tasks, policy, workers=3):
    if policy not in {"fifo", "round_robin", "service_time"}:
        raise ValueError(policy)
    pending = sorted(tasks, key=lambda task: (task.arrival, task.id))
    queued = []
    active = []  # (finish time, worker id, task)
    rotation = deque()
    service = {}
    results = []  # (task, start, finish)
    now = 0

    while pending or queued or active:
        next_arrival = pending[0].arrival if pending else float("inf")
        next_finish = min((item[0] for item in active), default=float("inf"))
        if not queued and not active:
            now = max(now, next_arrival)
        elif not queued or len(active) == workers:
            now = max(now, min(next_arrival, next_finish))

        finished = [item for item in active if item[0] <= now]
        active = [item for item in active if item[0] > now]
        for finish, _, task in finished:
            start = finish - task.duration
            results.append((task, start, finish))

        while pending and pending[0].arrival <= now:
            task = pending.pop(0)
            queued.append(task)
            if task.tenant not in service:
                service[task.tenant] = min(service.values(), default=0)
            if task.tenant not in rotation:
                rotation.append(task.tenant)

        free_workers = [w for w in range(workers) if all(a[1] != w for a in active)]
        while queued and free_workers:
            if policy == "fifo":
                task = min(queued, key=lambda item: (item.arrival, item.id))
            elif policy == "round_robin":
                while rotation and not any(q.tenant == rotation[0] for q in queued):
                    rotation.popleft()
                tenant = rotation.popleft()
                task = min((q for q in queued if q.tenant == tenant), key=lambda q: q.id)
                if any(q.tenant == tenant and q.id != task.id for q in queued):
                    rotation.append(tenant)
            else:
                tenant = min(
                    {q.tenant for q in queued},
                    key=lambda key: (service[key], min(q.arrival for q in queued if q.tenant == key), key),
                )
                task = min((q for q in queued if q.tenant == tenant), key=lambda q: q.id)
                service[tenant] += task.duration  # true duration is known in this toy workload
            queued.remove(task)
            worker = free_workers.pop(0)
            active.append((now + task.duration, worker, task))

    return sorted(results, key=lambda row: row[0].id)


def percentile(values, p):
    ordered = sorted(values)
    return ordered[max(0, (len(ordered) * p + 99) // 100 - 1)]


def metrics(results):
    by_tenant = {}
    for task, start, finish in results:
        by_tenant.setdefault(task.tenant, []).append((start - task.arrival, finish - task.arrival))
    return {
        "small_p95_wait": percentile([wait for wait, _ in by_tenant["small"]], 95),
        "small_p95_completion": percentile([latency for _, latency in by_tenant["small"]], 95),
        "bulk_max_wait": max(wait for wait, _ in by_tenant["bulk"]),
        "mean_wait": round(mean(wait for rows in by_tenant.values() for wait, _ in rows), 2),
        "makespan": max(finish for _, _, finish in results),
    }


def main():
    tasks = workload()
    print(f"Synthetic workload: {len(tasks)} tasks, 3 workers; durations in abstract time units")
    print("policy          small p95 wait  small p95 completion  bulk max wait  mean wait  makespan")
    for policy in ("fifo", "round_robin", "service_time"):
        result = simulate(tasks, policy)
        assert len(result) == len(tasks)
        assert sorted(task.id for task, _, _ in result) == list(range(len(tasks)))
        m = metrics(result)
        print(f"{policy:15} {m['small_p95_wait']:14} {m['small_p95_completion']:21} "
              f"{m['bulk_max_wait']:14} {m['mean_wait']:10.2f} {m['makespan']:9}")


if __name__ == "__main__":
    main()
