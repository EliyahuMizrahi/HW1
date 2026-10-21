#!/usr/bin/env python3
import argparse
import copy
import sys
from collections import deque

AGING_INTERVAL = 5   # priority: every 5 time units a process waits, its priority gets 1 better to prevent starvation
BOOST_INTERVAL = 50  # mlfq: every 50 time units everyone goes back to Q0, so nothing starves at the bottom

mlfq_trace = []  # (time, pid, queue, action) 


class SimProcess:
    def __init__(self, pid, arrival_time, burst_time, priority=0, io=0):
        self.pid = pid
        self.arrival_time = arrival_time  
        self.burst_time = burst_time       
        self.remaining_time = burst_time  
        self.priority = priority         
        self.io = io                      
        self.start_time = None            
        self.completion_time = None      


def read_workload(path):
    # one process per line: pid,arrival,burst[,priority[,io]]
    processes = []
    with open(path) as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:  # blank line, like an extra one at the end of the file
                continue
            fields = [x.strip() for x in line.split(",")]
            if fields[0].lower() == "pid":  # header row
                continue
            try:
                if len(fields) < 3 or len(fields) > 5:
                    raise ValueError("expected pid,arrival,burst[,priority[,io]]")
                numbers = [int(x) for x in fields[1:]]  
                proc = SimProcess(fields[0], *numbers)
                if proc.arrival_time < 0 or proc.burst_time <= 0 or proc.io < 0:
                    raise ValueError("arrival and io must be >= 0, burst must be > 0")
                if any(p.pid == proc.pid for p in processes):
                    raise ValueError(f"duplicate pid {proc.pid}")
            except ValueError as e:
                sys.exit(f"simulator: {path}, line {line_no}: {e}")  
            processes.append(proc)
    if not processes:
        sys.exit(f"simulator: {path}: no processes found")
    return processes


def by_arrival(processes):
    return sorted(processes, key=lambda p: p.arrival_time)


def run_non_preemptive(processes, pick):
    # FCFS, SJF and Priority only differ in who they pick next, so they share this loop
    # non-preemptive: once a process gets the CPU it runs its whole burst, nobody interrupts it
    pending = by_arrival(processes)  
    ready = []  
    segments = []
    time = 0
    while pending or ready:
        while pending and pending[0].arrival_time <= time:
            ready.append(pending.pop(0))  
        if not ready:  
            time = pending[0].arrival_time
            continue
        proc = pick(ready, time)
        ready.remove(proc)
        segments.append((proc.pid, time, time + proc.remaining_time))  
        time += proc.remaining_time
        proc.remaining_time = 0
    return segments


def fcfs_schedule(processes, quantum=None):
    # always the front of the line: one long job up front makes everyone behind it wait (convoy effect)
    return run_non_preemptive(processes, lambda ready, time: ready[0])


def sjf_schedule(processes, quantum=None):
    return run_non_preemptive(processes, lambda ready, time: min(ready, key=lambda p: p.remaining_time))


def effective_priority(proc, time):
    # aging: the longer it waits the better its priority gets, so a low priority job can't starve forever
    # it hasn't run yet (non-preemptive), so it's been waiting ever since it arrived
    return proc.priority - (time - proc.arrival_time) // AGING_INTERVAL


def priority_schedule(processes, quantum=None):
    # non-preemptive: a high priority arrival still waits for the current job to finish
    return run_non_preemptive(processes, lambda ready, time: min(ready, key=lambda p: effective_priority(p, time)))


def round_robin_schedule(processes, quantum=None):
    # preemptive: each process gets at most one quantum, then goes to the back of the line
    quantum = quantum or 4
    pending = by_arrival(processes)
    ready = deque()  
    segments = []
    time = 0
    while pending or ready:
        while pending and pending[0].arrival_time <= time:
            ready.append(pending.pop(0))
        if not ready:
            time = pending[0].arrival_time
            continue
        proc = ready.popleft()
        run = min(quantum, proc.remaining_time) 
        segments.append((proc.pid, time, time + run))
        time += run
        proc.remaining_time -= run
        while pending and pending[0].arrival_time <= time:
            ready.append(pending.pop(0))
        if proc.remaining_time > 0:  
            ready.append(proc)
    return segments


def mlfq_schedule(processes, quantum=None):
    # a few RR queues ranked by priority: jobs that keep using their whole slice act CPU-bound and sink
    base = quantum or 4
    quanta = [base, base * 2, base * 4]  # 4, 8, 16 by default
    queues = [deque(), deque(), deque()]  
    pending = by_arrival(processes)
    segments = []
    mlfq_trace.clear() 
    time = 0
    next_boost = BOOST_INTERVAL
    while pending or any(queues):
        while pending and pending[0].arrival_time <= time:
            queues[0].append(pending.pop(0))  

        if time >= next_boost:
            # priority boost: without it, a steady stream of short jobs could keep Q2 waiting forever
            if queues[1] or queues[2]:
                queues[0].extend(queues[1] + queues[2])
                queues[1].clear()
                queues[2].clear()
                mlfq_trace.append((time, "-", "-", "Priority boost -> every process back to Q0"))
            next_boost = (time // BOOST_INTERVAL + 1) * BOOST_INTERVAL

        level = next((i for i, q in enumerate(queues) if q), None)  # highest queue that has someone in it
        if level is None:  # CPU idle: jump ahead to the next arrival
            time = pending[0].arrival_time
            continue

        proc = queues[level].popleft()
        q = quanta[level]
        run = min(q, proc.remaining_time)
        blocks = 0 < proc.io < run  
        if blocks:
            run = proc.io
        end = time + run

        # a new arrival lands in Q0, so it goes before anything running from a lower queue
        preempted_by = None
        if level > 0 and pending and pending[0].arrival_time < end:
            preempted_by = pending[0].pid
            end = pending[0].arrival_time
            blocks = False

        segments.append((proc.pid, time, end))
        proc.remaining_time -= end - time
        start = time
        time = end
        while pending and pending[0].arrival_time <= time:  # same as RR: arrivals get in line before the nexts starts
            queues[0].append(pending.pop(0))

        if proc.remaining_time == 0:
            action = f"Finishes at t={end}"
        elif preempted_by:
            action = f"Preempted at t={end} by {preempted_by} -> stays in Q{level}"
            queues[level].append(proc)  # didn't get to use its whole quantum, so it isn't demoted
        elif blocks:
            action = f"Blocks for I/O after {end - start} -> stays in Q{level}"
            queues[level].append(proc)  # gave up the CPU early, it acts interactive, so it keeps its level
        elif level < len(queues) - 1:
            action = f"Runs full quantum ({q}) -> demoted to Q{level + 1}"
            queues[level + 1].append(proc)  # used the whole quantum, it acts CPU-bound, so it drops a level
        else:
            action = f"Runs full quantum ({q}) -> stays in Q{level}"
            queues[level].append(proc)  # already at the bottom, nowhere lower to go
        mlfq_trace.append((start, proc.pid, f"Q{level}", action))
    return segments


# all five take (processes, quantum=None) and return [(pid, start, end), ...], so they're interchangeable
ALGORITHMS = {
    "fcfs": ("FCFS", fcfs_schedule),
    "sjf": ("SJF", sjf_schedule),
    "rr": ("Round Robin", round_robin_schedule),
    "priority": ("Priority", priority_schedule),
    "mlfq": ("MLFQ", mlfq_schedule),
}


def compute_metrics(processes, segments):
    for proc in processes:
        runs = [(start, end) for pid, start, end in segments if pid == proc.pid]
        proc.start_time = min(start for start, end in runs)  
        proc.completion_time = max(end for start, end in runs)  
        proc.turnaround = proc.completion_time - proc.arrival_time  
        proc.waiting = proc.turnaround - proc.burst_time  
        proc.response = proc.start_time - proc.arrival_time  
    n = len(processes)
    return (sum(p.waiting for p in processes) / n,
            sum(p.turnaround for p in processes) / n,
            sum(p.response for p in processes) / n)


def run(workload, algo, quantum):
    processes = copy.deepcopy(workload) 
    segments = ALGORITHMS[algo][1](processes, quantum=quantum)
    averages = compute_metrics(processes, segments)
    return processes, averages


def print_table(columns, rows):
    widths = [max(len(name) + 2, 6, *(len(str(row[i])) + 2 for row in rows)) for i, name in enumerate(columns)]
    for row in [columns] + rows:
        print("".join(str(value).ljust(w) for value, w in zip(row, widths)).rstrip())


def print_one(workload, algo, quantum):
    processes, (avg_wait, avg_turn, avg_resp) = run(workload, algo, quantum)

    if algo == "mlfq":
        print_table(["Time", "PID", "Queue", "Action"], [list(row) for row in mlfq_trace])
        print()

    columns = ["PID", "Arrival", "Burst"]
    if algo == "priority":
        columns.append("Priority")
    columns += ["Start", "Completion", "Waiting", "Turnaround", "Response"]
    rows = []
    for p in sorted(processes, key=lambda p: p.start_time):  
        row = [p.pid, p.arrival_time, p.burst_time]
        if algo == "priority":
            row.append(p.priority)
        row += [p.start_time, p.completion_time, p.waiting, p.turnaround, p.response]
        rows.append(row)
    print_table(columns, rows)

    print(f"Average Waiting Time:    {avg_wait:.2f}")
    print(f"Average Turnaround Time: {avg_turn:.2f}")
    print(f"Average Response Time:   {avg_resp:.2f}")


def print_compare(workload, quantum):
    # same workload through all five, so the only thing that changes is the scheduling policy
    rows = []
    for algo, (name, _) in ALGORITHMS.items():
        _, (avg_wait, avg_turn, avg_resp) = run(workload, algo, quantum)
        rows.append([name, f"{avg_wait:.2f}", f"{avg_turn:.2f}", f"{avg_resp:.2f}"])
    print_table(["Algorithm", "Avg Waiting", "Avg Turnaround", "Avg Response"], rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CPU scheduler simulator (FCFS, SJF, Round Robin, Priority, MLFQ)")
    parser.add_argument("--workload", required=True, help="CSV file: pid,arrival,burst[,priority[,io]]")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--algo", choices=ALGORITHMS, help="run one algorithm and show per-process metrics")
    mode.add_argument("--compare", action="store_true", help="run all five algorithms and compare averages")
    parser.add_argument("--quantum", type=int, default=4,
                        help="Round Robin quantum, and MLFQ's Q0 quantum (Q1 = 2x, Q2 = 4x). Default 4")
    args = parser.parse_args()
    if args.quantum <= 0:
        parser.error("--quantum must be a positive integer")

    try:
        workload = read_workload(args.workload)
    except OSError as e:
        sys.exit(f"simulator: {args.workload}: {e.strerror}")

    if args.compare:
        print_compare(workload, args.quantum)
    else:
        print_one(workload, args.algo, args.quantum)
