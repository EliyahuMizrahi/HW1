import argparse
import queue
import threading
import time

STOP = None                      
print_lock = threading.Lock()    


def log(msg, quiet):
    if not quiet:
        with print_lock:
            print(msg, flush=True)


def work(kind):
    """Simulated processing of one item."""
    if kind == "io":
        time.sleep(0.02)                         
    else:
        sum(i * i for i in range(1_000_000))      


def producer(pid, buf, n_items, quiet):
    for n in range(n_items):
        item = (pid, n)
        try:
            buf.put_nowait(item)
        except queue.Full:
            log(f"[producer {pid}] buffer full - waiting", quiet)
            buf.put(item)                         
            log(f"[producer {pid}] resumed", quiet)
        log(f"[producer {pid}] produced item {n}      buffer size: {buf.qsize()}", quiet)


def consumer(cid, buf, kind, consumed, quiet):
    while True:
        item = buf.get()                          
        if item is STOP:
            log(f"[consumer {cid}] received stop signal - exiting", quiet)
            return
        log(f"[consumer {cid}] consumed item {item[1]} (from producer {item[0]})"
            f"      buffer size: {buf.qsize()}", quiet)
        work(kind)
        consumed.append(item)                     


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--producers", type=int, default=3)
    ap.add_argument("--consumers", type=int, default=3)
    ap.add_argument("--items", type=int, default=10, help="items per producer")
    ap.add_argument("--capacity", type=int, default=5)
    ap.add_argument("--work", choices=["io", "cpu"], default="io")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    buf = queue.Queue(maxsize=args.capacity)      
    results = [[] for _ in range(args.consumers)]

    start = time.perf_counter()
    producers = [threading.Thread(target=producer, args=(i + 1, buf, args.items, args.quiet))
                 for i in range(args.producers)]
    consumers = [threading.Thread(target=consumer, args=(i + 1, buf, args.work, results[i], args.quiet))
                 for i in range(args.consumers)]
    for t in producers + consumers:
        t.start()

    for t in producers:
        t.join()
    for _ in consumers:
        buf.put(STOP)                             
    for t in consumers:
        t.join()
    elapsed = time.perf_counter() - start

    consumed = [item for r in results for item in r]
    expected = args.producers * args.items
    print(f"all producers finished, all consumers finished, {buf.qsize()} items left")
    print(f"produced {expected}, consumed {len(consumed)}, unique {len(set(consumed))}")
    print(f"threading | work={args.work} | {elapsed:.3f} s")


if __name__ == "__main__":
    main()
