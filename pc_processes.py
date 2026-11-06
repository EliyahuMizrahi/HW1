import argparse
import multiprocessing as mp
import queue
import time

STOP = None    


def log(lock, msg, quiet):
    if not quiet:
        with lock:                                
            print(msg, flush=True)


def work(kind):
    """Simulated processing of one item."""
    if kind == "io":
        time.sleep(0.02)
    else:
        sum(i * i for i in range(1_000_000))


def producer(pid, buf, n_items, lock, quiet):
    for n in range(n_items):
        item = (pid, n)
        try:
            buf.put_nowait(item)
        except queue.Full:
            log(lock, f"[producer {pid}] buffer full - waiting", quiet)
            buf.put(item)                         
            log(lock, f"[producer {pid}] resumed", quiet)
        log(lock, f"[producer {pid}] produced item {n}      buffer size: {buf.qsize()}", quiet)


def consumer(cid, buf, kind, result_q, lock, quiet):
    consumed = []                                 
    while True:
        item = buf.get()                          
        if item is STOP:
            log(lock, f"[consumer {cid}] received stop signal - exiting", quiet)
            result_q.put(consumed)                
            return
        log(lock, f"[consumer {cid}] consumed item {item[1]} (from producer {item[0]})"
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

    buf = mp.Queue(maxsize=args.capacity)         
    result_q = mp.Queue()                         
    lock = mp.Lock()

    start = time.perf_counter()
    producers = [mp.Process(target=producer, args=(i + 1, buf, args.items, lock, args.quiet))
                 for i in range(args.producers)]
    consumers = [mp.Process(target=consumer, args=(i + 1, buf, args.work, result_q, lock, args.quiet))
                 for i in range(args.consumers)]
    for p in producers + consumers:
        p.start()

    for p in producers:
        p.join()
    for _ in consumers:
        buf.put(STOP)                             
    consumed = [item for _ in consumers for item in result_q.get()]
    for p in consumers:
        p.join()
    elapsed = time.perf_counter() - start

    expected = args.producers * args.items
    alive = sum(p.is_alive() for p in producers + consumers)
    print(f"all producers finished, all consumers finished, {buf.qsize()} items left")
    print(f"produced {expected}, consumed {len(consumed)}, unique {len(set(consumed))}, "
          f"processes still alive: {alive}")
    print(f"multiprocessing | work={args.work} | {elapsed:.3f} s")


if __name__ == "__main__":
    main()
