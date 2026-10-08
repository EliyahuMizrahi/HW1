import os
import shlex
import signal
import sys


class Job:
    def __init__(self, pid, command, background):
        self.pid = pid
        self.command = command
        self.background = background
        self.status = "Running"  # Running, Stopped, Done, Terminated


jobs = {}  # job_id -> Job
next_job_id = 1


def parse(line):
    line = line.strip()

    background = line.endswith("&")
    if background:
        line = line[:-1].strip()  # if the line ends with &, remove it and any trailing whitespace

    args = shlex.split(line)  # removes quotes and splits the line into arguments
    return args, background, line


def fmt(job_id, status, command):
    # pads the status so the command lines up: "[1]  Running    sleep 5", "[2]  Terminated  sleep 30"
    return f"[{job_id}]  {status}" + " " * max(2, 11 - len(status)) + command


def reap():
    # runs before every prompt: collect any children that finished, without blocking
    while True:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:  # we have no children at all
            return
        if pid == 0:  # we have children, but none have finished
            return
        for job in jobs.values():
            if job.pid == pid:
                job.status = "Terminated" if os.WIFSIGNALED(status) else "Done"
                break


def get_job(args, cmd):
    # shared by fg and kill: turns "fg 2" into (2, Job), or prints an error and returns None
    if len(args) < 2:
        print(f"myshell: {cmd}: usage: {cmd} <job_id>")
        return None
    try:
        job_id = int(args[1])
    except ValueError:
        print(f"myshell: {cmd}: {args[1]}: invalid job id")
        return None
    job = jobs.get(job_id)
    if job is None or job.status != "Running":
        print(f"myshell: {cmd}: {args[1]}: no such job")
        return None
    return job_id, job


def builtin_cd(args):
    target = os.path.expanduser(args[1]) if len(args) > 1 else os.environ.get("HOME", "/")  # plain "cd" goes home, "~" works too
    try:
        os.chdir(target)  # has to run in the shell itself, a forked child would only change its own directory
    except OSError as e:
        print(f"myshell: cd: {target}: {e.strerror}")


def builtin_jobs():
    for job_id in sorted(jobs):
        job = jobs[job_id]
        print(fmt(job_id, job.status, job.command))
    for job_id in [i for i, j in jobs.items() if j.status != "Running"]:
        del jobs[job_id]  # finished jobs are shown once, then forgotten


def builtin_fg(args):
    found = get_job(args, "fg")
    if not found:
        return
    job_id, job = found
    print(job.command)
    try:
        os.waitpid(job.pid, 0)  # no WNOHANG: block until this job finishes or is killed
    except ChildProcessError:
        pass
    del jobs[job_id]


def builtin_kill(args):
    found = get_job(args, "kill")
    if not found:
        return
    job_id, job = found
    try:
        os.kill(job.pid, signal.SIGTERM)
        os.waitpid(job.pid, 0)  # reap right away so it never turns into a zombie
    except (ProcessLookupError, ChildProcessError):
        pass
    print(fmt(job_id, "Terminated", job.command))
    del jobs[job_id]


def run_external(args, command_text, background):
    global next_job_id
    sys.stdout.flush()  # otherwise the child inherits our unprinted output and may print it a second time
    pid = os.fork()

    if pid == 0:
        # child: become the requested program
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)  # Python ignores SIGPIPE, put it back to normal for the program
        if not background:
            signal.signal(signal.SIGINT, signal.SIG_DFL)  # Ctrl-C should stop a foreground program
        # background jobs keep ignoring Ctrl-C (inherited from the shell), like sh does
        try:
            os.execvp(args[0], args)
        except FileNotFoundError:
            print(f"myshell: {args[0]}: command not found", flush=True)
        except PermissionError:
            print(f"myshell: {args[0]}: permission denied", flush=True)
        os._exit(127)  # we only get here if exec failed; without this the child would keep running the shell

    # parent: the shell
    if background:
        job_id = next_job_id
        next_job_id += 1
        jobs[job_id] = Job(pid, command_text, True)
        print(f"[{job_id}] {pid} {command_text}")
    else:
        try:
            os.waitpid(pid, 0)  # foreground: block until it finishes
        except ChildProcessError:
            pass


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # Ctrl-C kills the foreground program, not the shell

    while True:
        reap()

        try:
            line = input("myshell> ")
        except EOFError:  # Ctrl-D, same as exit
            print()
            break

        try:
            args, background, command_text = parse(line)
        except ValueError as e:  # unclosed quote
            print(f"myshell: parse error: {e}")
            continue

        if not args:  # empty line, nothing to run
            continue

        cmd = args[0]
        if cmd == "exit":
            break
        elif cmd == "cd":
            builtin_cd(args)
        elif cmd == "jobs":
            builtin_jobs()
        elif cmd == "fg":
            builtin_fg(args)
        elif cmd == "kill":
            builtin_kill(args)
        else:
            run_external(args, command_text, background)
