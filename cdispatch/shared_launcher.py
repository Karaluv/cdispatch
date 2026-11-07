"""Owns one srun step; its cancellation cannot address the enclosing allocation."""
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from .common import atomic,read,lock
from .shared import step_command


def launch(attempt,interactive=False):
    a=Path(attempt);proc=None;code=143;stop=False
    def terminate(*_):
        nonlocal stop
        stop=True
        if proc and proc.poll() is None:proc.terminate()
    old={sig:signal.signal(sig,terminate) for sig in (signal.SIGTERM,signal.SIGINT,signal.SIGHUP)}
    try:
        with lock(a/'launch.lock'):
            if (a/'cancel.json').exists():
                atomic(a/'driver.json',dict(returncode=143,time=time.time(),cancelled_before_launch=True));return 143
            if (a/'driver.json').exists() or (a/'started.json').exists():raise RuntimeError('Shared step already launched; refusing replay')
            atomic(a/'launch-intent.json',dict(time=time.time(),pid=os.getpid()))
            try:
                proc=subprocess.Popen(step_command(a,interactive and sys.stdin.isatty() and sys.stdout.isatty()),
                                      stdin=None if interactive else subprocess.DEVNULL)
                stopping_since=None
                while proc.poll() is None:
                    if interactive:atomic(a/'client.json',dict(time=time.time()))
                    if stop or (a/'cancel.json').exists():
                        if stopping_since is None:proc.terminate();stopping_since=time.time()
                        elif time.time()-stopping_since>35:proc.kill()
                    time.sleep(.5)
                code=proc.returncode
            finally:
                if proc and proc.poll() is None:proc.terminate();proc.wait(timeout=40)
                atomic(a/'driver.json',dict(returncode=proc.returncode if proc else 127,time=time.time()))
                if interactive:atomic(a/'client.json',dict(time=time.time(),finished=True,exit_code=code))
    finally:
        for sig,handler in old.items():signal.signal(sig,handler)
    return code


if __name__=='__main__':raise SystemExit(launch(sys.argv[1]))
