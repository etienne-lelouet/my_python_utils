#!/usr/bin/env python3

from utils import run_single_command
import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor

async def main():
    await run_single_command("echo Hello, World!", None, asynchronous=False, no_pipe=False, no_output=False, pty=False)

    print("no_output=True suppresses logging of command start and stop")
    await run_single_command("echo Hello, World!", None, asynchronous=True, no_pipe=False, no_output=True, pty=False)

    print("no_pipe=True disables printing of stdout and stderr when command completes (still can access them in the result object)")
    result = await run_single_command("echo Hello, World!", None, asynchronous=True, no_pipe=True, no_output=True, pty=False)
    print(f"Result object: return_code={result.return_code}, stdout={result.stdout.strip()}, stderr={result.stderr.strip()}")

    print("They can be combined, for a completely silent command execution")
    result = await run_single_command("echo Hello, World!", None, asynchronous=True, no_pipe=True, no_output=True, pty=False)

    print("I can await a run_single_command call")
    await run_single_command("sleep 5; echo Slept for 5s", None, asynchronous=True, no_pipe=False, no_output=False, pty=False)

    print("I can launch many commands in parallel and gather their results later")
    print("Warning ! If you don't wrap the calls in asyncio.create_task, they will not be started until awaited")
    tasks = []
    for i in range(2):
        tasks.append(asyncio.create_task(run_single_command(f"sleep  10; echo Task {i} done", None, asynchronous=True, no_pipe=True, no_output=True, pty=False)))

    print("I can do something else in the meantime")

    print(2+2)
    print (3*3)
    print("...")

    print("awaiting in all tasks in 10 seconds...")
    await asyncio.sleep(10)
    print("We waited for 10s, all tasks should be done now, so await should return immediately")
    print("Now awaiting all tasks...")

    tasks = await asyncio.gather(*tasks)

    for i, res in enumerate(tasks):
        print(f"Result of task {i}")
        print("---")
        print(f"Return code: {res.return_code}")
        print(f"Stdout: {res.stdout.strip()}")
        print(f"Stderr: {res.stderr.strip()}")
        print("---")

if __name__ == "__main__":
    asyncio.run(main())