# Tutorial 12: Routing CPU work to a Process Pool

The default taskforce in LAILA is `PythonAsyncThreadPoolTaskForce` — perfect for I/O-bound work but bottlenecked by the GIL for CPU-heavy tasks. For genuinely parallel CPU work, register a `PythonProcessPoolTaskForce` and route work to it.

## Prerequisites

```bash
pip install laila-core
```

!!! note
    Running this tutorial in a script (`python my_script.py`) requires the standard `if __name__ == "__main__":` guard around the submission code, because Python's `spawn` start method re-imports the main module in each worker process. Jupyter notebooks side-step this automatically; the snippets below run as-is in a notebook.

## Register the process pool

`PythonProcessPoolTaskForce` requires at least four workers (the model enforces `num_workers >= 4`). `add_taskforce` registers it under its own `global_id`:

```python
import laila
from laila.policy.central.command.taskforce.process_pool_executor.taskforce import (
    PythonProcessPoolTaskForce,
)

proc_tf = PythonProcessPoolTaskForce(
    num_workers=4,
    policy_id=laila.active_policy.global_id,
)
laila.command.add_taskforce(proc_tf)
proc_tf.start()

print(proc_tf.global_id)
```

`laila.command.taskforces` is a dict keyed by `global_id`. After this call there are two taskforces registered — the default async-thread pool and our new process pool.

## A CPU-bound workload

Computing `math.factorial(200_000)` is GIL-bound — async threads cannot parallelise it because the work happens inside a single interpreter. Process workers run in separate Python interpreters so they each get their own GIL.

We use `math.factorial` rather than a notebook-defined function because process pool workers must be able to **import** the callable they receive. Builtins, module-level functions in real Python files, and `functools.partial` over those all work; notebook-defined `def` functions and lambdas do not (the workers cannot resolve them by name):

```python
from math import factorial
from functools import partial

task = partial(factorial, 200_000)
```

## Submit on the alpha (async-thread) taskforce

Without a `taskforce_id`, `laila.command.submit` routes to `alpha_taskforce`. Four tasks run in parallel threads — but each is CPU-bound, so the GIL serialises them:

```python
import time

t0 = time.perf_counter()
laila.command.submit([task for _ in range(4)]).wait()
print("alpha pool:", round(time.perf_counter() - t0, 2), "s")
```

## Submit on the process pool

`laila.command.submit` wraps tasks with a coroutine-adapter closure that the process pool cannot pickle. For process-pool work, call the taskforce's `submit` directly with the same argument shape:

```python
t0 = time.perf_counter()
proc_tf.submit([task for _ in range(4)], wait=False).wait()
print("process pool:", round(time.perf_counter() - t0, 2), "s")
```

Tasks routed to a process pool must be **top-level picklable** callables — module-level functions or `functools.partial(fn, *args)`. Lambdas will not pickle, and functions defined in a Jupyter cell typically can't be re-imported by a spawn worker.

## Tear down both cleanly

`laila.terminate(wait=True)` drains every registered taskforce in turn. Pass `cancel_pending=True` to drop queued-but-unstarted submissions instead of waiting them out:

```python
laila.terminate(wait=True)
```

## When to use a process pool

| Workload | Taskforce |
|---|---|
| I/O, network, async serialization | Default async-thread pool |
| Pure-Python CPU work (hashing, parsing, image processing) | Process pool |
| Native extension calls that release the GIL (numpy, torch math) | Either — async-thread is often enough |
| Crash-prone code that should not take down the policy | Process pool (worker isolation) |

## Summary

- `PythonProcessPoolTaskForce(num_workers=N)` requires `N >= 4`.
- `laila.command.add_taskforce(tf)` registers it on the active policy.
- For the async-thread default, call `laila.command.submit([fn, ...])`.
- For the process pool, call `tf.submit([fn, ...], wait=False)` directly because `laila.command.submit` wraps tasks in a closure that processes cannot pickle.
- Tasks routed to a process pool must be top-level picklable callables — module-level functions or `functools.partial(fn, *args)`. Lambdas will not pickle.
- `laila.terminate(wait=True)` cleans up every pool on the active policy.

Next: [Tutorial 13 — Configuring LAILA from a TOML file](13_cli_and_toml_config.md).
