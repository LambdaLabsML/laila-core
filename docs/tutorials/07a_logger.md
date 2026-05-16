# Tutorial 7a: Observability with `laila.logger`

LAILA emits structured log records for every `memorize` / `remember` / `forget` and every future transition. By default the logger is silent — you opt in with `laila.enable_logging(...)`.

## Prerequisites

```bash
pip install laila-core
```

## Starting the logger

`enable_logging(display=True)` installs a `StreamHandler` on the `laila` logger. Every subsequent memory operation prints a structured record to stderr:

```python
import laila
from laila.macros.defaults import DefaultPool

laila.enable_logging(level="DEBUG", display=True)
laila.memory.extend(DefaultPool(), pool_nickname="obs")

entry = laila.constant(data={"hello": "world"}, nickname="obs_entry")
laila.memorize(entry, pool_nickname="obs").wait()
```

## Changing the level

`set_log_level` switches the underlying stdlib level. Going from `DEBUG` to `WARNING` silences the routine memorize/remember chatter without taking the logger down completely:

```python
laila.set_log_level("WARNING")
laila.memorize(laila.constant(data=42, nickname="quiet"), pool_nickname="obs").wait()
# (no output)
```

## Ad-hoc logger calls

`laila.logger` is a real logger — call `.info(...)`, `.warning(...)`, `.error(...)` for free-form messages:

```python
laila.set_log_level("INFO")
laila.logger.info("checkpoint reached")
laila.logger.warning("disk space low")
```

## Pool sink — persist records as entries

Pass `pool_nickname` to `enable_logging` and every record is memorized into that pool. This turns your log history into LAILA entries you can query later through the same `remember` API:

```python
laila.disable_logging()
laila.memory.extend(DefaultPool(), pool_nickname="log_store")
laila.enable_logging(level="INFO", pool_nickname="log_store", display=False)

laila.logger.info("first persisted record")
laila.logger.warning("second persisted record")
```

`display` and the pool sink are independent — you can have both, neither, or just one. When neither is configured, LAILA forces `display=True` so records are not silently dropped.

## Custom logger instance

`laila.logger` is a settable property. Assigning your own `Logger` instance resets the singleton so subsequent code picks up the replacement:

```python
from laila.logger import Logger

custom = Logger(level="DEBUG", display=True)
laila.logger = custom
laila.logger.info("running through the custom logger")
```

## On-disk layout — `set_default_directory`

`laila.set_default_directory("/some/path")` rewrites the four conventional sub-paths LAILA uses: `root`, `pools`, `logs`, and `secrets`. Today the logger's `FileHandler` is not wired automatically — the `logs/` slot is a package convention that custom handlers can target.

```python
laila.set_default_directory("~/.laila-tutorial")
from laila.macros.defaults import LAILA_DEFAULT_DIRECTORIES
print(LAILA_DEFAULT_DIRECTORIES["logs"])
```

## Tear-down

`laila.disable_logging()` stops the singleton. After this call, no further records reach the stream or pool sink until you re-enable:

```python
laila.disable_logging()
```

## Summary

- `enable_logging(level=..., display=..., pool_nickname=...)` is the one-call entry point.
- Display and pool sinks are independent — combine or omit either.
- `set_log_level` and `disable_logging` operate on the singleton at runtime.
- `laila.logger.<level>(...)` works for ad-hoc messages.
- `set_default_directory` controls where `logs/` and `secrets/` land for custom handlers.

Next: continue with the Intermediate track, starting with [Tutorial 8a — Saving the Environment to S3](08a_environment_to_s3.md).
