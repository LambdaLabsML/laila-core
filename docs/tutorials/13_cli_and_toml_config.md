# Tutorial 13: Configuring LAILA from a TOML file (or terminal)

Every CLI-capable LAILA class resolves its field values in this order:

1. Explicit kwarg passed at construction time.
2. The matching subtree under `laila.args`.
3. The class's declared default.

`laila.read_args(source)` populates `laila.args` from a TOML / JSON / `.env` / XML file, or from terminal arguments.

## Prerequisites

```bash
pip install laila-core
```

## TOML — the most common form

Write a `config.toml` with top-level keys and nested `[table]` blocks. Nested tables map onto nested DotMap attributes on `laila.args`:

```toml
# config.toml
AWS_REGION = "us-east-1"
AWS_BUCKET_NAME = "my-bucket"

[secrets]
api_token = "sk-demo-1234"
```

```python
import laila

laila.read_args("./config.toml")

print(laila.args.AWS_REGION)        # "us-east-1"
print(laila.args.AWS_BUCKET_NAME)    # "my-bucket"
print(laila.args.secrets.api_token)  # "sk-demo-1234"
```

## `.env` files

`.env` files are parsed line-by-line as `KEY=value`. Use them for secrets you want kept out of source control:

```dotenv
# secrets.env
AWS_ACCESS_KEY_ID=AKIAEXAMPLE
AWS_SECRET_ACCESS_KEY=very-secret
```

```python
laila.read_args("./secrets.env")
print(laila.args.AWS_ACCESS_KEY_ID)
```

## Terminal-style arguments

`laila.read_args("terminal", terminal_args=[...])` parses `key=value` pairs from the list. This is **not** GNU-style `--flag value`, but the literal `--flag=value` (or plain `flag=value`) is fine:

```python
laila.read_args("terminal", terminal_args=["LOG_LEVEL=DEBUG", "PROJECT=demo"])
print(laila.args.LOG_LEVEL)  # "DEBUG"
print(laila.args.PROJECT)    # "demo"
```

## CLI-capable classes pick up defaults from `laila.args`

Any class derived from `_LAILA_CLI_CAPABLE_CLASS` walks `laila.args` during validation. If a field is **not** passed explicitly and a matching key is found, the value is used as the default. For example, instantiating a `FilesystemPool` without arguments uses any matching keys you have loaded.

## Round-tripping the entire environment

`laila.args.environment` is special. Assigning a payload with non-empty `policies` triggers a full `laila.terminate(...)` and rebuild of every described policy. This is what [Tutorial 8a](08a_environment_to_s3.md) and [Tutorial 8b](08b_policy_from_manifest.md) use to capture and restore a setup across processes.

## Gotchas

- `from_terminal` parses **`key=value`** tokens. A bare `--flag` followed by a separate value will not work.
- Deeply nested TOML tables work, but the loader flattens with `_` at the first level — check `laila.args.toDict()` if a lookup feels off.
- `read_args` **merges** into `laila.args`. To start fresh, restart the kernel or call `laila.terminate()` first.

## Summary

- `laila.read_args(path)` supports `.toml`, `.json`, `.env`, `.xml`.
- `laila.read_args("terminal", terminal_args=[...])` accepts `key=value` tokens.
- Subsequent CLI-capable class construction picks up the loaded values as defaults.
- Assigning `laila.args.environment = {...}` triggers a full process-wide reload.

Next: [Tutorial 14 — Multi-pool Routing](14_multi_pool_routing.md).
