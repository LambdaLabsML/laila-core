# laila-C translation pipeline (requires a Claude API token)

laila-C is a **generic, platform-independent** C++17 port of laila. Going from a
user's Python `laila` program to compilable, platform-targeted C/C++ is an
**intelligent** step performed by Claude. That step requires a Claude API token,
so **laila-C requires a Claude API token to produce a build from Python**.

> The token is needed for *translation only*. The compiled laila-C binary that
> runs on the device does **not** call Claude and needs no token at runtime.

## Stages

```
Python (laila)  --py2c-->  laila-C (generic C++)  --c2target-->  targeted C  -->  flash
```

- **py2c** — Python `laila` source → generic laila-C (`laila->verb(...)`),
  platform-independent.
- **c2target** — generic laila-C → C specialized for one platform: selects the
  HAL backend, sets build options (e.g. `LAILA_SINGLE_CORE`), and replaces any
  capability the target cannot honor with `LAILA_UNSUPPORTED`.
- **flash** — compile with the platform toolchain and deploy.

## laila-C is the anchor (accuracy)

Two translators (py2c, c2target) both pivot through **generic laila-C** — the
single representation everyone agrees on. Going `Python → laila-C → architecture`
(rather than `Python → architecture` directly) means each hop is small and
checkable, so there are fewer chances for the model to drift.

Two mechanisms keep the pivot honest:

1. **API anchor.** The real public headers (`include/laila/*.hpp`) are injected
   verbatim into every stage prompt as the authoritative API. The model must use
   only those symbols — no invented `DefaultPolicy()`, `to_string()`, or made-up
   esp32 helpers.
2. **Compile-gate + repair loop.** Each stage's output must `-fsyntax-only`
   compile against the laila-C headers before it proceeds. On failure the
   compiler diagnostics are fed back to the model for a bounded number of repair
   rounds (`--max-fix-rounds`, default 2). The generic stage is gated on the host
   directly; platform MCU stages (esp32/rp2040/stm32) are gated via a
   host-checkable rewrite that tests only the laila-C surface. A failing gate sets
   a non-zero exit so CI catches it. Disable with `--no-verify`.

This makes "does everyone agree?" a literal, mechanical check: laila-C either
type-checks or it doesn't.

## Token

Set one of these before running the translator:

```bash
export LAILA_CLAUDE_TOKEN=sk-ant-...     # preferred
# or
export ANTHROPIC_API_KEY=sk-ant-...
```

Optional model override: `export LAILA_CLAUDE_MODEL=claude-sonnet-4-5`.

If no token is present, `tools/laila_translate.py` refuses to run.

## Usage

```bash
pip install anthropic
export LAILA_CLAUDE_TOKEN=sk-ant-...

# Full pipeline for an ESP32 build:
python tools/laila_translate.py app.py --target esp32 --stage full -o out/
#   out/generic.cpp        (platform-independent laila-C)
#   out/main_esp32.cpp     (ESP32-specialized)

# Just the first stage:
python tools/laila_translate.py app.py --stage py2c -o out/
```

Targets: `posix`, `baremetal_singlecore`, `esp32`, `rp2040`, `stm32`.

## Translation contract (what the model must preserve)

- Mirror laila's API 1:1; only mechanical differences (`.`→`->`,
  `Type::classmethod`, kwargs→`*Opts` structs).
- Preserve **all** capabilities: verbs (`memorize`/`remember`/`forget`/`build`),
  `Future`s, `Entry`, `Pool` (+ proxy chaining), `Constitution`, the
  policy/central structure, and `central.communication` protocols.
- Type-free payloads → `laila_c::LailaValue`.
- Constitutions: storage chains → transformation descriptors recovered by the
  recognized-snippet registry (never emit Python `exec`); user builder functions
  → registered C builders by `class_token`.
- Futures exist for every operation; single-core targets resolve them with the
  cooperative executor.
- Unsupported-on-target capability → emit `LAILA_UNSUPPORTED` at that point with
  a clear comment; never silently drop behavior.
