#!/usr/bin/env python3
"""laila-translate: the intelligent Python -> laila-C -> targeted-C pipeline.

laila-C is generic and platform-independent. Turning a user's Python `laila`
program into compilable, platform-targeted C/C++ is an *intelligent* step
performed by Claude. This CLI is that step, and it therefore REQUIRES a Claude
API token.

Pipeline stages (see docs/TRANSLATION.md):

    python  --(py2c)-->  laila-C (generic C++)  --(c2target)-->  targeted C  -->  flash

  * py2c      : Python `laila` source  ->  generic laila-C (uses laila->verb()).
  * c2target  : generic laila-C        ->  C specialized for one platform,
                selecting the HAL backend and raising LAILA_UNSUPPORTED for any
                capability the target cannot honor.
  * full      : run both stages in sequence.

Token (required): set ANTHROPIC_API_KEY or LAILA_CLAUDE_TOKEN. Without it this
tool refuses to run -- laila-C cannot be produced from Python without it.

Usage:
    export LAILA_CLAUDE_TOKEN=sk-ant-...
    python tools/laila_translate.py app.py --target esp32 --stage full -o out/
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile

SUPPORTED_TARGETS = [
    "posix",
    "baremetal_singlecore",
    "esp32",
    "rp2040",
    "stm32",
]

# Repo root (the laila-C tree: this file is at <root>/tools/laila_translate.py).
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_INCLUDE = os.path.join(_ROOT, "include")
_HAL_INCLUDE = os.path.join(_ROOT, "hal", "include")

# Public headers, facade-first, that form the authoritative laila-C API surface.
# These are injected verbatim so the model uses REAL symbols (the "anchor") rather
# than inventing them. ~52KB total -- comfortably within context.
_ANCHOR_HEADERS = [
    "laila.hpp",
    "value.hpp",
    "entry.hpp",
    "future.hpp",
    "policy.hpp",
    "communication.hpp",
    "pool.hpp",
    "pools.hpp",
    "status.hpp",
    "identity.hpp",
    "constitution.hpp",
    "manifest.hpp",
    "logger.hpp",
    "runtime.hpp",
    "json.hpp",
]


def api_anchor() -> str:
    """Concatenate the real public headers into an authoritative API reference."""
    parts = []
    for name in _ANCHOR_HEADERS:
        path = os.path.join(_INCLUDE, "laila", name)
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8") as f:
            parts.append(f"// ===== laila/{name} =====\n{f.read().rstrip()}\n")
    body = "\n".join(parts)
    return (
        "AUTHORITATIVE laila-C API (the anchor). Use ONLY symbols declared below; "
        "do NOT invent functions, methods, headers, or types. Key reminders: the "
        "facade is the global `laila` pointer (laila->verb(...)); the active policy "
        "comes from laila_c::get_active_policy(); activate via "
        "laila_c::activate_policy(PolicyPtr); a Future yields its value via "
        "->data() (LailaValue) or ->wait(); construct entries via "
        "laila_c::Entry::constant(...) / laila->constant(...). Read a LailaValue "
        "with as_string()/as_int()/... (there is no to_string()).\n"
        "----- BEGIN laila-C API -----\n" + body + "\n----- END laila-C API -----\n"
    )


def _compiler() -> str | None:
    for cc in ("g++", "c++", "clang++"):
        from shutil import which

        if which(cc):
            return cc
    return None


def syntax_check(cpp_text: str, defines: list[str] | None = None) -> tuple[bool, str]:
    """-fsyntax-only compile a laila-C translation unit against the real headers.

    This is the compile-gate that makes laila-C the agreed-upon anchor: code that
    does not type-check against include/ + hal/include cannot proceed downstream.
    Returns (ok, diagnostics). If no compiler is present, returns (True, "") so the
    pipeline still runs (verification simply skipped, with a warning by the caller).
    """
    cc = _compiler()
    if not cc:
        return True, ""
    with tempfile.NamedTemporaryFile("w", suffix=".cpp", delete=False) as tf:
        tf.write(cpp_text)
        tmp = tf.name
    try:
        cmd = [cc, "-std=c++17", "-fsyntax-only", "-I", _INCLUDE, "-I", _HAL_INCLUDE]
        cmd += defines or []
        cmd.append(tmp)
        proc = subprocess.run(cmd, capture_output=True, text=True)
        return proc.returncode == 0, proc.stderr.strip()
    finally:
        os.unlink(tmp)


SYSTEM_PROMPT = """You are the laila translation compiler. You translate programs
that use the Python `laila` library into laila-C (a generic, platform-independent
C++17 port) and then into C specialized for a concrete platform.

Hard rules:
- Mirror laila's API 1:1. The only mechanical changes are `.`->`->`,
  `module.fn`->`laila->fn`, `Type.classmethod`->`Type::classmethod`, and Python
  kwargs -> the matching *Opts struct fields.
- Preserve ALL capabilities: every verb (memorize/remember/forget/build),
  Future, Entry, Pool, Constitution, the policy/central structure, and
  central.communication protocols.
- Payloads are type-free: map Python values to laila_c::LailaValue.
- Constitutions: storage chains -> transformation descriptors recovered by the
  recognized-snippet registry (never emit Python `exec`); user builder functions
  -> registered C builders referenced by class_token.
- Futures must exist for every operation. On single-core targets, keep the same
  Future types resolved by the cooperative executor.
- If the chosen target cannot support a capability (e.g. an S3 pool with no
  network transport, a LoRa link with no radio, threads on a single-core MCU),
  emit code that raises Status::Unsupported (LAILA_UNSUPPORTED) at that point and
  add a clear comment. Never silently drop behavior.
- Output ONLY the translated source, no prose.

Golden rules (laila's vault/agent/policy.md + memory.md) -- NEVER violate:
- To touch a policy and its elements you ACTIVATE it. The ONLY sanctioned way to
  reach ANOTHER policy is central.communication (peers). Never emit a direct
  cross-policy memory/pool access.
- All memory access goes through central.memory via memorize/remember/forget;
  never poke pools directly.
- laila is "started" only with >=1 local policy. Any generated main() MUST
  create and activate a local policy before any verb runs.

Cross-policy routing (peer / policy_id):
- A memory verb that targets another policy carries a peer reference. In Python
  this is `policy_id=<peer global_id>` (set up via
  `laila.communication.add_connection(DefaultTCPIPProtocol(host,port,
  peer_secret_key))` + `add_tcpip_peer(host,port,secret)`), optionally with
  `pool_nickname=<pool on the peer>` and `persist=<bool>`.
- Translate to laila-C as:
    auto peer = laila->add_tcpip_peer(host, port, secret);   // or add_peer(uri,secret)
    RememberOpts o; o.policy_id = peer; o.pool_nickname = ...; o.persist = false;
    auto fut = laila->remember(entry_id, o);                  // routes via communication
  The active policy STAYS local; this returns a RemoteFuture. `.wait()`/`.data()`.
- For the inverse direction (a peer pulls from THIS device), the generated main
  must SERVE -- exactly as in laila: `laila->add_connection(DefaultTCPIPProtocol(
  host, port, peer_secret_key))`. This brings the inbound listener up and serves
  it in the background automatically (no listen()/poll() -- those do not exist).
  Read the bound port via the protocol's `bound_port()`. The dispatcher answers
  the peer's central.memory.{remember,memorize,forget} against THIS policy's own
  memory.
- `persist` on a remote read caches into the PEER's alpha pool; on a
  flash-backed peer (ESP32) prefer persist=false unless explicitly requested.
"""

STAGE_INSTRUCTIONS = {
    "py2c": "Translate the following Python `laila` program into generic laila-C "
    "(platform-independent C++17 using <laila/laila.hpp>). Keep it generic; "
    "do not assume a specific platform. Honor the cross-policy routing rule: "
    "policy_id/peer targets become laila->add_tcpip_peer(...) + "
    "RememberOpts.policy_id; inbound serving is just "
    "add_connection(DefaultTCPIPProtocol(...)) (it serves automatically; "
    "there is no listen()/poll()).",
    "c2target": "Specialize the following generic laila-C for platform '{target}'. "
    "Select the {target} HAL backend, set the appropriate build options "
    "(e.g. LAILA_SINGLE_CORE for single-core MCUs), and replace any "
    "capability the target cannot honor with a LAILA_UNSUPPORTED error. "
    "Use ONLY the laila-C API from the anchor below -- do not invent types, "
    "methods, or headers. For target 'esp32': "
    "(a) include ONLY <laila/laila.hpp> and <cstdio>; "
    '(b) the entry point is `extern "C" void app_main()`; '
    "(c) the ONLY device-specific extern is "
    "`void laila_esp32_wifi_connect(const char* ssid, const char* pass)` "
    "(provided by the scaffold) -- call it FIRST; do not invent any other "
    "esp32_*/laila_esp32_* helpers or platform headers; "
    "(d) create+activate a local policy with "
    "`auto p = std::make_shared<laila_c::Policy>(); laila_c::activate_policy(p);` "
    "(or rely on the lazily-created active policy via get_active_policy()); "
    "(e) read Future results with ->data()/->wait() and LailaValue accessors "
    "(as_string()/as_int()/...), NOT to_string(); use std::printf for logs; "
    "(f) if it serves peers, call "
    "laila->add_connection(std::make_shared<DefaultTCPIPProtocol>(...)) "
    "(it serves in the background automatically; there is no poll()); "
    "(g) use the deployment config values below for host/port/secret/ssid. "
    "Keep cooperative (single-core) Futures.",
}


# ESP-IDF project scaffold emitted alongside the translated main for `esp32`.
# These are static, token-free templates so a device build is one `idf.py build`
# away. The translated app goes in main/app_main.cpp; this wires Wi-Fi + laila-C.
def _scaffold_files(cfg: dict) -> dict:
    proj = cfg.get("project", "laila_app")
    return {
        "CMakeLists.txt": (
            "cmake_minimum_required(VERSION 3.16)\n"
            "include($ENV{IDF_PATH}/tools/cmake/project.cmake)\n"
            f"project({proj})\n"
        ),
        "sdkconfig.defaults": (
            "CONFIG_ESP_MAIN_TASK_STACK_SIZE=8192\n"
            "CONFIG_SPIFFS_OBJ_NAME_LEN=64\n"
            "CONFIG_LWIP_SO_REUSE=y\n"
        ),
        "main/CMakeLists.txt": (
            'idf_component_register(SRCS "app_main.cpp" "wifi.cpp"\n'
            '    INCLUDE_DIRS "."\n'
            "    REQUIRES laila esp_wifi esp_netif nvs_flash spiffs lwip)\n"
        ),
        "main/wifi.cpp": _WIFI_CPP,
        "README.md": (
            f"# {proj} (laila-C on ESP32)\n\n"
            "Generated by laila-translate. Build inside ESP-IDF:\n\n"
            "```bash\nidf.py set-target esp32\nidf.py build flash monitor\n```\n\n"
            "Place laila-C as a component (e.g. `components/laila`).\n"
        ),
    }


# Real Wi-Fi station bring-up (the function esp32_main.cpp forward-declares).
_WIFI_CPP = r"""// Wi-Fi station bring-up for laila-C on ESP32 (generated by laila-translate).
#include <cstring>
#include "esp_event.h"
#include "esp_netif.h"
#include "esp_wifi.h"
#include "freertos/FreeRTOS.h"
#include "freertos/event_groups.h"
#include "nvs_flash.h"

static EventGroupHandle_t s_wifi_eg;
static const int GOT_IP = BIT0;

static void on_evt(void*, esp_event_base_t base, int32_t id, void* data) {
  if (base == WIFI_EVENT && id == WIFI_EVENT_STA_START) esp_wifi_connect();
  else if (base == WIFI_EVENT && id == WIFI_EVENT_STA_DISCONNECTED) esp_wifi_connect();
  else if (base == IP_EVENT && id == IP_EVENT_STA_GOT_IP) xEventGroupSetBits(s_wifi_eg, GOT_IP);
}

extern "C" void laila_esp32_wifi_connect(const char* ssid, const char* pass) {
  if (nvs_flash_init() == ESP_ERR_NVS_NO_FREE_PAGES) { nvs_flash_erase(); nvs_flash_init(); }
  s_wifi_eg = xEventGroupCreate();
  esp_netif_init();
  esp_event_loop_create_default();
  esp_netif_create_default_wifi_sta();
  wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
  esp_wifi_init(&cfg);
  esp_event_handler_instance_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &on_evt, nullptr, nullptr);
  esp_event_handler_instance_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &on_evt, nullptr, nullptr);
  wifi_config_t wc = {};
  std::strncpy((char*)wc.sta.ssid, ssid, sizeof(wc.sta.ssid) - 1);
  std::strncpy((char*)wc.sta.password, pass, sizeof(wc.sta.password) - 1);
  esp_wifi_set_mode(WIFI_MODE_STA);
  esp_wifi_set_config(WIFI_IF_STA, &wc);
  esp_wifi_start();
  xEventGroupWaitBits(s_wifi_eg, GOT_IP, false, true, portMAX_DELAY);
}
"""


def get_token() -> str:
    token = os.environ.get("LAILA_CLAUDE_TOKEN") or os.environ.get("ANTHROPIC_API_KEY")
    if not token:
        sys.stderr.write(
            "ERROR: a Claude API token is required to run laila-translate.\n"
            "       Set LAILA_CLAUDE_TOKEN (or ANTHROPIC_API_KEY) and retry.\n"
            "       laila-C cannot be produced from Python without it.\n"
        )
        sys.exit(2)
    return token


def call_claude(token: str, model: str, system: str, user: str) -> str:
    try:
        from anthropic import Anthropic
    except ImportError:
        sys.stderr.write(
            "ERROR: the 'anthropic' package is not installed.\n"
            "       Install it with:  pip install anthropic\n"
        )
        sys.exit(3)
    client = Anthropic(api_key=token)
    resp = client.messages.create(
        model=model,
        max_tokens=8192,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    return "".join(block.text for block in resp.content if getattr(block, "type", "") == "text")


def _config_block(cfg: dict) -> str:
    """Render the deployment config the c2target stage bakes into the device main.
    Secrets are passed through to the generated source; keep the config file (and
    generated output) out of version control."""
    if not cfg:
        return ""
    lines = "\n".join(f"  {k} = {v!r}" for k, v in cfg.items() if v is not None)
    return f"\n----- DEPLOYMENT CONFIG (use these literal values) -----\n{lines}\n"


def _strip_fences(text: str) -> str:
    """Remove a wrapping markdown code fence (```cpp ... ```) if the model adds
    one despite being asked for raw source -- otherwise the output won't compile."""
    s = text.strip()
    if not s.startswith("```"):
        return text
    lines = s.splitlines()
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines) + "\n"


def run_stage(token: str, model: str, stage: str, source: str, target: str, cfg: dict) -> str:
    instruction = STAGE_INSTRUCTIONS[stage].format(target=target)
    extra = _config_block(cfg) if stage == "c2target" else ""
    user = (
        f"{instruction}{extra}\n\n{api_anchor()}\n"
        f"----- BEGIN SOURCE -----\n{source}\n----- END SOURCE -----\n"
    )
    return _strip_fences(call_claude(token, model, SYSTEM_PROMPT, user))


def to_host_checkable(cpp_text: str) -> str:
    """Rewrite a platform-specialized TU into a host syntax-checkable form so the
    laila-C API usage can be gated even when the real target can't compile on the
    host: app_main()->main(), stub the one allowed esp32 extern, and drop platform
    headers (esp_*, freertos/*, lwip/*, driver/*, nvs_flash). Only the laila-C
    surface remains under test."""
    out = re.sub(r'extern\s+"C"\s+void\s+app_main\s*\(\s*\)', "int main()", cpp_text)
    out = re.sub(
        r"^\s*#\s*include\s*[<\"](esp_|freertos/|lwip/|driver/|nvs_flash|esp_wifi|esp_netif).*$",
        "",
        out,
        flags=re.MULTILINE,
    )
    stub_line = 'extern "C" void laila_esp32_wifi_connect(const char*, const char*) {}'
    needs_stub = "laila_esp32_wifi_connect" in out and stub_line not in out
    return (stub_line + "\n" + out) if needs_stub else out


def _repair(token: str, model: str, code: str, errors: str) -> str:
    """Ask the model to fix laila-C compile errors using ONLY the real API."""
    user = (
        "The following laila-C code FAILED to compile against the authoritative "
        "API. Return ONLY corrected laila-C source (no prose, no fences) that uses "
        "ONLY symbols from the API below. Fix every diagnostic.\n\n"
        f"{api_anchor()}\n"
        f"----- COMPILER DIAGNOSTICS -----\n{errors}\n"
        f"----- CURRENT CODE -----\n{code}\n----- END -----\n"
    )
    return _strip_fences(call_claude(token, model, SYSTEM_PROMPT, user))


def verify_and_fix(
    token: str,
    model: str,
    code: str,
    defines: list[str],
    label: str,
    rounds: int,
    host_check: bool = False,
) -> tuple[str, bool]:
    """Compile-gate `code` against laila-C, repairing via the model up to `rounds`
    times. Returns (final_code, ok). `host_check` rewrites a platform TU to a host-
    checkable form for the gate only (the returned code is the real target code)."""
    if _compiler() is None:
        sys.stderr.write(f"[laila-translate] verify {label}: no C++ compiler found -- skipped\n")
        return code, True
    for attempt in range(rounds + 1):
        probe = to_host_checkable(code) if host_check else code
        ok, errs = syntax_check(probe, defines)
        if ok:
            sys.stderr.write(
                f"[laila-translate] verify {label}: OK"
                + (f" (after {attempt} fix round(s))" if attempt else "")
                + "\n"
            )
            return code, True
        if attempt == rounds:
            sys.stderr.write(
                f"[laila-translate] verify {label}: STILL FAILING after "
                f"{rounds} fix round(s); writing best effort.\n{errs[:1500]}\n"
            )
            return code, False
        sys.stderr.write(
            f"[laila-translate] verify {label}: fixing (round {attempt + 1}/{rounds})...\n"
        )
        code = _repair(token, model, code, errs)
    return code, True


def write_scaffold(out_dir: str, cfg: dict, app_main: str | None) -> None:
    """Emit the ESP-IDF project scaffold (Wi-Fi + build wiring) next to the app."""
    for rel, content in _scaffold_files(cfg).items():
        path = os.path.join(out_dir, rel)
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
    if app_main is not None:
        os.makedirs(os.path.join(out_dir, "main"), exist_ok=True)
        with open(os.path.join(out_dir, "main", "app_main.cpp"), "w", encoding="utf-8") as f:
            f.write(app_main)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Translate Python laila -> laila-C -> targeted C via Claude."
    )
    ap.add_argument("source", help="Path to the Python laila source file.")
    ap.add_argument(
        "--target",
        default="posix",
        choices=SUPPORTED_TARGETS,
        help="Target platform for the c2target/full stages.",
    )
    ap.add_argument(
        "--stage",
        default="full",
        choices=["py2c", "c2target", "full"],
        help="Which translation stage(s) to run.",
    )
    ap.add_argument(
        "--model",
        default=os.environ.get("LAILA_CLAUDE_MODEL", "claude-sonnet-4-5"),
        help="Claude model id (override with LAILA_CLAUDE_MODEL).",
    )
    ap.add_argument("-o", "--out-dir", default="laila_c_out", help="Output directory.")
    # Deployment config baked into the device main / scaffold (esp32).
    ap.add_argument("--project", default="laila_app", help="ESP-IDF project name.")
    ap.add_argument("--wifi-ssid", help="Wi-Fi SSID for the esp32 target.")
    ap.add_argument("--wifi-pass", help="Wi-Fi password for the esp32 target.")
    ap.add_argument("--peer-host", help="Peer (your machine) host/IP.")
    ap.add_argument("--peer-port", type=int, help="Peer port.")
    ap.add_argument("--peer-secret", help="Peer shared secret (peer_secret_key).")
    ap.add_argument(
        "--listen-port",
        type=int,
        help="Local port to serve on (so a peer can pull from this device).",
    )
    ap.add_argument(
        "--scaffold-only",
        action="store_true",
        help="Write the esp32 ESP-IDF scaffold (Wi-Fi + build) without calling Claude.",
    )
    ap.add_argument(
        "--no-verify",
        action="store_true",
        help="Skip the laila-C compile-gate (by default each stage must "
        "-fsyntax-only compile against the real laila-C headers).",
    )
    ap.add_argument(
        "--max-fix-rounds",
        type=int,
        default=2,
        help="Max model repair rounds when the compile-gate fails (default 2).",
    )
    args = ap.parse_args()

    cfg = {
        "project": args.project,
        "wifi_ssid": args.wifi_ssid,
        "wifi_pass": args.wifi_pass,
        "peer_host": args.peer_host,
        "peer_port": args.peer_port,
        "peer_secret": args.peer_secret,
        "listen_port": args.listen_port,
    }
    os.makedirs(args.out_dir, exist_ok=True)

    # Token-free path: just lay down the device scaffold (useful in CI / offline).
    if args.scaffold_only:
        if args.target != "esp32":
            sys.stderr.write("--scaffold-only currently targets esp32.\n")
            return 2
        write_scaffold(args.out_dir, cfg, app_main=None)
        sys.stderr.write(f"[laila-translate] scaffold-only -> {args.out_dir}/\n")
        return 0

    token = get_token()  # exits if missing -- the token is mandatory
    with open(args.source, encoding="utf-8") as f:
        source = f.read()

    # Single-core MCU backends compile the core with LAILA_SINGLE_CORE; platform
    # MCU TUs are gated via a host-checkable rewrite (only laila-C API is tested).
    single_core = {"esp32", "rp2040", "stm32", "baremetal_singlecore"}
    host_only = {"esp32", "rp2040", "stm32"}
    rounds = 0 if args.no_verify else max(0, args.max_fix_rounds)
    gate_ok = True

    stage1 = source
    if args.stage in ("py2c", "full"):
        sys.stderr.write(f"[laila-translate] py2c (model={args.model})\n")
        stage1 = run_stage(token, args.model, "py2c", source, args.target, cfg)
        if not args.no_verify:
            # The anchor gate: generic laila-C must type-check against the real API.
            stage1, ok = verify_and_fix(token, args.model, stage1, [], "py2c (generic)", rounds)
            gate_ok = gate_ok and ok
        with open(os.path.join(args.out_dir, "generic.cpp"), "w", encoding="utf-8") as f:
            f.write(stage1)

    if args.stage in ("c2target", "full"):
        sys.stderr.write(f"[laila-translate] c2target -> {args.target}\n")
        targeted = run_stage(token, args.model, "c2target", stage1, args.target, cfg)
        if not args.no_verify:
            defines = ["-DLAILA_SINGLE_CORE=1"] if args.target in single_core else []
            targeted, ok = verify_and_fix(
                token,
                args.model,
                targeted,
                defines,
                f"c2target ({args.target})",
                rounds,
                host_check=args.target in host_only,
            )
            gate_ok = gate_ok and ok
        if args.target == "esp32":
            # Translated app -> main/app_main.cpp, plus the ESP-IDF + Wi-Fi scaffold.
            write_scaffold(args.out_dir, cfg, app_main=targeted)
        else:
            with open(
                os.path.join(args.out_dir, f"main_{args.target}.cpp"), "w", encoding="utf-8"
            ) as f:
                f.write(targeted)

    status = "done" if gate_ok else "done (compile-gate FAILED -- see diagnostics above)"
    sys.stderr.write(f"[laila-translate] {status} -> {args.out_dir}/\n")
    return 0 if gate_ok else 4


if __name__ == "__main__":
    sys.exit(main())
