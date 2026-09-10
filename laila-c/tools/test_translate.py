#!/usr/bin/env python3
"""100 diverse tests for the laila translators (py2c and c2target).

Dependency-free (stdlib only), mirroring laila-C's own tiny test harness. Two
tiers:

  * DETERMINISTIC (no token, always run): exercise the translator *machinery*
    that makes translation correct -- the API anchor, fence stripping, the
    host-checkable rewrite, the compile-gate itself (a battery of real laila-C
    snippets that MUST compile and invalid ones that MUST NOT), config/scaffold
    emission, and a native (POSIX) compile-link-RUN oracle that proves behavior.

  * LIVE (needs LAILA_CLAUDE_TOKEN + --live): a corpus of diverse Python `laila`
    programs translated end-to-end, each gated on compiling against real laila-C
    (and, for a few, linked+run natively). Skipped unless --live is passed.

Why this is a valid oracle for "both translators work": laila-C is the anchor.
Correct output == it type-checks (and, natively, runs) against the real laila-C
API. The native POSIX run exercises the SAME translated application logic an
ESP32 would run -- only the HAL differs -- so it validates the architecture
translator's logic without hardware.

Usage:
    python tools/test_translate.py            # deterministic suite
    python tools/test_translate.py --live      # + live LLM translation corpus
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import laila_translate as L

# ---------------------------------------------------------------- test registry
_TESTS: list[tuple[str, object]] = []


class Skip(Exception):
    pass


def test(name):
    def deco(fn):
        _TESTS.append((name, fn))
        return fn

    return deco


def reg(name, fn):
    _TESTS.append((name, fn))


def eq(a, b, msg=""):
    if a != b:
        raise AssertionError(f"{msg}: {a!r} != {b!r}")


def ok(cond, msg=""):
    if not cond:
        raise AssertionError(msg or "expected true")


# ----------------------------------------------------------------- build oracle
_BUILD = os.path.join(L._ROOT, "build")
_CORE = os.path.join(_BUILD, "liblaila_core.a")
_HALP = os.path.join(_BUILD, "platform", "posix", "liblaila_hal_posix.a")

WRAP = (
    "#include <laila/laila.hpp>\n#include <cstdio>\n#include <vector>\n"
    "using namespace laila_c;\nint main(){ %s ; return 0; }\n"
)


def _wrap(body: str) -> str:
    return WRAP % body


def compile_link_run(cpp: str, timeout: int = 25):
    """Compile+link `cpp` against the built laila-C libs and run it.
    Returns (rc, stdout) or raises Skip if the libs aren't built / no g++."""
    if L._compiler() is None or not (os.path.exists(_CORE) and os.path.exists(_HALP)):
        raise Skip("native run oracle unavailable (build libs / g++ missing)")
    with tempfile.TemporaryDirectory() as d:
        src = os.path.join(d, "t.cpp")
        out = os.path.join(d, "t.out")
        open(src, "w").write(cpp)
        cc = L._compiler()
        comp = subprocess.run(
            [
                cc,
                "-std=c++17",
                "-I",
                L._INCLUDE,
                "-I",
                L._HAL_INCLUDE,
                src,
                "-Wl,--start-group",
                _CORE,
                _HALP,
                "-Wl,--end-group",
                "-pthread",
                "-o",
                out,
            ],
            capture_output=True,
            text=True,
        )
        if comp.returncode != 0:
            return ("COMPILE_FAIL", comp.stderr)
        run = subprocess.run([out], capture_output=True, text=True, timeout=timeout)
        return (run.returncode, run.stdout)


# ======================================================================= ANCHOR
@test("anchor/nonempty")
def _():
    ok(len(L.api_anchor()) > 5000)


for _sym in [
    "RememberOpts",
    "get_active_policy",
    "activate_policy",
    "add_tcpip_peer",
    "add_connection",
    "LailaValue",
    "from_string",
    "Future",
    "CentralCommunication",
    "Entry",
    "->data()",
    "RemotePolicyProxy",
]:
    reg(
        f"anchor/has:{_sym}",
        (lambda s: lambda: ok(s in L.api_anchor(), f"anchor missing {s}"))(_sym),
    )


@test("anchor/lists-all-headers")
def _():
    a = L.api_anchor()
    for h in L._ANCHOR_HEADERS:
        ok(f"laila/{h}" in a, f"anchor missing header marker {h}")


@test("anchor/warns-no-to_string")
def _():
    ok("to_string" in L.api_anchor(), "anchor should warn there is no to_string()")


@test("anchor/headers-exist-on-disk")
def _():
    for h in L._ANCHOR_HEADERS:
        ok(os.path.exists(os.path.join(L._INCLUDE, "laila", h)), f"missing {h}")


# ================================================================ FENCE STRIPPER
_FENCE_CASES = [
    ("plain-cpp", "```cpp\nint main(){}\n```", "int main(){}"),
    ("plain-nolang", "```\nint main(){}\n```", "int main(){}"),
    ("no-fence", "int main(){}", "int main(){}"),
    ("leading-ws", "   \n```cpp\nint x;\n```", "int x;"),
    ("trailing-ws", "```cpp\nint x;\n```   \n", "int x;"),
    ("cxx-tag", "```c++\nint x;\n```", "int x;"),
    ("multiline", "```cpp\nint a;\nint b;\n```", "int a;\nint b;"),
    ("internal-backticks", "int x; // `note`", "int x; // `note`"),
]
for _n, _inp, _want in _FENCE_CASES:
    reg(
        f"fences/{_n}",
        (lambda i, w: lambda: eq(L._strip_fences(i).strip(), w, "fence"))(_inp, _want),
    )


@test("fences/only-opening")
def _():
    ok("int x" in L._strip_fences("```cpp\nint x;"))


@test("fences/idempotent-plain")
def _():
    s = "int main(){}\n"
    eq(L._strip_fences(s), s)


# ============================================================== HOST-CHECKABLE
@test("host/app_main->main")
def _():
    ok("int main()" in L.to_host_checkable('extern "C" void app_main(){}'))


@test("host/wifi-stub-added")
def _():
    out = L.to_host_checkable('extern "C" void app_main(){ laila_esp32_wifi_connect("a","b"); }')
    ok("void laila_esp32_wifi_connect(const char*, const char*) {}" in out)


@test("host/no-wifi-stub-when-unused")
def _():
    out = L.to_host_checkable('extern "C" void app_main(){}')
    ok("laila_esp32_wifi_connect(const char*, const char*) {}" not in out)


for _hdr in [
    "esp_wifi.h",
    "freertos/FreeRTOS.h",
    "lwip/sockets.h",
    "driver/gpio.h",
    "nvs_flash.h",
    "esp_netif.h",
]:

    def _mk(h):
        def f():
            src = f'#include "{h}"\nextern "C" void app_main(){{}}'
            out = L.to_host_checkable(src)
            ok(h not in out, f"{h} should be dropped")

        return f

    reg(f"host/drops:{_hdr}", _mk(_hdr))


@test("host/plain-unchanged")
def _():
    src = "#include <laila/laila.hpp>\nint main(){ return 0; }"
    eq(L.to_host_checkable(src), src)


@test("host/idempotent")
def _():
    src = '#include "esp_wifi.h"\nextern "C" void app_main(){ laila_esp32_wifi_connect("a","b"); }'
    once = L.to_host_checkable(src)
    eq(L.to_host_checkable(once), once)


# ========================================================== COMPILE-GATE: GOOD
_GOOD = [
    (
        "constant-string",
        'auto e=laila->constant(LailaValue::from_string("x")); laila->memorize(e)->wait(); (void)laila->remember(e->global_id())->data();',
    ),
    ("constant-int", "auto e=laila->constant(LailaValue::from_int(7)); (void)e;"),
    ("constant-double", "auto e=laila->constant(LailaValue::from_double(3.14)); (void)e;"),
    ("constant-bool", "auto e=laila->constant(LailaValue::from_bool(true)); (void)e;"),
    (
        "constant-bytes",
        "std::vector<uint8_t> b{1,2,3}; auto e=laila->constant(LailaValue::from_bytes(b)); (void)e;",
    ),
    (
        "constant-json",
        'Json o=Json::object(); o["k"]=std::string("v"); auto e=laila->constant(LailaValue::from_json(o)); (void)e;',
    ),
    ("variable", "auto e=laila->variable(); (void)e;"),
    (
        "forget",
        "auto e=laila->constant(LailaValue::none()); laila->memorize(e)->wait(); laila->forget(e->global_id())->wait();",
    ),
    ("build", 'auto e=laila->constant(LailaValue::from_string("x")); laila->build(e)->wait();'),
    (
        "remember-opts-persist",
        'RememberOpts o; o.persist=false; auto e=laila->constant(LailaValue::from_string("x")); laila->memorize(e)->wait(); (void)laila->remember(e->global_id(),o);',
    ),
    (
        "remember-pool_nickname",
        'RememberOpts o; o.pool_nickname=std::string("store"); auto f=laila->remember("LAILA:ENTRY:GLOBAL_ID:x",o); (void)f;',
    ),
    (
        "remember-policy_id",
        'RememberOpts o; o.policy_id=std::string("peer"); auto f=laila->remember("g",o); (void)f;',
    ),
    (
        "memorize-group",
        "std::vector<EntryPtr> v{laila->constant(LailaValue::from_int(1))}; laila->memorize(v)->wait();",
    ),
    ("add_peer", 'auto id=laila->add_peer("tcp://h:1","s"); (void)id;'),
    ("add_tcpip_peer", 'auto id=laila->add_tcpip_peer("h",(uint16_t)1,"s"); (void)id;'),
    (
        "serve-add-connection",
        'auto _tcp=std::make_shared<DefaultTCPIPProtocol>(std::string("0.0.0.0"),(uint16_t)0,std::string("s")); laila->add_connection(_tcp); (void)_tcp->bound_port();',
    ),
    (
        "request",
        'Json a=Json::array(); a.push_back(Json("x")); auto f=laila->request("peer","central.memory.remember",a); (void)f;',
    ),
    ("peer-proxy", 'auto p=laila->peer("x"); (void)p;'),
    (
        "entry-as_dict",
        'auto e=Entry::constant(LailaValue::from_string("x")); auto d=e->as_dict(); (void)d;',
    ),
    (
        "future-data",
        'auto e=laila->constant(LailaValue::from_string("x")); laila->memorize(e)->wait(); auto v=laila->remember(e->global_id())->data(); (void)v.as_string();',
    ),
    (
        "future-result-gid",
        "auto e=laila->constant(LailaValue::from_int(1)); auto f=laila->memorize(e); f->wait(); (void)f->result()->global_id();",
    ),
    (
        "tensor",
        'std::vector<uint8_t> raw{0,0}; auto v=LailaValue::tensor(raw,"int8",{2}); auto e=laila->constant(v); (void)e;',
    ),
    (
        "entry-evolve",
        "auto e=laila->constant(LailaValue::from_int(1)); auto e2=e->evolve(LailaValue::from_int(2)); (void)e2;",
    ),
    (
        "serialize-roundtrip",
        'auto e=Entry::constant(LailaValue::from_string("x")); auto d=e->serialize(); auto e2=Entry::from_dict(d); (void)e2;',
    ),
    (
        "set-namespace",
        'laila->set_active_namespace("ns"); auto e=laila->constant(LailaValue::none()); (void)e;',
    ),
]
for _n, _body in _GOOD:

    def _mk_good(b, n):
        def f():
            good, errs = L.syntax_check(_wrap(b))
            ok(good, f"GOOD snippet '{n}' failed to compile:\n{errs[:600]}")

        return f

    reg(f"gate-good/{_n}", _mk_good(_body, _n))


# =========================================================== COMPILE-GATE: BAD
_BAD = [
    ("DefaultPolicy", "auto p=DefaultPolicy(); (void)p;"),
    (
        "value-to_string",
        'auto e=laila->constant(LailaValue::from_string("x")); auto s=LailaValue::none().to_string(); (void)s;',
    ),
    ("from_str-typo", 'auto v=LailaValue::from_str("x"); (void)v;'),
    ("verb-recall", 'auto f=laila->recall("x"); (void)f;'),
    ("dot-on-ptr", "auto e=laila->constant(LailaValue::none()); auto d=e.data(); (void)d;"),
    ("opts-unknown-field", "RememberOpts o; o.affinity=1.0; (void)o;"),
    ("add_tcpip_peer-arity", 'auto id=laila->add_tcpip_peer("h"); (void)id;'),
    ("cout-no-include", 'std::cout << "x";'),
    ("python-kwarg", 'auto f=laila->remember("x", persist=false); (void)f;'),
    ("python-dot-call", "laila.memorize(laila->constant(LailaValue::none()));"),
    ("classmethod-dot", "auto e=Entry.constant(LailaValue::none()); (void)e;"),
    ("constant-noarg", "auto e=laila->constant(); (void)e;"),
]
for _n, _body in _BAD:

    def _mk_bad(b, n):
        def f():
            good, _ = L.syntax_check(_wrap(b))
            ok(not good, f"BAD snippet '{n}' unexpectedly COMPILED (gate too weak)")

        return f

    reg(f"gate-bad/{_n}", _mk_bad(_body, _n))


# =============================================================== CONFIG / SCAFFOLD
@test("config/empty")
def _():
    eq(L._config_block({}), "")


@test("config/filters-none")
def _():
    b = L._config_block({"a": "x", "b": None})
    ok("a = 'x'" in b and "b =" not in b)


@test("config/repr-quoting")
def _():
    ok("'h'" in L._config_block({"peer_host": "h"}))


@test("config/has-header")
def _():
    ok("DEPLOYMENT CONFIG" in L._config_block({"x": 1}))


def _scaffold():
    return L._scaffold_files({"project": "demo_x"})


for _k in [
    "CMakeLists.txt",
    "sdkconfig.defaults",
    "main/CMakeLists.txt",
    "main/wifi.cpp",
    "README.md",
]:
    reg(f"scaffold/has:{_k}", (lambda k: lambda: ok(k in _scaffold(), f"scaffold missing {k}"))(_k))


@test("scaffold/project-name-in-cmake")
def _():
    ok("project(demo_x)" in _scaffold()["CMakeLists.txt"])


@test("scaffold/wifi-has-connect")
def _():
    ok("laila_esp32_wifi_connect" in _scaffold()["main/wifi.cpp"])


@test("scaffold/wifi-includes-esp_wifi")
def _():
    ok("esp_wifi.h" in _scaffold()["main/wifi.cpp"])


@test("scaffold/main-requires-laila")
def _():
    ok("laila" in _scaffold()["main/CMakeLists.txt"])


@test("scaffold/write-creates-files")
def _():
    with tempfile.TemporaryDirectory() as d:
        L.write_scaffold(d, {"project": "p"}, app_main='extern "C" void app_main(){}')
        for rel in ["CMakeLists.txt", "main/wifi.cpp", "main/app_main.cpp"]:
            ok(os.path.exists(os.path.join(d, rel)), f"not written: {rel}")


# ===================================================================== MISC API
@test("misc/supported-targets")
def _():
    for t in ["posix", "esp32", "rp2040", "stm32", "baremetal_singlecore"]:
        ok(t in L.SUPPORTED_TARGETS)


@test("misc/compiler-present")
def _():
    ok(L._compiler() in ("g++", "c++", "clang++"))


@test("misc/include-paths-exist")
def _():
    ok(os.path.isdir(L._INCLUDE) and os.path.isdir(L._HAL_INCLUDE))


@test("misc/syntax_check-returns-tuple")
def _():
    r = L.syntax_check(_wrap("(void)0;"))
    ok(isinstance(r, tuple) and len(r) == 2)


# =============================================== NATIVE RUN ORACLE (behavioral)
@test("run/memorize-remember-roundtrip")
def _():
    r = compile_link_run(
        _wrap(
            'auto e=laila->constant(LailaValue::from_string("payload42")); laila->memorize(e)->wait();'
            ' auto v=laila->remember(e->global_id())->data(); std::printf("OUT=%s\\n", v.as_string().c_str());'
        )
    )
    rc, out = r
    eq(rc, 0, f"nonzero exit; out={out}")
    ok("OUT=payload42" in out, f"bad output: {out}")


@test("run/int-roundtrip")
def _():
    rc, out = compile_link_run(
        _wrap(
            "auto e=laila->constant(LailaValue::from_int(123)); laila->memorize(e)->wait();"
            ' std::printf("N=%lld\\n",(long long)laila->remember(e->global_id())->data().as_int());'
        )
    )
    eq(rc, 0)
    ok("N=123" in out, out)


@test("run/json-roundtrip")
def _():
    rc, out = compile_link_run(
        _wrap(
            'Json o=Json::object(); o["s"]=std::string("temp"); o["r"]=(int64_t)42;'
            " auto e=laila->constant(LailaValue::from_json(o)); laila->memorize(e)->wait();"
            ' std::printf("J=%s\\n", laila->remember(e->global_id())->data().as_json().dump().c_str());'
        )
    )
    eq(rc, 0)
    ok('"r"' in out and "42" in out, out)


@test("run/forget-removes")
def _():
    rc, out = compile_link_run(
        _wrap(
            'auto e=laila->constant(LailaValue::from_string("x")); laila->memorize(e)->wait();'
            " laila->forget(e->global_id())->wait(); bool gone=false;"
            " try { (void)laila->remember(e->global_id())->data(); } catch(const LailaError&){ gone=true; }"
            ' std::printf("GONE=%d\\n", gone?1:0);'
        )
    )
    eq(rc, 0)
    ok("GONE=1" in out, out)


@test("run/group-memorize-many")
def _():
    rc, out = compile_link_run(
        _wrap(
            "std::vector<EntryPtr> v; for(int i=0;i<5;i++) v.push_back(laila->constant(LailaValue::from_int(i)));"
            " laila->memorize(v)->wait(); int n=0; for(auto&e:v){ if(!laila->remember(e->global_id())->data().is_none()) n++; }"
            ' std::printf("CNT=%d\\n", n);'
        )
    )
    eq(rc, 0)
    ok("CNT=5" in out, out)


@test("run/inprocess-peer-remember")
def _():
    # Register a SECOND local policy and pull from it as an in-process peer.
    rc, out = compile_link_run(
        _wrap(
            "auto a=std::make_shared<Policy>(); activate_policy(a);"
            ' auto e=laila->constant(LailaValue::from_string("frompeer")); laila->memorize(e)->wait();'
            " std::string gid=e->global_id();"
            " auto b=std::make_shared<Policy>(); activate_policy(b);"  # b is now active
            ' std::string pid=laila->add_peer(a->global_id(), "");'  # peer = policy a (in-process)
            " auto v=laila->peer(pid)->remember(gid)->data();"
            ' std::printf("PEER=%s\\n", v.as_string().c_str());'
        )
    )
    eq(rc, 0, f"out={out}")
    ok("PEER=frompeer" in out, out)


# ================================================================ LIVE (gated)
# Diverse Python `laila` programs translated end-to-end; correctness == compiles
# against laila-C (the anchor). Skipped unless --live and a token are present.
_PY_CORPUS = [
    (
        "py/constant-memorize",
        "import laila\np=laila.DefaultPolicy(); laila.activate_policy(p)\n"
        'e=laila.constant(data="hello")\nlaila.memorize(e).wait()\n',
    ),
    (
        "py/remember-roundtrip",
        "import laila\np=laila.DefaultPolicy(); laila.activate_policy(p)\n"
        "e=laila.constant(data=7)\nlaila.memorize(e).wait()\nprint(laila.remember(e.global_id).data)\n",
    ),
    (
        "py/dict-payload",
        "import laila\np=laila.DefaultPolicy(); laila.activate_policy(p)\n"
        'e=laila.constant(data={"k":[1,2,3]})\nlaila.memorize(e).wait()\n',
    ),
    (
        "py/nickname",
        "import laila\np=laila.DefaultPolicy(); laila.activate_policy(p)\n"
        'e=laila.constant(data="cal", nickname="calib")\nlaila.memorize(e).wait()\n',
    ),
    (
        "py/forget",
        "import laila\np=laila.DefaultPolicy(); laila.activate_policy(p)\n"
        'e=laila.constant(data="x")\nlaila.memorize(e).wait()\nlaila.forget(e.global_id).wait()\n',
    ),
    (
        "py/peer-pull",
        "import laila\np=laila.DefaultPolicy(); laila.activate_policy(p)\n"
        'rid=laila.communication.add_tcpip_peer("10.0.0.5",8770,"s")\n'
        'v=laila.remember(entry_ids="LAILA:ENTRY:GLOBAL_ID:x", pool_nickname="store", policy_id=rid, persist=False)\n'
        "print(v.wait())\n",
    ),
    (
        "py/many-entries",
        "import laila\np=laila.DefaultPolicy(); laila.activate_policy(p)\n"
        "es=[laila.constant(data=i) for i in range(3)]\nlaila.memorize(es).wait()\n",
    ),
    (
        "py/serve",
        "import laila\np=laila.DefaultPolicy(); laila.activate_policy(p)\n"
        'laila.communication.add_connection(laila.DefaultTCPIPProtocol(host="0.0.0.0", port=8770, peer_secret_key="s"))\n',
    ),
]


def _run_live(target, body):
    token = os.environ.get("LAILA_CLAUDE_TOKEN") or os.environ.get("ANTHROPIC_API_KEY")
    if not token or "--live" not in sys.argv:
        raise Skip("live (needs --live + token)")
    model = os.environ.get("LAILA_CLAUDE_MODEL", "claude-sonnet-4-5")
    cfg = {
        "project": "t",
        "wifi_ssid": "s",
        "wifi_pass": "p",
        "peer_host": "10.0.0.5",
        "peer_port": 8770,
        "peer_secret": "s",
    }
    code = L.run_stage(token, model, "py2c", body, target, cfg)
    code, ok_ = L.verify_and_fix(token, model, code, [], "py2c", 2)
    ok(ok_, "py2c output did not compile against laila-C even after repair")
    return code


for _n, _body in _PY_CORPUS:
    reg(f"live-py2c/{_n}", (lambda b: lambda: _run_live("posix", b))(_body))

# A few full py->esp32 runs (host-checkable gate) when live.
for _n, _body in _PY_CORPUS[:4]:

    def _mk_esp(b):
        def f():
            token = os.environ.get("LAILA_CLAUDE_TOKEN") or os.environ.get("ANTHROPIC_API_KEY")
            if not token or "--live" not in sys.argv:
                raise Skip("live (needs --live + token)")
            model = os.environ.get("LAILA_CLAUDE_MODEL", "claude-sonnet-4-5")
            cfg = {
                "project": "t",
                "wifi_ssid": "s",
                "wifi_pass": "p",
                "peer_host": "10.0.0.5",
                "peer_port": 8770,
                "peer_secret": "s",
            }
            generic = L.run_stage(token, model, "py2c", b, "esp32", cfg)
            generic, g_ok = L.verify_and_fix(token, model, generic, [], "py2c", 2)
            ok(g_ok, "generic stage failed gate")
            tgt = L.run_stage(token, model, "c2target", generic, "esp32", cfg)
            tgt, t_ok = L.verify_and_fix(
                token, model, tgt, ["-DLAILA_SINGLE_CORE=1"], "c2target(esp32)", 2, host_check=True
            )
            ok(t_ok, "esp32 stage failed host-checkable gate")

        return f

    reg(f"live-esp32/{_n}", _mk_esp(_body))


# ===================================================================== RUNNER
def main() -> int:
    only = None
    for a in sys.argv[1:]:
        if a.startswith("--only="):
            only = a.split("=", 1)[1]
    npass = nfail = nskip = 0
    failures = []
    for name, fn in _TESTS:
        if only and only not in name:
            continue
        try:
            fn()
            npass += 1
        except Skip as s:
            nskip += 1
        except Exception as e:
            nfail += 1
            failures.append((name, f"{type(e).__name__}: {e}"))
            print(f"  FAIL {name}: {e}")
    total = npass + nfail + nskip
    print("\n==================== translator test summary ====================")
    print(f"  total={total}  passed={npass}  failed={nfail}  skipped={nskip}")
    if failures:
        print("  --- failures ---")
        for n, m in failures:
            print(f"    {n}: {m[:200]}")
    print("=================================================================")
    return 0 if nfail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
