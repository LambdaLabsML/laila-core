#!/usr/bin/env bash
# Build and run the laila-C test suite across the platform backends that can run
# on a host (posix cooperative, posix threaded, baremetal single-core), then
# report a matrix. MCU backends (esp32/rp2040/stm32) require their SDK
# toolchains and are compile-only there (CI builds them within those SDKs).
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

declare -a NAMES=("posix-cooperative" "posix-threaded" "baremetal_singlecore")
declare -a CFGS=(
  "-DLAILA_PLATFORM=posix"
  "-DLAILA_PLATFORM=posix -DLAILA_POSIX_THREADED=ON"
  "-DLAILA_PLATFORM=baremetal_singlecore -DLAILA_SINGLE_CORE=ON"
)

pass=0
fail=0
echo "=================== laila-C multi-platform test matrix ==================="
for i in "${!NAMES[@]}"; do
  name="${NAMES[$i]}"
  cfg="${CFGS[$i]}"
  bdir="build_mp_${i}"
  echo
  echo "### [$name] configure+build ($cfg)"
  if ! cmake -S . -B "$bdir" $cfg -DLAILA_BUILD_EXAMPLES=OFF >/dev/null 2>&1; then
    echo "  CONFIGURE FAILED"; fail=$((fail+1)); continue
  fi
  if ! cmake --build "$bdir" -j >/dev/null 2>&1; then
    echo "  BUILD FAILED"; fail=$((fail+1)); continue
  fi
  echo "### [$name] run"
  if "./$bdir/tests/laila_tests" | tail -n 4; then
    pass=$((pass+1))
  else
    echo "  TESTS FAILED"; fail=$((fail+1))
  fi
done

echo
echo "=========================================================================="
echo "platforms passed: $pass   failed: $fail"
echo "MCU backends (esp32, rp2040, stm32): compile within their SDK toolchains."
[ "$fail" -eq 0 ]
