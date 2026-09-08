# Test binary = the framework + runner + every suite_*.cpp.
file(GLOB LAILA_SUITE_SOURCES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/suite_*.cpp)
add_executable(laila_tests test_runner.cpp laila_test.cpp ${LAILA_SUITE_SOURCES})
target_link_libraries(laila_tests PRIVATE laila_core)
# suite_async.cpp uses laila/async.hpp (C++20 coroutines). laila_core itself
# stays C++17; only the test binary is compiled at C++20.
target_compile_features(laila_tests PRIVATE cxx_std_20)
# The comm suite spins a localhost TCP echo server (std::thread) on host builds.
find_package(Threads REQUIRED)
target_link_libraries(laila_tests PRIVATE Threads::Threads)
# Cross-language vectors (real output of the Python laila lib; see gen_vectors.py).
target_compile_definitions(laila_tests PRIVATE
  LAILA_VECTORS_DIR="${CMAKE_CURRENT_SOURCE_DIR}/vectors")

# Full run (enforces the >=1000-assertion minimum) plus one CTest entry per
# suite, all labeled with the active platform so CI can slice by platform.
add_test(NAME laila_tests_all COMMAND laila_tests)
set_tests_properties(laila_tests_all PROPERTIES LABELS "laila;platform_${LAILA_PLATFORM}")

set(LAILA_TEST_SUITES json identity value transform constitution entry pool future policy peers comm xlang compdata interop platform stress manifest async)
foreach(s ${LAILA_TEST_SUITES})
  add_test(NAME laila_suite_${s} COMMAND laila_tests --suite ${s})
  set_tests_properties(laila_suite_${s} PROPERTIES LABELS "laila;suite_${s};platform_${LAILA_PLATFORM}")
endforeach()

# Cross-language interop harness binary, driven by tests/interop/run_interop.py
# to peer a laila-C policy with an (unmodified) Python `laila` policy over a real
# WebSocket connection (memorize / remember / forget, bi- and tri-directional).
add_executable(c_harness interop/c_harness.cpp)
target_link_libraries(c_harness PRIVATE laila_core Threads::Threads)
