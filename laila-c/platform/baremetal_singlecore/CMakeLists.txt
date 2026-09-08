# Generic single-core / bare-metal backend. Uses only freestanding-friendly
# standard headers (no OS, no threads, no filesystem), so it also builds and
# runs on a host for testing the cooperative path. Storage is in-RAM.
add_library(laila_hal_baremetal_singlecore STATIC baremetal_hal.cpp)
target_include_directories(laila_hal_baremetal_singlecore PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/../../include
  ${CMAKE_CURRENT_SOURCE_DIR}/../../hal/include)
