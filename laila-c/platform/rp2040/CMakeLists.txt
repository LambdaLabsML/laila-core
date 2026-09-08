# RP2040 (Raspberry Pi Pico SDK) HAL backend.
#
# Compiles within a pico-sdk project (pico_stdlib, pico_rand, pico_sync).
# Storage is in-RAM by default; a hardware_flash-backed port is a follow-up.
add_library(laila_hal_rp2040 STATIC rp2040_hal.cpp)
target_include_directories(laila_hal_rp2040 PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/../../include
  ${CMAKE_CURRENT_SOURCE_DIR}/../../hal/include)

if(NOT DEFINED ENV{PICO_SDK_PATH})
  message(WARNING
    "laila rp2040 backend: PICO_SDK_PATH unset. Build within a pico-sdk project.")
else()
  target_link_libraries(laila_hal_rp2040 PUBLIC pico_stdlib pico_rand pico_sync)
endif()
