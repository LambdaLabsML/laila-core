# ESP32 (ESP-IDF) HAL backend.
#
# laila-C is normally consumed from an ESP-IDF project as a component, or built
# within the IDF toolchain. This target compiles esp32_hal.cpp against the IDF
# headers (esp_timer, esp_random, FreeRTOS, and a mounted VFS for storage).
# Outside an IDF build this configuration cannot resolve those headers; that is
# expected (the translation step targets ESP-IDF for the esp32 platform).
add_library(laila_hal_esp32 STATIC esp32_hal.cpp)
target_include_directories(laila_hal_esp32 PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/../../include
  ${CMAKE_CURRENT_SOURCE_DIR}/../../hal/include)

if(NOT DEFINED ENV{IDF_PATH})
  message(WARNING
    "laila esp32 backend: ESP-IDF not detected (IDF_PATH unset). "
    "Build this target within an ESP-IDF project/toolchain.")
endif()
