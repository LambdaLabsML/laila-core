# laila-C as an ESP-IDF component: the generic core (src/*.cpp) + the esp32 HAL
# backend, compiled against the public headers. Paths are relative to this file
# (laila-c/emulator/components/laila -> laila-c/...).
file(GLOB LAILA_CORE_SRCS ${CMAKE_CURRENT_LIST_DIR}/../../../src/*.cpp)

idf_component_register(
  SRCS ${LAILA_CORE_SRCS}
       ${CMAKE_CURRENT_LIST_DIR}/../../../platform/esp32/esp32_hal.cpp
  INCLUDE_DIRS ${CMAKE_CURRENT_LIST_DIR}/../../../include
               ${CMAKE_CURRENT_LIST_DIR}/../../../hal/include
  REQUIRES esp_timer esp_hw_support esp_wifi esp_netif nvs_flash spiffs lwip)

target_compile_features(${COMPONENT_LIB} PUBLIC cxx_std_17)
target_compile_options(${COMPONENT_LIB} PRIVATE -Wno-unused-parameter)
