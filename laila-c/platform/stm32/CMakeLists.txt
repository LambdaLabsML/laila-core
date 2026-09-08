# STM32 (CMSIS / STM32 HAL + FreeRTOS) backend.
#
# Compiles within an STM32 firmware project that provides the CMSIS device
# header and FreeRTOS. The clock uses HAL_GetTick(); entropy uses the HAL RNG
# when LAILA_STM32_HAS_RNG is defined, else a counter fallback. Storage is
# in-RAM (internal-flash persistence is a follow-up).
add_library(laila_hal_stm32 STATIC stm32_hal.cpp)
target_include_directories(laila_hal_stm32 PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/../../include
  ${CMAKE_CURRENT_SOURCE_DIR}/../../hal/include)
# The firmware project supplies the CMSIS/HAL/FreeRTOS include paths and the
# arm-none-eabi toolchain (via CMAKE_TOOLCHAIN_FILE).
