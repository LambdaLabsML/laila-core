cmake_minimum_required(VERSION 3.16)
project(laila_c LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ---- Options (translation/targeting knobs) ----
set(LAILA_PLATFORM "posix" CACHE STRING "Target HAL backend: posix|esp32|rp2040|stm32|baremetal_singlecore")
option(LAILA_SINGLE_CORE "Use the cooperative single-core executor strategy" OFF)
option(LAILA_NO_EXCEPTIONS "Build without C++ exceptions; APIs return laila_status" OFF)
option(LAILA_BUILD_TESTS "Build host unit tests" ON)
option(LAILA_BUILD_EXAMPLES "Build examples" ON)
# Per-pool feature flags mirror laila's pip extras (pyproject.toml
# [project.optional-dependencies]): LAILA_WITH_S3=ON is the C analog of
# `pip install "laila-core[s3]"` (pulls aws-sdk-cpp, host/posix only).
option(LAILA_WITH_S3 "Build the aws-sdk-cpp-backed S3Pool ([s3] extra; host/posix only)" OFF)

if(LAILA_NO_EXCEPTIONS)
  add_compile_options(-fno-exceptions -fno-rtti)
  add_compile_definitions(LAILA_NO_EXCEPTIONS=1)
endif()
if(LAILA_SINGLE_CORE)
  add_compile_definitions(LAILA_SINGLE_CORE=1)
endif()

add_compile_options(-Wall -Wextra -Wno-unused-parameter)

# ---- Generic, platform-independent core ----
file(GLOB LAILA_CORE_SOURCES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
add_library(laila_core STATIC ${LAILA_CORE_SOURCES})
target_include_directories(laila_core PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/hal/include)

# ---- Platform HAL backend ----
add_subdirectory(platform/${LAILA_PLATFORM})
target_link_libraries(laila_core PUBLIC laila_hal_${LAILA_PLATFORM})

# ---- Optional: real S3 via aws-sdk-cpp ([s3] extra; host/posix only) ----
# When enabled, compiles the aws-sdk-cpp-backed BotoClient (src/aws/) and links
# the SDK so S3Pool talks to real S3. Off by default and never used on MCU
# targets, which keep raising LAILA_UNSUPPORTED from S3Pool::_get_client.
if(LAILA_WITH_S3)
  find_package(AWSSDK REQUIRED COMPONENTS s3)
  target_sources(laila_core PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/aws/aws_s3_client.cpp)
  target_link_libraries(laila_core PUBLIC ${AWSSDK_LINK_LIBRARIES})
  target_compile_definitions(laila_core PUBLIC LAILA_WITH_S3=1)
endif()

if(LAILA_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
if(LAILA_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()
