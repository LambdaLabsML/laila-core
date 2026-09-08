option(LAILA_POSIX_THREADED "Use a real worker-thread pool executor on POSIX" OFF)

add_library(laila_hal_posix STATIC posix_hal.cpp)
target_include_directories(laila_hal_posix PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/../../include
  ${CMAKE_CURRENT_SOURCE_DIR}/../../hal/include)
find_package(Threads REQUIRED)
target_link_libraries(laila_hal_posix PUBLIC Threads::Threads)
if(LAILA_POSIX_THREADED)
  target_compile_definitions(laila_hal_posix PRIVATE LAILA_POSIX_THREADED=1)
endif()
