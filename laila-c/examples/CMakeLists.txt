add_executable(example_data_types data_types.cpp)
target_link_libraries(example_data_types PRIVATE laila_core)

add_executable(example_esp32_peer_request esp32_peer_request/esp32_main.cpp)
target_link_libraries(example_esp32_peer_request PRIVATE laila_core)

# Coroutine (co_await) mirror of tutorial 07. Only this TU needs C++20; the
# laila_core library stays C++17.
add_executable(example_async_futures async_futures.cpp)
target_link_libraries(example_async_futures PRIVATE laila_core)
target_compile_features(example_async_futures PRIVATE cxx_std_20)
