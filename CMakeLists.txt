# ============================================================================
# SomeIpGateway CMake（可选）
# ============================================================================
cmake_minimum_required(VERSION 3.16)
project(SomeIpGateway LANGUAGES CXX C VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_compile_options(-Wall -Wextra -Wno-unused-parameter -O2)

add_executable(someip_gateway
    src/main.cpp
    src/PersistQueue.cpp
)
target_include_directories(someip_gateway PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(someip_gateway PRIVATE pthread)

add_executable(bench_client scripts/bench_client.c)
