cmake_minimum_required(VERSION 3.31)
project(pg_licht_mcp LANGUAGES CXX)

find_package(PkgConfig REQUIRED)

pkg_check_modules(PQXX REQUIRED IMPORTED_TARGET libpqxx)

find_package(nlohmann_json HINTS /usr/lib/x86_64-linux-gnu/pkgconfig)

set(CMAKE_CXX_COMPILER clang++)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-Wconversion -Wsign-conversion -Wuninitialized -Wextra -Wall)

add_executable(pg_licht_mcp src/main.cpp)

target_link_libraries(pg_licht_mcp
    PRIVATE
        nlohmann_json::nlohmann_json
        PkgConfig::PQXX
)

target_compile_options(pg_licht_mcp PRIVATE -Wall -Wextra -Wpedantic)

enable_testing()
find_package(GTest REQUIRED)

add_executable(pg_licht_mcp_test src/test_main.cpp)
target_link_libraries(pg_licht_mcp_test
    PRIVATE
        GTest::gtest
        GTest::gtest_main
        nlohmann_json::nlohmann_json
        PkgConfig::PQXX)

target_compile_options(pg_licht_mcp_test PRIVATE -Wall -Wextra -Wpedantic)

add_test(NAME mcp_test COMMAND pg_licht_mcp_test)

message(STATUS "Using compiler: ${CMAKE_CXX_COMPILER} (${CMAKE_CXX_COMPILER_ID})")
message(STATUS "Requested C++ standard: ${CMAKE_CXX_STANDARD}")
