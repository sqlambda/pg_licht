cmake_minimum_required(VERSION 3.20)
project(pg_licht_mcp VERSION 2.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(PkgConfig REQUIRED)
pkg_check_modules(PQXX REQUIRED IMPORTED_TARGET libpqxx)
# libpqxx's .pc only exposes libpq via Requires.private, which CMake's
# IMPORTED_TARGET does not follow (it only reads the public Libs/Requires).
# When libpqxx is linked statically this leaves every PQ* symbol unresolved
# at final link time, so link libpq explicitly via its own module — this is
# a no-op when libpqxx is a shared library (libpq is already a transitive
# runtime dependency there) and required when it's static.
pkg_check_modules(LIBPQ REQUIRED IMPORTED_TARGET libpq)
find_package(nlohmann_json REQUIRED)

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/SafetyFlags.cmake)

add_executable(pg_licht_mcp src/main.cpp)
target_compile_definitions(pg_licht_mcp PRIVATE PGLICHT_VERSION="${PROJECT_VERSION}")
target_link_libraries(pg_licht_mcp
    PRIVATE
        nlohmann_json::nlohmann_json
        PkgConfig::PQXX
        PkgConfig::LIBPQ
)
pglicht_harden(pg_licht_mcp)
pglicht_apply_sanitizers(pg_licht_mcp)
install(TARGETS pg_licht_mcp DESTINATION bin)

option(BUILD_TESTING "Build the test suite" ON)
if(BUILD_TESTING)
    enable_testing()
    find_package(GTest REQUIRED)
    add_executable(pg_licht_mcp_test src/test_main.cpp)
    target_compile_definitions(pg_licht_mcp_test PRIVATE PGLICHT_VERSION="${PROJECT_VERSION}")
    target_link_libraries(pg_licht_mcp_test
        PRIVATE
            GTest::gtest
            GTest::gtest_main
            nlohmann_json::nlohmann_json
            PkgConfig::PQXX
            PkgConfig::LIBPQ)
    pglicht_harden(pg_licht_mcp_test)
    pglicht_apply_sanitizers(pg_licht_mcp_test)
    add_test(NAME mcp_test COMMAND pg_licht_mcp_test)
    pglicht_add_valgrind_test(pg_licht_mcp_test)
endif()

message(STATUS "Using compiler: ${CMAKE_CXX_COMPILER} (${CMAKE_CXX_COMPILER_ID})")
message(STATUS "Requested C++ standard: ${CMAKE_CXX_STANDARD}")

set(CPACK_PACKAGE_NAME "pg-licht")
set(CPACK_PACKAGE_VERSION "${PROJECT_VERSION}")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "PostgreSQL MCP server for schema exploration")
set(CPACK_PACKAGE_VENDOR "sqlambda")
set(CPACK_PACKAGE_CONTACT "https://github.com/sqlambda/pg_licht")
set(CPACK_PACKAGE_HOMEPAGE_URL "https://github.com/sqlambda/pg_licht")

# libpqxx is linked statically and baked into the binary; libpq remains the
# only shared runtime dependency.
set(CPACK_DEBIAN_PACKAGE_DEPENDS "libpq5")
set(CPACK_DEBIAN_PACKAGE_SECTION "database")

set(CPACK_RPM_PACKAGE_REQUIRES "libpq5")
set(CPACK_RPM_PACKAGE_LICENSE "Apache-2.0")
set(CPACK_RPM_PACKAGE_GROUP "Applications/Databases")

include(CPack)
