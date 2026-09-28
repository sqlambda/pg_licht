cmake_minimum_required(VERSION 3.20)
project(pg_licht_mcp VERSION 4.5.0 LANGUAGES CXX)

# Standard bin/ and share/man/ layout, so `cmake --install` produces a tree that
# can be dropped onto a prefix as-is: the release tarballs are staged this way,
# CPack lays the same tree under /usr for the deb and rpm, and Homebrew installs
# it into its keg. In every case share/man/man1 is already on the default
# MANPATH, so `man pg_licht_mcp` works with no further setup.
include(GNUInstallDirs)

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

# The two registries compile as their own translation units. They are the
# largest literal blocks in the project -- 62 tool definitions and 12 prompts --
# and nothing else needs recompiling when one of them is edited.
set(PGLICHT_REGISTRY_SOURCES src/tool_defs.cpp src/prompt_defs.cpp)

add_executable(pg_licht_mcp src/main.cpp ${PGLICHT_REGISTRY_SOURCES})
target_compile_definitions(pg_licht_mcp PRIVATE PGLICHT_VERSION="${PROJECT_VERSION}")
target_link_libraries(pg_licht_mcp
    PRIVATE
        nlohmann_json::nlohmann_json
        PkgConfig::PQXX
        PkgConfig::LIBPQ
)
pglicht_harden(pg_licht_mcp)
pglicht_apply_sanitizers(pg_licht_mcp)
install(TARGETS pg_licht_mcp RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
# The man page is checked in rather than generated: the release containers
# (debian:trixie, rockylinux:9) carry no doc toolchain, and the release build
# runs `--target pg_licht_mcp` rather than `all`, so a generator target would
# never run and CPack would then fail on a missing install file.
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/man/pg_licht_mcp.1
        DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)

option(BUILD_TESTING "Build the test suite" ON)
if(BUILD_TESTING)
    enable_testing()
    find_package(GTest REQUIRED)
    add_executable(pg_licht_mcp_test src/test_main.cpp ${PGLICHT_REGISTRY_SOURCES})
    target_compile_definitions(pg_licht_mcp_test PRIVATE
        PGLICHT_VERSION="${PROJECT_VERSION}"
        # The shipped example connections file, so a test can prove it still
        # parses rather than leaving it to rot alongside the parser.
        PGLICHT_EXAMPLE_INI="${CMAKE_CURRENT_SOURCE_DIR}/test/connections.example.ini"
        PGLICHT_EXAMPLE_BUDGETS="${CMAKE_CURRENT_SOURCE_DIR}/test/budgets.example.ini"
        # The reference's example call for every tool, so a test can drive each
        # one through real dispatch with the arguments the documentation shows.
        PGLICHT_REFERENCE_EXAMPLES="${CMAKE_CURRENT_SOURCE_DIR}/../tools/reference/examples.json")
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

    # Since the man page is hand-written, lint it here rather than trusting
    # review. -Wwarning (not -Wall) is deliberate: it fails on real WARNINGs but
    # ignores STYLE notes such as "referenced manual not found: Xr libpq 3",
    # which fire on any machine without PostgreSQL's own man pages installed.
    find_program(MANDOC_EXECUTABLE mandoc)
    if(MANDOC_EXECUTABLE)
        add_test(NAME manpage_lint
                 COMMAND ${MANDOC_EXECUTABLE} -Tlint -Wwarning
                         ${CMAKE_CURRENT_SOURCE_DIR}/man/pg_licht_mcp.1)
    else()
        message(STATUS "mandoc not found: skipping the manpage_lint test")
    endif()

    # The command line. --version must print the version this was built as,
    # and an unknown option must be refused rather than taken for a connection
    # string -- which is what --version itself was through 4.3.3, starting a
    # server that exited silently. Neither touches a database.
    add_test(NAME cli_version COMMAND $<TARGET_FILE:pg_licht_mcp> --version)
    set_tests_properties(cli_version PROPERTIES
        PASS_REGULAR_EXPRESSION "^pg_licht_mcp ${PROJECT_VERSION}\n$")
    add_test(NAME cli_unknown_option COMMAND $<TARGET_FILE:pg_licht_mcp> --no-such-option)
    set_tests_properties(cli_unknown_option PROPERTIES
        PASS_REGULAR_EXPRESSION "unknown option: --no-such-option")

    # The response-shape contract, end to end against the real binary. Both
    # switches it covers are read once per process from the environment, so
    # they cannot be exercised from inside the gtest suite. It needs no
    # database: listConnections answers from the registry without connecting.
    add_test(NAME protocol_compat
             COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test/protocol-compat.sh
                     $<TARGET_FILE:pg_licht_mcp>)

    # llms.txt, generated from the binary as the release workflow does, so a
    # tool without a man page entry -- or a generator broken by a change to
    # tools/list -- fails here rather than at tag time. Needs mandoc to render
    # the man page the entries link into, and python3 for the generator.
    find_package(Python3 COMPONENTS Interpreter QUIET)
    if(MANDOC_EXECUTABLE AND Python3_Interpreter_FOUND)
        add_test(NAME llms_txt
                 COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test/llms-txt.sh
                         $<TARGET_FILE:pg_licht_mcp>)
        # The HTML reference and landing page, built as the release builds
        # them: every mocked example checked against its tool's schema, every
        # tool and prompt with a page, every internal link resolving.
        add_test(NAME reference
                 COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test/reference.sh
                         $<TARGET_FILE:pg_licht_mcp>)
    else()
        message(STATUS "mandoc or python3 not found: skipping the llms_txt and reference tests")
    endif()
endif()

message(STATUS "Using compiler: ${CMAKE_CXX_COMPILER} (${CMAKE_CXX_COMPILER_ID})")
message(STATUS "Requested C++ standard: ${CMAKE_CXX_STANDARD}")

set(CPACK_PACKAGE_NAME "pg-licht")
set(CPACK_PACKAGE_VERSION "${PROJECT_VERSION}")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "PostgreSQL MCP server for schema exploration")
set(CPACK_PACKAGE_VENDOR "sqlambda")
set(CPACK_PACKAGE_CONTACT "https://github.com/sqlambda/pg_licht")
set(CPACK_PACKAGE_HOMEPAGE_URL "https://github.com/sqlambda/pg_licht")

# libpqxx is linked statically and baked into the binary; libpq, libstdc++,
# libgcc and libc stay shared.
#
# The .deb's Depends is DERIVED, not declared: dpkg-shlibdeps reads the
# binary's NEEDED entries and the symbols it actually uses, and turns them into
# minimum versions from each library's symbols file. Measured on the 4.2.2
# binary: libc6 (>= 2.38), libgcc-s1 (>= 4.3), libpq5 (>= 10~~),
# libstdc++6 (>= 14). The hand-written "libpq5" it replaces was incomplete --
# it named none of the C++ runtime -- and carried no version at all. Every libpq
# function the binary calls predates PostgreSQL 10, so Debian's own libpq5
# satisfies it and the postgresql.org repository is not required; a Depends
# that demanded it would be a false dependency. Needs the `file` utility at
# package time, which the release workflow installs.
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
set(CPACK_DEBIAN_PACKAGE_SECTION "database")

# The RPM names PGDG's libpq5 on purpose: the PostgreSQL project's own package
# (standalone in pgdg-common, also provided by postgresqlNN-libs), never the
# distribution's libpq. INSTALL.md says to enable that repository first. The
# C++ runtime and libc requirements are added by rpmbuild's automatic
# dependency scan, as is libpq.so.5()(64bit), which libpq5 provides.
set(CPACK_RPM_PACKAGE_REQUIRES "libpq5")
set(CPACK_RPM_PACKAGE_LICENSE "Apache-2.0")
set(CPACK_RPM_PACKAGE_GROUP "Applications/Databases")

include(CPack)
