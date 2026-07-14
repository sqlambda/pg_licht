# SafetyFlags.cmake — static hygiene (always on) + dynamic bug detection (opt-in).
#
# Warnings and hardened-stdlib defines catch what the compiler can prove at
# compile time: uninitialized reads, narrowing conversions, shadowing. They
# cannot see use-after-free, double-free, data races, or signed overflow —
# those only exist at runtime, on whichever execution path a test happens to
# take. Sanitizers and Valgrind cover that dynamic axis; pglicht_harden()
# below is unconditional, pglicht_apply_sanitizers() is selected per build
# via PGLICHT_SANITIZER, and pglicht_add_valgrind_test() gives every test
# binary a Valgrind-wrapped ctest entry for free when sanitizers are off.

set(PGLICHT_SANITIZER "NONE" CACHE STRING
    "Sanitizer to build with: NONE, ADDRESS, UNDEFINED, ADDRESS_UNDEFINED, THREAD")
set_property(CACHE PGLICHT_SANITIZER PROPERTY STRINGS
    NONE ADDRESS UNDEFINED ADDRESS_UNDEFINED THREAD)

find_program(VALGRIND_EXECUTABLE valgrind)

# Always-on static hygiene: warnings-as-errors + hardened standard library.
function(pglicht_harden target)
    target_compile_options(${target} PRIVATE
        -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion
        -Wuninitialized -Wshadow -Werror
    )
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        # ~zero-cost bounds/precondition checks in libstdc++ (e.g. vector::operator[]).
        target_compile_definitions(${target} PRIVATE _GLIBCXX_ASSERTIONS)
    elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        target_compile_definitions(${target} PRIVATE _LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_FAST)
    endif()
endfunction()

# Opt-in dynamic bug detection, selected via -DPGLICHT_SANITIZER=<value>.
function(pglicht_apply_sanitizers target)
    if(PGLICHT_SANITIZER STREQUAL "NONE")
        return()
    elseif(PGLICHT_SANITIZER STREQUAL "ADDRESS")
        set(_flags -fsanitize=address)
    elseif(PGLICHT_SANITIZER STREQUAL "UNDEFINED")
        set(_flags -fsanitize=undefined)
    elseif(PGLICHT_SANITIZER STREQUAL "ADDRESS_UNDEFINED")
        set(_flags -fsanitize=address,undefined)
    elseif(PGLICHT_SANITIZER STREQUAL "THREAD")
        set(_flags -fsanitize=thread)
    else()
        message(FATAL_ERROR "Unknown PGLICHT_SANITIZER: ${PGLICHT_SANITIZER}")
    endif()

    target_compile_options(${target} PRIVATE -g -fno-omit-frame-pointer ${_flags})
    target_link_options(${target} PRIVATE ${_flags})
endfunction()

# Registers a Valgrind-wrapped ctest entry for `target`. Skipped when a
# sanitizer is active (an instrumented binary run under Valgrind produces
# instrumentation conflicts, not real findings) or when Valgrind is absent.
function(pglicht_add_valgrind_test target)
    if(NOT VALGRIND_EXECUTABLE)
        message(STATUS "Valgrind not found — skipping ${target}_valgrind test")
        return()
    endif()
    if(NOT PGLICHT_SANITIZER STREQUAL "NONE")
        message(STATUS "PGLICHT_SANITIZER=${PGLICHT_SANITIZER} active — skipping ${target}_valgrind test")
        return()
    endif()
    add_test(NAME ${target}_valgrind
        COMMAND ${VALGRIND_EXECUTABLE} --error-exitcode=99 --leak-check=full --track-origins=yes
                $<TARGET_FILE:${target}>
    )
endfunction()
