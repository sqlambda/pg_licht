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

# Warnings as errors: on by default in a git checkout, off otherwise.
#
# Hard-coded until 4.6, which made a new warning in a compiler newer than CI's
# a build that fails for whoever builds from source -- a Homebrew user on a new
# Xcode (the formula builds the release tarball on the user's machine), or a
# FreeBSD user on a newer clang. Neither can act on a warning. A checkout is
# where someone can, so that is where the zero-warning policy is enforced; and
# CI passes -DPGLICHT_WERROR=ON explicitly in every job, so the policy does not
# hang on how the runner checks the repository out.
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/../.git")
    set(_pglicht_werror_default ON)
else()
    set(_pglicht_werror_default OFF)
endif()
option(PGLICHT_WERROR "Treat compiler warnings as errors" ${_pglicht_werror_default})

# Always-on static hygiene: the warning set + hardened standard library.
function(pglicht_harden target)
    target_compile_options(${target} PRIVATE
        -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion
        -Wuninitialized -Wshadow
        # Added in 4.6, measured first: on GCC 14 and clang 22 they found
        # three casts to std::string of an expression that already was one,
        # and two C-style casts in the tests -- fixed in the same change.
        -Wnull-dereference -Wformat=2 -Wimplicit-fallthrough -Wold-style-cast
        -Wnon-virtual-dtor -Woverloaded-virtual -Wcast-qual -Wdouble-promotion
    )
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        target_compile_options(${target} PRIVATE -Wuseless-cast)
    elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        target_compile_options(${target} PRIVATE -Wextra-semi)
    endif()
    if(PGLICHT_WERROR)
        target_compile_options(${target} PRIVATE -Werror)
    endif()
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        # ~zero-cost bounds/precondition checks in libstdc++ (e.g. vector::operator[]).
        target_compile_definitions(${target} PRIVATE _GLIBCXX_ASSERTIONS)
    elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        target_compile_definitions(${target} PRIVATE _LIBCPP_HARDENING_MODE=_LIBCPP_HARDENING_MODE_FAST)
    endif()
endfunction()

# Hardening the binary itself: what the compiler and linker can add so a memory
# bug that slips past the tests is harder to exploit. Until 4.6 none of it was
# set, because CMake does not apply a distribution's default flags the way a
# package build does -- measured on the 4.5 release binary: PIE only because
# Debian's GCC defaults to it, GNU_RELRO without BIND_NOW, no fortified calls
# and no stack protector, in every deb, rpm and tarball.
#
# Every flag goes through a check, because the release matrix is three
# toolchains on two architectures: -fcf-protection is x86-64 only, Apple's ld
# rejects every -z option, and a flag a compiler does not know would fail the
# build under -Werror rather than be ignored.
include(CheckCXXCompilerFlag)
include(CheckLinkerFlag)
include(CheckPIESupported)
check_pie_supported(OUTPUT_VARIABLE _pglicht_pie_msg LANGUAGES CXX)

# Checked with -Werror: Apple clang accepts -fstack-clash-protection on arm64
# with only "argument unused during compilation", so a plain check passed and
# the -Werror build then failed on it -- found by CI's macOS job on 4.6's
# first push. A flag the compiler would only warn about is unsupported here.
set(_pglicht_saved_required_flags "${CMAKE_REQUIRED_FLAGS}")
set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} -Werror")
foreach(_flag -fstack-protector-strong -fstack-clash-protection -fcf-protection
              -ftrivial-auto-var-init=zero)
    string(MAKE_C_IDENTIFIER "PGLICHT_HAS${_flag}" _var)
    check_cxx_compiler_flag(${_flag} ${_var})
endforeach()
set(CMAKE_REQUIRED_FLAGS "${_pglicht_saved_required_flags}")
foreach(_flag -Wl,-z,relro -Wl,-z,now -Wl,-z,noexecstack)
    string(MAKE_C_IDENTIFIER "PGLICHT_HAS${_flag}" _var)
    check_linker_flag(CXX ${_flag} ${_var})
endforeach()

function(pglicht_harden_binary target)
    # PIE whether or not the compiler defaults to it. CMake adds -fPIE and
    # -pie only once check_pie_supported has run, above. Everything linked in
    # must be position-independent too, which is why the workflows build
    # their static libpqxx with CMAKE_POSITION_INDEPENDENT_CODE=ON: Rocky's
    # gcc-toolset does not default to PIE the way Debian's GCC does.
    set_property(TARGET ${target} PROPERTY POSITION_INDEPENDENT_CODE ON)
    foreach(_flag -fstack-protector-strong -fstack-clash-protection -fcf-protection)
        string(MAKE_C_IDENTIFIER "PGLICHT_HAS${_flag}" _var)
        if(${_var})
            target_compile_options(${target} PRIVATE ${_flag})
        endif()
    endforeach()
    foreach(_flag -Wl,-z,relro -Wl,-z,now -Wl,-z,noexecstack)
        string(MAKE_C_IDENTIFIER "PGLICHT_HAS${_flag}" _var)
        if(${_var})
            target_link_options(${target} PRIVATE ${_flag})
        endif()
    endforeach()
    # Fortified string and memory calls, checked against the object sizes the
    # compiler can see. Optimised builds only: glibc warns without
    # optimisation, which -Werror makes fatal. Never with a sanitizer: ASan
    # intercepts the same calls, and the two get in each other's way. -U first,
    # so a packager who already sets it gets no redefinition warning. Level 3
    # needs GCC 12 or clang 16 for __builtin_dynamic_object_size; older ones
    # fall back to level 2 on their own.
    if(PGLICHT_SANITIZER STREQUAL "NONE")
        target_compile_options(${target} PRIVATE
            $<$<CONFIG:Release,RelWithDebInfo,MinSizeRel>:-U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=3>)
    endif()
    # Locals the code never initialises start as zero rather than as whatever
    # the stack held, so a read the code gets wrong is a predictable zero, not
    # stale data from an earlier call. Release and MinSizeRel only -- what the
    # release workflow builds. Never RelWithDebInfo, which CI's valgrind job
    # runs, nor Debug or a sanitizer build: there it would hide the
    # uninitialised reads those tools exist to find.
    if(PGLICHT_SANITIZER STREQUAL "NONE" AND PGLICHT_HAS_ftrivial_auto_var_init_zero)
        target_compile_options(${target} PRIVATE
            $<$<CONFIG:Release,MinSizeRel>:-ftrivial-auto-var-init=zero>)
    endif()
endfunction()

# Registers the check that the hardening above actually reached the binary:
# readelf and nm on what was linked, failing unless every property is there.
# Only where it can hold -- an optimised build (fortify needs one), no
# sanitizer, and an ELF platform with the tools -- so a Debug developer build
# or macOS does not register a test that cannot pass.
find_program(READELF_EXECUTABLE readelf)
find_program(NM_EXECUTABLE nm)
function(pglicht_add_hardening_test target)
    if(NOT PGLICHT_SANITIZER STREQUAL "NONE" OR APPLE OR NOT READELF_EXECUTABLE OR NOT NM_EXECUTABLE)
        return()
    endif()
    if(NOT CMAKE_BUILD_TYPE MATCHES "^(Release|RelWithDebInfo|MinSizeRel)$")
        return()
    endif()
    # Fortified calls are required of GCC builds only: which calls qualify is
    # the compiler's choice (see the script), and the release ships GCC's.
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        set(_fortify "")
    else()
        set(_fortify "--fortify-optional")
    endif()
    add_test(NAME ${target}_hardening
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test/hardening-check.sh ${_fortify} $<TARGET_FILE:${target}>)
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
