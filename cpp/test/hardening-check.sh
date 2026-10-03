#!/usr/bin/env bash
#
# Fails unless the binary carries the hardening SafetyFlags.cmake asks for:
#   PIE, full RELRO (GNU_RELRO plus BIND_NOW), a non-executable stack,
#   the stack protector, and at least one fortified call.
#
# Measured on the 4.5 release binary, none of this was certain: PIE only
# because Debian's GCC defaults to it, RELRO without BIND_NOW, no fortified
# calls, no stack protector. A flag that silently stops applying -- a
# toolchain change, a check that starts failing -- is what this catches.
#
# ELF only (Linux, FreeBSD); readelf and nm from binutils or elftoolchain.
# Registered as a ctest for optimised, non-sanitizer builds, and run by the
# release workflow on every Linux artifact it ships.
#
# Usage: hardening-check.sh [--fortify-optional] <binary>
#
# --fortify-optional reports fortified calls without requiring them. A call is
# fortified only where the compiler knows the destination's size but not the
# length, and which calls qualify depends on the compiler: GCC and clang 22
# find some in this code, Ubuntu 24.04's clang 18 finds none, though it does
# fortify a program that has one (checked 2026-10-02). The binaries the
# release ships are GCC's, and release.yml checks them without the option.
set -euo pipefail
export LC_ALL=C   # readelf localises its output, and this matches English words

fortify_required=yes
if [ "${1:-}" = "--fortify-optional" ]; then fortify_required=no; shift; fi
bin=$1
[ -f "$bin" ] || { echo "no such binary: $bin" >&2; exit 2; }

fail=0
check() {
  local what=$1 ok=$2
  if [ "$ok" = yes ]; then echo "ok    $what"; else echo "FAIL  $what"; fail=1; fi
}
has() { grep -qE "$1" && echo yes || echo no; }

command -v readelf >/dev/null || { echo "hardening-check: readelf not found (binutils)" >&2; exit 1; }
dyn=$(readelf -d "$bin" 2>/dev/null)
hdr=$(readelf -h "$bin" 2>/dev/null)
seg=$(readelf -lW "$bin" 2>/dev/null)
syms=$(nm -D "$bin" 2>/dev/null || true)

check "PIE (ELF type DYN)"                   "$(echo "$hdr" | has 'Type:[[:space:]]+DYN')"
check "RELRO (GNU_RELRO segment)"            "$(echo "$seg" | has 'GNU_RELRO')"
check "full RELRO (BIND_NOW)"                "$(echo "$dyn" | has '\(BIND_NOW\)|\(FLAGS\).*NOW|\(FLAGS_1\).*NOW')"
# GNU_STACK present and not executable: its flags column is RW, never RWE.
stack=$(echo "$seg" | awk '/GNU_STACK/ {print $(NF-1)}')
check "non-executable stack (GNU_STACK $stack)" "$( [ -n "$stack" ] && [ "${stack#*E}" = "$stack" ] && echo yes || echo no)"
check "stack protector (__stack_chk_fail)"  "$(echo "$syms" | has '__stack_chk_fail')"
# Fortify is glibc's for C++: FreeBSD's libc fortifies C only (ssp/ssp.h is
# wrapped in !defined(__cplusplus)), so a C++ binary there has no __*_chk
# calls whatever the flags -- measured on 15.1. Required where libc is glibc,
# which is every binary the release ships.
if echo "$dyn" | grep -q 'Shared library: \[libc\.so\.6\]'; then
  found=$(echo "$syms" | has '__[a-z_]+_chk([@ ]|$)')
  if [ "$fortify_required" = yes ] || [ "$found" = yes ]; then
    check "fortified calls (__*_chk)"       "$found"
  else
    echo "n/a   fortified calls: none qualified with this compiler (--fortify-optional)"
  fi
else
  echo "n/a   fortified calls: this libc does not fortify C++ (only glibc does)"
fi

exit $fail
