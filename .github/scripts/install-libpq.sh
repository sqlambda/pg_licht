#!/usr/bin/env bash
# libpq from PGDG, for the jobs that only *run* the test binary.
#
# libpqxx is linked statically into that binary, so nothing here needs cmake,
# gtest, nlohmann or libpqxx itself -- only the one shared library left dynamic.
# PGDG rather than the distro's own package because the binary is built against
# PGDG's libpq headers, and matching the two removes any question of a symbol
# the older library does not carry.
set -euo pipefail

sudo apt-get install -y curl ca-certificates lsb-release
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
  | sudo gpg --dearmor -o /usr/share/keyrings/pgdg.gpg
echo "deb [signed-by=/usr/share/keyrings/pgdg.gpg] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
  | sudo tee /etc/apt/sources.list.d/pgdg.list
sudo apt-get update -q
sudo apt-get install -y libpq5
