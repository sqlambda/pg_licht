#!/usr/bin/env bash
# Install packages from PGDG, non-interactively and without waiting forever.
#
# Two ways an apt step on a hosted runner hangs indefinitely rather than
# failing, both of which cost a full six-hour job timeout when they happen:
#
#   * gpg --dearmor prompts "File exists. Overwrite?" and blocks on stdin if
#     the keyring is already present, which depends on the runner image.
#   * apt waits on /var/lib/dpkg/lock-frontend with no timeout when the image's
#     unattended-upgrades is mid-run.
#
# --batch --yes and DPkg::Lock::Timeout turn both into either success or a
# prompt failure. Every job also carries timeout-minutes as the backstop.
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
apt_get() { sudo apt-get -o DPkg::Lock::Timeout=300 "$@"; }

apt_get update -q
apt_get install -y curl ca-certificates lsb-release

curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
  | sudo gpg --batch --yes --dearmor -o /usr/share/keyrings/pgdg.gpg
echo "deb [signed-by=/usr/share/keyrings/pgdg.gpg] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
  | sudo tee /etc/apt/sources.list.d/pgdg.list >/dev/null

apt_get update -q
apt_get install -y "$@"
