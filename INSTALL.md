# Installing pg-licht

Pre-built packages and binaries for every supported platform. To compile from source
instead, see [BUILD.md](BUILD.md).

Every channel below installs the manual page, so once installed:

```bash
man pg_licht_mcp
```

is the reference for configuration, connection strings, all 40 operations, and MCP client
setup.

## Homebrew (macOS and Linux)

```bash
brew tap sqlambda/pg-licht
brew install pg-licht
```

## Pre-built packages and binaries

Download the latest release for your platform from the
[releases page](https://github.com/sqlambda/pg_licht/releases):

| Platform | File | Format |
|----------|------|--------|
| Debian 13 (Trixie) x86_64 | `pg_licht_mcp-linux-x86_64-debian13.deb` | `.deb` |
| Debian 13 (Trixie) arm64 | `pg_licht_mcp-linux-arm64-debian13.deb` | `.deb` |
| Rocky Linux 9 x86_64 | `pg_licht_mcp-linux-x86_64-rocky9.rpm` | `.rpm` |
| Rocky Linux 9 arm64 | `pg_licht_mcp-linux-arm64-rocky9.rpm` | `.rpm` |
| Linux x86_64 (generic) | `pg_licht_mcp-linux-x86_64.tar.gz` | tarball |
| Linux arm64 (generic) | `pg_licht_mcp-linux-arm64.tar.gz` | tarball |
| macOS Apple Silicon | `pg_licht_mcp-macos-arm64.tar.gz` | tarball |

### Debian / Ubuntu / other apt-based distros (`.deb`)

```bash
sudo apt install ./pg_licht_mcp-linux-x86_64-debian13.deb
```

Use `apt install ./file.deb` rather than `dpkg -i` so apt resolves the declared `libpq5`
dependency automatically. Installs `/usr/bin/pg_licht_mcp`, already on `PATH`.

### Rocky Linux / RHEL / Fedora / other dnf-based distros (`.rpm`)

```bash
sudo dnf install ./pg_licht_mcp-linux-x86_64-rocky9.rpm
```

Same reasoning: `dnf install ./file.rpm` resolves the declared `libpq5` dependency, while
`rpm -i` will not. Installs `/usr/bin/pg_licht_mcp`.

### Generic tarball (any glibc-based Linux, or macOS)

The archive holds a standard prefix tree — `bin/pg_licht_mcp` and
`share/man/man1/pg_licht_mcp.1` — so it can be unpacked directly onto a prefix.

System-wide, into `/usr/local`:

```bash
sudo tar -xzf pg_licht_mcp-linux-x86_64.tar.gz -C /usr/local
```

`/usr/local/bin` is on `PATH` and `/usr/local/share/man` is on the default `MANPATH`, so
both the binary and `man pg_licht_mcp` work immediately.

For a single user, into `~/.local`:

```bash
mkdir -p ~/.local
tar -xzf pg_licht_mcp-linux-x86_64.tar.gz -C ~/.local
```

Add `~/.local/bin` to your `PATH` if it is not already there. Most distributions add
`~/.local/share/man` to the man search path automatically; if `man pg_licht_mcp` does not
find it, export `MANPATH="$HOME/.local/share/man:$MANPATH"`.

## Verify the installation

```bash
command -v pg_licht_mcp     # where it landed
pg_licht_mcp --help         # usage and connection resolution order
man pg_licht_mcp            # the full manual
```

Then point it at a database:

```bash
DATABASE_URL="postgresql://user:pass@host/dbname" pg_licht_mcp
```

It speaks JSON-RPC 2.0 on stdin/stdout and is normally launched by an MCP client rather
than run by hand — see the EXAMPLES section of `man pg_licht_mcp` for Claude Code, Claude
Desktop, and Grok Build setup.

## Uninstall

```bash
brew uninstall pg-licht                 # Homebrew
sudo apt remove pg-licht                # deb
sudo dnf remove pg-licht                # rpm
```

For a tarball install, remove the two files it added:

```bash
sudo rm /usr/local/bin/pg_licht_mcp /usr/local/share/man/man1/pg_licht_mcp.1
```
