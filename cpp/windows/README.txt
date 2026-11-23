pg_licht_mcp for Windows (x86_64)

A read-only PostgreSQL MCP server. It speaks JSON-RPC 2.0 on standard input
and output, and is started by an MCP client, not by hand.

This archive holds one program, pg_licht_mcp.exe, which needs nothing beside
it: put it wherever you keep programs and give your MCP client its full path.

  pg_licht_mcp.exe --version
  pg_licht_mcp.exe --help

The manual, with the configuration file, every operation and the client
setup, is at:

  https://sqlambda.github.io/pg_licht/

What differs on Windows:

  - The default files are %APPDATA%\pg_licht\connections.ini and
    %APPDATA%\pg_licht\budgets.ini, beside the %APPDATA%\postgresql\ where
    libpq keeps pgpass.conf.
  - The permissions of those files are not tested, as libpq does not test
    its password file on Windows. Keep them in %APPDATA%, which only your
    own account can read.
  - There is no SIGHUP. A changed connections file is read at the next
    request.

To check the download: SHA256SUMS on the release page, or
  gh attestation verify pg_licht_mcp-windows-x86_64.zip --repo sqlambda/pg_licht

Source, issues and the licence: https://github.com/sqlambda/pg_licht
