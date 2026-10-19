# Security boundary

This service runs as the launching user and can index files readable by that
user. Only use it with trusted local clients and reviewed project configs.
Loopback binding does not protect against untrusted local processes or an
explicit reverse proxy/port forward. Public multi-user hosting is unsupported.

Enable authentication with MEMSVC_AUTH=1 and MEMSVC_API_TOKEN (setting the token
alone also enables it; auth enabled without a token fails startup). Bearer tokens
are compared in constant time. Writes require authentication; set
MEMSVC_READ_REQUIRES_TOKEN=1 to require it for reads too. /api/health and
/api/stats stay public and can expose operational metadata.
Registration accepts local roots/configs and is filesystem authority. There is no
sandboxed directory allowlist: don't grant API access to untrusted users.

POST routes authenticate before reading, limit bodies to 1 MiB, reject duplicate
or malformed lengths and transfer encodings, and time out socket reads after
10 seconds. These controls do not make http.server a public production gateway.

Never commit .env/.alert.env, runtime databases, backups or session transcripts.
Before publishing, inspect Git history with a secret scanner and rotate any
previously exposed credentials. The current-source inspection found no confirmed
live secret; it does not certify Git history or historical private content.
