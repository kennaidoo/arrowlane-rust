# Security policy

## Supported versions

The latest commit on `main` is the supported version. Older commits are not
supported.

## Reporting a vulnerability

Report privately through GitHub. Open the repository's Security tab and choose
Report a vulnerability. Do not open a public issue.

Include the affected commit, the steps to reproduce, and the impact.

We review reports on a best-effort basis. There is no service-level agreement.

## Scope

This library parses untrusted wire frames, shared memory descriptors, Arrow IPC
payloads, and NDJSON payloads. The `shm` lane is POSIX only. Report memory safety
issues, panics on untrusted input, FFI boundary defects, and bypasses of the
frame length or checksum checks.
