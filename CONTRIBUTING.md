# Contributing

This repository is the Rust implementation of arrowlane. The Python and Zig
implementations live in separate repositories.

## Before you start

- Open an issue for behaviour, wire-format or platform changes.
- Keep one concern per pull request.
- Include a test for logic changes, or state why no test is needed.
- Do not change a lane id. Add a new id instead.

## AI-assisted work

Disclose the tool and the extent of the assistance. You must understand the
code you submit. Low-effort generated changes will be closed.

## Review

Review is best effort. There is no service-level agreement. A change is ready
when the commands in AGENTS.md pass.

## Commit messages

Use `type(scope): description`, for example:

```text
fix(frame): reject a truncated payload before allocation
```

## Security

Do not report a vulnerability in a public issue. Follow SECURITY.md.
