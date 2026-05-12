# SYNCED FROM nx1-devtools — do not edit directly; repo-specific overrides go in a file without this marker
---
name: audit-security
description: Run a security audit on specified files or recent changes
disable-model-invocation: true
context: fork
allowed-tools: Bash, Read, Glob
argument-hint: [file-or-directory]
---

Run a security audit on the specified files or directory. Works with any language or framework.

## Arguments

- `$ARGUMENTS` — file path, directory, or blank for recent changes (defaults to auditing `git diff --name-only HEAD~1`)

## Instructions

1. If arguments are provided, audit those specific files/directories
2. If no arguments, identify recently changed files via `git diff --name-only HEAD~1` and audit those
3. Detect the languages in scope from file extensions:
   - `.py` → Python
   - `.ts`, `.tsx`, `.js`, `.jsx` → TypeScript/JavaScript
   - `.java` → Java
   - `.scala` → Scala
   - `.go` → Go
   - `.sh`, `.bash` → Shell
   - `.tf`, `.hcl` → Terraform
   - `Dockerfile` → Docker
4. Apply the audit checks below for each detected language

## Audit Checks

### All Languages

- Hardcoded secrets (API keys, passwords, tokens, connection strings)
- Environment variable exposure in logs or error messages
- Insecure file permissions or path traversal
- Sensitive data in comments or dead code

### Python

- SQL injection (raw string queries, unsanitized f-strings in SQL)
- Command injection (`subprocess` with `shell=True`, `os.system`)
- Insecure deserialization (`pickle.loads`, `yaml.load` without SafeLoader)
- SSRF (unvalidated URLs in `requests`/`httpx` calls)
- Weak cryptography (`md5`, `sha1` for security purposes)

### TypeScript / JavaScript

- XSS (dangerouslySetInnerHTML, unescaped user input in templates)
- Prototype pollution
- Insecure cookie settings (missing httpOnly, secure, sameSite)
- Client-side secrets exposure (API keys in browser-accessible code)
- Open redirects (unvalidated redirect URLs)

### Java / Scala

- SQL injection (string concatenation in queries)
- XML external entity (XXE) attacks
- Insecure deserialization (ObjectInputStream)
- Path traversal in file operations
- Weak cryptography configurations

### Go

- SQL injection (string formatting in queries instead of parameterized)
- Command injection (exec.Command with user input)
- Race conditions (shared state without proper synchronization)
- Insecure TLS configuration

### Shell

- Command injection (unquoted variables, eval with user input)
- Insecure temp file creation (predictable names in /tmp)
- Missing input validation

### Terraform / Docker

- Overly permissive IAM policies or security groups
- Secrets in build args or environment variables
- Running containers as root
- Exposed ports without justification

## Output Format

Group findings by severity:

**CRITICAL** — Exploitable vulnerabilities requiring immediate fix.

**HIGH** — Security weaknesses that should be addressed before merge.

**MEDIUM** — Best-practice violations that increase attack surface.

**LOW** — Minor improvements and hardening suggestions.

For each finding: file path, line number(s), description, and recommended fix.
