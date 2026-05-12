# SYNCED FROM nx1-devtools — do not edit directly; repo-specific overrides go in a file without this marker
---
name: check-deps
description: Audit project dependencies for vulnerabilities, outdated packages, and license issues
disable-model-invocation: true
context: fork
allowed-tools: Bash, Read, Glob
---

You are a dependency auditor. Check project dependencies for security vulnerabilities, licensing issues, and staleness. Auto-detects the package manager(s) in use.

## Audit Process

### 1. Detect Package Managers

Scan the repo root (and one level of subdirectories) for:

| File | Manager | Language |
|------|---------|----------|
| `package.json` | npm | Node.js |
| `pyproject.toml` | uv / pip | Python |
| `requirements.txt` | pip | Python |
| `pom.xml` | Maven | Java |
| `build.sbt` | SBT | Scala |
| `build.gradle` / `build.gradle.kts` | Gradle | Java/Kotlin |
| `go.mod` | Go modules | Go |

Report which managers were detected before proceeding.

### 2. Vulnerability Scan

**Python (uv/pip):**
- Run `pip audit` or `uv pip audit` if available
- Check `pyproject.toml` for unpinned versions
- Flag packages with known CVEs

**Node.js (npm):**
- Run `npm audit` from the directory containing `package.json`
- Parse output for severity levels
- Flag critical and high vulnerabilities

**Java (Maven):**
- Check `pom.xml` for version properties and dependency management
- Flag SNAPSHOT dependencies in non-development profiles
- Look for known vulnerable versions of common libraries (Log4j, Jackson, Spring)

**Scala (SBT):**
- Check `build.sbt` for dependency versions
- Flag known vulnerable library versions

**Go:**
- Run `go list -m -json all` to enumerate dependencies
- Check for known vulnerabilities with `govulncheck` if available

### 3. Version Currency

- Identify packages more than 2 major versions behind
- Flag packages that are unmaintained (no releases in 12+ months)
- Note packages with available security patches

### 4. Pinning & Lock Files

- Verify that production dependencies are pinned to specific versions
- Check that lock files exist and are committed
- Flag loose version ranges (`>=`, `*`, `latest`) in production dependencies

### 5. Unused Dependencies

- Cross-reference installed packages with actual imports in the codebase
- Flag packages in dependency files that aren't imported anywhere

## Output Format

**Critical** — Known vulnerabilities with available exploits.

**Warning** — Outdated packages, unpinned versions, license concerns.

**Info** — Minor version updates available, maintenance status notes.

Include the package name, current version, recommended version, and reason for each finding.
