# Security Policy

## Supported Versions

`laila-core` is in beta. Only the latest minor release line receives security
fixes.

| Version | Supported |
| ------- | --------- |
| 1.0.x   | yes       |
| < 1.0   | no        |

## Reporting a Vulnerability

**Please do not open a public GitHub issue for security problems.**

Use one of the following channels:

1. **Preferred — GitHub private vulnerability reporting**:
   <https://github.com/LambdaLabsML/laila-core/security/advisories/new>
2. **Email**: `security@lambdal.com` (PGP key available on request).

When reporting, please include:

- A description of the issue and its impact.
- Steps to reproduce, or a proof-of-concept.
- Affected versions / commit SHAs.
- Whether the issue has been disclosed publicly anywhere.

You can expect:

- An acknowledgement within **3 business days**.
- A triage assessment and target fix window within **10 business days**.
- A coordinated-disclosure timeline negotiated with you for any externally
  reported issue.

## Scope

In-scope:

- The `laila-core` Python package and its public APIs.
- The PyPI artifact and its publishing pipeline.
- The Docker image (once published) and its build pipeline.

Out of scope:

- Vulnerabilities in third-party storage backends (`boto3`, `redis`,
  `psycopg`, etc.). Please report those upstream.
- Issues in user-deployed infrastructure (S3 bucket permissions, Postgres
  network exposure, etc.) — those are operator concerns.

## Credential Rotation

The release pipeline holds the following long-lived secrets in the private
repo:

- `LAILA_CORE_MAIN` — SSH deploy key with `contents:write` on
  `LambdaLabsML/laila-core`. Migration to a GitHub App installation token
  is on the roadmap. Rotate at least every 12 months and immediately on any
  suspected exposure.

PyPI uses OIDC trusted publishing — no long-lived API token is stored.
