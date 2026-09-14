# Security Policy

## Supported versions

STIPHOS is Beta, carved out of mnemos-core so its release cadence is
decoupled from the stable core. Only the most recent commit on `main`
receives fixes; if you are running a pinned older release, upgrade to
current `main` before reporting an issue so the report is against supported
code.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability.

Report it as a **confidential issue** on the canonical GitLab project:
<https://gitlab.com/ncz-os/mnemos-stiphos/-/issues/new> — tick **"This issue
is confidential"** before you submit. Confidential issues are visible only
to project maintainers.

Please include:

- a description of the issue
- impact assessment
- reproduction steps
- the STIPHOS version/commit and backend in use
- any suggested remediation

Expect an acknowledgement within a week. There is no bug bounty.

## Secrets policy

- Never commit `.env` files or live credentials.
- Store agent/worker credentials outside the repository.
- Keep infrastructure-specific detail — hostnames, private addresses,
  internal topology — out of the repository entirely, including docs.
