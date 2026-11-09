# Security

Please report vulnerabilities privately: on this repository's Security tab, choose
"Report a vulnerability". Don't open a public issue for them.

What rift checks on each request, its limits, and its known gaps are in
[docs/PILOT.md](docs/PILOT.md#5-security).

## Verifying a release

From 0.1.3 on, release images are signed with cosign (keyless, by this repository's release
workflow) and carry a GitHub build-provenance attestation and an SBOM:

    cosign verify ghcr.io/olegnazarov23/rift:<version> \
      --certificate-identity-regexp '^https://github.com/olegnazarov23/rift/.github/workflows/release.yml@refs/tags/v' \
      --certificate-oidc-issuer https://token.actions.githubusercontent.com
    gh attestation verify oci://ghcr.io/olegnazarov23/rift:<version> --owner olegnazarov23
    docker buildx imagetools inspect ghcr.io/olegnazarov23/rift:<version> --format '{{ json .SBOM }}'

Each image is scanned with Trivy before it's published and isn't released with a fixable critical
vulnerability. It's built on distroless (no shell or package manager) from base images pinned by
digest. Dependencies are checked against the RustSec advisories on every push and weekly.
