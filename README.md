# HeroDevs fork of Digger

> [!WARNING]
> **Internal-use fork. Not supported by HeroDevs.**
>
> This is the HeroDevs Shared Services team's fork of [diggerhq/digger](https://github.com/diggerhq/digger). We run it to keep our own Terraform automation working while upstream development is paused. It isn't a HeroDevs product, and we don't offer support, security fixes, or compatibility promises to anyone else who uses it.

For the project itself, its documentation and its community, see [diggerhq/digger](https://github.com/diggerhq/digger).

## What's here

Our deployment changes live on `nes-main`, the default branch. `develop` tracks upstream without local changes. Fixes that would help everyone go upstream as pull requests against `develop`, so this fork can merge back cleanly if upstream development resumes.

A `nes/backend-ee/v*` tag on `nes-main` publishes two things to GHCR:

- The Enterprise Edition (EE) backend image, `ghcr.io/neverendingsupport/infra-fork-digger/digger-backend-ee`. It's built for arm64 only and needs a Digger EE license key.
- The `digger-backend` Helm chart, `oci://ghcr.io/neverendingsupport/charts/digger-backend`.

Each carries its own `-nes.N` suffix on its upstream base version. The release also records both in `version.yaml` on the `infra-prod` branch, which Argo CD deploys, so pushing a tag ships to production. The tagging steps are in the header of `.github/workflows/nes-backend-ee-release.yml`. [Deploy with Helm](docs/ce/self-host/deploy-helm.mdx) uses the published image and chart.
