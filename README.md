# Krabka website

This repository maintains [krabka.io](https://krabka.io), the website for the
Krabka streaming ecosystem. It helps Kafka users explore the components,
evaluate the available evidence, and reach the relevant setup and usage guides.
It also serves browser-hosted tools, the aggregated Helm chart index, and stable
brand downloads used by published charts.

The site brings together documentation from component repositories. Those
repositories remain the source of their guides and compiler-generated API
references. This repository owns their website presentation, navigation, and
links, not the implementation of every component. The website uses Astro and
Tailwind CSS; the browser tools have separate build requirements.

## Use Krabka

Start with the live site rather than building this repository to install a
broker or client. Choose the route that matches your next task:

- [Get started with Krabka](https://krabka.io/get-started) for the supported local setup and client examples.
- [Read the documentation](https://krabka.io/docs) for component guides and operational procedures.
- [Explore the Cluster Lab](https://krabka.io/docs/lab) to run browser-hosted cluster scenarios and deliberate faults.
- [Inspect verification evidence](https://krabka.io/verification) for selected proved kernels, model bounds, and other correctness checks.
- [Find compiler API references](https://krabka.io/api) published by the component repositories.

The browser tools are exploration tools, not a substitute for deployment
qualification. Keep the stated compatibility limits and verification scope with
any result you share.

## Work on the website

Read [Contributing](CONTRIBUTING.md) and the
[website style guide](docs/style_guides/website_style_guide.md) before changing
copy or presentation. Edit imported component guides in their source repository,
not in generated copies under `src/content/docs/`.

For a pages-only preview, use Node.js 22.12.0 or newer (CI uses Node 24), npm,
Git, and Git LFS. Network access is needed for package installation, the benchmark
archive, and component synchronization. From a checkout of this repository:

```sh
npm ci
git lfs pull --include "benchmarks/archive/*.tar.gz"
npm run sync-docs
npm run sync-proofs
npm run start
```

If Git LFS is not initialized, run `git lfs install` first. Open the URL printed
by Astro, normally `http://localhost:4321`. This path does not compile Rust or
produce working playground, broker, or Why3 modules. A page loading successfully
does not establish that its browser tool is available. See
[development setup](docs/development.md) for full build tiers and recovery from
missing inputs. The benchmark archive is required by the preview even for a
copy edit.

Before requesting review, run the [required checks](docs/testing.md). Record
anything you could not test and distinguish local results from deployed-site
verification. Keep changes focused; this documentation does not authorize
publishing, deployment, or repository-setting changes.

## Repository documentation

Use these maintenance guides for the task at hand:

- [Develop and preview the website](docs/development.md): prerequisites, synchronization, build tiers, and commands by task.
- [Understand the architecture](docs/architecture.md): content ownership, browser tools, verification data, and a small source map.
- [Run checks and review results](docs/testing.md): required gates, integration prerequisites, artifacts, and review ownership.
- [Maintain generated sitemaps](docs/sitemaps.md): build-time generation, coverage checks, and published XML checks.
- [Maintain publishing and stable URLs](docs/publishing.md): deployment, chart aggregation, component references, and brand downloads.
- [Apply the website style guide](docs/style_guides/website_style_guide.md): audience, prose registers, evidence, links, and presentation policy.

The [benchmark instructions](benchmarks/README.md),
[lab design](playground/docs/lab-design.md),
[real-broker contract](playground/docs/lab-real-broker.md), and
[Why3 build guide](why3-web/README.md) remain the canonical subsystem documents.
Dated measurements and unfinished manual acceptance work belong in the
[issue #91 review record](docs/reviews/issue-91.md), not the evergreen setup path.

## Contributing, security, and licenses

Use [Contributing](CONTRIBUTING.md) for the issue and pull-request workflow and
[AGENTS.md](AGENTS.md) for coding-agent safeguards. Report vulnerabilities
privately through the route in [Security](SECURITY.md), not a public issue.

Original website code and code samples use
[Apache 2.0](LICENSES/Apache-2.0.txt). Original prose uses
[CC BY 4.0](LICENSES/CC-BY-4.0.txt). Imported and third-party material retains its
existing terms. Read the [license scope map](LICENSE) for mixed files,
attribution, and artwork treatment, and the [trademark policy](trademark.md) for
use of the name, logo, and wordmark. Copyright licensing and permission to imply
official branding are separate questions.
