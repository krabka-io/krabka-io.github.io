# Issue #91: dated baseline and open acceptance work

Moved from the README on 25 October 2026. The following October baseline is historical, not a current performance claim or a completed acceptance report. Recurring review ownership is in [testing](../testing.md#review-ownership).

The October 2026 build baseline used upstream guides and proof sessions synced on October 4 and the deployed WASM modules. It was measured before a new Rust build or production deployment:

| Payload | Raw bytes | Gzip bytes |
| --- | ---: | ---: |
| Ordinary entry-page HTML | 31–99 KB | 7.6–19.6 KB |
| Benchmark HTML | 388,472 | 84,648 |
| Proof explorer HTML | 2,828,662 | 249,113 |
| Lab JavaScript | 814,225 | 242,038 |
| Broker WASM | 15,226,339 | 5,666,758 |
| Playground WASM | 6,569,972 | 2,481,890 |
| Brand preview images | 26,374 | 26,604 |

The group sizes sum files; gzip sizes estimate per-file compression, not the site's transfer encoding. Review `scripts/page-budgets.json` before changing a ceiling. For current asset procedures, see [publishing](../publishing.md).

## Deployment enforcement review: 26 October 2026

Read-only GitHub API checks reviewed `main` at `bc4cb2f07e93438ad32d228494198a27d417d2bc`:

- [Applicable branch rules](https://api.github.com/repos/krabka-io/krabka-io.github.io/rules/branches/main) require the `build` check from GitHub Actions, integration ID `15368`.
- [Active ruleset `main-2`](https://github.com/krabka-io/krabka-io.github.io/rules/24037990) targets `refs/heads/main`. It permits an always-on bypass for repository role ID `5`, the administrator role. It does not require the branch to be up to date, reviewer approvals, or code-owner review. Do not describe this protection as impossible to bypass.
- [Active ruleset `main-1`](https://github.com/krabka-io/krabka-io.github.io/rules/24037989) prevents branch deletion and non-fast-forward updates, with no bypass actors returned.
- The [Pages environment](https://api.github.com/repos/krabka-io/krabka-io.github.io/environments/github-pages) has a branch policy limited to `main`. The response contains no required-reviewer rule and permits administrator bypass.
- The reviewed tree has no `CODEOWNERS` file. [Review ownership](../testing.md#review-ownership) assigns the website maintainer role to production smoke checks; it does not name an individual responder.
- The [reviewed deployment run](https://github.com/krabka-io/krabka-io.github.io/actions/runs/40268491903) passed `build`, `deploy`, and `production-smoke`.

These results verify configuration and an existing successful run. No deliberate failed merge, recovery deployment, or repository-setting change was attempted. The legacy branch-protection endpoint returned 404; the active rulesets independently establish the protection above. Use [the recovery procedure](../publishing.md#recover-from-a-failed-deployment) for future incidents. A named recovery responder and authorization remain specific to each incident.

## Manual task protocol for issue #91

Use a clean environment and record exact image digests, client versions, device/browser, network profile, theme, date, participant count, completion, elapsed time, and failures. Keep cleanup separate and use disposable clusters for recovery tests.

1. Observe five first-time participants: find the browser trial, run the local quickstart, produce and read `hello krabka`, then find compatibility and migration guidance. Record assistance and where each person stops. The proposed criterion is four independent completions out of five.
2. Ask five participants to predict and run the minimum-ISR lab experiment, explain the visible result, and find the local-install next step. Record the same measures. Repeat these tasks in March with the same protocol; do not infer traffic or conversion rates from the small sample.
3. Have a second engineer rehearse backup/restore and upgrade/recovery on a disposable cluster. Record capture boundaries, source/target digests, readiness and replication checks, expected output, observed output, and unsupported cases. A previous image is a recovery option only after persisted-format compatibility has been tested.
4. Review keyboard order, visible focus, accessible names, announcements, contrast, reduced motion, screen-reader reading, and 200%/400% zoom against WCAG 2.2 AA. Check phone, tablet, and desktop widths in both themes, including app controls. Record remaining gaps. Automated structural checks do not establish accessibility conformance.
5. Review available analytics and search questions before selecting the next tutorial. Run its published commands from a clean environment. Check the ten queries in `scripts/check-site.mjs` and record missing or misleading results.

The observed user studies, second-engineer rehearsals, full accessibility review, field measurements, and demand-based tutorial selection remain open acceptance work in [#91](https://github.com/krabka-io/krabka-io.github.io/issues/91).
