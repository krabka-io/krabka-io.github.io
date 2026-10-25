# Security policy

## Report a vulnerability

Send a report to <security@krabka.io>. Do not put sensitive vulnerability
reports in public issues or pull requests.

Include the affected URL or component, the revision you tested when available,
the steps to reproduce the problem, and the impact you observed. A proof of
concept helps, but a clear description is enough. Do not include credentials
or unrelated private data.

A maintainer acknowledges a report within five business days and keeps the
reporter informed until the fix lands. Please give the maintainers reasonable
time to fix the problem before public disclosure.

## Scope and maintenance

This policy covers the deployed Krabka website, this repository's build and
publishing pipeline, and its browser-hosted tools. Reports can include script
injection, exposure of sensitive build data, or a way to replace published
content without authorization.

Maintenance targets the deployed site and current source on main. This website
policy does not promise backports to historical website revisions or define
support for broker releases. Report a vulnerability in another Krabka component
under that component repository's security policy.

For ordinary documentation errors and non-sensitive website defects, open a
normal repository issue instead.
