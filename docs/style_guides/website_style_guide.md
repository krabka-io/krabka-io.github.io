# Krabka website style guide

**Status:** Adopted for this website repository on 25 October 2026.

The worked revisions are proposed examples, not implemented website changes or fresh verification of claims.

## Purpose and scope

The website should inspire visitors to explore the Krabka ecosystem, help them reach a useful first result, and provide documentation that supports continued use. A good experience should make Krabka easy to recommend through word of mouth.

Help readers make an informed choice and reach a useful result with minimal unnecessary effort. Make limitations easy to find, instructions verifiable, and help accessible. Never improve apparent conversion by hiding information readers need.

This guide covers website prose, content structure, and the presentation constraints below. Browser-tool UI design is out of scope and needs a separate UI design document; introductory prose around the tools can follow this guide. It does not authorize a redesign or changes to other repositories. Its two-register policy replaces the website-wide STE requirement and the restriction of a marketing voice to the homepage. Choose the register by the purpose of each passage, not its page location. Technical terminology must remain consistent with the related repository documentation.

The badge and arrow rules below replace the older blanket bans. The tile guidance below replaces the blanket ban on equal task-tile grids: a functional navigation or comparison layout is acceptable, but decorative grids are not. Other existing contributor safeguards remain in force.

## Audience

Write primarily for existing Apache Kafka users who are evaluating Krabka. Assume familiarity with Kafka concepts, but do not assume familiarity with Krabka or its ecosystem.

The following profiles are working audience hypotheses, not validated customer research or promises about product capabilities.

### The performance-constrained operator

This reader faces throughput limits or latency requirements and needs to understand their options. Help them assess whether Krabka's measured performance applies to their workload. Explain the test conditions and relevant limitations rather than promising the same improvement everywhere.

### The infrastructure-conscious team

This team wants more capacity from existing hardware or wants to understand resource requirements before expanding infrastructure. Present resource measurements where available. Do not infer cost savings from throughput alone or promise reduced operational effort without evidence.

### The Kafka-dependent application team

This team is interested in alternatives but depends on existing clients and workflows. Explain supported compatibility, differences, and evaluation steps so they can assess switching from Kafka to Krabka without unnecessary uncertainty. Do not imply complete compatibility or effortless migration unless evidence supports the claim.

Use these profiles to identify reader questions, not to construct sales pitches. Revise them as user feedback provides better evidence.

## Voice and technical register

### Plain English for introductions and benefits

Use plain English with a warm, inviting, and positive tone for ecosystem introductions, benefits, and invitations to explore. Be specific and matter-of-fact. Welcome readers without suggesting that their current choices are foolish or that another project is inferior.

This register is available throughout the website, not just on the homepage. It does not exempt technical passages from the technical register or claims from evidence requirements.

Avoid empty enthusiasm, unsupported superlatives, and sales pressure. Words such as "seamless," "revolutionary," and "blazing fast" do not substitute for an explanation or evidence.

### STE for technical material

Use ASD-STE100 Simplified Technical English where technically appropriate, including documentation, benchmark explanations and methodology, and task-oriented getting-started guides.

Use the terminology and technical meaning of the related repository documentation. Keep commands, prerequisites, metric definitions, and qualifications consistent with the relevant version, and link to the corresponding document. Readers should not need to relearn a concept when moving between the website and a repository.

Use direct instructions and consistent terms. Name the actor when it matters, keep individual instructions focused, and explain expected results. Preserve technical accuracy when simpler wording would change the meaning. Do not claim formal STE compliance unless it has been checked.

Follow the related repository's [prose style guide](https://github.com/krabka-io/krabka-broker/blob/main/docs/style_guides/prose_style_guide.md) for technical prose, subject to this guide's explicit website-specific overrides. Preserve identifiers, protocol vocabulary, executable syntax, and quoted output rather than rewriting them as ordinary prose. Use the applicable technical register for captions and text alternatives that explain technical material. Repository contributor prose, including commit messages and pull requests, remains subject to its existing STE requirement.

When describing software behavior, use "must" only for requirements the documented code enforces through an observable failure or rejection. Use "should" for recommendations. Do not change a recommendation into a requirement while editing, weaken a warning, or drop a qualification or negation. This distinction describes software contracts; it does not weaken the editorial requirements of this guide.

If source documents conflict, flag the discrepancy for resolution instead of inventing a website-specific interpretation. Do not import historical prose restrictions that conflict with this guide's two-register policy.

A page may contain both registers. An introductory passage can be welcoming while its technical explanation follows the repository's terminology and technical-writing conventions.

### Worked examples from the feature pages

These examples pair verbatim source passages captured on 25 October 2026 with proposed revisions. They illustrate the purpose of each passage, not a required page structure, and do not depend on the original pages remaining available.

The source passages below were reviewed when this guide was prepared. The proposed wording does not establish that the implementation supports a claim. Check technical claims against the relevant source and version before publication. These examples have not been checked for formal ASD-STE100 compliance.

#### Features overview: invite and orient

**Before:**

> The broker is written in safe Rust. Separate repositories provide clients, stream processing, connectors, and Kubernetes operations. Selected broker decisions have Creusot proofs and Stateright models.

For a reader deciding what to explore, introduce the available components before their implementation and verification details.

**After (proposed):**

> Explore Krabka's broker, clients, connectors, and tools. Find out which components fit your Kafka workflows, then follow their configuration and usage guides.

This is an invitation to investigate fit, not a promise that every Kafka workflow is supported. Keep compatibility information discoverable. Retain the original technical information where the page explains implementation and verification.

#### KRaft detail: explain behavior and verification

**Before:**

> krabka implements the Apache Kafka KRaft consensus protocol directly in memory-safe Rust (krabka-raft and krabka-kraft-core), with leader elections, metadata compaction, and no ZooKeeper dependency. The election and quorum decisions are Creusot-proved kernels, and the consensus engine has a Stateright model.

Separate the behavior from the verification scope; introduce crate names when readers need an implementation reference.

**After (proposed):**

> Krabka implements Kafka's KRaft consensus protocol for cluster metadata. It supports leader elections and metadata compaction without ZooKeeper.
>
> Creusot proves selected election and quorum decision kernels. A Stateright model checks consensus behavior within the model's stated bounds.

Keep "selected" and the model-bounds qualification. Do not turn these claims into "formally verified consensus" or imply that the entire broker is proved correct. Retain links to the verification evidence and implementation details beside the relevant explanation.

The overview invitation and this explanation can appear on the same page. Choose the register for the reader's immediate question: what to explore, how something works, or how to perform a task.

## House conventions

### American English

Use American English in editorial prose, documentation copy, and interface text. Preserve official names, identifiers, commands, and verbatim quotations rather than changing their spelling.

### Product name and technical identifiers

Use "Krabka" as a capitalized proper noun in ordinary writing, including headings and documentation. Preserve lowercase spelling in binaries, code, package and crate names, domain names, and other technical identifiers, such as `krabka-broker` and `krabka.io`. The wordmark remains lowercase.

### Numbers and precision

Make numbers easy to read without hiding material differences. When a number exceeds five or six digits in prose, assess whether a rounded value with a written magnitude would be clearer. For example, use "about 43.2 million" instead of "43,162,146" when the exact count is unnecessary. This is a readability judgment, not a mandatory rounding threshold.

Retain exact values when precision matters, including data tables, reproducible instructions, and comparisons where rounding would change the conclusion. Identify the metric and unit, and keep exact source data accessible when presenting a rounded measurement. Continue to compute numerical claims from source data rather than maintaining a rounded value by hand.

## Evidence and benchmark claims

Benchmark results are a central strength of the site. Give them prominence and enough context for readers to evaluate their relevance.

- Scope each claim to the workload and conditions actually tested.
- Distinguish measured results from expectations or hypotheses.
- Keep material qualifications beside the claim and link to the methodology.
- Keep throughput, latency, resource use, and cost claims distinct.
- Compare projects respectfully and explain measured differences without denigration.

Identify tested software versions, hardware, configuration, units, and relevant limitations. Explain how metrics were calculated where interpretation depends on it. Do not cherry-pick results to imply a broader conclusion than the evidence supports.

Preserve scope words such as "selected kernels," "within each model's bounds," and "supported Kafka APIs" wherever they qualify a claim. Do not compare memory use against brokers that preallocate memory, such as a JVM heap or Redpanda's reservation. Compare CPU or state Krabka's own memory figure instead.

Compute numerical claims from source data at build time rather than typing them into copy. Add a guard that changes the wording when the data no longer supports the claim, following the existing `src/utils/home-benchmark.mjs` pattern.

Provide stable links to individual results and their methodology. Where possible, include reproducible instructions and downloadable results. Keep context close enough that a shared chart or quoted result remains meaningful.

Explain the main finding in text rather than requiring readers to decipher a chart. Charts must have accessible accompanying data and must not rely on color alone to communicate differences.

## Natural prose

Use straight double quotation marks (`"`) and straight apostrophes (`'`), not curly quotation marks or apostrophes.

Use the sentence length required by the meaning. Do not pad, shorten, or split sentences to fit a visual rhythm or make list items the same length. In technical material, apply the relevant technical-writing conventions without changing the meaning.

Avoid em dashes, generic enthusiasm, and repetitive slogans. Do not use two-fragment slogans such as "Fast broker. Safe Rust." or comparison formulas such as "Not X. It's Y." and "X, not just Y." When a comparison matters, express it in one complete sentence.

Do not use filler such as "seamless," "robust," "leverage," "powerful," "blazing," or "effortless." Describe the specific behavior or benefit instead. Avoid generic labels such as "Hub" and "Directory" for ordinary pages; name their actual subject or purpose.

Do not manufacture triplets to fill space. If two points are sufficient, express them in prose. Choose concrete subjects, familiar words, and precise verbs. Do not add a closing sentence that merely repeats a heading or slogan.

Use sentence case for headings, links, labels, and buttons. Do not join names with "&." Link and button text should describe the destination or action rather than rely on vague wording such as "Click here." A link that ends a section should start with a verb and state what the reader does next, such as "Run the complete examples."

## Links and cross-references

Use links to help readers take the next useful step or inspect supporting evidence, such as configuration instructions, compatibility limits, and verification evidence.

- Keep the main explanation self-contained; links provide depth rather than missing context.
- Use descriptive link text, such as "Configure a KRaft quorum," rather than "click here" or "learn more."
- Link directly to the relevant destination, using a specific section when helpful, and avoid repeating the same link throughout a passage.
- Keep essential qualifications beside the claim; do not hide limitations or verification scope behind a link.
- Preserve quoted examples by including the actual source text, so they remain useful if the original page changes or disappears.

Ordinary website prose can link to related material. A worked example must not rely on a link to supply its "before" text.

## Lists and information hierarchy

Build a useful hierarchy that introduces the subject, answers the reader's question, and supports a relevant next step. Headings must describe the content beneath them. Do not add floating headings or subtitles that contribute no useful information.

- Use ordered lists only for three or more clear, discrete steps that must occur in sequence.
- Use unordered lists for three or more related items that benefit from scanning.
- Keep list items short, generally no longer than one sentence.

The three-item threshold is a condition for using a list, not a reason to invent another item. For two-step tasks, use short paragraphs with clearly separated commands. When an item needs a longer explanation, use paragraphs or a suitable table instead of forcing it into a list.

Do not turn paragraphs into tiles merely to fill a layout. Tiles are appropriate when they help readers navigate or compare distinct items. Avoid the default pattern of three horizontal tiles with numbers that have little bearing on the surrounding explanation.

## Presentation constraints and reuse

Do not use gradient text or glows. Do not use grids of equal task tiles as decoration or as a substitute for a meaningful information hierarchy.

Do not use round or rectangular decorative badges, also called pills, that add no meaning to the page. Functional filter controls are acceptable.

Avoid routine arrow decoration. An arrow may occasionally help communicate an action, such as a call to action, but requires confirmation before implementation. Preserve required syntax in code examples; this rule does not require altering executable code.

Reserve monospace for technical content such as code, commands, and crate names. Do not use it as a general visual style or in tables. A proposed monospace exception inside a table requires confirmation before implementation.

Inspect and reuse existing components and assets, including CSS and colors, wherever suitable. If no suitable option exists, explain the gap and propose the specific addition, then wait for confirmation before implementing it. Reuse does not justify retaining a pattern that conflicts with this guide; propose a compliant alternative when necessary.

Use theme variables (`--color-*`, `--code-*`) and Tailwind color classes. Do not hardcode hex or rgba colors in pages or components. The light theme redefines variables in `src/styles/theme-light.css`.

Check every presentation change in light and dark themes at phone width (375 px) and desktop width. The browser tools, including Cluster Lab, the playgrounds, and the proof explorer, intentionally stay dark in the light theme. Preserve that behavior; do not theme the tools individually ahead of the shared-token refactor.

## Exploration and onboarding

Each page should answer its reader's immediate question and provide a relevant next step. The homepage helps visitors decide whether to explore. Benchmark pages support evaluation. Getting-started pages help readers reach a useful first result.

Avoid competing primary calls to action while keeping documentation and compatibility information easy to find. Do not put avoidable gates between readers and the information needed to evaluate Krabka.

Conversion means useful progress, not just a download or button click. A candidate activation milestone is completing a documented first run and successfully producing and consuming a message. Confirm this milestone against the supported product experience before using it as a formal measure.

### Complete and recoverable instructions

- State prerequisites and supported versions before the task begins.
- Provide complete, copyable commands with clearly identified placeholders.
- Show the expected result and how to verify success.
- Explain likely failures and how to recover from them.
- Label evaluation shortcuts so readers do not mistake them for deployment guidance.

Do not make readers search another page to discover an unstated prerequisite. Link to deeper explanations without making them mandatory detours when the task can stand on its own.

### Compatibility and migration

Make supported clients, workflow differences, and known limitations discoverable before asking readers to invest time. Distinguish an evaluation quick start from migration guidance. Clearly separate available functionality from planned work.

## Documentation for continued use

Technical pages must remain consistent with the running software as well as the repository documentation. Identify the version covered where behavior varies. Changes to commands or supported behavior must trigger review of affected website content.

Keep documentation navigable beyond the first run. Explain supported configuration and operational tasks at the appropriate depth, and provide clear routes to help and to report documentation problems.

Preserve useful links when content moves, using redirects where appropriate. Repeated questions and failed onboarding attempts should inform documentation improvements.

## Search visibility

Use audience-relevant search terms naturally where the page actually addresses them. Candidate terms include "Apache Kafka," "throughput," "latency," and "benchmark methodology." These are topic suggestions, not claims about measured keyword demand. Do not distort wording or repeat keywords to attract traffic.

Give each page a distinct descriptive title, a clear main heading, and an accurate meta description. As editorial starting points, review titles around 50 to 60 characters and descriptions around 140 to 160 characters. These are not mandatory limits and must not cause padding or loss of meaning. Search engines may truncate or replace displayed text.

### Sitemap freshness

Use the [SEO and sitemap design](../architecture.md#seo-and-sitemaps) for sitemap
layout and metadata provenance. Separate website and documentation URLs in the
index. Supply `lastmod` only from a verified significant content update, including
the original source history for imported Markdown; never from build or copy time.
Check supplied dates and optional frequency hints, but do not require them or
invent values to fill gaps. `changefreq` is not a crawl schedule and Google
ignores it. See the Google sitemap reference in the design for current guidance.

### Example benchmark metadata

**Title:** Krabka benchmarks: Kafka throughput and latency tests

**Main heading:** Krabka benchmark results

**Description:** Explore Krabka benchmark results for Kafka workloads, including throughput, latency, test configurations, and methodology to assess relevance to your deployment.

Use this example only if the page contains the material described. Useful section headings may include "Test configuration," "Throughput results," "Latency results," and "Limitations." Do not create empty sections to match a template.

## Accessibility and AI discoverability

Make important content available as readable HTML without requiring interaction to reveal the core explanation. Use semantic headings, descriptive links, and stable section anchors. Interactive components must support keyboard navigation and visible focus, with accessible names and labels. Images need appropriate text alternatives.

Provide accessible data tables alongside benchmark charts. Use proper table headers and keep units and qualifications attached to results so extracted passages retain their meaning.

Structure content around genuine reader questions, such as "How were these benchmarks run?" Do not create repetitive question sections solely to attract search or assistant traffic. Human readers and automated systems should receive the same substantive information.

Maintain crawlable links, canonical URLs, and a sitemap. Review crawler access deliberately, distinguishing search discovery from model-training permissions. Do not promise inclusion in search results or in answers from Claude, OpenAI, Gemini, or other assistants.

Ordinary SEO foundations remain relevant to Google's AI search features. That guidance does not establish the requirements of other providers or justify speculative AI-only markup.

## Analytics and privacy

Cloudflare is the site's analytics platform. Use it to understand aggregate visitor journeys and identify roadblocks while meeting GDPR and CCPA obligations with minimal administrative and popup burden.

Keep measurement proportionate and avoid introducing unnecessary tracking or additional analytics services. Do not assume that selecting Cloudflare alone establishes compliance or removes consent obligations; assess the actual configuration and data collection before changing them.

Where available and appropriate, examine onboarding completion and points of abandonment rather than treating downloads as proof of success. Supplement aggregate analytics with support questions and documentation feedback. Do not claim to measure an outcome that the available data cannot establish.

## Review and approval

Before publication, check that the page answers a real audience question, uses the appropriate register, supports its claims, and provides a useful next step. Verify technical instructions against the relevant software version. Check accessibility and remove decorative wording or layout that obscures the content.

New components or assets, occasional arrow use, and monospace exceptions in tables require confirmation before implementation. Adoption of this guide does not approve a particular exception or authorize changes outside the website's agreed scope.

This guide applies to the website repository. It does not adopt organization-wide policy.

## Contributor implementation safeguards

These safeguards support the guide; they are not content to show in the visitor journey.

### Code samples

The local quickstart client snippets run against a real broker in CI through `scripts/check-snippets.mjs`. Do not change their record values. Homepage code excerpts are cut from those snippets at build time through `src/utils/client-excerpts.mjs`; do not maintain handwritten copies on the homepage. Every `<CodeBlock>` on a page listed in `scripts/snippets/extract.mjs` needs CI coverage there.

### Documentation ownership and links

`src/content/docs/` is generated from component repositories by `scripts/sync-docs.mjs` and is not committed. Edit those guides in their source repository rather than editing generated copies. This website guide does not replace source-repository policy. Use `getPath()` for internal website links.

### Required checks

Before opening a pull request, run the repository's required checks:

```sh
npm run build:site
npm run test:website
npm run check-links
npm run check-seo
```

`build:site` builds the pages; `npm run build` also compiles the WebAssembly tools. Keep pull requests small enough to review in one sitting and describe anything that could not be tested. Apply prose cleanup proportionately to the files being changed, not as an unrelated repository-wide rewrite.

## Repository home and authority

This guide lives at `docs/style_guides/website_style_guide.md`, separate from generated visitor documentation under `src/content/docs/`. [AGENTS.md](../../AGENTS.md) requires it before changes to copy, content structure, or presentation; `CLAUDE.md` imports that entry point. Its explicit website overrides take precedence over the linked broker prose guide. Operational safeguards and required checks remain in `AGENTS.md`.

## References

Repository sources inform the contributor requirements. The Google references inform search guidance. None override the explicit website-specific decisions above.

- [Website contributor guidance reviewed for this draft](https://github.com/krabka-io/krabka-io.github.io/blob/329bbeb5d3f03ef7ae2f55e86ffdee4a8585f225/AGENTS.md)
- [Repository prose style guide](https://github.com/krabka-io/krabka-broker/blob/main/docs/style_guides/prose_style_guide.md)
- [Broker style-guide directory](https://github.com/krabka-io/krabka-broker/tree/main/docs/style_guides)
- [Google guidance on title links](https://developers.google.com/search/docs/appearance/title-link)
- [Google guidance on search snippets and meta descriptions](https://developers.google.com/search/docs/appearance/snippet)
- [Google guidance on AI features and websites](https://developers.google.com/search/docs/appearance/ai-features)
