# Sitemaps 0.9 schemas

Unmodified official schema bytes, downloaded on 26 October 2026:

- `sitemap.xsd`: https://www.sitemaps.org/schemas/sitemap/0.9/sitemap.xsd
  SHA-256: `fbec77e84467050d5667394a57cc5cdfe1239d80aca1d61e9e0827173c3d9df9`
- `siteindex.xsd`: https://www.sitemaps.org/schemas/sitemap/0.9/siteindex.xsd
  SHA-256: `68d166ae92a07d5e8dbb0290d28c74aba24d896b721c0d3b1cee78c6aee9781e`

Build validation reads only these local files. Schema updates must be explicit,
with a reviewed diff and updated hashes. Unknown extensions fail strict schema
validation; add their schemas explicitly if the site starts publishing them.
