import type { APIRoute } from 'astro';
import { getReleases } from '../utils/versions';

const esc = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

// RSS 2.0 feed of the releases listed on /releases.
export const GET: APIRoute = async ({ site }) => {
  const releases = await getReleases();
  const base = new URL('/', site ?? 'https://krabka.io').href;
  const items = releases
    .map(
      (r) => `    <item>
      <title>${esc(r.name)}</title>
      <link>${esc(r.url)}</link>
      <guid isPermaLink="true">${esc(r.url)}</guid>
      <category>${esc(r.repo)}</category>${r.date ? `\n      <pubDate>${new Date(r.date).toUTCString()}</pubDate>` : ''}${r.notes ? `\n      <description>${esc(r.notes.slice(0, 2000))}</description>` : ''}
    </item>`,
    )
    .join('\n');
  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
  <channel>
    <title>krabka releases</title>
    <link>${base}releases/</link>
    <atom:link href="${base}releases.xml" rel="self" type="application/rss+xml" />
    <description>Releases of the krabka broker, clients, stream libraries and tools.</description>
    <language>en</language>
${items}
  </channel>
</rss>
`;
  return new Response(xml, { headers: { 'Content-Type': 'application/rss+xml; charset=utf-8' } });
};
