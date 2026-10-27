// Shared strict XML reader for build checks and published sitemap checks.
import sax from 'sax';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

export const MAX_ENTRIES = 50_000;
export const MAX_BYTES = 52_428_800;

const namespace = 'http://www.sitemaps.org/schemas/sitemap/0.9';

// Validate calendar fields explicitly: Date.parse normalizes dates such as February 30.
export function validLastmod(value) {
  const match =
    /^(\d{4})-(\d{2})-(\d{2})(?:T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(Z|[+-]\d{2}:\d{2}))?$/.exec(
      value,
    );
  if (!match) return false;
  const [, year, month, day, hour, minute, second, zone] = match;
  const leap = +year % 4 === 0 && (+year % 100 !== 0 || +year % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  if (+year === 0 || +month < 1 || +month > 12 || +day < 1 || +day > days[+month - 1]) return false;
  if (hour !== undefined && (+hour > 23 || +minute > 59 || +second > 59)) return false;
  if (zone && zone !== 'Z') {
    const [hours, minutes] = zone.slice(1).split(':').map(Number);
    if (hours > 14 || minutes > 59 || (hours === 14 && minutes !== 0)) return false;
  }
  return true;
}

export function readSitemap(xml, root, source, { maxBytes = MAX_BYTES } = {}) {
  const bytes = Buffer.isBuffer(xml) ? xml : Buffer.from(xml);
  if (bytes.length > maxBytes)
    throw new Error(`${source}: exceeds ${MAX_BYTES} uncompressed bytes`);
  xml = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
  if (/<\?xml[^>]*encoding\s*=\s*['"](?!utf-8['"])[^'"]+['"]/i.test(xml))
    throw new Error(`${source}: sitemaps must use UTF-8`);
  const parser = sax.parser(true, { xmlns: true });
  const stack = [];
  const locations = [];
  const entry = root === 'sitemapindex' ? 'sitemap' : 'url';
  let text = '';
  let rootSeen = false;
  let locationCount = 0;
  let fields = new Set();
  let lastField = -1;
  const order =
    root === 'sitemapindex' ? ['loc', 'lastmod'] : ['loc', 'lastmod', 'changefreq', 'priority'];
  parser.onopentag = (tag) => {
    if (!stack.length) {
      if (rootSeen || tag.local !== root || tag.uri !== namespace)
        throw new Error(`${source}: expected ${root} XML`);
      rootSeen = true;
    }
    if (stack.length === 3 && order.includes(stack[2]))
      throw new Error(`${source}: nested content in ${stack[2]}`);
    stack.push(tag.local);
    if (stack.length === 2 && tag.local === entry) {
      if (tag.uri !== namespace) throw new Error(`${source}: invalid ${entry} namespace`);
      locationCount = 0;
      fields = new Set();
      lastField = -1;
    }
    if (stack.length === 3 && ['loc', 'lastmod', 'changefreq', 'priority'].includes(tag.local)) {
      const position = order.indexOf(tag.local);
      if (
        stack[1] !== entry ||
        tag.uri !== namespace ||
        position < 0 ||
        fields.has(tag.local) ||
        position < lastField
      ) {
        throw new Error(`${source}: invalid, duplicate or out-of-order ${tag.local}`);
      }
      fields.add(tag.local);
      lastField = position;
      text = '';
    }
    if (stack.length === 3 && tag.local === 'loc') {
      locationCount++;
    }
  };
  parser.ontext = parser.oncdata = (value) => {
    if (stack.length === 3 && order.includes(stack[2])) text += value;
  };
  parser.onclosetag = () => {
    if (stack.length === 3) {
      const field = stack[2];
      const value = text.trim();
      if (field === 'lastmod' && !validLastmod(value))
        throw new Error(`${source}: invalid lastmod: ${value}`);
      if (
        field === 'changefreq' &&
        !/^(always|hourly|daily|weekly|monthly|yearly|never)$/.test(value)
      )
        throw new Error(`${source}: invalid changefreq: ${value}`);
      if (
        field === 'priority' &&
        (!/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/.test(value) || +value < 0 || +value > 1)
      )
        throw new Error(`${source}: invalid priority: ${value}`);
    }
    if (stack.length === 3 && stack[2] === 'loc') {
      const value = text.trim();
      if (!value) throw new Error(`${source}: empty loc`);
      protocolURL(value, source);
      locations.push(value);
      if (locations.length > MAX_ENTRIES)
        throw new Error(`${source}: exceeds ${MAX_ENTRIES} entries`);
    }
    if (stack.length === 2 && stack[1] === entry && locationCount !== 1)
      throw new Error(`${source}: each ${entry} needs one loc`);
    stack.pop();
  };
  parser.ondoctype = () => {
    throw new Error(`${source}: DOCTYPE is not allowed`);
  };
  parser.onerror = (error) => {
    throw new Error(`${source}: ${error.message}`);
  };
  parser.write(xml).close();
  if (stack.length) throw new Error(`${source}: unclosed XML elements`);
  if (!rootSeen || !locations.length) throw new Error(`${source}: empty ${root}`);
  if (new Set(locations).size !== locations.length)
    throw new Error(`${source}: duplicate locations`);
  return locations;
}

export function siteURL(value, site, source) {
  const url = protocolURL(value, source);
  const base = new URL(site);
  if (url.origin !== base.origin || url.username || url.password || url.search || url.hash) {
    throw new Error(`${source}: URL is not a clean same-origin URL: ${value}`);
  }
  return url;
}

// Protocol URL rules are distinct from this site's stricter clean-URL policy.
export function protocolURL(value, source) {
  if (
    typeof value !== 'string' ||
    value.length >= 2048 ||
    !/^https?:\/\//.test(value) ||
    /[^\x21-\x7e]|[<>"{}|\\^`]|%(?![0-9a-f]{2})/i.test(value)
  ) {
    throw new Error(`${source}: invalid URL length or encoding: ${value}`);
  }
  try {
    decodeURI(value);
  } catch {
    throw new Error(`${source}: invalid URL encoding: ${value}`);
  }
  return new URL(value);
}

export function scopedURL(value, site, sitemap) {
  const url = siteURL(value, site, sitemap);
  const scope = new URL('.', sitemap).pathname;
  if (!decodeURIComponent(url.pathname).startsWith(decodeURIComponent(scope)))
    throw new Error(`${sitemap}: URL outside directory scope: ${value}`);
  return url;
}

export function validateXSD(records) {
  const result = spawnSync(
    'python3',
    [fileURLToPath(new URL('./validate-sitemap-xsd.py', import.meta.url))],
    {
      input: JSON.stringify(records),
      encoding: 'utf8',
      timeout: 30_000,
      maxBuffer: 4 * 1024 * 1024,
    },
  );
  if (result.error) throw new Error(`Sitemap XSD validator could not run: ${result.error.message}`);
  if (result.status !== 0) throw new Error(result.stderr.trim() || 'Sitemap XSD validation failed');
}

export function advertisedSitemaps(robots, site) {
  const urls = robots
    .split(/\r?\n/)
    .map((line) => /^sitemap:\s*(\S+)/i.exec(line.trim())?.[1])
    .filter(Boolean);
  const expected = new URL('/sitemap-index.xml', site).href;
  if (!urls.includes(expected)) throw new Error(`robots.txt must advertise ${expected}`);
  for (const url of urls) siteURL(url, site, 'robots.txt');
  return [...new Set(urls)];
}

// Evaluate every declared agent group: an explicit bot group can override '*'.
export function assertCrawlable(robots, urls) {
  const groups = [];
  let group;
  for (const raw of robots.split(/\r?\n/)) {
    const line = raw.replace(/#.*$/, '').trim();
    const match = /^(user-agent|allow|disallow):\s*(.*)$/i.exec(line);
    if (!match) continue;
    const [, field, value] = match;
    if (field.toLowerCase() === 'user-agent') {
      if (!group || group.rules.length) groups.push((group = { agents: [], rules: [] }));
      group.agents.push(value.toLowerCase());
    } else if (group && value) group.rules.push({ allow: field.toLowerCase() === 'allow', value });
  }
  const agents = new Set(['*', ...groups.flatMap((g) => g.agents)]);
  for (const agent of agents) {
    const specific = groups.filter((g) => g.agents.includes(agent));
    const applicable = specific.length ? specific : groups.filter((g) => g.agents.includes('*'));
    for (const url of urls) {
      const target = new URL(url).pathname;
      let best;
      for (const rule of applicable.flatMap((g) => g.rules)) {
        const pattern = [...rule.value]
          .map((c, i) =>
            c === '*'
              ? '.*'
              : c === '$' && i === rule.value.length - 1
                ? '$'
                : '\\^$.*+?()[]{}|'.includes(c)
                  ? '\\' + c
                  : c,
          )
          .join('');
        if (!new RegExp('^' + pattern).test(target)) continue;
        const length = rule.value.replace(/[*$]/g, '').length;
        if (!best || length > best.length || (length === best.length && rule.allow))
          best = { ...rule, length };
      }
      if (best && !best.allow) throw new Error(`robots.txt blocks ${url} for ${agent}`);
    }
  }
}

export function assertSection(location, entries) {
  const name = new URL(location).pathname.split('/').pop();
  const match = /^sitemap-(docs|website)-\d+(?:-part-\d+)?\.xml$/.exec(name);
  if (!match) throw new Error(`Unrecognized sitemap section: ${name}`);
  for (const entry of entries) {
    const docs = /^\/docs(?:\/|$)/.test(new URL(entry).pathname);
    if (docs !== (match[1] === 'docs')) throw new Error(`${name}: wrong section for ${entry}`);
  }
}
