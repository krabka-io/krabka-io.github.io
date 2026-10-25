// Shared strict XML reader for build checks and published sitemap checks.
import sax from 'sax';

const namespace = 'http://www.sitemaps.org/schemas/sitemap/0.9';

export function readSitemap(xml, root, source) {
  const parser = sax.parser(true, { xmlns: true });
  const stack = [];
  const locations = [];
  const entry = root === 'sitemapindex' ? 'sitemap' : 'url';
  let text = '';
  let rootSeen = false;
  let locationCount = 0;
  parser.onopentag = (tag) => {
    if (!stack.length) {
      if (rootSeen || tag.local !== root || tag.uri !== namespace) throw new Error(`${source}: expected ${root} XML`);
      rootSeen = true;
    }
    stack.push(tag.local);
    if (stack.length === 2 && tag.local === entry) locationCount = 0;
    if (stack.length === 3 && tag.local === 'loc') {
      if (stack[1] !== entry || tag.uri !== namespace) throw new Error(`${source}: invalid loc element`);
      locationCount++;
      text = '';
    }
  };
  parser.ontext = parser.oncdata = (value) => {
    if (stack.length === 3 && stack[2] === 'loc') text += value;
  };
  parser.onclosetag = () => {
    if (stack.length === 3 && stack[2] === 'loc') {
      const value = text.trim();
      if (!value) throw new Error(`${source}: empty loc`);
      locations.push(value);
    }
    if (stack.length === 2 && stack[1] === entry && locationCount !== 1) throw new Error(`${source}: each ${entry} needs one loc`);
    stack.pop();
  };
  parser.ondoctype = () => { throw new Error(`${source}: DOCTYPE is not allowed`); };
  parser.onerror = (error) => { throw new Error(`${source}: ${error.message}`); };
  parser.write(xml).close();
  if (stack.length) throw new Error(`${source}: unclosed XML elements`);
  if (!rootSeen || !locations.length) throw new Error(`${source}: empty ${root}`);
  if (new Set(locations).size !== locations.length) throw new Error(`${source}: duplicate locations`);
  return locations;
}

export function siteURL(value, site, source) {
  const url = new URL(value);
  const base = new URL(site);
  if (url.origin !== base.origin || url.username || url.password || url.search || url.hash) {
    throw new Error(`${source}: URL is not a clean same-origin URL: ${value}`);
  }
  return url;
}
