import path from 'node:path';

// Matches, in one pass: fenced code, inline code (both left alone), or a markdown link.
// Generic calls in Go samples such as `NewSerde[Order](schema, cache)` look like links,
// so anything inside code must never reach the link branch. A fence is closed by a run
// of its own length or longer (so a ``` example inside a ```` fence stays code), and one
// left open runs to the end of the file, as in CommonMark. The last branch is a rustdoc
// intra-doc link, [`crate::Path`], which has no target off docs.rs: it becomes plain
// code, or the page would show the brackets around it.
const CODE_OR_LINK = /(^[ \t]*(`{3,}|~{3,})[^\n]*\n[\s\S]*?(?:^[ \t]*\2[^\n]*$|(?![\s\S]))|``[^`]+``|`[^`\n]+`)|\[([^\]]+)\]\(([^)]+)\)|\[(`[^`\n]+`)\](?![(\[:])/gm;

/** Rewrite relative links in a synced guide so they resolve on krabka.io or GitHub. */
export function rewriteDocLinks(content, { docsSubdir, repo, sourceFile = '', sourcePath, sourceRef = 'main', publishedFiles, publishedPaths }) {
  return content.replace(CODE_OR_LINK, (match, code, _fence, label, target, intraDoc, offset) => {
    if (code) return match;
    if (intraDoc) return intraDoc;

    // Ignore absolute URLs, anchors, and protocols; a real path has no spaces or commas.
    if (/^(?:[a-z][a-z\d+.-]*:|\/|#)/i.test(target) || /[\s,]/.test(target)) {
      return match;
    }

    const matchPath = /^([^?#]*)([?#].*)?$/.exec(target);
    const rawPath = decodeURIComponent(matchPath[1]);
    const suffix = matchPath[2] ?? '';
    const base = sourcePath ? path.posix.dirname(sourcePath) : path.posix.join('docs', path.posix.dirname(sourceFile));
    const repoPath = path.posix.normalize(path.posix.join(base, rawPath));
    if (repoPath.startsWith('../')) throw new Error(`Documentation link leaves the repository: ${target}`);

    // Explicitly imported READMEs can live outside docs/ and link to each other.
    const imported = publishedPaths?.get(repoPath.toLowerCase());
    if (imported) return `[${label}](/docs/${docsSubdir}/${imported.replace(/\.md$/i, '').toLowerCase()}${suffix})`;

    // 1. Link to sibling markdown guide in the same docs collection
    const guide = repoPath.startsWith('docs/') ? repoPath.slice(5).toLowerCase() : null;
    if (!sourcePath && /\.md$/i.test(repoPath) && guide && (!publishedFiles || publishedFiles.has(guide) || !guide.includes('/') || guide.startsWith('operations/'))) {
      const slug = repoPath.slice(5).replace(/\.md$/i, '').toLowerCase();
      const destUrl = slug === 'index' ? `/docs/${docsSubdir}` : `/docs/${docsSubdir}/${slug}`;
      return `[${label}](${destUrl}${suffix})`;
    }

    // 2. Relative link to repository code/files (crates/, tests/, examples/, root docs, etc.)
    const encodedPath = repoPath.split('/').map(encodeURIComponent).join('/');
    const url = content[offset - 1] === '!'
      ? `https://raw.githubusercontent.com/krabka-io/${repo}/${sourceRef}/${encodedPath}`
      : `https://github.com/krabka-io/${repo}/blob/${sourceRef}/${encodedPath}`;
    return `[${label}](${url}${suffix})`;
  });
}
