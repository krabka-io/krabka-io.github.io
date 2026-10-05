import fs from 'node:fs';
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
export function rewriteDocLinks(content, { docsSubdir, repo, sourceDocsDir }) {
  return content.replace(CODE_OR_LINK, (match, code, _fence, label, target, intraDoc) => {
    if (code) return match;
    if (intraDoc) return intraDoc;

    // Ignore absolute URLs, anchors, and protocols; a real path has no spaces or commas.
    if (/^(https?:|mailto:|#)/.test(target) || /[\s,]/.test(target)) {
      return match;
    }

    const [rawPath, anchor] = target.split('#');
    const anchorSuffix = anchor ? `#${anchor}` : '';

    // 1. Link to sibling markdown guide in the same docs collection
    if (rawPath.endsWith('.md') && !rawPath.includes('/')) {
      const slug = rawPath.replace(/\.md$/, '');
      const destUrl = slug === 'index' ? `/docs/${docsSubdir}` : `/docs/${docsSubdir}/${slug}`;
      return `[${label}](${destUrl}${anchorSuffix})`;
    }

    // 2. Relative link to repository code/files (crates/, tests/, examples/, root docs, etc.)
    const cleanPath = rawPath.replace(/^(\.\.\/)+/, '').replace(/^\.\//, '');
    const isDocsSibling = fs.existsSync(path.join(sourceDocsDir, cleanPath));
    const repoPath = isDocsSibling ? `docs/${cleanPath}` : cleanPath;
    return `[${label}](https://github.com/krabka-io/${repo}/blob/main/${repoPath}${anchorSuffix})`;
  });
}
