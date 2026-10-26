"""Check static HTML anchor reachability of the canonical build inventory."""
import json
import sys
from urllib.parse import urljoin, urlsplit, urlunsplit
from pathlib import Path
from lxml import html


def normalized(url):
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/") or "/", "", ""))


def check(data):
    graph = {}
    for page in data["pages"]:
        document = html.fromstring(Path(page["file"]).read_bytes())
        base = urljoin(page["base"], document.xpath("//base/@href")[0]) if document.xpath("//base/@href") else page["base"]
        graph[normalized(page["base"])] = {normalized(urljoin(base, href)) for href in document.xpath("//a[@href]/@href")}
    reached = set()
    pending = [normalized(data["site"])]
    while pending:
        url = pending.pop()
        if url in reached or url not in graph:
            continue
        reached.add(url)
        pending.extend(graph[url] - reached)
    missing = sorted(graph.keys() - reached)
    if missing:
        raise ValueError("Sitemap-listed orphan pages (no HTML-link path from home): " + ", ".join(missing))


if __name__ == "__main__":
    try:
        check(json.load(sys.stdin))
    except (ValueError, OSError) as error:
        sys.exit(str(error))
