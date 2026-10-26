"""Validate a JSON list of {file, root} records against vendored schemas."""
import base64
import json
from pathlib import Path
import sys

try:
    from lxml import etree
except ImportError:
    sys.exit("Sitemap XSD validation needs lxml: python3 -m pip install -r scripts/requirements-sitemap.txt")


def validate(records):
    parser = etree.XMLParser(resolve_entities=False, load_dtd=False, no_network=True)
    directory = Path(__file__).resolve().parent / "schemas"
    schemas = {
        root: etree.XMLSchema(etree.parse(str(directory / filename), parser))
        for root, filename in {"sitemapindex": "siteindex.xsd", "urlset": "sitemap.xsd"}.items()
    }
    for record in records:
        filename = record["file"]
        try:
            # Parse the original bytes, not a potentially lossy Node UTF-8 decode.
            data = base64.b64decode(record["base64"], validate=True) if "base64" in record else Path(filename).read_bytes()
            data.decode("utf-8-sig", errors="strict")
            document = etree.fromstring(data, parser)
            if document.getroottree().docinfo.doctype:
                raise ValueError("DOCTYPE is not allowed")
            if document.getroottree().docinfo.encoding.upper().replace("-", "") != "UTF8":
                raise ValueError("sitemaps must use UTF-8")
            schemas[record["root"]].assertValid(document)
        except (etree.LxmlError, ValueError, OSError) as error:
            raise ValueError(f"{filename}: XSD validation failed: {error}") from error


if __name__ == "__main__":
    try:
        validate(json.load(sys.stdin))
    except (ValueError, OSError, KeyError, etree.LxmlError) as error:
        sys.exit(str(error))
