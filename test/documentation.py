#!/usr/bin/env python3
"""Check maintained local Markdown links and expose named, executable guide snippets."""
from collections import Counter
import hashlib
from pathlib import Path
import re
from urllib.parse import unquote, urlsplit
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
GUIDES = ROOT / "docs/guides"
ASSETS = ROOT / "docs/assets"


def snippets():
    result = {}
    for path in sorted(GUIDES.glob("*.md")):
        text = path.read_text()
        blocks = list(re.finditer(r"(?ms)^<!-- verify:([a-z0-9-]+) -->\n```(sql|sh|json|python)\n(.*?)\n```$", text))
        if len(blocks) != text.count("<!-- verify:"):
            raise ValueError(f"Malformed verification marker or code fence in {path.relative_to(ROOT)}")
        for match in blocks:
            name, language, source = match.groups()
            if name in result:
                raise ValueError(f"Duplicate guide snippet: {name}")
            result[name] = {"path": str(path.relative_to(ROOT)), "language": language, "source": source + "\n",
                            "sha256": hashlib.sha256((source + "\n").encode()).hexdigest()}
    return result


def maintained_paths():
    return [ROOT / "README.md", ROOT / "CONTRIBUTING.md", *sorted((ROOT / "docs").glob("*.md")), *sorted(GUIDES.glob("*.md"))]


def check_links():
    errors, anchor_cache, count = [], {}, 0

    def anchors(path):
        if path in anchor_cache:
            return anchor_cache[path]
        found, repeats, fence = set(), Counter(), None
        for line in path.read_text().splitlines():
            marker = re.match(r"^\s*(`{3,}|~{3,})", line)
            if marker:
                if fence is None:
                    fence = marker.group(1)[0]
                elif marker.group(1)[0] == fence:
                    fence = None
                continue
            if fence is not None:
                continue
            heading = re.match(r"^#{1,6}\s+(.+?)\s*#*\s*$", line)
            if heading:
                text = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", heading.group(1))
                text = re.sub(r"<[^>]*>", "", text).lower()
                slug = re.sub(r"[^\w\- ]", "", text).replace(" ", "-")
                found.add(slug + (f"-{repeats[slug]}" if repeats[slug] else ""))
                repeats[slug] += 1
            found.update(re.findall(r'<a\s+(?:id|name)="([^"]+)"', line))
        if fence is not None:
            errors.append(f"{path.relative_to(ROOT)}: unclosed code fence")
        anchor_cache[path] = found
        return found

    paths = maintained_paths()
    for source in paths:
        text = source.read_text()
        anchors(source)
        for match in re.finditer(r'\[[^\]]+\]\(([^\s)]+)(?:\s+"[^"]*")?\)', text):
            destination = urlsplit(match.group(1))
            if destination.scheme or destination.netloc:
                continue
            target = (source.parent / unquote(destination.path)).resolve() if destination.path else source
            count += 1
            if not target.exists() or (destination.fragment and target.suffix == ".md" and unquote(destination.fragment) not in anchors(target)):
                line = text[:match.start()].count("\n") + 1
                errors.append(f"{source.relative_to(ROOT)}:{line}: missing path/anchor {match.group(1)}")
    if errors:
        raise ValueError("\n".join(errors))
    svgs = sorted(ASSETS.glob("*.svg"))
    if len(svgs) < 6:
        raise ValueError("Expected at least 6 SVG diagrams in docs/assets")
    titles = {}
    for svg in svgs:
        root = ET.parse(svg).getroot()
        body = svg.read_text()
        if "light-dark(" not in body or "color-scheme: light dark" not in body:
            raise ValueError(f"SVG {svg.relative_to(ROOT)} must use light-dark() tokens with color-scheme: light dark")
        if "<script" in body:
            raise ValueError(f"SVG {svg.relative_to(ROOT)} must not contain script")
        for token in ("@keyframes", "animation:", "@media (prefers-reduced-motion: reduce)"):
            if token not in body:
                raise ValueError(f"SVG {svg.relative_to(ROOT)} must contain {token!r} for accessible CSS animation")
        title = root.find("{http://www.w3.org/2000/svg}title")
        label = (title.text or "").strip() if title is not None else ""
        match = re.match(r"^FIG (\d{2}) — ", label)
        if not match:
            raise ValueError(f"SVG {svg.relative_to(ROOT)} must have a <title> beginning 'FIG NN — '")
        if match.group(1) in titles:
            raise ValueError(f"Duplicate figure number FIG {match.group(1)} in {titles[match.group(1)]} and {svg.name}")
        titles[match.group(1)] = svg.name
    if set(titles) != {f"{number:02d}" for number in range(1, len(svgs) + 1)}:
        raise ValueError(f"SVG figure numbers must be contiguous FIG 01..FIG {len(svgs):02d}, got {sorted(titles)}")
    markdown = "\n".join(path.read_text() for path in paths)
    for svg in svgs:
        if svg.name not in markdown:
            raise ValueError(f"SVG {svg.name} is not referenced from maintained Markdown")
    for source in paths:
        if any(token in source.read_text() for token in ("├──", "└──", "--WAL-->", "replaceable pgvs3 gateways -------")):
            raise ValueError(f"ASCII diagram remains in {source.relative_to(ROOT)}; use docs/assets/*.svg instead")
    quarto_cfg = (ROOT / "docs/_quarto.yml").read_text()
    for href in re.findall(r"href:\s*([^\s#]+\.md)", quarto_cfg):
        if not (ROOT / "docs" / href).exists():
            raise ValueError(f"Missing Quarto href target: docs/{href}")
    return {"documents": len(paths), "local_links": count, "snippets": len(snippets()), "svgs": len(svgs)}


if __name__ == "__main__":
    print(check_links())
