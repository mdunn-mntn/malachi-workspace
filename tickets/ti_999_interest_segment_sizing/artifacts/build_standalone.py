"""Build the standalone version of the TI-999 RevealJS deck.

Reads ti_999_presentation_deck.html, inlines:
  - reveal.css + theme/white.css
  - reveal.js
  - all <img> references (PNGs in this folder) as base64

Writes ti_999_presentation_deck_standalone.html.
"""
import base64
import re
import urllib.request
from pathlib import Path

HERE = Path(__file__).parent
SRC = HERE / "ti_999_presentation_deck.html"
DST = HERE / "ti_999_presentation_deck_standalone.html"

CDN_FILES = [
    ("reveal.css", "https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.css", True),
    ("white.css",  "https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/theme/white.css", True),
    ("reveal.js",  "https://cdn.jsdelivr.net/npm/reveal.js@5.1.0/dist/reveal.js", False),
]
TMP = Path("/tmp")


def fetch(name: str, url: str) -> str:
    path = TMP / name
    if not path.exists():
        print(f"download {url} -> {path}")
        urllib.request.urlretrieve(url, str(path))
    return path.read_text(encoding="utf-8")


def inline_images(html: str) -> str:
    def repl(match: re.Match) -> str:
        src = match.group(1)
        png_path = HERE / src
        if not png_path.exists():
            print(f"  WARN: image not found at {png_path} — leaving src as-is")
            return match.group(0)
        data = base64.b64encode(png_path.read_bytes()).decode("ascii")
        return f'src="data:image/png;base64,{data}"'

    return re.sub(r'src="(ti_999_chart_[^"]+\.png)"', repl, html)


def main() -> None:
    html = SRC.read_text(encoding="utf-8")

    for name, url, is_css in CDN_FILES:
        body = fetch(name, url)
        if is_css:
            html = html.replace(f'<link rel="stylesheet" href="{url}">', f"<style>{body}</style>")
        else:
            html = html.replace(f'<script src="{url}"></script>', f"<script>{body}</script>")

    html = inline_images(html)

    DST.write_text(html, encoding="utf-8")
    size_kb = DST.stat().st_size // 1024
    print(f"wrote {DST}  ({size_kb} KB)")


if __name__ == "__main__":
    main()
