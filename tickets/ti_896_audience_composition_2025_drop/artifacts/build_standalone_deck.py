"""TI-896 — build standalone deck.

Reads ti_896_deck.html (CDN version), inlines reveal.js CSS + JS from CDN,
and replaces CHART_*_PLACEHOLDER strings with base64-embedded PNGs.
Output: ti_896_deck_standalone.html.
"""
from __future__ import annotations

import base64
import re
from pathlib import Path
from urllib.request import urlopen

ARTIFACTS = Path(__file__).resolve().parent

REVEAL_VERSION = "5.1.0"
CDN_BASE = f"https://cdn.jsdelivr.net/npm/reveal.js@{REVEAL_VERSION}/dist"

CHART_MAP = {
    "CHART_01_PLACEHOLDER":  "ti_896_chart_01_pp_jump.png",
    "CHART_02_PLACEHOLDER":  "ti_896_chart_02_cohort_composition.png",
    "CHART_03_PLACEHOLDER":  "ti_896_chart_03_retargeting.png",
    "CHART_04_PLACEHOLDER":  "ti_896_chart_04_shift_magnitudes.png",
    "CHART_05_PLACEHOLDER":  "ti_896_chart_05_pp_spend_share.png",
    "CHART_05B_PLACEHOLDER": "ti_896_chart_05b_mm_spend_cliff.png",
    "CHART_06_PLACEHOLDER":  "ti_896_chart_06_pp_default_vs_custom.png",
    "CHART_07_PLACEHOLDER":  "ti_896_chart_07_pp_vs_conv_scatter.png",
    "CHART_08_PLACEHOLDER":  "ti_896_chart_08_default_vs_custom_roas.png",
    "CHART_09_PLACEHOLDER":  "ti_896_chart_09_weekly_cohort_roas.png",
}


def fetch(url: str) -> str:
    with urlopen(url) as resp:
        return resp.read().decode("utf-8")


def png_to_data_uri(path: Path) -> str:
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"data:image/png;base64,{encoded}"


def main() -> None:
    src = (ARTIFACTS / "ti_896_deck.html").read_text()

    print("fetching reveal.css...")
    reveal_css = fetch(f"{CDN_BASE}/reveal.css")
    print("fetching white theme...")
    theme_css = fetch(f"{CDN_BASE}/theme/white.css")
    print("fetching reveal.js...")
    reveal_js = fetch(f"{CDN_BASE}/reveal.js")

    # Replace stylesheet links + external script with inline (use lambda to avoid re escape parsing)
    src = re.sub(
        r'<link rel="stylesheet" href="https://cdn\.jsdelivr\.net/npm/reveal\.js@[^"]+/dist/reveal\.css">',
        lambda _: f"<style>{reveal_css}</style>",
        src,
    )
    src = re.sub(
        r'<link rel="stylesheet" href="https://cdn\.jsdelivr\.net/npm/reveal\.js@[^"]+/dist/theme/white\.css">',
        lambda _: f"<style>{theme_css}</style>",
        src,
    )
    src = re.sub(
        r'<script src="https://cdn\.jsdelivr\.net/npm/reveal\.js@[^"]+/dist/reveal\.js"></script>',
        lambda _: f"<script>{reveal_js}</script>",
        src,
    )

    # Replace chart placeholders with base64 data URIs
    for placeholder, fname in CHART_MAP.items():
        path = ARTIFACTS / fname
        if not path.exists():
            print(f"  warning: {fname} not found; leaving placeholder")
            continue
        data_uri = png_to_data_uri(path)
        before = src.count(placeholder)
        src = src.replace(placeholder, data_uri)
        print(f"  embedded {fname} ({path.stat().st_size//1024} KB) into {before} placeholder(s)")

    out = ARTIFACTS / "ti_896_deck_standalone.html"
    out.write_text(src)
    print(f"\nwrote {out} ({out.stat().st_size // 1024} KB)")


if __name__ == "__main__":
    main()
