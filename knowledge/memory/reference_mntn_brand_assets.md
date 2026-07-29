---
name: reference_mntn_brand_assets
description: "Official MNTN brand kit — hex palette, fonts (Neue Haas + Inter), logo kit location, brand.mountain.com portal (pw mountain123); use for ANY deliverable (xlsx, decks, charts)"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 3b55570d-c509-4bdc-a8b6-68fa3f480871
doc_type: memory
keywords: [brand assets, mntn palette, hex colors, neue haas, inter font, logo kit, brand.mountain.com, deliverable, xlsx]
domain: [business, workflow]
lifecycle: active
last_verified: 2026-07-21
---
Official MNTN brand assets (from `brand.mountain.com`, 2025 guidelines; user pulled the kit 2026-07-21).
Use these for ANY branded deliverable — .xlsx (see [[reference_xlsx_master_format]]), decks
([[reference_deck_standards]]), charts (the dataviz palette swap).

**Portal:** `https://brand.mountain.com/` — password `mountain123`. Holds guidelines + downloadable assets.

**Local copy (GITIGNORED):** `documentation/mntn_assets/` — logo kit (`MNTN Logo Kit 2025/`), fonts
(`Fonts for Company Use/` — Inter TTFs + paid Neue Haas OTFs), `color-library_*/*.ase` (full swatches).
Never commit (paid fonts + licensed logos).

**Colors (official hexes):**
- Primary brights: Mountain Blue `#0AABC5`, Mountain Green `#1AC9AA` (core, in logo), Mountain Green
  `#22E5BE`, Mountain Blue `#26D1EA`, Pacific Blue `#0853E6`, Pacific Blue `#0E44BF`.
- Neutrals (Slate Grey): `#323B4E`, `#262E3C`, `#191E28`; Glacier White `#FFFFFF`, `#F6F6F6`.
- Copy rule: **Slate `#191E28` for copy on light backgrounds; Glacier `#F6F6F6` for copy on dark.**
- Highlight + Gradient palettes also exist (exact hexes only in the `.ase`, not the guideline text).

**Fonts:** headline = **Neue Haas Grotesk Text Pro** (paid), body/UI = **Inter** (open/OFL). Inter renders
natively in Google Sheets → good default for xlsx/anything web-delivered (installed to ~/Library/Fonts).

**Logo kit rules:** Primary = Horizontal Colored (Grey- or White-wordmark). Use the **White-wordmark**
variant on dark/image backgrounds; **drop-shadow** marks are for white/Glacier backgrounds ONLY; never
split the "M" symbol into a sub-brand. Files: `01_Primary Logo/PNG/MNTN_Logo_Horizontal_Colored_{White,
Grey}_2025@2x.png` (transparent RGBA); Stacked variants for vertical space; Symbol-only for avatars.

**Org/routing:** brand-assets contact = Matt Collins (marketing). For slides that go external, craft in
your own template then file a ticket for Marwan's design team to apply the latest MNTN slide styles
(internal-only audiences: the existing templates suffice, no design-team round-trip needed).
