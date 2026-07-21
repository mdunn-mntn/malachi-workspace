# MNTN brand assets for the .xlsx builder

Drop official brand assets here and every workbook built with `lib/mntn_xlsx.py` picks them up
automatically — **no code change needed.**

## 1. Logo → `mntn_logo.png`
- A **transparent-background PNG**, ideally the **white/reversed** MNTN logo (it sits on the deep-navy
  cover band, so a white or light logo reads best).
- Landscape/horizontal lockup works better than a tall stacked one (the band is ~54px tall).
- Save it here as exactly **`mntn_logo.png`**. That's it — the cover uses it in place of the "MNTN"
  wordmark on the next build.

## 2. Brand colors → `brand.json` (optional)
- Create `brand.json` here to override any palette token without editing code:
  ```json
  {
    "INK": "10263B",
    "PRIMARY": "1A3C5E",
    "ACCENT": "1F8FE5"
  }
  ```
- Any subset is fine; unspecified tokens keep their defaults. `#` prefix optional.
- Tokens: `INK` (cover band), `PRIMARY` (table headers/titles), `ACCENT` (rule/links/key numbers),
  `BAND` (row zebra), `POS`/`NEG`/`WARN` (traffic-lights). See `BRAND` in `lib/mntn_xlsx.py`.

Then rebuild: `python3 lib/mntn_xlsx_demo.py` and check the sample.

*(These asset files are gitignored — binary logo + local brand overrides don't belong in the repo.)*
