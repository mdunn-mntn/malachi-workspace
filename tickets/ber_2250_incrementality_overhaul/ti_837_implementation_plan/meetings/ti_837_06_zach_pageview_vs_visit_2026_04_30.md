# Zach Schoenberger — page_view vs visit (Slack, 2026-04-30)

**Authoritative clarification of what's in `guid_log` vs `clickpass_log`.**

| Term | Table | Definition |
|---|---|---|
| **page view** | `guid_log` | Each page view event on the advertiser site by a tracked household. Fires regardless of MNTN ad served. |
| **visit** | `clickpass_log` | A click or a VV. MNTN-attributed: served impression → site visit within ~30 days → pixel match. |

**Why this matters for incrementality:** "guid lift" is page-view lift, not visit lift. At the IP level (after `COUNT_DISTINCT(ip)`), the metric is functionally similar. But terminology in deck/docs needs to be precise: don't say "guid visits" → say "page views" or "site activity per IP."

**What clickpass_log actually requires** (Malachi's earlier framing, confirmed by Zach):
1. MNTN serves user an impression (CTV or display)
2. Within ~30 days, user visits the advertiser's site
3. MNTN's pixel fires on the advertiser site and matches the visitor back to the impression
4. → row in `clickpass_log`

**Updates applied:**
- `knowledge/data_catalog.md` — guid_log + clickpass_log entries
- `artifacts/ti_837_methodology_explainer.md` — "two outcomes" table

**Deck implication:** the wedge slide currently calls both "visits." Strictly, guid is page-views. Whether to update the deck's wording is a [TI-842 deck-fix](https://mntn.atlassian.net/browse/TI-842) judgment call — probably worth a one-line caveat in the methodology slide rather than a full rename.
