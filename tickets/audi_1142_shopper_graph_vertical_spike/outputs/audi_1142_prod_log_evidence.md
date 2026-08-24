# AUDI-1142: prod pod log evidence for /vertical

Source: `gs://mntn-data-archive-prod/ti_argocd_logs/shopper_graph/<date>/<HH-MM>.jsonl` (30-min exports, 48 files/day). Days mined: 2026-08-22 (46 files, 2 exports missing) and 2026-08-23 (48 files, latest complete day). Read-only `gsutil cp` to scratchpad; counted with Python substring matching on the `line` field.

## Headline

97% of POST /vertical requests fail per day (622/636 and 626/643), and none of the failures are scrape exceptions. Every 400 is the same path: scrape returns text that fails `valid_scraped_text` (bot challenge / too short), self-selection fallback runs and fails (no `ui.onboarding_advertiser_company_details` row), handler returns 400. 563 of the failing advertiser_ids are identical across both days, so this is a recurring population re-failing daily, with a batch spike at hour 00 UTC (343 and 375 of the day's completions).

## Counts

| Metric | 2026-08-22 | 2026-08-23 |
|---|---|---|
| VERTICAL_HANDLER COMPLETE, total | 636 | 643 |
| ... status=400 | 622 | 626 |
| ... status=200 | 14 | 17 |
| SCRAPING FAILED (exception path) | 0 | 0 |
| VALIDATION PASSED (scrape text valid, LLM path) | 11 | 17 |
| Self-selection fallback attempted | 625 | 626 |
| Self-selection fallback succeeded | 3 | 0 |
| Distinct advertiser_ids on miss path (all POSTs reached scrape) | 634 | 643 |
| ... distinct AIDs ending 400 | 622 | 626 |
| Same 400 AIDs on both days | 563 | 563 |
| VERTICAL_HANDLER START (all POST, 0 GET) | 634 | 641 |
| AUTOPILOT_HANDLER COMPLETE | 526 | 551 |
| AUTOPILOT_FROM_URL_HANDLER COMPLETE | 0 | 0 |
| 429 service-busy rejections | 0 | 0 |

Start/complete differ by 2/day from requests spanning the day boundary. VALIDATION PASSED (11) + fallback succeeded (3) = 14 = the day's 200s on 08-22; 17 + 0 = 17 on 08-23, so the accounting closes: every 400 is the fallback-failed path, zero are missing-company_url 400s.

## Reading

- The GET path is unused in prod logs (0 GETs). All traffic is POST, and every POST fell through to the expensive scrape path (no request short-circuited on an existing `fpa.advertiser_verticals` row with vertical_id supplied).
- Scraping never raises; it returns challenge-page or too-short text ~97% of the time, so `SCRAPING FAILED` is the wrong marker to alert on. The failure signature in logs is `Attempting self-selection fallback` followed by a 400 COMPLETE.
- /autopilot_from_url has zero prod traffic on both days and the 429 service-busy path never fired, so the from_url pattern and its rate limiting are untested in prod at current load.

## Exact grep markers (verified against code before counting)

| Marker (substring) | Source |
|---|---|
| `VERTICAL_HANDLER COMPLETE: advertiser_id=..., status=...` | `middleware/k8s/api.py:178` |
| `VERTICAL_HANDLER START: method=..., advertiser_id=...` | `middleware/k8s/shopper_graph_wrapper/vertical_wrapper.py:590` |
| `SCRAPING FAILED: advertiser_id=..., url=..., error=...` | `vertical_wrapper.py:667` |
| `VALIDATION PASSED: advertiser_id=...` | `vertical_wrapper.py:695` |
| `Attempting self-selection fallback for advertiser` | `vertical_wrapper.py:456` |
| `Self-selection fallback succeeded` | `vertical_wrapper.py` (insert branch) |
| `AUTOPILOT_HANDLER COMPLETE` | `shopper_graph_wrapper/autopilot_wrapper.py:397` |
| `AUTOPILOT_FROM_URL_HANDLER COMPLETE` (+`(cached)` variant) | `autopilot_wrapper.py:451,500` |
| `Service busy, rejecting request with 429` | `autopilot_wrapper.py:466` |

Repo for line numbers: local clone `/Users/malachi/Developer/work/mntn/shopper_graph` at time of analysis. Raw logs (not committed): scratchpad `sg_logs/2026-08-2{2,3}/`.
