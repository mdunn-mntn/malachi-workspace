# Kindred Bravely (35094) — systematic audience audit

## Where delivery & revenue go, by stage

| Stage | Camps | Impressions | Spend | Conv | Revenue | ROAS | CPA |
|---|--:|--:|--:|--:|--:|--:|--:|
| Prospecting | 6 | 7.1M | $159K | 2,563 | $296K | 1.9x | $62 |
| Retargeting | 6 | 7.4M | $70K | 15,758 | $1.9M | 26.5x | $4 |
| Multi-Touch S2 | 6 | 4.0M | $18K | 2 | $663 | 0.0x | $9K |
| Multi-Touch S3 | 6 | 2.0M | $9K | 345 | $38K | 4.3x | $25 |
| Ego | 6 | 82 | $1 | 0 | $0 | 0.0x | — |

**Key structural findings:**
- Each *campaign group* is a full funnel (Prospecting F1 + Multi-Touch S2/S3 + Ego + a separate Retargeting group). Group-level metrics conflate stages — classify by `objective_id` (1=Prospect, 4=Retarget, 5=MT-S2, 6=MT-S3, 7=Ego).
- **Retargeting (89071) is the revenue engine** — ~27x ROAS, 85% of revenue. Prospecting's YoY decline is a top-funnel-reach story.
- Prospecting runs on CTV; the Multi-Touch stages run on **display** (channel mix within a 'CTV' group).

## Per-campaign audience audit

| Stage | Campaign | Ch | Archetype | Interest | Geo | Imps | ROAS | Flags |
|---|---|--|---|---|--|--:|--:|---|
| Prospecting | 69884 Beeswax Television Prospecti | CTV | MM OR 3P | MM+3P | 20 | 2.5M | 2.4x | narrow geo 20/210 |
| Prospecting | 109926 Beeswax Television Prospecti | CTV | MM OR 3P | MM+3P | 38 | 2.2M | 1.8x | — |
| Prospecting | 96108 Beeswax Television Prospecti | CTV | MM OR 3P | MM+3P | 152 | 690K | 1.5x | thin geo 152/210 |
| Prospecting | 115946 Beeswax Television Prospecti | CTV | MM OR 3P · net-new gate | MM+3P | 20 | 595K | 1.3x | net-new gate, narrow geo 20/210 |
| Prospecting | 115943 Beeswax Television Prospecti | CTV | MM OR 3P · net-new gate | MM+3P | 20 | 584K | 1.5x | net-new gate, narrow geo 20/210 |
| Prospecting | 115945 Beeswax Television Prospecti | CTV | MM OR 3P · net-new gate | MM+3P | 20 | 583K | 1.3x | net-new gate, narrow geo 20/210 |
| Retargeting | 89071 TV Retargeting - Multi-Touch | disp | retargeting (site/cart) | — | 1 | 2.5M | 64.4x | display (not CTV) |
| Retargeting | 89071 TV Retargeting - Multi-Touch | disp | retargeting (site/cart) | — | 1 | 1.6M | 63.1x | display (not CTV) |
| Retargeting | 89071 TV Retargeting - Television  | CTV | retargeting (site/cart) | — | 1 | 841K | 13.1x | — |
| Retargeting | 89071 TV Retargeting - Multi-Touch | disp | retargeting (site/cart) | — | 1 | 822K | 49.6x | display (not CTV) |
| Retargeting | 89071 TV Retargeting - Television  | CTV | retargeting (site/cart) | — | 1 | 818K | 11.5x | — |
| Retargeting | 89071 TV Retargeting - Television  | CTV | retargeting (site/cart) | — | 1 | 798K | 10.1x | — |
| Multi-Touch S2 | 69884 Multi-Touch | disp | multi-touch pool | — | 20 | 1.6M | 0.1x | display (not CTV) |
| Multi-Touch S2 | 109926 Multi-Touch | disp | multi-touch pool | — | 38 | 1.1M | 0.0x | display (not CTV) |
| Multi-Touch S2 | 96108 Multi-Touch | disp | multi-touch pool | — | 152 | 503K | 0.0x | display (not CTV) |
| Multi-Touch S2 | 115943 Multi-Touch | disp | multi-touch pool | — | 20 | 288K | 0.0x | display (not CTV) |
| Multi-Touch S2 | 115945 Multi-Touch | disp | multi-touch pool | — | 20 | 287K | 0.0x | display (not CTV) |
| Multi-Touch S2 | 115946 Multi-Touch | disp | multi-touch pool | — | 20 | 287K | 0.1x | display (not CTV) |
| Multi-Touch S3 | 69884 Multi-Touch - Plus | disp | multi-touch pool | — | 20 | 777K | 7.5x | display (not CTV) |
| Multi-Touch S3 | 109926 Multi-Touch - Plus | disp | multi-touch pool | — | 38 | 688K | 3.1x | display (not CTV) |
| Multi-Touch S3 | 115946 Multi-Touch - Plus | disp | multi-touch pool | — | 20 | 139K | 1.9x | display (not CTV) |
| Multi-Touch S3 | 115943 Multi-Touch - Plus | disp | multi-touch pool | — | 20 | 138K | 1.3x | display (not CTV) |
| Multi-Touch S3 | 115945 Multi-Touch - Plus | disp | multi-touch pool | — | 20 | 137K | 1.6x | display (not CTV) |
| Multi-Touch S3 | 96108 Multi-Touch - Plus | disp | multi-touch pool | — | 152 | 134K | 1.6x | display (not CTV) |
