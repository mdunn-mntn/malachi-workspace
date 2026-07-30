---
name: Bidstream Feature Store Initiative
description: TI-789 epic — bidstream feature extraction and DS13/DS19 audience augmentation with Alex Knorr and Ryan Kleck
type: project
doc_type: memory
keywords: [bidstream initiative, ti-789, feature store, audience augmentation, ds13, ds19, fangorn, augmentor_log, bidder_auction_events, alex knorr, ryan kleck]
domain: [project, audience-scoring, bidding]
lifecycle: archived
last_verified: 2026-07-29
---
Epic TI-789: Bidstream Feature Extraction & Audience Augmentation. Team: Malachi, Alex Knorr, Ryan Kleck.

Two workstreams:
1. **Feature Store** (TI-790, TI-791, TI-792, TI-793): Extract bidstream features → model for importance → integrate into fangorn
2. **Audience Augmentation** (TI-794, TI-795, TI-796): Expand DS13/DS19 via bidstream signals → validate incrementality → holdout experiment → production integration + RTC exploration

**Why:** Fangorn uses IP-level features to predict visits/conversions. Adding bidstream signals (augmentor_log, bidder_auction_events) could improve targeting performance and expand audience pools.

**How to apply:** When working on any TI-789 child ticket, context is bidstream data at gs://mntn-data-archive-prod/ (parquet). augmentor_log has 30-day TTL. Data is massive — always sample. Filter blank IPs and non-US geo. Multiple bid providers (Magnite primary). Weekly syncs on Wednesdays.
