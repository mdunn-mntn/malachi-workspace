# War-room shared charts — 2026-04-22

Charts shared by other workstream owners in the war room. Images are in the Slack channel `C0ATVHK2EDV` — not re-committable here. Descriptions below so future readers (and the presentation) have calibration.

These inform our framing: the audience-composition shift is ONE lane of a bigger picture. We report our lane without overclaiming causation.

---

## 1. MoM Spend Change — Consecutively Active Advertisers (Feb 2025 → present)

Two stacked panels. Owner: Will Cavey.

**Top panel: % of consecutively-active advertisers declining MoM.**
- Starts ~51% (Feb 2025), dips to ~36% (May 2025), then climbs steadily.
- Peaks at **~70% by Feb–Mar 2026**.
- Trend slope: **+1.7 pp / month, R² = 0.40**.

**Bottom panel: Median % cut among decliners.**
- Noisy, hovering 5–20% across the period.
- Trend slope: ≈0 / month, R² = 0.00 (flat).

**Takeaway (Will's framing):** It's a **volume problem, not a magnitude problem** — more advertisers are cutting budget, but each cut isn't getting bigger. Budget-contraction velocity is accelerating.

## 2. New Advertiser Short-Term CLV by Cohort

Stacked panels. Owner: Will Cavey.

**Top: Median cumulative 3M and 6M spend per cohort month.**
- 3M median: slope **−$363 / month** (R² = 0.48). Feb 2025 cohort ≈ $12.7K → Feb 2026 cohort ≈ $6K — roughly **50% decline** in 3-month CLV.
- 6M median: slope **−$706 / month** (R² = 0.41). Feb 2025 ≈ $16K → late-2025 cohorts ≈ $12K.

**Bottom: New advertisers per cohort month.**
- Bar counts 170–310 per month. Mild declining trend.

**Takeaway:** New-cohort CLV is down materially, and new-advertiser count is slightly down. The customer base is getting smaller AND less valuable per customer.

## 3. Monthly Revenue + Revenue per AID (Oct 2024 → Apr 2026)

Dual-axis. Owner: Ray.

- **Bars = total monthly revenue.** Range $28M–$50M. Nov 2025 spikes to ≈$49M (holiday). Total revenue is holding together, barely.
- **Line = Revenue per AID.** ≈$31K (Oct 2024) → ≈$15K (Apr 2026). **Revenue per advertiser fell ~50% over 18 months.**

**Takeaway:** The revenue-per-AID collapse is *the* war-room metric. Total rev is propped up by bigger AID count; per-advertiser monetisation is halving.

## 4. Order-data availability per month (new-advertiser cohorts)

Three column pairs (count + %) per month: `has_unique_order_ids`, `has_order_amt`, `has_conversions`. Owner: Ray.

- **`has_order_amt`**: declining materially. ~64% (Nov 2024) → **~37% (Jan 2026)**.
- **`has_unique_order_ids`**: stable at 73–84%.
- **`has_conversions`**: variable but declining — ~93% (Nov 2024) → ~75% (Apr 2026).

**Takeaway (Alex Bloore's framing):** The real decline is on *use of Order Amount* — which ROAS depends on. Conversions still fire; advertisers just aren't passing order amounts. Part instrumentation, part lead-gen-customer mix.

---

## What this means for our (TI-896) deliverable

**We're in our lane.** Audience composition is one of the workstreams Richard asked for. We report it straight — the only material shift is Interest-audience adoption tripling in the drop window.

**Don't overclaim.** The audience shift is correlated with Peak Performance launch, not causally tied to the revenue-per-AID decline. Rev-per-AID was already declining for the full 18 months before Peak Performance shipped. Interest's ramp is *one* moving piece in a system-wide contraction; we won't position it as "the cause."

**Complementary, not competing.** Our chart should live alongside Ray's rev-per-AID line and Will's velocity chart. If the deck gets rolled into the master exec summary, framing should be: "Within targeting, here's what moved."
