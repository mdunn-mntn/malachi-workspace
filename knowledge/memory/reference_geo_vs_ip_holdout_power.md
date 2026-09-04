---
name: reference_geo_vs_ip_holdout_power
description: "A geo holdout carries MORE effective sample than the 10% IP ghost-bid holdout and still needs more spend, because the control market dilutes the lift by the addressable share"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [geo holdout, geo test, DMA test, matched market, ghost bid holdout, user-level holdout, dilution, addressable share, design effect, ICC, cluster randomization, GeoLift, LiftLab, Haus, ElevenLabs, MDE calculator, estimand]
domain: [experimentation, incrementality]
lifecycle: active
last_verified: 2026-09-04
---
**"A geo holdout has more scale so it needs less spend" is half right and lands on the wrong conclusion.**
After the cluster design effect a 210-DMA split IS worth more effective sample than a 90/10 IP split
(~2.6M individual-equivalents vs ~720k at a between-market CV of 0.20), and the standard error of the
lift **as measured** is within ~1.4x. Geo does not lose on N.

**Geo loses on dilution.** The ghost-bid control is exactly the IPs the bidder would have served, so the
measured relative lift is the lift on the exposed. A control DMA holds everyone, and MNTN reaches ~1-2%
of households, so the observed effect is the true effect times the addressable share `a`. The SE does not
shrink with `a` but the signal does, so required sample scales `1/a^2`. **Adding non-addressable people to
the control market strictly reduces power** — baseline noise, zero signal. More people per DMA cannot help
either: between-market variance is a property of the market, not a sampling term.

**How to apply:** if someone proposes geo for POWER, redirect. If the 10% control cell is too thin, the fix
is a bigger IP holdout (~1.67x on SE at 20%), which is open decision item 1 of the iROAS playbook, not a geo
split. Choose geo for the ESTIMAND: device-agnostic (no lossy CTV-IP to web-IP join), offline and in-store
conversion, walled gardens, cross-channel cannibalization, and a counterfactual the advertiser can audit
against their own sales. Quote MNTN's $500k/month geo floor at ~15% MDE as an estimate, never as measured:
addressable share, between-DMA CV, and pre/post R-squared are all assumed and none has been measured here.

**The MDE calculator does not cover this** — two-proportion binomial on per-IP rates, no cluster count, no
ICC, no design effect. Valid for the ghost-bid IP holdout only. [[reference_mde_surface_choice]]

Full derivation, the spend table, the ElevenLabs head-to-head, and the reconciliation of this file's own
geo ranking against its geo power note: `knowledge/experimentation.md` § "Geo holdout vs IP-level ghost-bid
holdout". Related: [[feedback_ghost_holdout_not_frequency_capped]], [[project_incrementality_experiment]].
