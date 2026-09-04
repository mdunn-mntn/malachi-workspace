# Slack draft — reply to Edgar, geo vs user-level holdout (2026-09-04)

Yep, user-level. The calculator only models the ghost-bid IP holdout. It's a two-proportion test
on per-IP visit and conversion rates, so there's nowhere in it to even put a geo design.

On the geo question, the scale part is real but it doesn't come out where you'd expect. A control
DMA does carry more bodies than a 10% ghost holdout. The problem is what's in it. The control
market holds everybody, and we only reach a percent or two of households, so a 5% lift among the
people we actually served shows up as a tiny fraction of a percent at the market level. Your
standard error stays about the same and the signal you're chasing shrinks by the reach rate.
That's why geo ends up needing more spend, not less.

Our own number is $500k a month minimum to see 2-8% lift, and even then MDE lands near 15%. Haus
wants 500 to 1000 conversions a week and $10M a year cross-channel before they'll call a geo test
valid. Ghost bidding on the holdout we already have gets a visits readout around $200k a month,
with no incremental budget and no advertiser sign-off.

ElevenLabs is the one place we've run both. Geo and ghost-bid both came back at basically zero
conversion lift. And their history is your own lesson from the 50-test review, geo lift showed up
in the Bay Area and then TX/FL and diluted away once it went national at a million a month.

Where geo wins is what it measures, not what it costs. It picks up cross-device, offline and
in-store, walled gardens, and cannibalization of other channels, and the advertiser can audit it
against their own sales. Ghost bidding is blind to all of that by construction. So it's a
different question, not a cheaper one.

One correction on what I said earlier. Realized holdout at campaign grain runs 4.7% to 13.6% with
a median near 8.9%, and it drifts down more often than up. The gate we ship is 7 to just under 11.
