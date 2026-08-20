# Slack DM draft, Cristina Szumilo (@csz-mntn), re: mntn-devops#4724

Ready to send. PR un-drafted 2026-08-20, reviewers requested (@SteelHouse/devops + csz-mntn),
all checks green, mergeable. Everything below the marker is the message.

## Message

Hi Cristina, can you review and approve https://github.com/SteelHouse/mntn-devops/pull/4724? One additive read-only bucket-IAM binding. It's been a draft since the 7th with nobody assigned, my fault.

It gives `audience-intelligence@` objectViewer on the Dataproc PHS temp bucket so a scheduled crawler can read ipdsc/tpa Spark event logs. The PAM path maxes out at 18h so it can't back a cron. Bucket-scoped, mirrors the `mntn-marketo` pattern. `dataproc.viewer` is already standing from DEV-8182.
