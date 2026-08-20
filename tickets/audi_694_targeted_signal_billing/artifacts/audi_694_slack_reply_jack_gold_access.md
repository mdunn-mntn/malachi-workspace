# Draft reply to Jack re: dw-main-gold access (2026-08-20)

No, I never requested it, I have standing read on dw-main-gold, which is why I assumed you'd have it
too. Sorry, that was a bad assumption on my part.

Before you go through access though, check what the error actually says. If it's "does not have
bigquery.jobs.create permission in project mntn-coredw-prod" (or whatever your default project is),
that's the billing project defaulting wrong rather than gold itself. Rerun it billing to gold, i.e.
add `--project_id=dw-main-gold --location=us-central1` to the CLI call, or set gold as the billing
project in the console.

If it's an actual Access Denied on the table, then it's PAM. Everyone else on gold goes through it in
8 hour windows, Wei included:

```
gcloud pam grants create --entitlement=bq-read --project=dw-main-gold --location=global \
  --requested-duration=28800s \
  --justification="AUDI-694 crediting simulations on reporting.ddp_crm_graph_cpm"
```

Approval is DevOps and it auto-revokes at expiry, so you'd be re-requesting each session. If that
gets old, say the word and I'll pull the 214k rows out to a file you can work off directly.
