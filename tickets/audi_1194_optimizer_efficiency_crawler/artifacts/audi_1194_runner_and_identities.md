# AUDI-1194 — moving the sweep off the laptop: runner and identities

**Scoping doc, 2026-08-20. Nothing here is built.** This is a distinct unit of work from
AUDI-1194 (which is the crawler itself) and should get its own ticket before any of it starts.
Proposed frame: *"Run the Spark optimizer unattended on a shared identity, with no long-lived
credential on any laptop."* Task, not Spike. Leverage: velocity multiplier + it is the last
thing standing between the crawler and being genuinely automatic.

---

## 1. The actual problem is not identity, it is expiry

The sweep already runs on a schedule. What it cannot do is run **unattended**, because every
credential it touches is a personal one that dies:

| Surface | What it uses today | How it dies |
|---|---|---|
| GCS (`spark-events`, PHS temp bucket) | `malachi@mountain.com` gcloud SSO | SSO session expiry |
| Dataproc `batches list/describe` | same | same |
| Airflow API (coverage pass) | astro CLI bearer from `~/.astro/config.yaml` | **~1 hour** (IMP-040) |
| Databricks (`EXPLAIN COST`) | U2M OAuth profile `malachi@mountain.com` | refresh token expires, renewal is interactive |
| GitHub (`gh`) | personal creds | fine, but personal |

A Monday-morning laptop with a stale token produces a green cron run and an empty backlog.
That is the failure mode this closes.

## 2. The rule this has to respect

MNTN decommissioned local Slack apps and API keys on **2026-06-10**. The reading that matters
is not "no credentials ever" — Airflow and Databricks have no keyless path — it is:

> **No long-lived credential on a personal machine, and no downloaded service-account key
> anywhere.**

So the answer is not "get service accounts and put their keys in `~/.config`". It is a runner
with a workload identity, plus Secret Manager for the two services that genuinely need a secret.

## 3. Recommended shape

**GitHub Actions, scheduled, with GCP Workload Identity Federation.**

Why this over the alternatives: mntn-devops already runs GH Actions at scale (CodeQL, Wiz,
ArgoCD, Terragrunt checks), so there is an existing pattern, an audit trail, and nobody has to
own a VM. WIF means **no service-account key exists at all** — the runner exchanges its OIDC
token for a short-lived GCP access token at job start.

Considered and not recommended:

- **Cloud Run Job + Cloud Scheduler** — also keyless and arguably a better fit if the sweep grows
  past the GH Actions 6-hour ceiling or needs a private VPC path to Airflow. Worth revisiting;
  today it is more infrastructure for the same result.
- **The Pi** — explicitly ruled out. Project instructions forbid an API key on it, and this needs
  two.
- **Downloaded SA keys on the Mac** — the pattern that was decommissioned. Not an option.

## 4. Identity per surface

| Surface | Identity | Credential | Least-privilege scope |
|---|---|---|---|
| **GCP** (GCS + Dataproc read) | GCP SA `spark-optimizer@mntn-prj-prod-00` | **None** — WIF, short-lived token | `roles/storage.objectViewer` on `mntn-data-archive-prod` and the PHS temp bucket; `roles/dataproc.viewer` on `mntn-prj-prod-00` |
| **Airflow** | Astro Deployment API token | Token in Secret Manager | Read-only on the `airflow-ti` deployment. The coverage pass only does `GET /dags` and `GET /dags/{id}/tasks` |
| **Databricks** | Databricks **service principal** | OAuth M2M client secret in Secret Manager | `CAN_VIEW` on jobs; `CAN_USE` on one SQL warehouse. `EXPLAIN COST` needs SELECT on the tables it plans |
| **GitHub** | GitHub App, org-installed | App private key in Secret Manager | **`contents: read`, `metadata: read`. Nothing else.** |

### GitHub, specifically: it must not be able to open a PR

You asked for read access and no PRs. Make that **structural, not a policy note**: a GitHub App
whose manifest grants only `contents: read` and `metadata: read` *cannot* open a pull request,
comment, or push — the token the API mints simply has no such permission. A PAT with repo scope
could, so do not use one.

The one thing that would push against this: if the runner wanted to commit the daily backlog and
ledger back to `malachi-workspace`, it would need `contents: write`. **Don't do that.** Write the
sweep's artifacts to GCS instead (`gs://mntn-data-archive-prod/optimizer/<date>/`) and keep the
repo copy a human action. That keeps the GitHub identity genuinely read-only and removes the only
reason the runner would ever need to write anywhere.

### The grants that already exist

Two of the four are effectively done, because DEV-8182 and mntn-devops#4724 were both written
against a **group**, not a person:

- `roles/dataproc.viewer` on `mntn-prj-prod-00` → `group:audience-intelligence@mountain.com` (standing)
- `roles/storage.objectViewer` on the PHS temp bucket → same group (mntn-devops#4724, in review)

So the GCP half is one mntn-devops PR: add the new SA to that group, or mirror the two bindings
onto it directly. Group membership is simpler and keeps the existing bindings as the single
source of truth.

## 5. What each piece costs

| Step | Where | Effort | Blocked on |
|---|---|---|---|
| GCP SA + WIF pool/provider binding | mntn-devops PR | S | Cristina's review |
| Add SA to `audience-intelligence@` (or mirror bindings) | same PR | S | — |
| Astro deployment API token | Astro UI (org admin) | S | who owns the Astro org |
| Databricks service principal + M2M secret | Databricks admin console | S | you are already in `admins` |
| GitHub App, read-only, org-installed | GitHub org admin | S–M | org owner approval |
| Move the sweep into a workflow file + Secret Manager reads | this repo | M | the four above |
| Digest delivery through `compass-slack` | mntn-devops | M | see §6 |

## 6. Slack delivery, honestly

The digest is written to a file today and nothing posts it. The approved route is the
**`compass-slack` automation already living in `mntn-devops/automations/apps/`** — it is
server-side, already has a Slack credential, and is owned by devops. The ask is an endpoint that
accepts a rendered digest for a named channel, not a new Slack app and not a webhook URL sitting
in a repo secret.

`digest.render()` already emits Slack-flavoured markup (`<url|label>` links, `*bold*`), so the
posting side is a transport, not a rewrite. Until that lands the digest stays a file, which is a
working state, not a blocked one.

## 7. What this does not fix

- **Nothing here widens what the optimizer can read.** It still profiles Spark tasks only; the 38
  active DAGs with no Spark task stay invisible (they are at least named now).
- **Databricks plans still need a query to plan.** A service principal does not solve the missing
  bridge from an enumerated job run to the SQL it ran.
- **A runner does not make the findings right.** The verify-before-send rule stays a human step;
  automating the send is explicitly out of scope.

## 8. Open questions for whoever picks this up

1. Who owns the Astro org and can mint a deployment API token?
2. Does the GitHub App need org-owner approval, or can it be installed on the single repo?
3. GH Actions or Cloud Run Job? GH Actions is the recommendation, but if the Airflow API is ever
   moved behind a VPC, Cloud Run becomes the only one that works.
4. Does `compass-slack` already accept an arbitrary rendered message, or does it need an endpoint?
