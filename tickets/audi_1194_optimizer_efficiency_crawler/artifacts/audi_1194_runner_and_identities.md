# AUDI-1194 — moving the sweep off the laptop: runner and identities

**2026-08-20. The application half is built and verified; the infrastructure half needs a
mntn-devops PR.** Frame: *"Run the Spark optimizer unattended on a shared identity, with no
long-lived credential on any laptop."*

**Done here:** a credential-free container image, artifacts published to GCS instead of
committed to a repo (so the runner needs no GitHub identity at all), and the recommendation
corrected from GitHub Actions to Cloud Run after reading the OIDC allow-list.

**Needs someone else:** the GCP service account and its bindings, the Crossplane job manifest
and scheduler, an Astro deployment token, and a Databricks service principal.

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
with an attached identity, plus the org's approved secret store for anything that genuinely
cannot be eliminated.

**The org already has a paved road here, and it is not Secret Manager by default** (SOP 052,
`docs/standard_operating_procedures/052-secrets-management-strategy.md`): secrets default to
**Vault/ESO**, with Secret Manager as a **narrow, documented exception**. SOP 060 covers the
GitHub side — Actions get short-lived **Octo STS** App tokens, never PATs
(`docs/standard_operating_procedures/060-github-actions-octo-sts-tokens.md`). SOP 063 is the
least-privilege principle this whole doc is an instance of (`063-security-principles.md:136-152`).
Conform to those rather than introducing a new exception; the fewer secrets this job needs, the
less of that conversation applies.

## 3. Recommended shape — **corrected 2026-08-20 after reading the OIDC config**

**Cloud Run Job + Cloud Scheduler in `mntn-prj-prod-00`.** Not GitHub Actions.

The first draft of this doc recommended GH Actions with Workload Identity Federation. Reading
the actual pool config kills that:

```hcl
# terragrunt/gcp/resources/mntn/prod/platform/mntn-prj-prod-00/oidc/terragrunt.hcl
attribute_condition = "attribute.repository in ['SteelHouse/mntn-devops',
  'SteelHouse/mntn-argocd', ... 'SteelHouse/ai-engineering-services']"
```

The pool `mntn-prj-prod-gh-oidc` (project number `995798185124`, provider `github`)
allow-lists **23 `SteelHouse/*` repositories**. This crawler lives in
**`mdunn-mntn/malachi-workspace` — a personal repo**, which is not on that list and should not
be added to it: putting a personal repository on a prod OIDC allow-list means anyone with push
access to it can mint prod GCP tokens. Devops would be right to refuse, and `airflow-ti` is not
on the list either, so relocating the code does not obviously fix it.

Cloud Run sidesteps the question entirely: the job runs **as a GCP service account in the prod
project**, triggered by Cloud Scheduler. No repository identity is involved, no OIDC trust has
to be widened, and still no service-account key exists anywhere.

**There is a precedent, but only half of it is verified.** `daily-jedi-media-spend` in
`dw-finance-compliance` is a scheduled Cloud Run job. What is confirmed is the **IAM** split:
Terragrunt owns the invoker binding (`.../jedi-media-spend-job/terragrunt.hcl:1-12`) because
`provider-gcp-cloudrun` v2.5.x ships `V2Job` but not `V2JobIAMMember` (DEV-8121), and Crossplane
owns the job's other IAM under `argocd-v2/.../dw-finance-compliance/iam/jedi-media-spend-job/`.

> **CORRECTED 2026-08-20 (Compass design review).** An earlier draft of this doc said
> "Crossplane owns the `V2Job` manifest". **That was inferred from the Terragrunt header comment,
> not verified.** The comment says Crossplane owns the job's *IAM*; it does not say where the job
> resource lives. A grep for `kind: V2Job` across `argocd-v2/mgmt/platform/crossplane` returns
> **no matches**, and `jedi-media-spend` does not appear in `mntn-argocd` at all. The compute
> manifest's home is **unresolved** — possibly a third repo or a composite claim that is not
> indexed. **Find it before copying the pattern**, or you inherit an IAM-only shape and miss the
> compute resource's real owner and promotion gates. Ask the `dw-finance-compliance` owner or
> DEV-8121's author.

### What is already built and verified

- **The image is credential-free by construction** — `airflow_optimizer/Dockerfile`, on
  `google/cloud-sdk:slim` because gsutil is not optional (`gcloud storage cp` corrupts the
  `.zstd` logs). Runs as a non-root user; a build-time crawl of the test fixtures fails the
  build if the parser is broken, so a bad image never reaches the scheduler.
- **Artifacts go to GCS, never to a repo** — `sweep.publish()` copies the backlog, digest,
  coverage report and ledger to `OPTIMIZER_GCS_PREFIX`. Verified 2026-08-20 against
  `gs://mntn-data-archive-prod/optimizer/` with a real sweep: three artifacts up, then removed.
  This is what keeps the GitHub identity genuinely read-only, because the runner never needs
  write access to anything but a bucket prefix.

## 4. Identity per surface

| Surface | Identity | Credential | Least-privilege scope |
|---|---|---|---|
| **GCP** (GCS + Dataproc read, artifact write) | GCP SA `spark-optimizer@mntn-prj-prod-00` | **None** — the Cloud Run job's attached identity | `roles/storage.objectViewer` on `mntn-data-archive-prod` and the PHS temp bucket; `roles/storage.objectCreator` on the `optimizer/` prefix only; `roles/dataproc.viewer` on the project |
| **Scheduler → job** | Cloud Scheduler SA | None | `roles/run.jobsExecutor` on this job only (the `daily-jedi-media-spend` Terragrunt pattern) |
| **Airflow** | Astro Deployment API token | Approved store — **Vault/ESO by default** (SOP 052); Secret Manager only as a documented exception | Read-only on the `airflow-ti` deployment. The coverage pass only does `GET /dags` and `GET /dags/{id}/tasks` |
| **Databricks** | Databricks **service principal** | OAuth M2M client secret in the approved store (see SOP 052; **not** assumed Secret Manager) | `CAN_VIEW` on jobs; `CAN_USE` on one SQL warehouse; SELECT on the tables `EXPLAIN COST` plans |
| **GitHub** | **not needed at all** | — | The runner reads no repo and writes no repo. If a read is ever wanted: a single-repo `contents: read` App via the **Octo STS** pattern (SOP 060), which *structurally* cannot open a PR. Never a PAT — SOP 052's FAQ prohibits it outright |

### GitHub, specifically: the cleanest answer is no GitHub access

You asked for read access with no PRs. Publishing artifacts to GCS instead of committing them
removes the runner's only reason to touch GitHub, so the safest scope is **none**. If a repo
read is later needed, a GitHub App limited to `contents: read` and `metadata: read` cannot open
a pull request, comment, or push — the minted token simply carries no such permission. A PAT
with `repo` scope could, so do not use one.

### The grants that already exist

Two of the four are effectively done, because DEV-8182 and mntn-devops#4724 were both written
against a **group**, not a person:

- `roles/dataproc.viewer` on `mntn-prj-prod-00` → `group:audience-intelligence@mountain.com` (standing)
- `roles/storage.objectViewer` on the PHS temp bucket → same group (mntn-devops#4724, in review)

So the GCP half is one mntn-devops PR: add the new SA to that group, or mirror the two bindings
onto it directly. Group membership is simpler and keeps the existing bindings as the single
source of truth.

### Databricks: there is already a convention, so I did not mint a third identity

The workspace already has two service principals, and they follow a pattern:

| Service principal | applicationId | Groups |
|---|---|---|
| `prod_runner` | `397d710b-4c85-4a96-b009-a07c1d373204` | `producers_prod` |
| `dev_runner` | `81b867bc-e052-4b4a-8881-39a3321f73e2` | `producers_dev` |

I have workspace `admins` and could have created a `spark_optimizer` principal, but stopped:
reusing `prod_runner` widens the blast radius of an identity that presumably drives the dbt
runs, and inventing a third name cuts across a convention I do not own. That is a call for
whoever owns these.

**Recommended:** a dedicated `spark_optimizer` service principal, in **no** `producers_*`
group — the optimizer reads, it never produces. It needs exactly `CAN_VIEW` on jobs,
`CAN_USE` on one SQL warehouse, and SELECT on the tables `EXPLAIN COST` plans.

**Do not mint the secret until the store is settled.** SOP 052 makes Vault/ESO the default and
Secret Manager a narrow exception, so the destination is an open question, not a given. Whatever
it is, pipe the secret straight into it — an OAuth M2M secret is shown once, and echoing it to a
terminal puts it in the scrollback:

```bash
# destination TBC pending the SOP 052 answer; shape only
databricks service-principal-secrets create <sp-id> -p malachi@mountain.com -o json \
  | jq -r .secret | <write to the approved store, never to a file>
```

## 5. What each piece costs

| Step | Where | Effort | Status / blocked on |
|---|---|---|---|
| Container image | this repo | S | **done** — `airflow_optimizer/Dockerfile` |
| Artifacts to GCS, no repo write | this repo | S | **done** — `sweep.publish()`, verified against prod |
| GCP SA `spark-optimizer@` + role bindings | mntn-devops PR | S | Cristina's review |
| Add the SA to `audience-intelligence@` (or mirror the two existing bindings) | same PR | S | — |
| Crossplane `V2Job` manifest + Cloud Scheduler | mntn-argocd / mntn-devops | M | follows `daily-jedi-media-spend` |
| `run.jobsExecutor` binding for the scheduler SA | mntn-devops PR | S | Crossplane cannot express it (DEV-8121) |
| Astro deployment API token → Secret Manager | Astro UI (org admin) | S | who owns the Astro org |
| Databricks service principal + M2M secret | Databricks admin console | S | you are in `admins`; mint the secret straight into Secret Manager |
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

## 8. Open questions — after the Compass review, 2026-08-20

Compass confirmed the identity direction (dedicated SA + Cloud Run, no personal SSO, no key) and
flagged that MNTN's paved road already refuses human-carried credentials for automation, so the
job should **conform rather than customise**. Four things it could not settle:

1. **Where does the `V2Job` manifest actually live?** `kind: V2Job` is not in the Crossplane tree
   and `jedi-media-spend` is not in `mntn-argocd`. Ask the `dw-finance-compliance` owner or
   DEV-8121's author before copying the pattern.
2. **SA bindings: join the group or stand alone?** DEV-8182 and mntn-devops#4724 both granted to
   `group:audience-intelligence@mountain.com` rather than a principal. Needs the IAM owner.
3. **Does Octo STS (SOP 060) even apply to a Cloud Run job**, or only to Actions workflows? And
   who approves a single-repo `contents: read` install?
4. **Vault/ESO or Secret Manager** for the Databricks M2M secret, under SOP 052's narrow-exception
   rule (`052-secrets-management-strategy.md:175-185`)?

Plus the one Compass is structurally blind to: **who owns the Astro org** and can mint a
deployment token. INC-006 established the Astronomer deploy is not in Compass's monitored fleet —
that question goes to `#dev-basecamp` or devops directly.

**Verification Compass asked for, worth doing either way:** once the job is live, confirm IAM
Policy Analyzer shows **no personal-account binding left** on the two buckets or the project. That
is the actual test that the personal-SSO path is gone, not just supplemented.
