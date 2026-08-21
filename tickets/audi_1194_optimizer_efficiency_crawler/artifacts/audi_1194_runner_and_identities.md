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

**The org already has a paved road here, and Secret Manager is not it** (SOP 052,
`052-secrets-management-strategy.md`). Its checklist leads with *"Identity possible? ALWAYS prefer
Workload Identity. If identity works, no secret is allowed."* Secret Manager is acceptable only
when **all four** hold: the workload is Google-only and natively consumes Secret Manager, the API
is enabled and not blocked by `restrictServiceUsage`, Workload Identity cannot eliminate the
secret, and the exception is recorded with an owner.

A Databricks OAuth M2M secret and an Astro API token are **third-party credentials**, so they fail
the first condition outright. **The store is Vault, via `mntn-team-credentials`.**

> **RESOLVED 2026-08-20.** An earlier draft of this doc said "Secret Manager". It also logged an
> apparent contradiction between SOP 052 (Vault/ESO) and the June note (SOPS-in-ArgoCD). They do
> not compete — **SOPS-in-git is the transport INTO Vault, split by repo** (SOP 055):
> `mntn-argocd` `apps-v3/secrets/**.enc.yaml` uses *Rotate a SOPS Secret*, while
> `mntn-team-credentials` `secrets/**.enc.yaml` uses *Update Team Secret*, which also runs
> `sync-manifests`. Using `rotate-secret` on the latter **breaks Vault delivery**. SOP 065 adds
> that the SOPS template is "not for new secrets and never for `mntn-team-credentials`". KMS
> decrypt is disabled for engineers, which is why Basecamp is the only self-serve path.

**And the team already owns this exact credential class** — verified directly, not inferred:

```yaml
# SteelHouse/mntn-team-credentials
#   secrets/team-engineering-targeting/databricks/teamsecret.yaml
kind: TeamSecret
spec:
  owner: group:team-engineering-targeting
  vault: { path: teams/team-engineering-targeting/databricks }
  keys: [host, client_id, client_secret]
  environments: [prod]
  description: Databricks Secrets for ShopperGraph
```

Siblings under that team path today: `coredb`, `kafka-config`, `openai`, `reportingdb`,
`sendgrid`, `targeting-secrets`, `vector-search`. **Add a new sibling entry for the optimizer
rather than extending the ShopperGraph one** — same team, same store, different workload, so
conflating them would make revocation hit both. Use the **Update Team Secret** template. No new
store, no exception to document.

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
| **Airflow** | Astro Deployment API token | **Vault** via `mntn-team-credentials` (assumed by analogy — no Astro precedent found) | Read-only on the `airflow-ti` deployment. The coverage pass only does `GET /dags` and `GET /dags/{id}/tasks` |
| **Databricks** | Databricks **service principal** | **Vault**, new sibling entry under `secrets/team-engineering-targeting/` (verified path) | `CAN_VIEW` on jobs; `CAN_USE` on one SQL warehouse; SELECT on the tables `EXPLAIN COST` plans |
| **GitHub** | **none** | — | The runner reads and writes no repo. **Octo STS (SOP 060) is Actions-only by its own scope statement and does not reach a Cloud Run job**, so there is no paved road to take even if a read were wanted. Never a PAT — SOP 052's FAQ prohibits it |

### GitHub, specifically: the cleanest answer is no GitHub access

You asked for read access with no PRs. Publishing artifacts to GCS instead of committing them
removes the runner's only reason to touch GitHub, so the safest scope is **none** — and that is
now also the *easiest* answer, because the org's paved road does not reach this workload:

**SOP 060's Octo STS pattern does not apply to a Cloud Run job.** Its scope statement is explicit:
it brokers a short-lived GitHub App installation token *inside a GitHub Actions workflow*, using
the runner's GitHub OIDC token at every job start. A Cloud Run job has no Actions runner and so no
runner OIDC token to broker with. Whether the self-hosted Octo STS instance would accept a
Google-issued OIDC token is **unconfirmed** — nothing retrieved addresses it.

So: take no GitHub access. If source lookup is later wanted, ask team-engineering-dev-ops
(SOP 060 owner, `last_reviewed: 2026-07-06`) whether Octo STS accepts a non-Actions issuer, and
if not, what the approved read-only cross-repo mechanism is for a GCP compute workload. **Do not
fall back to a PAT** — the SOP 052 FAQ prohibits it outright.

### The grants that already exist

Two of the four are effectively done, because DEV-8182 and mntn-devops#4724 were both written
against a **group**, not a person:

- `roles/dataproc.viewer` on `mntn-prj-prod-00` → `group:audience-intelligence@mountain.com` (standing)
- `roles/storage.objectViewer` on the PHS temp bucket → same group (mntn-devops#4724, in review)

So the GCP half is one mntn-devops PR. **Give the SA its own bindings; do not put it in the
group.** The reason is auditability, from the IAM audit's own declared limits (2026-08-20 review):

- **Google Workspace group membership is not expanded** by the audit — that needs a standing
  Workspace admin role it does not hold. A grant that reaches a principal through a group is
  therefore invisible to the org's own IAM audit.
- The audit's `iacAttribution` is partial anyway: **499 bindings attributed to Crossplane, 0 to
  Terragrunt, 2,040 unmatched**, because Terraform state is not read. Unmatched is recorded as
  unknown, never as clickops.

A binding the audit can see is one that can be reviewed, trended and revoked with evidence.
Membership in an unexpandable group defeats that, and the group also contains humans, so it
blurs attribution. Direct bindings keep the grant visible in CAI and traceable to one IaC file.

**Still open:** whether the org nonetheless *prefers* the group pattern. That is a human ruling
from team-engineering-dev-ops and the DEV-8182 author, not a query — the dataset that would
settle it is the one that cannot see groups.

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

**The store is settled: Vault, via the Update Team Secret template.** Add a sibling to
`secrets/team-engineering-targeting/` (not the existing `databricks/` entry, which belongs to
ShopperGraph). An OAuth M2M secret is shown once — never echo it to a terminal, or it is in the
scrollback. Do **not** use `rotate-secret` on `mntn-team-credentials`; that breaks Vault delivery
(SOP 055).

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

## 8. Status after the second Compass review, 2026-08-20

Both specialists (`iam-advisor`, `secrets-advisor`) **aborted mid-run** — `cancelled: Request
aborted`, blocked at 10% confidence. What came back is partial tool-level evidence, so it is
leads plus a few things that were fully retrieved.

### Settled

- **Secret store: Vault**, via `mntn-team-credentials` / Update Team Secret. Third-party
  credentials fail SOP 052's Google-only test on their face, and the team already owns the
  credential class (verified path above). No Secret Manager exception to document.
- **The SOPS-vs-Vault contradiction is closed** — they coexist by repo; SOPS-in-git is the
  transport into Vault (SOP 055 / SOP 065).
- **Octo STS does not reach this workload** — SOP 060 is Actions-scoped by its own first sentence.
  Reinforces taking no GitHub access at all.
- **SA gets direct bindings, not group membership** — the IAM audit cannot expand Workspace
  groups, so a group-routed grant is invisible to the org's own audit.

### Still open, and do not open PRs on these

1. **Where does the `V2Job` manifest live?** Unlocated across both reviews.
2. **Does the org nonetheless prefer the group pattern?** Human ruling from
   team-engineering-dev-ops / the DEV-8182 author.
3. **Is conditional IAM used in this project?** The audit run in scope
   (`1467fed0b5abae3f6067486537adea29`) was collected 2026-08-18 12:56 UTC, **58 hours stale**,
   with `denyPolicies` returning 0 rows and marked partial. Absence there is not evidence of
   absence.
4. **Octo STS with a non-Actions OIDC issuer** — unconfirmed; ask the SOP 060 owner.
5. **SOP 052's effective date** — front matter never retrieved.
6. **Astro token store** — no Astro precedent found anywhere; Vault is an analogy, not evidence.
7. **Who owns the Astro org.** Compass is structurally blind here (INC-006): the Astronomer
   deploy is not in its monitored fleet. Ask `#dev-basecamp` or devops.

### Verification to run once it is live

Confirm **no personal-account binding remains** on the two buckets or the project — IAM Policy
Analyzer, and the SA resolving in `v_current_findings` with a resolvable `iac_source`. That is the
test that the personal-SSO path is *gone*, not merely supplemented.

---

## 5. RESOLVED 2026-08-20 — the manifest's home, read directly from the repos

§3 recorded the Cloud Run compute manifest's home as **unresolved** after Compass found
`kind: V2Job` greps empty. Cloning both repos closes it, and the answer is that the earlier
search was looking for the wrong artifact in the wrong repo.

**The manifest lives in `mntn-argocd`, as a claim, not a rendered resource.**
`apps-v3/<team>/<service>/service.yaml` is `kind: PlatformService`
(`apiVersion: platform.mntn.dev/v1alpha1`) naming chart **`gcp-cloudrun` v0.1.0**, with
`values-<env>.yaml` beside it, targeting `crossplane-system` on `mntn-gke-management-01`.
`V2Job` never appears in git because the chart renders it at deploy time. Three users today:
`apps-v3/billing/ironclad-{fetcher,extractor,receiver}`.

**`daily-jedi-media-spend` was the wrong precedent.** Only its `run.jobsExecutor` binding is in
`mntn-devops`; it does not appear in `mntn-argocd` at all, and the `intake` path its header cites
(`apps-v3/integrations/intake/values-prod.yaml`) **does not exist** — that comment is stale.
Chasing it is what produced the unresolved finding. **`ironclad-fetcher` is the pattern.**

**Two things ironclad-fetcher settles that this doc had open:**

1. **The SA is created by the chart, and Terragrunt only adds IAM.** `values-prod.yaml` sets
   `serviceAccount.enabled: true` + `externalName`, and its header reads *"GSA + bucket/project
   IAM: DEV-7890 Terragrunt (adopt via serviceAccount.externalName)."* So §4's split is right but
   inverted from what was assumed: the argocd PR owns the identity, the devops PR owns the grants.
2. **A Cloud Run job reads Vault at runtime via GCP auth — not Secret Manager, not an
   ExternalSecret, not a key.** `VAULT_ADDR`, `VAULT_GCP_AUTH_ROLE: ironclad-cloud-run`,
   `VAULT_SECRET_PATH: teams/team-engineering-billing/ironclad-prod`, with the header stating
   *"runtime Vault GCP auth — not GSM or GKE ExternalSecret."* This is the missing half of §2:
   the `TeamSecret` puts the values in Vault, and a Vault GCP auth role is what lets the running
   job read them. **§4's Astro row said "assumed by analogy — no Astro precedent found"; the
   analogy is now a verified mechanism**, though still not an Astro-specific one.

**What stays open, narrowed:** whether `gcp-cloudrun` renders a Cloud Run **Job** plus a Scheduler
trigger or only a **Service** — all three users declare `cloudRun.enabled` Services, no
`cloudRunJob` or scheduler key appears anywhere in `apps-v3`, and the chart itself is not in
`mntn-argocd` to read. And how a Vault GCP auth role is requested. Both are questions for
team-engineering-dev-ops.

**Also resolved: reuse an existing SA?** Ryan Kleck suggested `airflow-ti-prod` or a targeting
user. `airflow-ti-prod@mntn-prj-prod-00` holds `dataproc.editor`, `storage.objectAdmin` and
`bigquery.admin`, so it would work — and that is the argument against it, since a read-only
observer would inherit write on everything it observes and its audit-log activity would be
indistinguishable from the pipeline's own. **No targeting service account exists in that project**
(only `airflow-ti-prod`, `airflow-camperbid-prod`, `airflow-reporting-prod`). Ryan and Dustin
Niehoff independently landed on "make a new one"; Dustin scoped it as *"Dplat would give the
service account storage access, but you'll need to make the service account."*

---

## 6. 2026-08-20 — Databricks principal created and validated; Astro blocked on a role

**Databricks: done, and verified end to end.** Victor Savitskiy has left, so the naming question
in §4 had no owner to answer it; created it under the read-only reasoning already recorded there.

```
displayName    spark_optimizer
applicationId  07f36af7-614d-4d57-8143-2dbcd3cb58c2
id             215387379744816
entitlements   workspace-access, databricks-sql-access
groups         users            # auto-assigned; NOT producers_prod / producers_dev
secret expires 2028-08-20
```

The OAuth secret is in the login Keychain (`security find-generic-password -a spark_optimizer -s
databricks-sp-secret -w`) until the Vault `TeamSecret` lands. It is not in `~/.databrickscfg`, not
in the repo, and was never printed to a terminal.

**Validation, as the principal and not as me** — `DATABRICKS_CLIENT_ID` / `_CLIENT_SECRET` M2M,
`POST /api/2.0/sql/statements` on warehouse `14b311ac86ee2ca2`, `EXPLAIN COST SELECT 1`:
`SUCCEEDED`, returning `Statistics(sizeInBytes=12.0 B, rowCount=1, ...)`. That is the
Databricks-format annotation all five plan detectors parse, so the acquisition path IMP-033
flagged as unproven is now proven at the auth layer.

**Two CLI details that cost time:**
- The secret subcommand is **`databricks service-principal-secrets-proxy create <id>`** at the
  workspace level. `service-principal-secrets` alone is the *account*-level API and errors as an
  unknown command against a workspace host.
- Use **`permissions update`** (PATCH, additive), never `permissions set` (PUT, replaces the whole
  ACL). `update` left `IS_OWNER` and the `users` `CAN_USE` binding intact while adding the SP.

**Still open on Databricks: `USE SCHEMA` on `system.lakeflow`.** `databricks grants get SCHEMA
system.lakeflow` fails for **me** with `User does not have USE SCHEMA`, so this is not a grant I
can make — it needs a metastore admin (metastore `c5dc6763-eaae-4d6c-9ae2-7af6147595bb`; the
owning principal on the warehouse is `dbxaccountadmin-sa2@mntn-databricks.iam.gserviceaccount.com`,
which looks Terraform-managed). **`CAN_VIEW` on jobs is not a substitute**: job permissions are
per-`job_id`, and the runs the optimizer cares about are ephemeral `SUBMIT_RUN` submissions with
no stable id. `system.lakeflow` is the only enumeration surface.

**Astro: blocked, and the owner is Ryan Kleck, not Victor.**
`astro deployment token create` returns `deployment with id cmd6bd10c0gl901rfuokgryiq is
forbidden`. Reading the roles explains it: I am `ORGANIZATION_MEMBER` + **`WORKSPACE_OPERATOR`**,
and minting a deployment token needs `WORKSPACE_OWNER`. Token *list* works, which is why this read
as an auth failure at first rather than a permission one.

**The precedent closes the "is there a read-only role" question without needing Victor.** The `ti`
deployment already carries exactly this kind of token:

```
dag-run-duration-watchdog | DEPLOYMENT | DEPLOYMENT_ADMIN | created by Ryan Kleck, 141 days ago
"Bearer token for dag_run_duration_watchdog to GET running DagRuns via Airflow REST API."
```

A read-only bot on `DEPLOYMENT_ADMIN` is already the house pattern here, set by the person who
owns the workspace. `astro organization team list` and the token list return **no custom roles**,
so `DEPLOYMENT_ADMIN` is not a compromise anyone can improve on today — it is the only option the
CLI offers. **Ask Ryan Kleck** (`WORKSPACE_OWNER`, and the author of the precedent) to either mint
the token or grant `WORKSPACE_OWNER`. Other workspace owners: Dustin Niehoff, Jordan Piepkow,
Scotty Pate, Alyson, Sean Yang.

---

## 7. 2026-08-20 — the chart layer, read directly. Cloud Run Job is not on the menu

Cristina approved the `ironclad-fetcher` shape in principle ("using ironclad billing is a really
solid example of offloading these types of tasks to service accounts"). Reading the actual charts
changes the mechanism, not the principle.

**The charts live in `SteelHouse/mntn-helm` `charts/`, published to `ghcr.io/steelhouse/mntn-helm`.**
Neither `mntn-devops` nor `mntn-argocd` holds them, which is why §5 could find the claim but not
the template. The GitHub Contents API reads them without GHCR package scope (`helm pull` /
`ghcr.io/v2/...` both 403 on a default `gh` token).

**`gcp-cloudrun` renders a Service and nothing else.** Its README's own resource table is the
whole surface: `V2Service`, `RegionNetworkEndpointGroup`, an imported `ServiceAccount`,
`ProjectIAMMember`, `ServiceIAMMember`. Templates on disk: `service.yaml`, `neg.yaml`,
`invoker.yaml`, `serviceaccount.yaml`, `iam-bindings.yaml`. **There is no `V2Job` template and no
Cloud Scheduler resource anywhere in the chart.** So "Cloud Run Job + Cloud Scheduler" — the shape
§3 recommended and the one that went to devops — cannot be built from the paved road as written.

**The targeting team already runs scheduled work, and it is a GKE CronJob.**
`apps-v3/targeting/audience-service/cron/` and `apps-v3/targeting/3p-audience-builder-api/cron/`
both use chart **`cron-jobs`** from the same registry, deploying to `prod-targeting` on
`mntn-gke-prod-01`. That chart has `cron-jobs.yaml` (a real `kind: CronJob`, with `schedule`,
`concurrencyPolicy: Forbid`, history limits) and `externalsecret.yaml` (ESO → Vault, one Secret
shared by N jobs, one Vault read). **Its ExternalSecret is the Vault delivery mechanism §6 was
still missing** — no Vault GCP auth role needed, no Secret Manager, no key.

**Its one gap, verified by reading all 80 lines of the template: there is no `serviceAccountName`
in the pod spec, and no `annotations` block on the pod.** Without those, the CronJob pod runs on
the node's default KSA and cannot use GKE Workload Identity, so it has **no GCP identity at all**
— which is the entire point of the exercise. A one-line addition to that template fixes it.

### The three options, priced honestly

| Shape | What it costs |
|---|---|
| Cloud Run **Job** + Scheduler | Not renderable. Needs new templates in `gcp-cloudrun` for `V2Job` and a Scheduler resource. Largest chart change, no in-repo precedent to copy. |
| Cloud Run **Service** + Scheduler | Chart renders the Service and the `run.invoker` binding, so the trigger works. But the sweep is a batch script; it would need an HTTP handler wrapped around it purely to satisfy the shape, and the Scheduler resource still has no template. |
| **GKE CronJob** via `cron-jobs` | One added line (`serviceAccountName`) in `mntn-helm`. Everything else exists: my own team's folder, my own team's namespace and cluster, native cron, native Vault. |

**Recommendation: GKE CronJob.** It is the smallest change to the paved road, it lands in
`apps-v3/targeting/` beside work the team already owns, and it removes the Vault question rather
than answering it. The Workload Identity binding has an in-repo precedent in the same project:
`terragrunt/.../mntn-prj-prod-00/iam/workload_identity/qa-optimization-ml/terragrunt.hcl` binds
`prod-optimization-ml-sa` in namespace `prod-optimization-ml` to a GSA in `mntn-prj-prod-00`.

**This supersedes §3's "Cloud Run Job + Cloud Scheduler" recommendation**, which was written
before the chart was readable. §3's *reasoning* stands unchanged — no repository identity, no
OIDC trust widened, no service-account key — and a GKE CronJob under Workload Identity satisfies
all three the same way. Only the compute primitive changes. The GCP service account and its four
bindings in ask 1 are unaffected; what changes is what attaches to it.

**Revised PR set:**

| Repo | Change |
|---|---|
| `mntn-helm` | add `serviceAccountName` (and a pod `annotations` block) to `charts/cron-jobs/templates/cron-jobs.yaml`, bump chart version |
| `mntn-devops` | `spark-optimizer` GSA + the four bindings + a `workload_identity/spark-optimizer` module mirroring `qa-optimization-ml` |
| `mntn-argocd` | `apps-v3/targeting/spark-optimizer/cron/{config,values-prod}.yaml`, mirroring `audience-service/cron/` |
| `mntn-team-credentials` | `TeamSecret` for the Astro and Databricks tokens, read by the chart's ExternalSecret |

---

## 8. 2026-08-20 — Compass design review: two findings kept, one refuted

Compass returned three root-cause gaps. Two are correct and change the plan. One is wrong, and
the repo's own comments say so in two places.

**KEPT — the `cron-jobs` chart change is real, and the version is 1.1.0.** Compass independently
confirmed §7's reading of the pod spec and priced the fix: add
`serviceAccountName: {{ .Values.serviceAccountName | default "default" }}`, bump `Chart.yaml`
1.1.0 → 1.2.0 (minor, defaulted, so existing consumers are untouched). Version verified directly.

**KEPT, and it corrects §7 — the secret template is Onboard Team Secret, not Update Team Secret.**
SOP 065 (`065-secrets-self-service-via-basecamp.md:24-48`): *Update* rotates or adds keys inside an
existing secret; *Onboard* creates a new secret at `secrets/<team>/<secret>/`. The plan is a
**new** sibling beside the ShopperGraph `databricks` entry, precisely so revocation cannot cross
between them, so it is an onboard. **§2, §6 and §7 all said Update Team Secret — that is wrong,
and it is wrong for a reason worth keeping: `mntn-team-credentials` uses the Update template
rather than `rotate-secret`, and I carried "not rotate-secret" forward into "therefore update"
without checking that a third template covers creation.** Target path:
`secrets/team-engineering-targeting/spark-optimizer-secrets/`.

**REFUTED — the DEV-7921 PAM finding.** Compass read the DEV-7921 comment in
`mntn-prj-prod-00/iam/terragrunt.hcl` as retiring standing Dataproc access and called a new
standing `dataproc.viewer` a reopening of a closed decision. What DEV-7921 retired was
**`dataproc.editor`** — the mutating role. The same file, eight lines below the comment Compass
quoted, says the opposite about the viewer role:

```hcl
# DEV-8182: ... dataproc.viewer is default-allow (no PAM request); compute.viewer is
# already granted org-wide ... storage.objectViewer stays JIT via the
# audi-storage-object-view entitlement (18h)
bindings = { "roles/dataproc.viewer" = ["group:audience-intelligence@mountain.com"] }
```

and `terragrunt/gcp/iam/pam/mntn-prj-prod-00/terragrunt.hcl:44` states it from the PAM side:
*"compute.viewer/dataproc.viewer are standing in resources/.../mntn-prj-prod-00/iam."*
So a standing `dataproc.viewer` is the documented policy in this project, not an exception to
argue for. **No PAM entitlement is needed and no exception has to be written.** This is the same
Compass failure mode already recorded in `reference_compass`: strong at "does this artifact
exist", weaker at mechanism — it found the right file and misread which role the ticket retired.

The half of the finding that survives is `storage.objectViewer`, which the same comment says
**does** stay JIT (`audi-storage-object-view`, 18h) — for the group. Whether a workload GSA gets
it standing while humans get it JIT is a real question, and it is the one to actually raise.

**Better precedent than `qa-optimization-ml`, found while checking Compass.** A GKE workload with
standing Spark-event-log read on this exact project already ships:

```hcl
# terragrunt/.../mntn-gke-prod-01/iam/workload_identity/data_eng_mcp/terragrunt.hcl
inputs = {
  name      = "data-eng-mcp-sa"
  namespace = "mcp-data-eng"
  additional_projects = {
    # Dataproc permissions for batch analysis + Spark event log access
    "mntn-prj-prod-00" : ["roles/dataproc.viewer", "roles/logging.viewer",
                          "roles/storage.objectViewer"]
    ...
  }
}
```

Same cluster, same project, same purpose, **standing not JIT**, and it grants a superset of what
the optimizer asks for. It settles the question Compass raised, and it is a single file: the
`_envcommon/iam/workload_identity/common.hcl` module creates the GSA and binds the KSA in one
place. **Cite it in the PR.** Keep the bucket-scoped `objectViewer`/`objectCreator` from the
`mntn-marketo` pattern rather than copying `data_eng_mcp`'s project-level `storage.objectViewer`
— tighter, and Compass's healthy control endorses exactly that shape.

**Compass's own caveat, honoured:** no IAM advisor ran this round, so every finding above is
IaC-declared and not live-state confirmed, and no run ID or collection age exists to cite.

---

## 9. 2026-08-20 — the platform already exists: `mntn-devops/automations/`

Chasing the image-build question found the answer to the whole problem. **`mntn-devops` contains a
purpose-built automations platform** — `automations/` with `docs/ADD_AN_AUTOMATION.md`,
`templates/automation/{cronjob,externalsecret,deployment,build-and-push}.yaml`, a shared
`build-automation-images.yaml` workflow, and a running `slack-bot-c3po`. Its stated scope is
*"Slack bot, cron job, small service"*, and its ownership model explicitly supports source living
in `automations/<name>/` for *"small internal utilities"*. This is that.

**It removes four open problems at once:**

1. **The image build.** Images publish to GAR at
   `us-central1-docker.pkg.dev/devops-425515/automations/<name>`. The doc states plainly:
   *"No extra IAM for a new image"* — `github-actions@mntn-prj-prod-00` already holds
   `artifactregistry.writer` and the node SA holds `artifactregistry.reader` on the whole repo.
2. **The GitHub Actions OIDC blocker from §3 is gone.** §3 ruled out Actions + Workload Identity
   Federation because the code sat in `mdunn-mntn/malachi-workspace`, a personal repo, and the
   prod pool allow-lists 23 `SteelHouse/*` repos. **`mntn-devops` is on that allow-list.** Moving
   the source there makes the documented WIF path
   (`workloadIdentityPools/mntn-prj-prod-gh-oidc/providers/github` →
   `github-actions@mntn-prj-prod-00`) available with no pool change and no key. The §3 reasoning
   was right; the conclusion only followed from where the code happened to live.
3. **The `cron-jobs` chart PR is moot.** `templates/automation/cronjob.yaml` already carries
   `serviceAccountName: automations-runner`, `concurrencyPolicy: Forbid`, `timeZone: UTC` and
   history limits. **Compass's second gap and §7's one-line `mntn-helm` change are no longer
   needed** — that gap was real for the `cron-jobs` chart and simply does not apply to this path.
4. **Secrets.** `templates/automation/externalsecret.yaml` plus `docs/SECRETS.md` define the
   contract. The Onboard-Team-Secret correction from §8 still holds; only the consumer changes.

**Read this before copying anything:** the prod overlay renders **everything in `base`**
(`resources: [../../base]`) and removes workloads only via explicit `$patch: delete`. Registering
in `base` therefore **deploys to prod on the next sync**. `slack-bot-c3po` carries a prod
delete-patch for exactly this reason. A new automation must ship its own delete-patch to stay out
of prod until it is deliberately promoted.

Also note the base manifests keep the `ghcr.io/steelhouse/<name>` image ref as the **match key**
for the CI writeback and the kustomize image transformer; the nonprod overlay redirects the real
pull to GAR via `newName`. Do not "fix" the GHCR ref in base — it is load-bearing.

### The one real gap

**`automations-runner` has no GCP identity.** `automations/apps/base/rbac.yaml` defines it with an
in-namespace Role for `secrets`/`configmaps`/`pods` and **no `iam.gke.io/gcp-service-account`
annotation**, so a pod running as it cannot reach GCS or Dataproc at all. Its own comment records
why the previous credential is gone: *"The previous ghcr-pull-secret wrapped a long-lived GitHub
PAT, which was revoked and took the workloads offline — see SOP 063, identity over credentials."*

Two ways to close it, and the choice matters:

- **Annotate `automations-runner`** — one line, but every automation in the namespace inherits the
  optimizer's GCS and Dataproc read. Wrong direction for a shared runner.
- **A dedicated KSA `spark-optimizer-runner`** in the `automations` namespace, bound by Workload
  Identity to the new `spark-optimizer` GSA, with `serviceAccountName` on the CronJob pointing at
  it instead. Blast radius stays at one workload, and the existing shared Role can be reused by
  RoleBinding.

Take the second. The binding mirrors
`terragrunt/.../mntn-gke-prod-01/iam/workload_identity/data_eng_mcp/terragrunt.hcl` (§8), which
already grants a GKE workload standing `dataproc.viewer` + `storage.objectViewer` on
`mntn-prj-prod-00` for *"Spark event log access"* — the same purpose, on the same project.

**Open:** which cluster the `automations` namespace runs on. The prod overlay says only *"now
deployed to the mgmt cluster"* without naming it, and `data_eng_mcp`'s binding is written against
`mntn-gke-prod-01`. The Workload Identity binding is cluster-specific, so confirm the cluster
before writing that unit.

### Final PR set

| Repo | Change |
|---|---|
| `mntn-devops` | `automations/spark-optimizer/` — source + Dockerfile; a build job in `.github/workflows/build-automation-images.yaml` copying `build-slack-bot-c3po` |
| `mntn-devops` | `automations/apps/base/spark-optimizer/{cronjob,externalsecret,kustomization}.yaml`, registered in `base/kustomization.yaml`, plus a **prod `$patch: delete`** |
| `mntn-devops` | `automations/apps/overlays/nonprod/kustomization.yaml` — GHCR match key → GAR `newName` |
| `mntn-devops` | GSA `spark-optimizer` + four bindings + a `workload_identity` unit for KSA `spark-optimizer-runner` |
| Basecamp | **Onboard** Team Secret at `secrets/team-engineering-targeting/spark-optimizer-secrets/` |

`mntn-argocd` and `mntn-helm` drop out entirely. Everything lands in one repo.
