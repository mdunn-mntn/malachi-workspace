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
