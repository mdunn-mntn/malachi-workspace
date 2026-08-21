---
name: reference_gcs_iam_creator_vs_user
description: "GCS objectCreator cannot overwrite an existing object, and impersonation from a pod's ADC needs serviceAccountTokenCreator, not workloadIdentityUser."
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [storage.objectCreator, storage.objectUser, objectAdmin, overwrite existing object, IAM condition prefix, serviceAccountTokenCreator, workloadIdentityUser, CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT, impersonation, Astro KSA, airflow-ti-prod, ape-ingestion, airflow_astronomer]
domain: [infra, workflow]
lifecycle: active
last_verified: 2026-08-21
---
Two IAM mistakes caught in review of mntn-devops#4971 (Cristina Szumilo, 2026-08-21). Both were
written from a plausible-looking precedent and both would have failed in production.

**1. `roles/storage.objectCreator` cannot overwrite an existing object.** It grants
`storage.objects.create` only; replacing an object also needs `storage.objects.delete`. So a
creator-only grant works for **date-stamped** output and fails for anything republished under a
**stable** name. The optimizer writes three date-stamped files and one append-only ledger at a
fixed key: it would have succeeded on day one and failed from day two, and the ledger is exactly
the file that separates a standing defect from a new one, so the loss would have been quiet.

Use **`roles/storage.objectUser`** (create + delete + read, no bucket admin, no `setIamPolicy`)
and keep the blast radius on the **IAM condition**, not on the weak role:
```hcl
condition {
  title      = "optimizer-output-prefix-only"
  expression = "resource.name.startsWith(\"projects/_/buckets/<bucket>/objects/<prefix>/\")"
}
```
Rule of thumb: pick the role from *what the workload does to the object*, and the scope from the
condition. Reaching for `objectCreator` because it "sounds least privileged" buys nothing here
and breaks the workload.

**2. Impersonation from a pod's ADC is `roles/iam.serviceAccountTokenCreator`, NOT
`roles/iam.workloadIdentityUser`.** These answer different questions:
- **workloadIdentityUser** — "this **Kubernetes** SA *is* that GCP SA"; the member is a
  `…svc.id.goog[<ns>/<ksa>]` principal. Right when a pod's own identity should BE the GSA.
- **serviceAccountTokenCreator** — "this **GCP** SA may mint a token for that GCP SA"; the member
  is a `serviceAccount:x@project.iam.gserviceaccount.com`. Right when code sets
  `CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT` (or `--impersonate-service-account`), because the
  caller is then the pod's already-resolved GCP identity, not the KSA.

**Concretely for Astro/airflow-ti:** an `airflow-ti` task impersonating a GSA runs as
**`airflow-ti-prod@mntn-prj-prod-00`**. Binding the deployment's component KSAs
(`hcdp-<cluster>.svc.id.goog[<ns>/<ns>-worker-serviceaccount]`, etc.) with
`workloadIdentityUser` is a **no-op**, and on a shared deployment it is also the wrong direction:
it would let any DAG in the deployment become that account if the WI target ever moved. Peer to
copy: `terragrunt/.../mntn-gke-prod-01/iam/workload_identity/airflow_astronomer/terragrunt.hcl`.

**The `ape-ingestion` pattern (`astro_ksa_members` + `workloadIdentityUser`) fits a DEDICATED
Astro deployment whose WI target IS that GSA.** `airflow-ti` is shared, so it does not apply.
Check whether a precedent's deployment is dedicated or shared before copying its identity block.

Flow to state in the PR so a reviewer can check it in one line:
`airflow-ti-prod (ADC) --TokenCreator--> impersonate spark-optimizer --> gcloud/gsutil as it.`

See [[project_airflow_optimizer]] for the workload, and [[reference_mntn_devops_permissions]] for
how the PR gets approved.
