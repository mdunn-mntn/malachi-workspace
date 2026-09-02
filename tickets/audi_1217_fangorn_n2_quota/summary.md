---
doc_type: ticket
title: "AUDI-1217: fangorn N2 quota — size a us-central1 increase and cap the QA cluster"
status: done
date: 2026-08-24
summary: "Closed Won't Do 2026-09-02 on owner decision (Brian McAdams). Verified: zero quota refusals and zero fangorn inference/challenger stockouts since the instance-flexibility-policy fix shipped 2026-08-25."
result: "WON'T DO. Of the three remedies, two are refused by devops (shrink the cluster, raise the N2_CPUS ceiling) and the third shipped 2026-08-25 as a 3-family instance flexibility policy on both inference pipelines, which is strictly better than the proposed n2->n2d swap. Measured from the ClusterController audit log on 2026-09-02: 0 quota refusals (status.code=3) since 2026-08-24 22:58Z, and 0 stockouts (code=14) on fangorn-inference-*/fangorn-challenger-* since 2026-08-25 22:48Z (27 in the two days before). Residual, unfixed and out of scope: DISKS_TOTAL_GB is family-agnostic so the policy cannot spread it, and two concurrent full-shape clusters still exceed it by ~43,192 GB."
question: "Which of the three remedies for the fangorn N2 quota exposure should we pursue: shrink the cluster, raise the us-central1 N2_CPUS ceiling, or move n2 -> n2d?"
framing_state: locked
---

# AUDI-1217: fangorn N2 quota — size a us-central1 increase and cap the QA cluster

**Jira:** https://mntn.atlassian.net/browse/AUDI-1217 · **Type:** Spike · **Resolution:** Won't Do (2026-09-02)
**Status:** done (closed won't-do) · **Date Started:** 2026-08-24 · **Date Closed:** 2026-09-02
**Assignee:** Malachi · **Decision owner:** Brian McAdams (Sr MLE, Fangorn/ML)

---

## 0. Framing

- **Question (the unknown):** Which of the three remedies for the fangorn Dataproc quota exposure
  should we pursue — shrink the 290-worker cluster, raise the us-central1 `N2_CPUS` ceiling from
  5,000 to ~15,000, or move the worker family off `n2`?
- **Goal (why / the decision):** `create-dataproc-cluster` refusals page prod on-call and cost a full
  DAG run each time. The ticket exists only to get the owning team's call on which lever to pull;
  Malachi carries whichever one is chosen. Waiting on it: Brian McAdams, who owns the pipeline.
- **Objective (done-when):** A recorded owner decision on each of the three options, and either a
  filed quota request or a stated reason not to file one.
- **Approach (how):** Put the three options to Brian directly, then verify the post-decision claim
  empirically against the `ClusterController` admin audit log and the live regional quotas rather
  than taking "already addressed" on trust.
- **What would change the answer:** A quota refusal (`status.code=3`) or a fangorn inference/
  challenger stockout (`code=14`) recorded after the 2026-08-25 fix. Either one reopens this.

## 1. Introduction

`fangorn_inference_pipeline_run` (project `mntn-targeting-prj-prod`, region `us-central1`) runs two
sequential Vertex pipelines, each of which spins up a 290-worker ephemeral Dataproc cluster:
1 x `n2-standard-32` master + 290 x `n2-standard-16` workers = **4,672 N2 vCPU and 145,500 GB of
persistent disk**. The regional `N2_CPUS` limit is 5,000, so one cluster is 93% of the pool.

This ticket was opened out of on-call INC-025 (2026-08-24), where a QA-owned cluster held the whole
pool and prod's challenger create was refused twice. Full incident: `on-call/oncall_runbook.md`
§3 INC-025. Pipeline wiring and the six failure surfaces:
`knowledge/memory/reference_fangorn_inference_dataproc.md`.

## 2. The Problem

Two distinct failure classes wear the same Airflow error text (`code: 9`, failed task
`[create-dataproc-cluster]`), and both trace to the same underlying condition — the cluster is sized
at the edge of what the region will give it:

- **Quota refusal** (`ClusterController` `status.code=3`) — MNTN's own regional ceiling refuses the
  create. INC-025: `Insufficient 'N2_CPUS' quota. Requested 4672.0, available 328.0` plus
  `Insufficient 'DISKS_TOTAL_GB' quota. Requested 145500.0, available 74280.0`.
- **Stockout** (`status.code=14`, `UNAVAILABLE`, `COMPUTE_ENGINE`) — Google has no machines of that
  type in the zone. On 2026-08-25 this ran over nine hours across `us-central1-a` and `-b`.

Impact: each occurrence pages prod on-call and loses a DAG run. QA and prod run in the **same GCP
project** against the **same regional pools**, so a QA run can starve prod from outside prod.

## 3. Plan of Action

1. Put the three remedies to Brian McAdams (owner) in `#dev-platform-discussion`. (2026-09-02)
2. Take his decision on each.
3. Verify the "already addressed" claim empirically before closing — audit log for both failure
   classes, and the live regional quotas.
4. Close the ticket to match the verified state; carry any residual to `improvements_backlog.md`.

## 4. Investigation & Findings

### 4.1 Owner decision (Brian McAdams, Slack, 2026-09-02)

| Option | Decision | Stated reason |
|---|---|---|
| Shrink the 290-worker cluster | **No** | Not available; the cluster is sized for the workload. |
| Raise `N2_CPUS` 5,000 -> ~15,000 | **No** | Devops will not raise the ceiling. Brian took this to them in office hours with Alyson. |
| Move `n2` -> `n2d` | **Already done, better** | Sean Yang shipped a multi-family worker pool 2026-08-25, with `n2d` as one of the options. |

Brian also pointed at `#dev-platform-discussion` 2026-07-29, where Edris Mohsin enabled Dataproc
quota auto-scaling in response to his request.

**One correction to the record.** The alert Malachi linked in that thread
(`fangorn_inference_pipeline_run/inference_pipeline`, 2026-08-24 11:00 PT) is a **stockout**, not a
quota refusal, and Brian is right about it. That does not touch the ticket's evidence base: INC-025's
`challenger_inference_pipeline` failure the same evening was a genuine quota refusal, settled by
arithmetic and re-confirmed from the audit log below. Both classes happened on 2026-08-24. The
ticket closes because the remedies are exhausted, not because the quota refusals did not occur.

### 4.2 The fix that shipped, read from source (verified 2026-09-02 against `origin/main`)

`SteelHouse/targeting-infra-ml`:

| Commit | Date | Author | File |
|---|---|---|---|
| `fe4e3d6` | 2026-08-25 | Sean Yang | `vertex/fangorn/pipelines/fangorn_inference_dataproc_pipeline.py` |
| `0b19f29` | 2026-08-25 | Sean Yang | `vertex/fangorn/pipelines/fangorn_challenger_inference_pipeline.py` |
| `7247996` | 2026-09-01 | Alex Knorr | `vertex/fangorn_conversions/pipelines/fangorn_conversions_inference_pipeline.py` |

Each replaces the fixed `machine_type_uri` on `worker_config` with:

```python
"instance_flexibility_policy": {
    "instance_selection_list": [
        {"machine_types": ["n2-standard-16", "n2d-standard-16", "e2-standard-16"], "rank": 0},
    ],
},
```

**The library pin is load-bearing.** `instance_flexibility_policy` is absent from
`InstanceGroupConfig` in `google-cloud-dataproc` 5.4.0 and present in 5.10.1, so on the old pin the
field is silently dropped and the cluster is still single-family. All three `create_dataproc_cluster`
components carry `google-cloud-dataproc==5.10.1`; the sibling `submit_*` and `delete_*` components in
the same files still pin 5.4.0, which is correct — they do not build a cluster config.

**This corrects a stale line in `reference_fangorn_inference_dataproc.md`,** which said only
`fangorn_inference_dataproc` was covered. The challenger got the identical change the same day
(`0b19f29`); the memory doc was written before that commit landed.

### 4.3 Post-fix outcome, measured from the `ClusterController` admin audit log (2026-09-02)

**Quota refusals — `protoPayload.status.code=3`, 14-day window:**

| Timestamp (UTC) | Principal | Cluster | Metric |
|---|---|---|---|
| 2026-08-24 22:46:29 | `vertex-ai@` (prod) | `fangorn-challenger-54637823` | `DISKS_TOTAL_GB` + `N2_CPUS` |
| 2026-08-24 22:58:48 | `vertex-ai@` (prod) | `fangorn-challenger-a483e22d` | `DISKS_TOTAL_GB` + `N2_CPUS` |

That is the INC-025 pair and nothing else. **Zero quota refusals in the 8 days since the fix.**

**Stockouts — `protoPayload.status.code=14`, 9-day window: 28 events.** 27 of them fall on
2026-08-24 and 2026-08-25 (the nine-hour regional shortage). Since `2026-08-25 22:48Z` there has
been exactly **one**, and it is not a covered pipeline:

| Timestamp (UTC) | Cluster | Covered by the policy? |
|---|---|---|
| 2026-09-01 21:07:56 | `fangorn-conversions-training-8012099b` | **No** — see §8 |

**Zero stockouts on `fangorn-inference-*` and `fangorn-challenger-*` since the fix shipped**, against
27 in the two days before it. That is the ticket's answer.

### 4.4 The quota ceiling did not move; the request spread instead

Live regional quotas for `mntn-targeting-prj-prod` / `us-central1`, read 2026-09-02:

| Metric | Limit 2026-08-25 | Limit 2026-09-02 |
|---|---|---|
| `N2_CPUS` | 5,000 | **5,000 (unchanged)** |
| `N2D_CPUS` | 5,000 | 5,000 |
| `CPUS` | 5,000 | 5,000 |
| `DISKS_TOTAL_GB` | 225,280 | **247,808 (+22,528)** |

**Option 2 was delivered by option 3, without a quota request.** Nothing raised `N2_CPUS`. But the
three machine types in the policy bill to three separate regional metrics — `n2-standard-16` to
`N2_CPUS`, `n2d-standard-16` to `N2D_CPUS`, `e2-standard-16` to the generic `CPUS`. The 4,672-vCPU
request that used to concentrate on one 5,000 pool can now spread across roughly 15,000 vCPU of
headroom. That is the same relief the "raise to ~15,000" ask was asking for.

**Caveat on that relief:** all three types are 16 vCPU / 64 GB so there is no memory skew, but
`e2-standard-16` has no local SSD and is a different performance class. A cluster that lands mostly
on E2 will start and may then run long. Read what was actually acquired with
`gcloud dataproc clusters describe`; do not assume the preferred type won.

**Brian's auto-scaling reference is not settled by this evidence.** `N2_CPUS` is unchanged at 5,000,
so whatever the auto-adjuster is doing it is not scaling the CPU metric this pipeline hits.
`DISKS_TOTAL_GB` did rise +22,528 in the same window, which is consistent with an adjuster acting on
disk, but a single before/after pair does not establish the cause. The discriminating check is the
Cloud Quotas adjuster settings, which need `cloudquotas.quotas.get` — my account is denied
(`PERMISSION_DENIED` on `projects/mntn-targeting-prj-prod/locations/global/quotaAdjusterSettings`).
Either way the fix that carried the load here is the flexibility policy, not the adjuster.

### 4.5 Residual: the disk half does not spread

`DISKS_TOTAL_GB` is family-agnostic, so the instance flexibility policy gives it nothing. The disk
request is unchanged whichever machine type wins:

- 1 master x 500 GB + 290 workers x 500 GB `pd-standard` = **145,500 GB**, which matches the
  `Requested 145500.0` in the INC-025 refusal exactly.
- Two concurrent full-shape clusters = **291,000 GB** against a **247,808 GB** limit. Still refused,
  by ~43,192 GB.

And the shared-pool condition that caused INC-025 is still in place. `vertex-ai-qa@` continues to
create full-shape clusters in the same project — `fangorn-challenger-883bbd2b` on 2026-08-26
13:19Z, `fangorn-conversions-*` on 2026-09-01 and 2026-09-02. The overlap has not recurred in 8 days
because the QA and prod windows have not collided, not because the condition was removed. If they
collide again the failure lands on `DISKS_TOTAL_GB` rather than on `N2_CPUS`.

This is logged, not fixed. See §8.

## 5. Solution

**Closed Won't Do.** No work performed against the three options, because:

1. **Shrink the cluster** — refused by the owner.
2. **Raise `N2_CPUS` to ~15,000** — refused by devops. Effectively delivered anyway by (3), which
   spreads the same request across three 5,000-vCPU metrics.
3. **Move `n2` -> `n2d`** — superseded by a strictly better fix already in prod: a 3-family
   `instance_flexibility_policy` on both prod inference pipelines
   (`fe4e3d6` + `0b19f29`, 2026-08-25), extended to the conversions inference pipeline
   (`7247996`, 2026-09-01).

The related masking bug from the same incident shipped separately and is already closed:
[targeting-infra-ml#93](https://github.com/SteelHouse/targeting-infra-ml/pull/93) (IMP-071, merged
2026-08-24) makes `_delete_cluster_before_retry` tolerate an absent cluster, so a refused create
surfaces its real error instead of a `NotFound: 404`.

## 6. Questions Answered

- **Q:** Which of the three remedies should we pursue?
  **A:** None. Two are refused by devops (shrink, raise the ceiling) and the third is already in prod
  in a better form.

- **Q:** Did the fix actually work, or is "already addressed" an assumption?
  **A:** It worked, measured. Zero quota refusals since 2026-08-24 22:58Z and zero stockouts on the
  two fangorn inference pipelines since 2026-08-25 22:48Z, against 27 stockouts in the two days
  immediately before. Read from the `ClusterController` audit log, not from the Airflow alerts.

- **Q:** Was the 2026-08-24 11:00 PT alert a quota failure or a stockout?
  **A:** A stockout (`503 UNAVAILABLE`, `STOCKOUT`, `us-central1-a`). Brian is right. INC-025's
  challenger failure the same evening was separately a genuine quota refusal (`status.code=3`,
  confirmed again in §4.3), so both classes occurred on 2026-08-24 and neither reading replaces the
  other.

- **Q:** Did quota auto-scaling raise the ceiling?
  **A:** Not for `N2_CPUS` — still 5,000 today. `DISKS_TOTAL_GB` rose 225,280 -> 247,808 over the
  same window, cause not established at my access level (`cloudquotas.quotas.get` denied).

- **Q:** Is the QA-starves-prod exposure from INC-025 closed?
  **A:** No, only narrowed. The CPU half now spreads across three metrics; the disk half cannot.
  Two concurrent full-shape clusters still exceed `DISKS_TOTAL_GB` by ~43,192 GB, and QA still
  creates full-shape clusters in the same project.

## 7. Data Documentation Updates

- `knowledge/memory/reference_fangorn_inference_dataproc.md` — corrected the stale "only
  `fangorn_inference_dataproc` is covered by #94" line (the challenger got `0b19f29` the same day),
  and appended the measured post-fix base rate, the three-metric spreading mechanism, the
  `DISKS_TOTAL_GB` residual, and the conversions-training gap.
- `on-call/oncall_runbook.md` — §2 catalog row and §3 INC-025 marked resolved with the fix and the
  post-fix measurement.
- `improvements_backlog.md` — IMP-070 closed as superseded; two residual rows added.

## 8. Open Items / Follow-ups

Neither of these belongs to AUDI-1217. Both are logged in `improvements_backlog.md`.

1. **`fangorn_conversions_training_pipeline.py` is not covered by the flexibility policy.** It still
   hardcodes `machine_type_uri: n2-highmem-16` on 1 master + 16 workers, and its
   `create_dataproc_cluster` component pins `google-cloud-dataproc==5.4.0` — so adding the policy
   without also bumping the pin would drop the field silently. It took the only stockout since the
   fix (2026-09-01 21:07Z). Small cluster, low blast radius, one-line change plus the pin. Owner:
   targeting-ml (Alex Knorr shipped the sibling inference file on 2026-09-01).
2. **`DISKS_TOTAL_GB` remains a single-family-agnostic ceiling two full clusters cannot share.**
   Nothing to do while QA and prod windows stay apart; the cheap guard if it recurs is a smaller
   `boot_disk_size_gb` on workers or QA on a reduced shape. Watch for `status.code=3` naming
   `DISKS_TOTAL_GB` alone, with `N2_CPUS` absent — that signature means the CPU half is working and
   only disk is binding.

## Context carried in from INC-025 (2026-08-24)

Prod `fangorn_inference_pipeline_run/challenger_inference_pipeline` failed both tries when
`CreateCluster` was refused: `Insufficient 'N2_CPUS' quota. Requested 4672.0, available 328.0`.

**Settled by arithmetic.** `fangorn-inference-26f05d0f`, created 22:44Z by **`vertex-ai-qa@`**, is
`1x n2-standard-32 + 290x n2-standard-16` = **4,672 N2 vCPU**. The regional limit is 5,000, and
5,000 - 4,672 = **328**, exactly the available figure in the refusal. One QA cluster held the whole
pool. `fangorn-hhid-inference-*` is `n2d` and bills to `N2D_CPUS`, so it held zero; the sibling
`inference_pipeline` is sequentially upstream and its cluster was already gone.

**Base rate at the time: 7 refusals in 30 days, every one served to prod** (`status.code=3`: 07-27
x1, 07-30 x4, 08-24 x2), none to `vertex-ai-qa@`. July's followed a `code 14` stockout (the INC-008
self-block); 08-24 had no stockout at all. **Superseded by §4.3:** zero refusals in the 8 days after
the 2026-08-25 fix.

**Identity, not cluster name, says whose a cluster is** — `fangorn-inference-*` is the same name in
both environments; only `protoPayload.authenticationInfo.principalEmail` distinguishes them.

## Reproduce the verification

```bash
# quota refusals (MNTN's own ceiling said no)
gcloud logging read 'resource.type="cloud_dataproc_cluster" AND protoPayload.methodName="google.cloud.dataproc.v1.ClusterController.CreateCluster" AND protoPayload.status.code=3' \
  --project mntn-targeting-prj-prod --freshness=14d --limit=25 \
  --format="csv[no-heading](timestamp,protoPayload.authenticationInfo.principalEmail,protoPayload.resourceName,protoPayload.status.message)"

# stockouts (Google had no machines)
gcloud logging read 'resource.type="cloud_dataproc_cluster" AND protoPayload.methodName="google.cloud.dataproc.v1.ClusterController.CreateCluster" AND protoPayload.status.code=14' \
  --project mntn-targeting-prj-prod --freshness=9d --limit=40 \
  --format="csv[no-heading](timestamp,protoPayload.resourceName)"

# live ceilings — read after the fact the region looks innocent, so pair it with the audit log
gcloud compute regions describe us-central1 --project mntn-targeting-prj-prod --format="value(quotas)" \
  | tr ';' '\n' | grep -E "N2_CPUS|N2D_CPUS|'CPUS'|DISKS_TOTAL_GB"
```
