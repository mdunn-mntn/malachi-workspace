---
name: compass-mntn-infra-investigator-atlas-code-mcp
description: "What Compass is, how to access it (Atlas Code MCP endpoint, compass-query, A2A), coverage gaps, secret-rotation policy"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 6b830d36-17fc-4962-b8c0-c9c838b6e689
doc_type: memory
keywords: [compass, SOP 052, SOP 060, SOP 063, octo sts, vault ESO, secrets management strategy, standard operating procedures, design review, atlas code mcp, backstage, infra investigator, agent-gateway, contextforge, harvey yau, dev-basecamp, quickframe coverage gap, a2a]
domain: [infra, routing-people]
lifecycle: active
last_verified: 2026-08-20
---
**Compass** = MNTN's internal multi-agent AI infrastructure investigator, embedded in Backstage (Internal Developer Platform). Router → parallel domain Advisors (Infra/Billing/GCP/Knowledge/GitHub) → live evidence knowledge graph with contradiction detection + provenance + readiness-based synthesis. Read-only (remediation via GitOps PRs). Built on MCP + Google A2A + ContextForge (IBM MCP gateway) + Grafana LGTM. Whitepaper 2026-03; org rollout 2026-06 (Harvey Yau). Questions: **#dev-basecamp**.

**Access:**
- **Atlas Code MCP** (300+ codebase/GCP/docs tools) → Claude Code: `claude mcp add mountain --transport http https://agent-gateway.management.in.mountain.com/mcp/code`
- **Compass-as-a-service:** HTTP `POST /api/basecamp-chat/a2a`, MCP `compass-query` tool via ContextForge, or Google A2A (`/message:send` / `:stream`).
- **Shareable conversation links:** `basecamp.in.mountain.com/mcp-chat?join=<uuid>`.

**Coverage gap:** Quickframe (QF) platforms mostly NOT covered (built outside core infra) — only thin billing/GCP. Don't use for deep QF work.

**Coverage gaps found INC-006 (2026-07-29):** (1) **The Airflow/Astronomer deploy that runs `airflow-ti` DAGs is NOT in Compass's monitored GKE/Loki fleet** — it searched all 5 clusters (`mntn-gke-mgmt/prod/bidder-01,02/restricted`) + 844 ArgoCD apps and found zero `airflow`/`openai_batch_runner` pods, so it **cannot pull Airflow task/pod logs or tracebacks** (likely Cloud Composer or a cluster/project outside the fleet). (2) Compass hit **`PERMISSION_DENIED` on `gs://mntn-data-archive-prod`** (a different, unidentified GCP project, not `mntn-prj-prod-00`) — it can't read that bucket. **I (malachi@mountain.com via gcloud) CAN read `mntn-data-archive-prod`**, so on a Compass RCA that's GCS-blocked, close the GCS half myself. Compass is strong at static code (code_read/code_blame/code_history over repos) even when live infra is blind — lean on it for source-level root cause, then verify the runtime state with my own GCS/BQ access.

**Mechanism-calibration (INC-006, 2026-07-29):** Compass was CORRECT that the bug exists (`output_file_id` null → `files.content(None)` throws) but NARROW on its effect — it called it a one-shot deterministic kill. Reading the source + re-checking GCS myself showed the real mechanism: the fetch loop is not fault-isolated, so one bad batch aborts the whole loop and strands the rest, and the cycle limps forward without ever finishing. Lesson: treat Compass's failure-mechanism / "deterministic" claims as hypotheses; it identifies the faulty line well, but confirm the actual runtime EFFECT (loop behavior, partial state) yourself.

**Relevant to our killed Slack bot:** Compass's Slack integration + knowledge-doc authoring are ROADMAP (§9), not shipped — so Compass does NOT yet replace a scheduled Slack-scrape→markdown pipeline. See [[reference_pi5_server]] (bot decommissioned 2026-06-10) and `knowledge/mntn_business.md` Compass section.

**Secret policy (paired):** no local-env API keys / no local Slack apps. Secrets → SOPS-encrypted in ArgoCD repo, rotated via Basecamp tool (KMS Decrypt disabled for individuals; Vault optional/unsupported). *[Recorded 2026-06-10, source: the decommissioning conversation.]*

**CONTRADICTION, unresolved (2026-08-20).** The line above says **SOPS-in-ArgoCD, Vault optional/unsupported**. Compass's design review cites **SOP 052** (`052-secrets-management-strategy.md:175-185`) as **Vault/ESO by default, Secret Manager a narrow documented exception** — and does not mention SOPS at all. Both are kept; neither is deleted.
- *Evidence for SOPS/ArgoCD:* what I was told at the 2026-06-10 decommissioning, conversational.
- *Evidence for Vault/ESO:* a numbered SOP with line references, quoted by Compass in a design review.
- *Reconciling hypothesis:* the policy moved between June and August (SOP 052 postdates the bot decommissioning), OR they address different layers — SOPS for GitOps-delivered cluster secrets, Vault/ESO for workload-runtime secrets, Secret Manager for neither by default.
- *The check that settles it:* read `docs/standard_operating_procedures/052-secrets-management-strategy.md` directly and look for its effective date and whether it supersedes the SOPS guidance. Do this before storing a secret for any new workload.

**SOPs Compass surfaced that were not otherwise on my radar (2026-08-20, AUDI-1194 identity review).** Compass cites `docs/standard_operating_procedures/` as an authoritative corpus; three that govern any automation identity work:
- **SOP 052 secrets** (`052-secrets-management-strategy.md:175-185`): secrets default to **Vault/ESO**; **Secret Manager is a narrow, documented exception**, not the default. The paired FAQ (`secrets-management-announcement-and-faq.md:65-86`) prohibits PATs and hardcoded credentials outright. Do NOT assume Secret Manager for a new workload.
- **SOP 060 GitHub Actions Octo STS** (`060-github-actions-octo-sts-tokens.md`): Actions get short-lived **Octo STS App tokens**, never PATs. OPEN: whether it applies to a Cloud Run job rather than an Actions workflow.
- **SOP 063 security principles** (`063-security-principles.md:136-152`): least privilege, SP-06. The rule Compass applies to any "runs on my laptop under personal SSO" job — that is a standing-credential gap regardless of what it reads.

**Calibration, second data point (AUDI-1194, 2026-08-20).** Compass caught a real error of mine: I read a Terragrunt header comment saying "Crossplane owns the rest of jedi-media-spend-job **IAM**" and wrote it up as "Crossplane owns the **V2Job manifest**". Compass grepped `kind: V2Job` across `argocd-v2/mgmt/platform/crossplane` (no matches) and `mntn-argocd` (no `jedi-media-spend` at all) and returned the manifest's home as **unresolved evidence, not a fact**. That is the pattern to expect: **Compass is strong at "does this artifact actually exist where you think", weaker at mechanism** (see the INC-006 note above). Use it to falsify structural claims; keep confirming runtime behaviour yourself.

**It also reports honestly when a specialist did not answer.** In the same review it flagged that its iam-advisor and secrets-advisor questions came back without a validated, corroborated response and told me not to finalise those decisions from the review alone — rather than filling the gap with documentation inference. Treat an unanswered sub-question as unanswered, and re-dispatch it.
