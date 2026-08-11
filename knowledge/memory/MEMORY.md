# MNTN Workspace Memory — hot tier (always loaded)

> **HOT TIER — only facts relevant to (nearly) every session.** Everything else is grep-on-demand at 0 tokens: grep `knowledge/_ROUTING.md` → open the one `memory/<file>.md` it names; browse `_MEMORY_INDEX.md`, refresh queue `_MEMORY_LIFECYCLE.md`. `/capture` writes new memory; a new fact gets a `_ROUTING` entry, **not** a line here. Only genuinely always-on rules earn a line. [[project_hot_path_budget]]

## Data docs (git source of truth)
- `data_catalog.md` schemas/partitions/join keys · `data_knowledge.md` business logic + gotchas · `strategic_north_star.md` OKR leverage filter (all under `knowledge/`).

## Always-on working rules (how I write & work)
- **BLUF / terse.** Lead every human-facing comm (chat, Jira, Slack, deck, standup) with the conclusion; cut filler. Terse Comms caps apply to Jira/PR/commit/xlsx. **Slack thread replies = human prose** (conversational, contractions, no em-dashes, few colons, plain statements), not the bulleted Jira shape. [[feedback_bluf_communication]] [[feedback_terse_chat_replies]] [[feedback_slack_reply_voice]] [[feedback_terse_tickets]]
- **No em-dashes, no name-dropping** in written deliverables. [[feedback_no_emdash_no_namedrop]]
- **MNTN only** — never mention IPSOS or other orgs; Todoist = MNTN section of MindWyre, on-request only. [[feedback_mntn_only]] [[feedback_todoist]]
- **Simple & factual** — simplest deliverable, no invented terms/columns, plain facts + caveats, no unsolicited next-steps. [[feedback_minimize_complexity]] [[feedback_facts_not_presentation]] [[feedback_no_unsolicited_suggestions]]
- **Sparse code comments** — one line max if ever; write self-documenting code, put the why in the PR/commit/ticket, not block comments. [[feedback_sparse_code_comments]]
- **Hold the evidenced verdict** — don't fold to a domain owner's plausible-but-hedged pushback; treat it as a hypothesis, keep the evidenced answer, settle it with a discriminating test. [[feedback_hold_evidenced_verdict]]
- **Rank descending** — primary metric, most on top, every table/chart. [[feedback_rank_desc_always]]
- **Default deliverable = branded `.xlsx`** to the Drive mount `My Drive/Tickets/<KEY>/` (lib/mntn_xlsx.py). [[feedback_xlsx_default_output]] [[reference_xlsx_master_format]]
- **IPs from source log tables**, never proxies (CIL). [[feedback_source_table_ips]]
- **No naive pre/post** for advertiser KPIs — CausalImpact + cluster-bootstrap DiD, report SE/CI/p. [[feedback_no_naive_pre_post]]
- **BQ:** always via `bq_run.sh` (perf log + us-central1 reservation); sample/APPROX first; never preempt a long query; no cost warnings. [[feedback_bq_workflow]] [[feedback_fast_first_bq]] [[reference_bq_location_reservation]]
- **Airflow-ti:** never modify DAGs / push main; feature-flag; check Dataproc cost before any backfill. [[feedback_airflow_prod_safety]] [[feedback_dataproc_cost_awareness]]
- **Background/async work:** never passive-wait — arm a stall-detector Monitor; a HUNG task sends NO completion notification. [[feedback_background_work_liveness]]
- **Self-review** — update `self_review/self_review_2.md` after every ticket; argue the rubric (Speed/Craft/Adaptability). [[feedback_self_review_habit]] [[feedback_rubric_strategy]]
- **Tickets** — `/frame` opens (framing gate blocks in_progress), `/capture` closes; don't extend stale/reassigned tickets. [[reference_ticket_framing_gate]] [[feedback_dont_extend_old_tickets]]
- **On-call** — any alert → `/oncall`; log to §3 incident + §2 signature + incident_log.jsonl; never hot-patch prod. [[reference_oncall_runbook]]
- **Hot path is a budget** — CLAUDE.md holds behavioral rules + pointers only; new procedure goes in a skill or knowledge doc. A rule may only move off-hot if a real trigger reloads it. [[project_hot_path_budget]]

## Stack, SQLMesh + critical gotchas (full detail in git knowledge/data_catalog.md + data_knowledge.md)
- **Stack:** bronze.integrationprod (Postgres CDC dims) · bronze.raw (events 10–90d TTL) → SQLMesh → silver.logdata/summarydata/aggregates · silver.core (views over integrationprod.core_*). silver=dw-main-silver, bronze=dw-main-bronze; clean name → versioned sqlmesh__*.
- **Epochs/TTL:** epochs spend_log=ns/bidder_bid_events=ms/auction_events=µs · CIL floor 2023-10-01 (scores NULL pre-2025-06); bid_events+bid_logs_enriched 90d, event_log_filtered 60d, augmentor+bid_price 10d · dims filter deleted=FALSE AND is_test=FALSE.
- **Stage/channel/holdout:** objective_id UNRELIABLE→funnel_level; Prospecting=obj IN (1,5,6) (1=Prosp/4=RT/5=MT-S2/6=MT-S3/7=Ego) · channel_id CTV8/display1 · product_id 1=PTV/2=Select/3=QF · 10% holdout MD5('{AID}:{IP}') mod 1000 0–99, ITT.
- **Joins/IP:** bid_logs dedup ROW_NUMBER, bid_ip=COALESCE(NULLIF(ip,'0.0.0.0'),impression_log.bid_ip,event_log.bid_ip) · win_logs campaign_alt_id=cg_id/line_item_alt_id=campaign_id · no IP→IP bridging in BQ.
- **Metric gotchas:** ip vs ip_raw (ui_visits/visits) · *_facts/all_facts `hour`=DATETIME not TIMESTAMP · is_new=client JS pixel (41–56% mismatch normal) · ui_conversions.order_amt (order_amt_usd=NULL) · RTC model_params~'realtime_conquest_score=10000' · audiences=templates/audience_segments=targeting · device_type INT bronze/STRING silver.
- **Sources/floors:** agg__daily_sum_by_campaign cheapest but Sep 2025+ uniques~0→sum_by_campaign_by_day for long pre-periods · 2025-01-01 floor is LOG tables only (CIL 2023-10, ui_visits 2023-01, all_facts ~2020-10) · fpa_advertiser_verticals type0 parent(37)/type1 sub(148), advertiser_name UNRELIABLE→JOIN advertisers.company_name · filter low-imp <1,000 weeks · WGU=31357.
