# dsh adoption — go/no-go assessment (2026-08-21)

Assessed against the program kill-criteria (design C §5) after building Phases 1-6. The spike's
question — can a pinned dsh sidecar run our skills, bq gate, and a harvesting engine with every unit
passing its adversarial gate — is answered **YES**. Verdict: **GO to continued adoption at L0/L1**,
with the named blockers before any unattended run.

## What was proven (evidence)
| Claim | Evidence |
|---|---|
| dsh boots pinned, Anthropic via Keychain | headless "OK" exit 0; `--dump-config` composes 3 profiles |
| 6 skills mount verbatim | `/frame` loaded by name headless; catalog lists all 6 |
| bq gate governs (deny raw, cost-cap, provenance) | 35 vitest, multiline guard-deny behavioral PASS in-harness; cap fail-closed tested |
| recall/orient/commands port cleanly | 20 vitest; recall content-asserted in session log; orient read-only |
| keyless replay-eval works | 2 goldens zero-drift + corrupted-fixture negative control |
| machine gate computes PASS/FAIL correctly | verify_gate: metric-less refused, drift/findings fail, clean passes |
| floors enforced in code | FLOORS.yml commit guard blocks unapproved, passes approved |
| full engine loop | harvest 15 -> real LLM hypothesize w/ metric -> PROPOSE; rung-0 auto-adopt; rollback drill |
| host kit unaffected | verify.sh full + hooks_selftest green with dsh present; 55 dsh tests green |
| chaos resilience | credential-revocation fail-closed, corrupt-config refused at boot, kill switch halts all entrypoints |

## Kill-criteria status (none tripped)
- **K1 churn** (>6h/mo breakage x2mo): build cost ~0 breakage so far; one py3.11-vs-3.9 portability bug, fixed in minutes (the integration run caught it). GREEN.
- **K2 security** (1 exfil/credential incident): none. Egress cage INSTALLED + verified 2026-08-21 (egress_selftest all green). GREEN.
- **K3 reliability** (<90% corpus pass 3 wks): all runnable scenarios pass; corpus is small (5 probes + mined skeletons) — grows with real use. GREEN, immature.
- **K4 economics** (>3x baseline cost 4wks or >$200/mo): engine spend near zero (harvest keyless; one hypothesize ~$0.01). GREEN.
- **K5 upstream** (repo stalls / adverse terms): rc still active; pinned exact. GREEN.

## Blockers before autonomy (not defects — sequenced gates)
1. **gcloud auth** (user): unblocks the one live-BigQuery integration assertion. Everything else is proven.
2. ~~Egress cage~~ DONE 2026-08-21 — installed + verified.
3. **Soak** (calendar): 10 working days of daily-driver use with zero Sev-1 before L1 autonomy — starts when you begin using it.

## Recommendation
Adopt as a supervised L0 sidecar now (analyst profile for inspection, the engine's keyless harvest daily).
Do NOT grant L1/unattended until egress is green and the soak clock is clean. The autonomy ladder auto-adopts
rung-0 only today; higher rungs unlock on the logged track record. The whole program stays severable: deleting
`dsh-lab/` and `engine/` leaves the working kit, knowledge base, and Pi loops untouched.
