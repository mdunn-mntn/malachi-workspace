# ipdsc_ds_49: what it is, what we changed, and what it cost — 2026-09-05

**The 64 MB setting, not the volume growth, is what raised this job's cost: input rose 15% before the change with no move in cost-per-unit, and cost-per-unit then jumped 30-80% the moment the setting landed — so revert it, since the spill it fixed was never billed and the speed it bought has already disappeared.**

## What the job is
Every night this job reads seven days of MNTN's own streaming-TV ad-auction records and boils them down to one list: for each home internet address, which streaming channels that household had an MNTN ad opportunity on. Last night's list covered 93.8 million addresses across 207 channels; the biggest are platform lineups like Samsung TV+ News, Paramount Streaming - Comedy and LG Entertainment, alongside real networks like Peacock, Tubi, CNN and CBS Local News. It is rebuilt from scratch daily and is one of twenty such lists merged into the nightly file that tells MNTN's ad-buying system what is known about each internet address. The business reason it exists is so campaigns could target or exclude "households that watch this channel" the way they target interest categories today. Nothing in live campaign setup currently references it (zero hits across all 425,054 targeting-segment definitions and all 68,265 audience definitions), but a hard failure blocks that entire nightly file for all twenty lists and pages the Targeting squad. One correction to the framing: ipdsc_ds_49 is a task inside the DAG tpa_ipdsc_export, not a DAG. That is good news for this analysis, because it means it is metered on its own and its cost is not mixed with its siblings.

## What we changed
PR #1272 added one line to the job, merged 2026-09-03 19:47 UTC, first effective on the run of 09-04 03:07 UTC. It halved the chunk of data each worker reads at a time, from the 128 MB default to 64 MB. There was no prior setting; the job had always used the default.

What it bought is real. The job stopped overflowing memory onto disk: spill fell from 24.1 GB on disk and 457 GB in memory to 1.4 GB and 19 GB, a 94% cut. That was the stated goal and it hit it. It also initially ran 28% faster.

What it cost. Halving the chunk size doubled the number of work chunks (658 to 1,480 on the first run). Under autoscaling the cluster answered by more than doubling the machines it grabbed, 48 to 116. Google bills machine-time, not wall-clock, so the bill went up. Daily cost, in the units Google meters (DCU-hours, roughly machine-hours): 11.5 to 13.5 across the sixteen days before the change, then 18.7, 24.3, and 29.0 on the three runs since. Last night's run is 2.5x the pre-change level.

The speed win has since evaporated. Batch wall-clock went 335s, 358s, then 423s on last night's run, against a pre-change average of about 438s. We are now paying 2.5x for a job that finishes at roughly the same time it always did.

Worth saying plainly: the spill it eliminated was never separately billed. Google's second meter, shuffle storage, is provisioned per machine, not per byte spilled — it is an exact fixed multiple of the machine meter on every run. So the 94% spill cut recovered nothing on any invoice line, and that meter rose 81% alongside the other one.

## Did the extra volume explain the cost?
No. And separately, the input growth was not new advertisers either. Two findings.

FIRST, on the advertiser hypothesis: not supported. The number of advertisers actually delivering ads was flat straight through the step, 7,005-7,180 per day from 08-18 to 09-04, with 7,145 on 09-01 itself. That day had the lowest impressions (57.6M) and lowest media spend of the whole window. Advertisers with a first-ever delivery on 09-01 numbered 15 — the window maximum, but 0.2% of the base and three orders of magnitude too small to move anything. The raw upstream bid stream did not grow either: it fell 8% and 12% across the step. The growth is concentrated in three or four supply publishers whose matched traffic jumped 5-20x overnight (Samsung TV+ News 0.55B to 4.21B events, Paramount Comedy 1.98B to 6.55B, LG Entertainment 1.47B to 2.21B) while every other publisher stayed flat and the publisher count never moved (203-208). The likeliest cause is a change to the site-to-publisher mapping table, not a change in demand: one operator edited 53 rows of it in a 27-minute session at midnight UTC on 09-01, touching Pluto TV, Vizio WatchFree and Xumo bundles. That is a lead, not a conclusion — proving it needs the Postgres change history, which is not reachable from here.

SECOND, on whether volume explains the cost: it does not, and there is a clean natural control in the data. The job reads a rolling seven-day window, so the 09-01 step entered gradually. That means two runs — 09-01 and 09-02 — already carried 7% and 15% more input while still on the OLD 128 MB setting. Their cost per unit of input was 0.272 and 0.257 DCU-hours per GiB read, dead inside the pre-change band of 0.235-0.279 across thirty runs. Volume rose 15% and the unit cost did not move. Then the setting changed, and unit cost went 0.338, 0.408, 0.465 on the three runs since — 30%, 58% and 80% above the pre-change mean.

Attribution at last night's run: the job cost 11.5 DCU-hours before any of this. At the old efficiency, today's volume alone would cost about 16.2. It actually cost 29.0. So of the 17.6 DCU-hour rise, roughly 4.7 is volume and roughly 12.9 is the config change — about one quarter volume, three quarters the setting.

The mechanism is also a fingerprint volume cannot fake. Work chunks per unit of input sat at 12.3-12.8 across all thirty pre-change runs while input itself varied by 37%; it jumped to 26.7 the instant the setting landed and has stayed there. Only halving the chunk size does that.

Two honest caveats. Both post-change runs read more data than the old setting was ever observed handling (max 52.3 GiB pre, 55-62 GiB post), so a cost nonlinearity above that range cannot be excluded from observation alone. And unit cost is still rising at a fixed setting (0.338 to 0.408 to 0.465 with the config byte-identical), which nobody has explained; the leading candidate is that smaller chunks weaken the pre-grouping step so more rows cross the network, and that penalty grows with volume. Neither caveat rescues the volume hypothesis — both post-change runs sit far outside the band, and the pre-change control already showed volume moving without unit cost moving. This is separable, and it separates against the change.

## Recommendation
Revert ipdsc_ds_49 to the 128 MB default. Delete the one added line, or revert only that file's hunk of commit 9ae505a — do not revert the whole commit, it also carries an unrelated change to conv_log_derived_ip, which is fine and should be left alone.

Reason: the change is costing about 12.9 DCU-hours a day and widening, and it bought two things that turn out to be worth nothing. The spill it eliminated is not billed per byte on this platform. The speed it bought is already gone — last night's run was slower than the pre-change average — and even at its best the 1.4 minutes it saved bought nothing downstream, because the DAG then sits behind an eight-hour sensor waiting on an external pipeline's file, idling 1 to 3 hours on every recent run. The job was never the constraint; the last sibling to finish was ipdsc_ds_35 on seven of the last nine runs, never this one.

Cost of being wrong: spill returns to roughly 30 GB on disk and 450 GB in memory, and the job takes a minute or two longer. Neither is billed and neither moves the DAG. The residual risk is that the old setting has never been observed at the volumes we now have (52.3 GiB max previously, heading to about 71 GiB at saturation), and spill grows faster than linearly, so the revert run will spill more than it ever has. Projected at about 33 GB over roughly 63 machines is half a gigabyte each, nowhere near a disk limit, but that knee is unmeasured. If it does fail, it fails visibly, on one run, and we put the line back.

Do not take the alternatives. Capping machines cannot work: about 60% of the increase is real task time, not idle machines, and even at perfect utilization the job lands above the pre-change band. Trying 96 MB is a guess with no measured baseline at any volume. Keeping it and accepting the cost means paying an unbounded, still-widening bill for a benefit that has already expired.

## How we would know
Revert the single line today. Three post-revert runs land on the mornings of 09-07, 09-08 and 09-09.

Score it on DCU-hours per GiB of input the run actually read, taken from the run's own Spark log, NOT on DCU-hours per day. This is the critical instruction. The input window is still filling in from the 09-01 step and does not saturate until the run of 09-08, at roughly 80 GiB. So daily cost will keep rising for two more days no matter what we do, and a daily total will read as failure even if the revert works perfectly.

Pass: unit cost returns to 0.235-0.279. Fail: it stays near 0.4 or higher, which would mean something other than the config is driving this and we look at the still-unexplained day-over-day escalation instead. The gap is large enough that one run distinguishes them; three make it unambiguous.

Answer date: 2026-09-09.

Expect the daily total to land near 18-19 DCU-hours at saturated volume if the revert works, versus 33 or more if we leave the setting in place. That is not a return to the 11-13 range and should not be scored as one — the volume step is permanent and costs about 5 DCU-hours a day on its own.

## Open questions

- What actually caused the 09-01 input step. The evidence points at a site-to-publisher mapping edit rather than anything on the advertiser side, but the mapping table is a Postgres CDC mirror that keeps only the current state, so the prior values of the 53 rows edited that morning are not recoverable from BigQuery. Settling it needs the Postgres audit history or a word with whoever made the edits. Worth chasing: it is a second, independent cost driver of about 5 DCU-hours a day, and if it was an unintentional mapping widening it may be reversible.
- Why unit cost is still climbing with the config unchanged (0.338, 0.408, 0.465 across three runs). If the config penalty scales with volume, cost keeps growing even after the revert, and 96 MB or a memory increase becomes the next question. Three post-revert runs will show whether it is the config scaling or something else.
- Whether anything outside airflow-ti consumes this list. Zero references in the targeting tables and none in the pipeline repo beyond the export, the geo build and the monitors, but the MemDB service and the bidder are separate codebases not checked out here. This matters if anyone ever asks whether the job is worth running at all.
- What the job is for going forward. It has run since 2026-04-13 and no live campaign uses it. If a Publisher Network targeting product is not coming, the cheapest optimization available is switching it off, which is worth more than any tuning discussed here. That is a product question, not an engineering one.
- Dollar cost. Everything above is in DCU-hours, Google's compute meter. I never verified the per-DCU-hour rate for us-central1 or reconciled against an invoice, so the magnitudes are relative, not absolute.

## Method

76 agents across four independent lanes (what the job is, its input volume, its cost, the change itself). Every material claim went through a default-refute verifier before entering this document. Cost figures come from the Dataproc REST API's `approximateUsage.milliDcuSeconds` and from per-run Spark event logs, neither of which touches the optimizer's own ledger.

