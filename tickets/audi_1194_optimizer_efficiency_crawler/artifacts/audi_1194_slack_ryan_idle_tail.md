# Slack DM draft, Ryan Kleck, re: site_network_hourly idle tail

Supersedes the WITHDRAWN stage-9 ask in `audi_1194_slack_ryan_site_network_hourly.md`. Evidence:
302 runs, 2026-08-04..08-26, `audi_1194_peak_concurrency.md` + `audi_1194_stage_read_parallelism.py`.
Everything below the marker is the message.

## Message

Hey Ryan, one ask on site_network_hourly: can you check what's keeping its executors alive after the initial burst? Dynamic allocation not scaling down, shuffle tracking, cached blocks, whatever it turns out to be.

Over its last 302 runs it holds a median 241 executor-hours to do 27.5 hours of task work. The ceiling itself is right, at peak every slot it holds is busy, but after the burst the fleet sits at 2-3% utilization for the rest of the run. That idle tail is 18,300 of the job's 21,200 executor-hours since Aug 4, about 86%, and it's the biggest single cost item the optimizer sees fleet-wide.

Happy to profile a run after any change, the daily sweep picks it up automatically.

## Evidence for follow-ups

- Peak: 1,988 concurrent tasks = 497 executors x 4 slots, measured with a 100ms slot-handoff
  tolerance (raw instantaneous counts overshoot; 0.12% of busy time sits above 4/slot).
- Mean: 43.9 concurrent tasks against 2,160 slots held (2.2%).
- `idle_reserved_executors` fires on 236 of 302 runs, median 11% utilization.
- Lowering `maxExecutors` would lengthen the saturated peak; the lever is the tail.
- The earlier stage-9 shuffle-fetch ask was withdrawn: median 0.28% of run executor-hours.
