---
name: reference_docker_desktop_concurrent_build_stall
description: Docker Desktop 28.0.4 on this Mac — while another session runs `docker build --pull`, every other build stalls at "load metadata for docker.io/..." and any `docker run --platform linux/amd64` hangs on registry access; run local images by id without --platform and stagger builds across parallel agents.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [docker, docker desktop, docker build hang, docker build stall, load metadata, docker.io/library/python, platform linux/amd64, registry stall, concurrent docker builds, parallel sprint agents, docker run by image id, docker 28.0.4, openai_batch_runner local image, airflow-camperbid build, bind mount worktree over /app, AUDI-1279]
domain: [infra, workflow]
lifecycle: active
last_verified: 2026-09-03
---

**Docker Desktop 28.0.4 on this Mac serializes registry access across sessions.** While one session runs
`docker build --pull` (2026-09-03: the airflow-camperbid build), a second session's `docker build` never gets past
`load metadata for docker.io/library/python:3.11` (AUDI-1279: 06:50Z to 07:07Z, killed), and `docker run --platform
linux/amd64 <tag>` hangs too, because the platform flag makes the daemon consult the registry even when the image is local.

**Why:** cost 17 minutes and the ticket's only in-container evidence of a Dockerfile change (the `ENV PYTHONUNBUFFERED=1`
line was passed as `-e` instead and stays unbuilt until the owner's deploy workflow).

**How to apply:**
- Check `docker ps` / the other agents' logs for a running `--pull` build before starting one; stagger builds across
  parallel `/sprint` agents.
- Run an already-built local image **by image id, without `--platform`** (`docker images` → `docker run --rm <id> ...`).
- To test branch code without a rebuild, bind-mount the worktree read-only over the image's app dir
  (`-v <worktree>/openai:/app:ro`) and pass any new `ENV` lines as `-e`; record that the Dockerfile line itself is untested.
- A stalled build is a stall, not slow: kill it at ~5 min of no output ([[feedback_background_work_liveness]]).

Related: [[reference_shopper_graph_deploy]] (the `openai_batch_runner` image and the staging recipe).
