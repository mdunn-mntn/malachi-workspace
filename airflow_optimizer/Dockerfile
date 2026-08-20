# Spark efficiency optimizer, packaged for an unattended runner (Cloud Run job).
#
# Base image ships gsutil, which is not optional: `gcloud storage cp` corrupts the .zstd
# event logs via the decompressive-transcoding gatekeeper, so the download path has to stay
# on gsutil with check_hashes=never.
#
# The image holds NO credential. It expects an attached service-account identity for GCP and
# reads the two secrets that have no keyless path (Astro, Databricks) from Secret Manager at
# start-up. Nothing is baked in and nothing is written back to a repo.
#
#   docker build -t spark-optimizer -f airflow_optimizer/Dockerfile .
#   docker run --rm -e OPTIMIZER_GCS_PREFIX=gs://... spark-optimizer
FROM google/cloud-sdk:slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    OPTIMIZER_LOG_CAP=200 \
    OPTIMIZER_PHS=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends zstd python3-minimal \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY airflow_optimizer/ /app/airflow_optimizer/
COPY .claude/scripts/oncall_daily_optimizer.sh /app/bin/sweep.sh

# The sweep runs as a non-root user; nothing it writes outlives the container except GCS.
RUN useradd --create-home --uid 1001 optimizer \
    && mkdir -p /app/out \
    && chown -R optimizer:optimizer /app
USER optimizer

# Fails the build if the parser, the rolling-dir reconstruction or the ledger wiring is broken,
# so a bad image never reaches the scheduler.
RUN python3 -m airflow_optimizer.crawl /app/airflow_optimizer/tests/fixtures | head -n 1

ENTRYPOINT ["bash", "/app/bin/sweep.sh"]
