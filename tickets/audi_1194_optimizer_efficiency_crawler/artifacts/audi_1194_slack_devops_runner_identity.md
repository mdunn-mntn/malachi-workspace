# Slack draft, #dev-basecamp (or the devops channel), re: identity for the optimizer runner

Self-contained: assumes the reader knows nothing about the job. Answers land in `audi_1194_runner_and_identities.md` §8.

## Message

Hi all, I need help picking the right identity path for a small scheduled job, and I'd rather ask than guess and open the wrong PRs.

*What it is.* It reads the Spark event logs our Dataproc batches already write, finds jobs that succeeded but wasted compute, and writes a daily markdown report. Read-only apart from that report. It runs on my laptop today under my personal Google SSO, so it silently stops working whenever my session expires, and a laptop compromise carries the same access the job has.

*Where I want to end up.* A Cloud Run job in mntn-prj-prod-00 on Cloud Scheduler, running as its own service account, no key anywhere. It needs to read objects in gs://mntn-data-archive-prod and in the Dataproc temp bucket dataproc-temp-us-central1-995798185124-svhwvc6j, call dataproc batches list/describe on the project, and write to one prefix, gs://mntn-data-archive-prod/optimizer/.

*1. Where should the Cloud Run job resource itself be defined?* I found the IAM half of daily-jedi-media-spend in mntn-devops and its comment says Crossplane owns the rest, but I can't find the job definition. `kind: V2Job` greps empty across argocd-v2/mgmt/platform/crossplane, and jedi-media-spend isn't in mntn-argocd at all. Point me at one scheduled Cloud Run job end to end and I'll copy it exactly.

*2. Own IAM bindings for the new service account, or add it to a group?* The two grants it depends on were both written against group:audience-intelligence@mountain.com rather than a person: dataproc.viewer (DEV-8182) and objectViewer on the temp bucket (open PR mntn-devops#4724). Adding the SA to that group is simplest. My hesitation is that our IAM audit doesn't expand Workspace group membership, so a grant reaching the SA through a group is invisible to it. Which do you prefer?

*3. Any approved way for a Cloud Run job to read a single GitHub repo?* Optional, it would just let the report name the exact config line to change. SOP 060 scopes Octo STS to Actions workflows and a Cloud Run job has no Actions runner, so my plan is to give it no GitHub access at all. Correct me if there's a path I've missed. Not reaching for a PAT either way.

*4. Who owns the Astro org, and who owns our Databricks service principals?* I need a read-only Astro deployment API token so the job can list DAGs, and a read-only Databricks service principal. A name is enough.

Already sorted, no need to re-answer: secrets go to Vault via mntn-team-credentials, Update Team Secret, new entry under our team path. The container and the job are built and tested. This is the last piece.
