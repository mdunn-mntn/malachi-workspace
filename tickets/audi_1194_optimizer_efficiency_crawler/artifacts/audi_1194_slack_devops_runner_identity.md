# Slack draft, devops (#dev-basecamp or Cristina), re: identity for the optimizer runner

Everything below the marker is the message. Answers land in `audi_1194_runner_and_identities.md` §8.

## Message

Hey, moving a daily read-only job off my personal SSO onto a Cloud Run job in mntn-prj-prod-00. Four things I couldn't find in the repos.

1. Where does the job manifest live for a scheduled Cloud Run job? I found the IAM split for daily-jedi-media-spend, but `kind: V2Job` greps empty in argocd-v2 and mntn-argocd.

2. Should its runtime SA get its own bindings, or join audience-intelligence@ like DEV-8182 and #4724 did?

3. Any approved way for a Cloud Run job to read one GitHub repo? SOP 060 scopes Octo STS to Actions, so I think the answer is no GitHub access at all.

4. Who owns the Astro org and the Databricks service principals?
