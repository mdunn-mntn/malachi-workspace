No mntn-devops PR is needed: Mode already has the access this ticket was going to grant.

What
Standby only. The grant would add roles/bigquery.resourceViewer on dw-main-bronze for
mode-analytics@dw-main-bronze, mirroring the spark-optimizer grant merged for the same read.

Why it is not opened
mode-analytics holds one role on dw-main-bronze, the custom medallion_bronze_reader, and that
role already includes bigquery.jobs.listAll and bigquery.jobs.create. spark-optimizer needed a
grant because it held no role on dw-main-bronze at all.

Validation
Live project policy for the role list; the terraform creating that role for its permission list;
the Mode service account confirmed from the dataset ACL the report reads. The dashboard query
ran at 0.178 GB and 10.6 slot-seconds a day of window.

Open only if the Mode run returns 403, meaning the role drifted from its terraform.
