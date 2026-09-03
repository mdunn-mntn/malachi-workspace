32 of 67 alerting DAGs were invisible to the Airflow debugger: their team tag was not on its watch list. Adds the tag and a CI check that blocks the next miss.

What
* include/airflow_debugger/daily.py: "Targeting" appended to PAGING_TAGS (25 DAGs now visible).
* tests/dags/test_alerting_tag_coverage.py: parses the DAG sources, no Airflow install. Attribution's 7 DAGs are excluded; their alerts go to that team's channel.
* .github/workflows/pr_alerting_tag_coverage.yaml: runs it on PRs touching dags/, the team configs or the list.

Why
PR #1248 fixed two misses by hand; the next DAG could reopen the gap.

Validation
* Without the list change 25 files fail; with it 152 pass, 7 skipped.
* Probe DAGs (unwatched team, runtime-built tag, no team binding) each fail.
* Python 3.12: dags/tpa/category_taxonomy.py uses an f-string form 3.11 rejects.
