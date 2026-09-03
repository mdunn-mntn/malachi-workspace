"""Replay the publisher over a corpus of stage-metric rows and count what it would have filed.

    python3 artifacts/audi_1317_noise_replay.py <worktree> <as_of> <metrics.jsonl> [more.jsonl...]

Prints the per-DAG verdict counts and every finding the sweep would have written that day.
"""

import json
import sys
from collections import defaultdict


def main():
    worktree, as_of = sys.argv[1:3]
    sys.path.insert(0, worktree)
    from include.spark_optimizer import ledger, regression_guard as guard

    rows = []
    for path in sys.argv[3:]:
        rows += [json.loads(line) for line in open(path) if line.strip()]
    by_dag = defaultdict(list)
    for r in rows:
        by_dag[r.get("dag_id", "")].append(r)

    judged, with_baseline, fired, findings = 0, 0, 0, []
    print(f"{len(rows)} rows, {len(by_dag)} jobs, as of {as_of}\n")
    print(f"{'job':<42} {'runs':>5} {'window':>7} {'stages':>7} {'gated':>6} {'REGR':>5}")
    for dag in sorted(by_dag):
        result = guard.evaluate(by_dag[dag], dag, as_of)
        if not result["latest"]:
            continue
        judged += 1
        gated = [v for v in result["verdicts"] if v.n >= 5]
        hot = [v for v in result["verdicts"] if v.regression]
        if gated:
            with_baseline += 1
        if hot:
            fired += 1
        print(f"{dag[:42]:<42} {len({r['app_id'] for r in by_dag[dag]}):>5} "
              f"{result['runs']:>7} {len(result['latest']):>7} {len(gated):>6} {len(hot):>5}")
        for v in hot:
            findings.append((dag, guard.title_for(v, result),
                             f"regression_{v.metric}:{v.stage_id}", v.adaptive))

    print(f"\njobs with a newest run: {judged}")
    print(f"jobs with at least one gated stage (>= 5 window runs): {with_baseline}")
    print(f"jobs the publisher would file a regression for: {fired}")
    print(f"ledger rows it would write: {len(findings)}")
    for dag, title, key, adaptive in findings:
        print(f"  {dag}  {key}{'  [adaptive]' if adaptive else ''}\n    {title}")
    assert ledger


if __name__ == "__main__":
    main()
