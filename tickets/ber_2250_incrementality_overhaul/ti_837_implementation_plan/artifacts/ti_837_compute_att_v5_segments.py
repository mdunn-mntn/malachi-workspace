"""TI-837 v5: Compute ATT per segment.

The v5 BQ output JSON has a `segment` field. This script splits by segment
and runs the existing compute_att.py logic for each, producing 4 separate
meta-analysis JSONs.

Usage:
    python ti_837_compute_att_v5_segments.py
"""
import json
import subprocess
from pathlib import Path

ROOT = Path("/Users/malachi/Developer/work/mntn/workspace/tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan")
OUTPUTS = ROOT / "outputs"
ARTIFACTS = ROOT / "artifacts"
SEGMENTS = ["all", "prosp", "stage1", "rtg"]


def main():
    src = OUTPUTS / "ti_837_lift_30adv_7day_v5_2026_04_20_to_26.json"
    rows = json.load(open(src))
    print(f"Total rows: {len(rows)}")

    by_segment = {s: [] for s in SEGMENTS}
    for r in rows:
        seg = r.get("segment")
        if seg in by_segment:
            r2 = {k: v for k, v in r.items() if k != "segment"}
            by_segment[seg].append(r2)

    for seg in SEGMENTS:
        print(f"\n=== segment: {seg} ({len(by_segment[seg])} rows) ===")
        seg_path = OUTPUTS / f"_v5_segment_{seg}.json"
        json.dump(by_segment[seg], open(seg_path, "w"), indent=2)

        meta_out = OUTPUTS / f"ti_837_meta_analysis_30adv_v5_segment_{seg}_2026_04_20_to_26.json"
        cell_out = OUTPUTS / f"ti_837_per_cell_table_30adv_v5_segment_{seg}.csv"

        cmd = [
            "python3",
            str(ARTIFACTS / "ti_837_compute_att.py"),
            str(seg_path),
            "--out-json", str(meta_out),
            "--out-csv", str(cell_out),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"FAILED for segment {seg}")
            print(result.stderr[-2000:])
        else:
            # Print headline from this segment
            d = json.load(open(meta_out))
            for tier in ("high", "peak", "mid"):
                if tier in d.get("per_tier_ivw", {}):
                    g = d["per_tier_ivw"][tier].get("guid", {})
                    c = d["per_tier_ivw"][tier].get("clickpass", {})
                    g_att = g.get("att", 0) * 100
                    c_att = c.get("att", 0) * 100
                    print(f"  {tier}: guid={g_att:+.3f}pp, clickpass={c_att:+.3f}pp")


if __name__ == "__main__":
    main()
