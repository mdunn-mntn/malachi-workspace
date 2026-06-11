#!/usr/bin/env python3
"""TI-1026 — classify the 379 DS19 'MNTN Matched' keywords by relevance to a
boutique HIIT fitness studio (Orange Theory). Heuristic + curated lists.

Buckets:
  core        — unambiguously on-target (fitness, workout, studio, HIIT, strength/cardio)
  adjacent    — plausibly relevant but broad (apparel, nutrition, wellness, recovery, wearables)
  off_target  — clearly irrelevant to a fitness studio (the 'template junk')
  too_broad   — single/generic terms that match huge low-intent traffic (dilutive)

Output: outputs/ti_1026_keyword_classification.csv (keyword, bucket, action)
The off_target + too_broad lists are the prune recommendation; core+adjacent stay.
"""
import csv
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE.parent / "outputs"
SRC = OUT / "ti_1026_ds19_keywords.csv"

# --- curated lists (lowercased exact-name match) ---
# OFF_TARGET = high-confidence "not a fitness studio" keywords (the template junk). DROP candidates.
OFF_TARGET = {
    "above ground pools", "abrasives", "antifreeze", "arcade and gaming machines",
    "adhesive tapes", "advent calendars", "analog watches", "ballasts", "barcode reader",
    "bathtubs", "beer mugs", "blenders", "bluetooth audio devices", "chef apparel",
    "coffee grinders", "compact suv", "conveyor belt products", "cpus",
    "dashboard and instrumentation", "data analysis tools", "dental and medical adhesives",
    "design patterns", "entertainment software", "food storage containers", "guitar parts and accessories",
    "ignition", "maintenance and cleaning services", "mattress", "mirrors", "montessori",
    "motorcycle lighting and electrical", "outdoor electronics and equipment",
    "outdoor surveillance equipment", "pillows", "protective covers", "restroom supplies",
    "route planning software", "rug accessories", "signaling system", "spelling and reading programs",
    "stem kits", "strap-on vibrators", "suspension kits", "sway bars",
    "townhouse", "transformers", "virtual reality headsets", "wireless earphones", "skydiving",
    "butter", "juices", "beer mugs",
}
# TOO_BROAD = generic single/2-word terms that match huge low-intent traffic. REVIEW candidates.
TOO_BROAD = {
    "class", "power", "silver", "experience", "challenges", "community", "clubs", "events",
    "live events", "virtual events", "nightlife events", "awards events", "autumn leaf festival",
    "trainer", "benches", "plates", "tumbler", "vests",
    "franchise", "franchise services", "corporate services", "room rentals", "equipment rental",
    "online registration", "online community", "online courses", "job fairs", "individual contributor",
    "scientific content", "life sciences", "score tracking", "flow technology", "body controls",
    "assessment services", "assessment tools", "membership services", "exclusive memberships",
    "social marketing", "party planning", "event setup", "workshops", "workshops and classes",
    "recipe",
}

def classify(name: str) -> str:
    """Conservative: default unknowns to KEEP. Only flag high-confidence off-target.
    The exact KEEP/DROP line is for Kelly/Sales to finalize — this surfaces candidates."""
    n = name.strip().lower()
    if n in OFF_TARGET:
        return "off_target"
    if n in TOO_BROAD:
        return "too_broad"
    return "core_or_adjacent"  # default to KEEP — don't over-recommend dropping


def main():
    rows = []
    with SRC.open() as f:
        r = csv.DictReader(f)
        for row in r:
            name = row["name"]
            if not name:
                continue
            b = classify(name)
            action = {"core_or_adjacent": "KEEP",
                      "too_broad": "REVIEW (broad, low-intent)",
                      "off_target": "DROP (off-target)"}[b]
            rows.append((row["data_source_category_id"], name, b, action))

    out = OUT / "ti_1026_keyword_classification.csv"
    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["data_source_category_id", "keyword", "bucket", "recommended_action"])
        w.writerows(sorted(rows, key=lambda x: (x[2], x[1])))

    from collections import Counter
    c = Counter(b for _, _, b, _ in rows)
    total = len(rows)
    print(f"Total keywords classified: {total}")
    for b in ("core_or_adjacent", "too_broad", "off_target"):
        print(f"  {b:<16}: {c[b]:>3}  ({100*c[b]/total:.1f}%)")
    print(f"\n  KEEP:            {c['core_or_adjacent']} ({100*c['core_or_adjacent']/total:.1f}%)")
    print(f"  DROP (off-tgt):  {c['off_target']} ({100*c['off_target']/total:.1f}%)")
    print(f"  REVIEW (broad):  {c['too_broad']} ({100*c['too_broad']/total:.1f}%)")
    print("\n=== off_target examples ===")
    for _, name, b, _ in sorted(rows, key=lambda x: x[1]):
        if b == "off_target":
            print("  -", name)


if __name__ == "__main__":
    main()
