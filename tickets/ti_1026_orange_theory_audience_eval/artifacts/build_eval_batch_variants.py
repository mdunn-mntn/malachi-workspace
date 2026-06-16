#!/usr/bin/env python3
"""TI-1026 / TI-1037: build eval_batch payloads to validate the audience-size story against the
authoritative API (audience-service /eval_batch, VPN-only). 2x2: {MM-only, MM+3P} x {geo, no-geo}.

Run the generated runner.sh from a corp-network/VPN machine. Comparing the 4 returned sizes:
  - (full vs mm_only)       -> how much the 3P OR inflates the official size
  - (with-geo vs no-geo)    -> whether eval_batch applies the 946-studio radii fence
"""
import json, copy
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE.parent / "outputs"
ADV = 39718
EXPR = json.loads((OUT / "ti_1026_audience_34668_expression.json").read_text())

def strip_ds(expr, ds_to_drop):
    """Return a copy with the given data_source_id removed from the interest.include OR-array."""
    e = copy.deepcopy(expr)
    for block in e.get("interest", {}).get("include", []):
        if "or" in block:
            block["or"] = [c for c in block["or"] if c.get("data_source_id") != ds_to_drop]
    return e

def strip_geo(expr):
    e = copy.deepcopy(expr)
    if "geo" in e:
        e["geo"]["radii_include"] = []
        e["geo"]["radii_exclude"] = []
    return e

variants = {
    "A_full_geo":        EXPR,                                  # MM + 3P, with geo  (the real audience)
    "B_full_nogeo":      strip_geo(EXPR),                       # MM + 3P, no geo
    "C_mm_only_geo":     strip_ds(EXPR, 35),                    # MM only (drop DS35 3P), with geo
    "D_mm_only_nogeo":   strip_geo(strip_ds(EXPR, 35)),         # MM only, no geo
    "E_3p_only_geo":     strip_ds(EXPR, 19),                    # 3P only (drop DS19 MM), with geo
}

for name, expr in variants.items():
    payload = [{"advertiserId": ADV, "expressionTypeId": 2, "expression": json.dumps(expr)}]
    (OUT / f"ti_1026_evalbatch_{name}.json").write_text(json.dumps(payload))

# runner
runner = OUT / "ti_1026_evalbatch_runner.sh"
lines = ["#!/bin/bash",
         "# Run from a corp-network / VPN machine. Returns the audience size for each variant.",
         "URL=https://audience-service.prod.in.mountain.com/eval_batch",
         "for f in ti_1026_evalbatch_*.json; do",
         '  echo "=== $f ===";',
         '  curl -s -X POST "$URL" -H "Content-Type: application/json" -d @"$f";',
         '  echo;',
         "done"]
runner.write_text("\n".join(lines) + "\n")
runner.chmod(0o755)

print("wrote variants:", *[f"ti_1026_evalbatch_{n}.json" for n in variants])
print("runner: outputs/ti_1026_evalbatch_runner.sh")
print("\nWhat each comparison answers:")
print("  A vs C  -> how much the 3P OR inflates the official size (with geo)")
print("  B vs D  -> same, without geo")
print("  A vs B  -> does eval_batch apply the 946-studio geo fence? (equal => NOT applied)")
print("  C vs D  -> same, MM-only")
print("  E       -> 3P-only contribution")
