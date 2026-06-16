#!/bin/bash
# Run from a corp-network / VPN machine. Returns the audience size for each variant.
URL=https://audience-service.prod.in.mountain.com/eval_batch
for f in ti_1026_evalbatch_*.json; do
  echo "=== $f ===";
  curl -s -X POST "$URL" -H "Content-Type: application/json" -d @"$f";
  echo;
done
