#!/bin/sh
# Usage: TOKEN=$(gcloud auth print-access-token) audi_1271_fetch_log.sh <gs://bucket/object> <out_dir>  (GCS JSON API; gsutil -m stalls on this Mac)
uri="$1"; out="$2"
bucket=$(echo "$uri" | sed -E 's#^gs://([^/]+)/.*#\1#')
object=$(echo "$uri" | sed -E 's#^gs://[^/]+/##' | sed 's#/#%2F#g')
name=$(basename "$uri")
curl -sS --fail -H "Authorization: Bearer $TOKEN" "https://storage.googleapis.com/storage/v1/b/$bucket/o/$object?alt=media" -o "$out/$name" && echo "ok $name $(stat -f %z "$out/$name")" || echo "FAIL $name"
