#!/bin/sh
# Usage: audi_1271_identify_log.sh <gs uri>  -> prints "<uri>,<Spark App Name>" from the first 256 KiB of the zstd log
uri="$1"
name=$(gsutil -o "GSUtil:check_hashes=never" cat -r 0-262143 "$uri" 2>/dev/null | zstd -dc 2>/dev/null | grep -o '"App Name":"[^"]*"' | head -1 | cut -d: -f2- | tr -d '"')
echo "$uri,$name"
