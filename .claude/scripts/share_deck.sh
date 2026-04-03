#!/bin/bash
# Share an HTML presentation deck via GitHub Gist + githack rendering
# Usage: bash .claude/scripts/share_deck.sh path/to/deck_standalone.html
#
# Creates a public gist and returns a rendered URL you can share in Slack.

set -euo pipefail

FILE="${1:?Usage: share_deck.sh <path-to-html-file>}"

if [[ ! -f "$FILE" ]]; then
    echo "Error: File not found: $FILE"
    exit 1
fi

FILENAME=$(basename "$FILE")

echo "Creating gist for: $FILENAME"
GIST_URL=$(gh gist create --public "$FILE" 2>&1 | grep "https://gist.github.com")
GIST_ID=$(echo "$GIST_URL" | grep -oE '[a-f0-9]{32}')

if [[ -z "$GIST_ID" ]]; then
    echo "Error: Failed to create gist"
    echo "$GIST_URL"
    exit 1
fi

RENDERED_URL="https://gist.githack.com/mdunn-mntn/${GIST_ID}/raw/${FILENAME}"

echo ""
echo "Gist:     $GIST_URL"
echo "Rendered: $RENDERED_URL"
echo ""
echo "$RENDERED_URL" | pbcopy
echo "(Rendered URL copied to clipboard)"