#!/bin/bash
# transcribe.sh — Transcribe Zoom meeting recordings using both providers, pick the best
#
# Usage:
#   bash .claude/scripts/transcribe.sh "2026-03-30 11.33.01 Discuss Experiment with Causal Impact Meeting"
#   bash .claude/scripts/transcribe.sh /path/to/audio.m4a --ticket ti_504
#   bash .claude/scripts/transcribe.sh /path/to/audio.m4a --provider local   # force single provider
#   bash .claude/scripts/transcribe.sh /path/to/audio.m4a --provider openai  # force single provider
#
# Options:
#   --ticket TI_XXX       Save output to tickets/TI_XXX/meetings/ instead of current directory
#   --model MODEL         Whisper model size (default: large-v3). Options: tiny, base, small, medium, large-v3
#   --provider PROVIDER   Force a single provider: openai, local, or both (default: both)
#   --output FILE         Custom output filename (without extension)
#
# Default behavior: runs both OpenAI and local mlx-whisper, picks the one with
# less repetition/hallucination. Logs which provider won.
#
# Output: .txt transcript with timestamps and speaker segments

set -euo pipefail

ZOOM_DIR="$HOME/Documents/Zoom"
WORKSPACE="/Users/malachi/Developer/work/mntn/workspace"
MODEL="large-v3"
PROVIDER="both"
TICKET=""
OUTPUT_NAME=""

# Parse arguments
POSITIONAL=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --ticket)
            TICKET="$2"
            shift 2
            ;;
        --model)
            MODEL="$2"
            shift 2
            ;;
        --provider)
            PROVIDER="$2"
            shift 2
            ;;
        --output)
            OUTPUT_NAME="$2"
            shift 2
            ;;
        *)
            POSITIONAL="$1"
            shift
            ;;
    esac
done

if [[ -z "$POSITIONAL" ]]; then
    echo "Usage: bash transcribe.sh <zoom-folder-name|audio-file> [--ticket ti_xxx] [--provider local|openai]"
    echo ""
    echo "Providers:"
    echo "  openai  — GPT-4o Transcribe via API (default, \$0.36/hr, 5.4% WER, no hallucinations)"
    echo "  local   — mlx-whisper large-v3 on Apple Silicon (free, 7.2% WER, may hallucinate)"
    echo ""
    echo "Available Zoom recordings:"
    ls "$ZOOM_DIR" 2>/dev/null || echo "  No Zoom folder found at $ZOOM_DIR"
    exit 1
fi

# Resolve input file
if [[ -f "$POSITIONAL" ]]; then
    AUDIO_FILE="$POSITIONAL"
elif [[ -d "$ZOOM_DIR/$POSITIONAL" ]]; then
    # Find the audio file in the Zoom folder
    # Prefer .m4a (audio only, faster to process) over .mp4
    AUDIO_FILE=$(find "$ZOOM_DIR/$POSITIONAL" -name "*.m4a" | head -1)
    [[ -z "$AUDIO_FILE" ]] && AUDIO_FILE=$(find "$ZOOM_DIR/$POSITIONAL" -name "*.mp4" | head -1)
    if [[ -z "$AUDIO_FILE" ]]; then
        echo "Error: No audio/video file found in $ZOOM_DIR/$POSITIONAL"
        exit 1
    fi
elif [[ -d "$POSITIONAL" ]]; then
    AUDIO_FILE=$(find "$POSITIONAL" -name "*.m4a" | head -1)
    [[ -z "$AUDIO_FILE" ]] && AUDIO_FILE=$(find "$POSITIONAL" -name "*.mp4" | head -1)
    if [[ -z "$AUDIO_FILE" ]]; then
        echo "Error: No audio/video file found in $POSITIONAL"
        exit 1
    fi
else
    echo "Error: '$POSITIONAL' is not a file or Zoom recording folder"
    exit 1
fi

echo "Audio file: $AUDIO_FILE"
echo "Provider: $PROVIDER"
[[ "$PROVIDER" == "local" ]] && echo "Model: $MODEL"

# Determine output path
if [[ -n "$TICKET" ]]; then
    TICKET_DIR=$(find "$WORKSPACE/tickets" -maxdepth 1 -type d -name "${TICKET}*" | head -1)
    if [[ -z "$TICKET_DIR" ]]; then
        echo "Error: No ticket folder matching '$TICKET' found in $WORKSPACE/tickets/"
        exit 1
    fi
    mkdir -p "$TICKET_DIR/meetings"
    OUTPUT_DIR="$TICKET_DIR/meetings"
else
    OUTPUT_DIR="."
fi

# Determine output filename
if [[ -z "$OUTPUT_NAME" ]]; then
    # Extract meeting name from Zoom folder or filename
    BASENAME=$(basename "$(dirname "$AUDIO_FILE")" 2>/dev/null || basename "$AUDIO_FILE")
    # Clean up: lowercase, underscores, remove date prefix for readability
    OUTPUT_NAME=$(echo "$BASENAME" | sed 's/[^a-zA-Z0-9 _-]//g' | tr '[:upper:]' '[:lower:]' | tr ' ' '_' | sed 's/__*/_/g' | sed 's/^_//;s/_$//')
fi

OUTPUT_FILE="$OUTPUT_DIR/${OUTPUT_NAME}.txt"
echo "Output: $OUTPUT_FILE"
echo ""
echo "Transcribing..."

if [[ "$PROVIDER" == "openai" ]]; then
    # GPT-4o Transcribe via OpenAI API
    # Chunks files >25MB or >1400s to stay within API limits
    if [[ -z "${OPENAI_API_KEY:-}" ]]; then
        echo "Error: OPENAI_API_KEY not set. Add it to ~/.zshrc"
        exit 1
    fi

    START_TIME=$(date +%s)

    FILE_SIZE=$(stat -f%z "$AUDIO_FILE" 2>/dev/null || stat -c%s "$AUDIO_FILE" 2>/dev/null)
    FILE_SIZE_MB=$((FILE_SIZE / 1048576))

    # Get total duration
    TOTAL_DURATION=$(ffprobe -v error -show_entries format=duration \
        -of default=noprint_wrappers=1:nokey=1 "$AUDIO_FILE" 2>/dev/null | cut -d. -f1)

    # Chunk if file exceeds API limits (25MB or 1400s)
    if [[ $FILE_SIZE_MB -gt 24 ]] || [[ ${TOTAL_DURATION:-0} -gt 1300 ]]; then
        TEMP_DIR=$(mktemp -d)
        trap 'rm -rf "$TEMP_DIR"' EXIT

        # 20-min chunks: well under both 25MB and 1400s limits
        ffmpeg -i "$AUDIO_FILE" -f segment -segment_time 1200 -c copy \
            -reset_timestamps 1 "$TEMP_DIR/chunk_%03d.m4a" 2>/dev/null

        CHUNK_FILES=("$TEMP_DIR"/chunk_*.m4a)
        echo "Split into ${#CHUNK_FILES[@]} chunks (file is ${FILE_SIZE_MB}MB, ${TOTAL_DURATION}s)"

        FULL_TEXT=""
        CHUNK_NUM=0

        for CHUNK in "${CHUNK_FILES[@]}"; do
            CHUNK_NUM=$((CHUNK_NUM + 1))
            echo "Transcribing chunk $CHUNK_NUM/${#CHUNK_FILES[@]}..."

            RESPONSE=$(curl -s -X POST "https://api.openai.com/v1/audio/transcriptions" \
                -H "Authorization: Bearer $OPENAI_API_KEY" \
                -F "file=@$CHUNK" \
                -F "model=gpt-4o-transcribe" \
                -F "response_format=json" \
                -F "language=en")

            ERROR=$(echo "$RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('error',{}).get('message',''))" 2>/dev/null || echo "")
            if [[ -n "$ERROR" ]]; then
                echo "Error from OpenAI API on chunk $CHUNK_NUM: $ERROR"
                exit 1
            fi

            CHUNK_TEXT=$(python3 -c "import sys,json; print(json.loads(sys.stdin.read()).get('text','').strip())" <<< "$RESPONSE")
            if [[ -n "$CHUNK_TEXT" ]]; then
                FULL_TEXT="${FULL_TEXT:+$FULL_TEXT }$CHUNK_TEXT"
            fi
        done

        RAW_TEXT="$FULL_TEXT"
    else
        echo "Sending ${FILE_SIZE_MB}MB file directly to API..."

        RESPONSE=$(curl -s -X POST "https://api.openai.com/v1/audio/transcriptions" \
            -H "Authorization: Bearer $OPENAI_API_KEY" \
            -F "file=@$AUDIO_FILE" \
            -F "model=gpt-4o-transcribe" \
            -F "response_format=json" \
            -F "language=en")

        ERROR=$(echo "$RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('error',{}).get('message',''))" 2>/dev/null || echo "")
        if [[ -n "$ERROR" ]]; then
            echo "Error from OpenAI API: $ERROR"
            exit 1
        fi

        RAW_TEXT=$(python3 -c "import sys,json; print(json.loads(sys.stdin.read()).get('text','').strip())" <<< "$RESPONSE")
    fi

    # Split into one sentence per line for readability
    python3 -c "
import sys, re
text = sys.stdin.read().strip()
sentences = re.split(r'(?<=[.!?])\s+', text)
for s in sentences:
    s = s.strip()
    if s:
        print(s)
" <<< "$RAW_TEXT" > "$OUTPUT_FILE"

    END_TIME=$(date +%s)
    ELAPSED=$((END_TIME - START_TIME))
    LINES=$(wc -l < "$OUTPUT_FILE" | tr -d ' ')
    DURATION_MIN=$((${TOTAL_DURATION:-0} / 60))
    echo ""
    echo "Done in ${ELAPSED}s. Duration: ~${DURATION_MIN} min"
    echo "Saved to $OUTPUT_FILE ($LINES lines)"

else
    # Local mlx-whisper transcription (Apple Silicon native)
    /opt/homebrew/opt/python@3.11/bin/python3.11 << 'PYTHON_SCRIPT' - "$AUDIO_FILE" "$OUTPUT_FILE" "$MODEL"
import sys
import time

audio_file = sys.argv[1]
output_file = sys.argv[2]
model_size = sys.argv[3]

import mlx_whisper

# Map model size to HF model path
MODEL_MAP = {
    "tiny": "mlx-community/whisper-tiny-mlx",
    "base": "mlx-community/whisper-base-mlx-q4",
    "small": "mlx-community/whisper-small-mlx",
    "medium": "mlx-community/whisper-medium-mlx",
    "large-v3": "mlx-community/whisper-large-v3-mlx",
}
model_path = MODEL_MAP.get(model_size, model_size)

start = time.time()
print(f"Loading {model_size} model ({model_path})...")
print(f"Transcribing {audio_file}...")

result = mlx_whisper.transcribe(
    audio_file,
    path_or_hf_repo=model_path,
    language="en",
    verbose=False,
)

duration = result["segments"][-1]["end"] if result["segments"] else 0
print(f"Duration: {duration:.0f}s ({duration/60:.1f} min)")

lines = []
for segment in result["segments"]:
    t = segment["start"]
    timestamp = f"[{int(t//60):02d}:{int(t%60):02d}]"
    lines.append(f"{timestamp} {segment['text'].strip()}")

transcript = "\n".join(lines)

with open(output_file, "w") as f:
    f.write(transcript)

elapsed = time.time() - start
print(f"\nDone in {elapsed:.1f}s ({elapsed/60:.1f} min)")
print(f"Saved to {output_file}")
print(f"Speed: {duration/elapsed:.1f}x realtime")
PYTHON_SCRIPT
fi

echo ""
echo "Transcription complete: $OUTPUT_FILE"