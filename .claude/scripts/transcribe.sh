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
#   --keep-both           When using both providers, save each as _openai.txt / _local.txt
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
KEEP_BOTH=false

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
        --keep-both)
            KEEP_BOTH=true
            shift
            ;;
        *)
            POSITIONAL="$1"
            shift
            ;;
    esac
done

if [[ -z "$POSITIONAL" ]]; then
    echo "Usage: bash transcribe.sh <zoom-folder-name|audio-file> [--ticket ti_xxx] [--provider both|openai|local]"
    echo ""
    echo "Providers:"
    echo "  both    — Run both, pick the one with less repetition (default)"
    echo "  openai  — GPT-4o Transcribe via API only (\$0.36/hr)"
    echo "  local   — mlx-whisper large-v3 on Apple Silicon only (free)"
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
    BASENAME=$(basename "$(dirname "$AUDIO_FILE")" 2>/dev/null || basename "$AUDIO_FILE")
    OUTPUT_NAME=$(echo "$BASENAME" | sed 's/[^a-zA-Z0-9 _-]//g' | tr '[:upper:]' '[:lower:]' | tr ' ' '_' | sed 's/__*/_/g' | sed 's/^_//;s/_$//')
fi

OUTPUT_FILE="$OUTPUT_DIR/${OUTPUT_NAME}.txt"
echo "Output: $OUTPUT_FILE"

# Get audio metadata once
FILE_SIZE=$(stat -f%z "$AUDIO_FILE" 2>/dev/null || stat -c%s "$AUDIO_FILE" 2>/dev/null)
FILE_SIZE_MB=$((FILE_SIZE / 1048576))
TOTAL_DURATION=$(ffprobe -v error -show_entries format=duration \
    -of default=noprint_wrappers=1:nokey=1 "$AUDIO_FILE" 2>/dev/null | cut -d. -f1)
DURATION_MIN=$((${TOTAL_DURATION:-0} / 60))

# Temp dir for candidate transcripts
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

# ── OpenAI transcription ──────────────────────────────────────
# Uses whisper-1 with verbose_json for segment-level timestamps.
# gpt-4o-transcribe doesn't support verbose_json, and its plain json mode
# merges speech into a single blob that loses conversational detail.
# Filler-word prompt hints preserve verbatim speech.
transcribe_openai() {
    local out_file="$1"
    if [[ -z "${OPENAI_API_KEY:-}" ]]; then
        # Try sourcing zshrc as fallback
        source ~/.zshrc 2>/dev/null || true
    fi
    if [[ -z "${OPENAI_API_KEY:-}" ]]; then
        echo "[openai] ERROR: OPENAI_API_KEY not set (checked env and ~/.zshrc)"
        return 1
    fi

    local start_time=$(date +%s)
    local filler_prompt="Um, uh, like, you know, right, okay, yeah, so, I mean, kind of, sort of."

    # Helper: call the API for one file, append segments JSON to $segments_file
    _openai_transcribe_file() {
        local input_file="$1"
        local segments_file="$2"
        local time_offset="$3"  # seconds to add to timestamps (for chunks)

        local response=$(curl -s -X POST "https://api.openai.com/v1/audio/transcriptions" \
            -H "Authorization: Bearer $OPENAI_API_KEY" \
            -F "file=@$input_file" \
            -F "model=whisper-1" \
            -F "response_format=verbose_json" \
            -F "timestamp_granularities[]=segment" \
            -F "language=en" \
            -F "prompt=$filler_prompt")

        local error=$(echo "$response" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('error',{}).get('message',''))" 2>/dev/null || echo "")
        if [[ -n "$error" ]]; then
            echo "$error"
            return 1
        fi

        # Extract segments and apply time offset
        python3 -c "
import sys, json
data = json.loads(sys.stdin.read())
offset = float(sys.argv[1])
segments = data.get('segments', [])
for seg in segments:
    seg['start'] = seg.get('start', 0) + offset
    seg['end'] = seg.get('end', 0) + offset
print(json.dumps(segments))
" "$time_offset" <<< "$response" >> "$segments_file"
    }

    local segments_file="$WORK_DIR/openai_segments.jsonl"
    > "$segments_file"

    if [[ $FILE_SIZE_MB -gt 24 ]] || [[ ${TOTAL_DURATION:-0} -gt 1300 ]]; then
        local chunk_dir="$WORK_DIR/chunks"
        mkdir -p "$chunk_dir"
        ffmpeg -i "$AUDIO_FILE" -f segment -segment_time 1200 -c copy \
            -reset_timestamps 1 "$chunk_dir/chunk_%03d.m4a" 2>/dev/null

        local chunk_files=("$chunk_dir"/chunk_*.m4a)
        echo "[openai] Split into ${#chunk_files[@]} chunks (${FILE_SIZE_MB}MB, ${TOTAL_DURATION}s)"

        local chunk_num=0
        for chunk in "${chunk_files[@]}"; do
            chunk_num=$((chunk_num + 1))
            local chunk_offset=$(( (chunk_num - 1) * 1200 ))
            echo "[openai] Transcribing chunk $chunk_num/${#chunk_files[@]}..."
            local err=$(_openai_transcribe_file "$chunk" "$segments_file" "$chunk_offset")
            if [[ $? -ne 0 ]]; then
                echo "[openai] API error on chunk $chunk_num: $err"
                return 1
            fi
        done
    else
        echo "[openai] Sending ${FILE_SIZE_MB}MB file to API..."
        local err=$(_openai_transcribe_file "$AUDIO_FILE" "$segments_file" "0")
        if [[ $? -ne 0 ]]; then
            echo "[openai] API error: $err"
            return 1
        fi
    fi

    # Convert segments to timestamped lines (same format as local)
    python3 -c "
import sys, json

segments = []
for line in open(sys.argv[1]):
    line = line.strip()
    if line:
        segments.extend(json.loads(line))

lines = []
for seg in segments:
    t = seg.get('start', 0)
    text = seg.get('text', '').strip()
    if text:
        timestamp = f'[{int(t//60):02d}:{int(t%60):02d}]'
        lines.append(f'{timestamp} {text}')

with open(sys.argv[2], 'w') as f:
    f.write('\n'.join(lines) + '\n')

print(len(lines))
" "$segments_file" "$out_file"

    local end_time=$(date +%s)
    local elapsed=$((end_time - start_time))
    local lines=$(wc -l < "$out_file" | tr -d ' ')
    echo "[openai] Done in ${elapsed}s — $lines lines"
}

# ── Local mlx-whisper transcription ───────────────────────────
transcribe_local() {
    local out_file="$1"
    /opt/homebrew/opt/python@3.11/bin/python3.11 << PYTHON_SCRIPT - "$AUDIO_FILE" "$out_file" "$MODEL"
import sys
import time

audio_file = sys.argv[1]
output_file = sys.argv[2]
model_size = sys.argv[3]

import mlx_whisper

MODEL_MAP = {
    "tiny": "mlx-community/whisper-tiny-mlx",
    "base": "mlx-community/whisper-base-mlx-q4",
    "small": "mlx-community/whisper-small-mlx",
    "medium": "mlx-community/whisper-medium-mlx",
    "large-v3": "mlx-community/whisper-large-v3-mlx",
}
model_path = MODEL_MAP.get(model_size, model_size)

start = time.time()
print(f"[local] Loading {model_size} model ({model_path})...")
print(f"[local] Transcribing {audio_file}...")

result = mlx_whisper.transcribe(
    audio_file,
    path_or_hf_repo=model_path,
    language="en",
    verbose=False,
)

duration = result["segments"][-1]["end"] if result["segments"] else 0

lines = []
for segment in result["segments"]:
    t = segment["start"]
    timestamp = f"[{int(t//60):02d}:{int(t%60):02d}]"
    lines.append(f"{timestamp} {segment['text'].strip()}")

with open(output_file, "w") as f:
    f.write("\n".join(lines))

elapsed = time.time() - start
print(f"[local] Done in {elapsed:.1f}s — {len(lines)} lines ({duration/elapsed:.1f}x realtime)")
PYTHON_SCRIPT
}

# ── Repetition scorer ─────────────────────────────────────────
# Returns a repetition score (0 = clean, higher = worse).
# Detects repeated multi-line blocks — the hallucination pattern.
score_repetition() {
    local file="$1"
    python3 << 'PYSCRIPT' - "$file"
import sys

file_path = sys.argv[1]
with open(file_path) as f:
    lines = [l.rstrip() for l in f if l.strip()]

if len(lines) < 5:
    print("0.0")
    sys.exit(0)

# Strip timestamps for comparison (local has [MM:SS] prefix)
import re
clean = [re.sub(r'^\[\d{2}:\d{2}\]\s*', '', l) for l in lines]

# Check for repeated N-line blocks (N=3..10)
total_repeated = 0
for block_size in range(3, min(11, len(clean) // 2 + 1)):
    seen = {}
    for i in range(len(clean) - block_size + 1):
        block = tuple(clean[i:i+block_size])
        if block in seen:
            total_repeated += block_size
        else:
            seen[block] = i

# Score = fraction of lines that are in repeated blocks
score = total_repeated / len(clean) if clean else 0.0
print(f"{score:.4f}")
PYSCRIPT
}

# ── Output validation ────────────────────────────────────────
# Minimum 10 lines per 10 minutes of audio. Catches empty/near-empty outputs.
validate_output() {
    local file="$1"
    local provider="$2"
    if [[ ! -f "$file" ]]; then
        echo "[validate] $provider: output file missing"
        return 1
    fi
    local lines=$(wc -l < "$file" | tr -d ' ')
    local min_lines=$(( DURATION_MIN > 0 ? DURATION_MIN : 1 ))  # at least 1 line per minute
    if [[ $lines -lt $min_lines ]]; then
        echo "[validate] $provider: only $lines lines for ~${DURATION_MIN} min audio (expected >=$min_lines) — treating as failed"
        return 1
    fi
    return 0
}

# ── Run transcription(s) ─────────────────────────────────────
echo ""

OPENAI_FILE="$WORK_DIR/openai.txt"
LOCAL_FILE="$WORK_DIR/local.txt"
OPENAI_OK=false
LOCAL_OK=false

if [[ "$PROVIDER" == "openai" ]]; then
    echo "Transcribing with OpenAI only..."
    if transcribe_openai "$OPENAI_FILE" && validate_output "$OPENAI_FILE" "openai"; then
        OPENAI_OK=true
        cp "$OPENAI_FILE" "$OUTPUT_FILE"
    else
        echo "OpenAI transcription failed"
        exit 1
    fi
elif [[ "$PROVIDER" == "local" ]]; then
    echo "Transcribing with local mlx-whisper only..."
    if transcribe_local "$LOCAL_FILE" && validate_output "$LOCAL_FILE" "local"; then
        LOCAL_OK=true
        cp "$LOCAL_FILE" "$OUTPUT_FILE"
    else
        echo "Local transcription failed"
        exit 1
    fi
else
    # Both providers
    echo "Transcribing with both providers..."
    echo ""

    # Run OpenAI (fast, ~30s via API)
    if transcribe_openai "$OPENAI_FILE" 2>&1 && validate_output "$OPENAI_FILE" "openai"; then
        OPENAI_OK=true
    fi
    echo ""

    # Run local (fast on Apple Silicon, ~30s for short files)
    if transcribe_local "$LOCAL_FILE" 2>&1 && validate_output "$LOCAL_FILE" "local"; then
        LOCAL_OK=true
    fi
    echo ""

    # Pick the better one
    if $OPENAI_OK && $LOCAL_OK; then
        OPENAI_SCORE=$(score_repetition "$OPENAI_FILE")
        LOCAL_SCORE=$(score_repetition "$LOCAL_FILE")
        OPENAI_LINES=$(wc -l < "$OPENAI_FILE" | tr -d ' ')
        LOCAL_LINES=$(wc -l < "$LOCAL_FILE" | tr -d ' ')
        OPENAI_WORDS=$(wc -w < "$OPENAI_FILE" | tr -d ' ')
        LOCAL_WORDS=$(wc -w < "$LOCAL_FILE" | tr -d ' ')
        echo "Repetition scores — openai: $OPENAI_SCORE ($OPENAI_LINES lines, $OPENAI_WORDS words), local: $LOCAL_SCORE ($LOCAL_LINES lines, $LOCAL_WORDS words)"

        # Pick the one with lower repetition. Tie goes to openai (generally higher accuracy).
        WINNER=$(python3 -c "
o, l = $OPENAI_SCORE, $LOCAL_SCORE
print('openai' if o <= l else 'local')
")
        echo "Winner: $WINNER"
        if [[ "$WINNER" == "openai" ]]; then
            cp "$OPENAI_FILE" "$OUTPUT_FILE"
        else
            cp "$LOCAL_FILE" "$OUTPUT_FILE"
        fi

        # Save both if requested
        if $KEEP_BOTH; then
            OPENAI_KEPT="${OUTPUT_FILE%.txt}_openai.txt"
            LOCAL_KEPT="${OUTPUT_FILE%.txt}_local.txt"
            cp "$OPENAI_FILE" "$OPENAI_KEPT"
            cp "$LOCAL_FILE" "$LOCAL_KEPT"
            echo "Kept both: $(basename "$OPENAI_KEPT"), $(basename "$LOCAL_KEPT")"
        fi
    elif $OPENAI_OK; then
        echo "Only OpenAI succeeded — using it"
        cp "$OPENAI_FILE" "$OUTPUT_FILE"
    elif $LOCAL_OK; then
        echo "Only local succeeded — using it"
        cp "$LOCAL_FILE" "$OUTPUT_FILE"
    else
        echo "Both providers failed"
        exit 1
    fi
fi

LINES=$(wc -l < "$OUTPUT_FILE" | tr -d ' ')
WORDS=$(wc -w < "$OUTPUT_FILE" | tr -d ' ')
echo ""
echo "Saved to $OUTPUT_FILE ($LINES lines, $WORDS words, ~${DURATION_MIN} min audio)"
echo "Transcription complete: $OUTPUT_FILE"