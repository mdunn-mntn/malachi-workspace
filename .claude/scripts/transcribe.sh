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
# Default behavior: runs both OpenAI (whisper-1) and local mlx-whisper, then
# merges the best of both — OpenAI as accuracy backbone, local patched in where
# it captured significantly more speech. Hallucination detection prevents bad
# local segments from contaminating the merge.
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
    echo "  both    — Run both, merge best of each (openai accuracy + local coverage) (default)"
    echo "  openai  — whisper-1 via API only"
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
# Anti-hallucination settings:
#   temperature=0          — greedy decoding, no random sampling
#   condition_on_previous_text=False — prevents hallucination loops from self-reinforcing
#   no_speech_threshold=0.6 — aggressively suppresses silence hallucinations
#   compression_ratio_threshold=1.8 — tighter threshold to detect repetition early
# Long files (>20min) are chunked to match OpenAI's strategy for stability.
transcribe_local() {
    local out_file="$1"
    /opt/homebrew/opt/python@3.11/bin/python3.11 << PYTHON_SCRIPT - "$AUDIO_FILE" "$out_file" "$MODEL" "$TOTAL_DURATION"
import sys
import time
import os
import subprocess
import tempfile

audio_file = sys.argv[1]
output_file = sys.argv[2]
model_size = sys.argv[3]
total_duration = int(sys.argv[4]) if len(sys.argv) > 4 and sys.argv[4] else 0

import mlx_whisper

MODEL_MAP = {
    "tiny": "mlx-community/whisper-tiny-mlx",
    "base": "mlx-community/whisper-base-mlx-q4",
    "small": "mlx-community/whisper-small-mlx",
    "medium": "mlx-community/whisper-medium-mlx",
    "large-v3": "mlx-community/whisper-large-v3-mlx",
}
model_path = MODEL_MAP.get(model_size, model_size)

TRANSCRIBE_OPTS = dict(
    path_or_hf_repo=model_path,
    language="en",
    verbose=False,
    temperature=0,
    condition_on_previous_text=False,
    no_speech_threshold=0.6,
    compression_ratio_threshold=1.8,
)

CHUNK_SECONDS = 1200  # 20 minutes — matches OpenAI chunking

start = time.time()
print(f"[local] Loading {model_size} model ({model_path})...")

all_segments = []

if total_duration > CHUNK_SECONDS:
    # Chunk the audio to prevent long-range hallucination accumulation
    chunk_dir = tempfile.mkdtemp()
    subprocess.run([
        "ffmpeg", "-i", audio_file, "-f", "segment",
        "-segment_time", str(CHUNK_SECONDS), "-c", "copy",
        "-reset_timestamps", "1",
        os.path.join(chunk_dir, "chunk_%03d.m4a")
    ], capture_output=True)

    chunk_files = sorted(f for f in os.listdir(chunk_dir) if f.endswith(".m4a"))
    print(f"[local] Split into {len(chunk_files)} chunks ({total_duration}s)")

    for i, chunk_name in enumerate(chunk_files):
        chunk_path = os.path.join(chunk_dir, chunk_name)
        time_offset = i * CHUNK_SECONDS
        print(f"[local] Transcribing chunk {i+1}/{len(chunk_files)}...")

        result = mlx_whisper.transcribe(chunk_path, **TRANSCRIBE_OPTS)

        for seg in result.get("segments", []):
            seg["start"] += time_offset
            seg["end"] += time_offset
            all_segments.append(seg)

    # Cleanup
    import shutil
    shutil.rmtree(chunk_dir, ignore_errors=True)
else:
    print(f"[local] Transcribing {audio_file}...")
    result = mlx_whisper.transcribe(audio_file, **TRANSCRIBE_OPTS)
    all_segments = result.get("segments", [])

duration = all_segments[-1]["end"] if all_segments else 0

lines = []
for segment in all_segments:
    t = segment["start"]
    text = segment["text"].strip()
    if text:
        timestamp = f"[{int(t//60):02d}:{int(t%60):02d}]"
        lines.append(f"{timestamp} {text}")

with open(output_file, "w") as f:
    f.write("\n".join(lines) + "\n")

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

# ── Segment merger ────────────────────────────────────────────
# OpenAI backbone + local coverage patches.
# OpenAI wins on accuracy (proper nouns, punctuation, technical terms).
# Local wins on coverage (captures speech in gaps OpenAI missed).
# For each 15s window: use OpenAI unless local has >60% more substantive words
# AND local isn't hallucinating. This avoids word-level mixing artifacts
# (duplicate phrases at bucket boundaries) by switching whole segments.
merge_transcripts() {
    local openai_file="$1"
    local local_file="$2"
    local merged_file="$3"

    python3 << 'MERGESCRIPT' - "$openai_file" "$local_file" "$merged_file"
import sys, re
from collections import defaultdict, Counter

openai_file, local_file, merged_file = sys.argv[1], sys.argv[2], sys.argv[3]

filler = {'mm-hmm', 'mmhmm', 'mm', 'hmm', 'uh', 'um', 'uh-huh', 'yeah',
          'okay', 'ok', 'right', 'sure', 'yep', 'mhm', 'huh', 'ah', 'oh',
          'so', 'like'}

def parse_segments(filepath):
    segments = []
    with open(filepath) as f:
        for line in f:
            m = re.match(r'\[(\d{2}):(\d{2})\]\s*(.*)', line.rstrip())
            if m:
                t = int(m.group(1)) * 60 + int(m.group(2))
                segments.append((t, m.group(3)))
    return segments

def sub_count(text):
    return sum(1 for w in text.lower().split()
               if re.sub(r'[^\w]', '', w) not in filler and len(re.sub(r'[^\w]', '', w)) > 0)

def has_repetition(lines, threshold=3):
    """Detect hallucination loops (same text repeated 3+ times in a window)."""
    texts = [text.strip().lower() for _, text in lines if text.strip()]
    counts = Counter(texts)
    return any(c >= threshold for c in counts.values())

def bucket_segments(segments, bucket_size=15):
    buckets = defaultdict(list)
    for t, text in segments:
        key = (t // bucket_size) * bucket_size
        buckets[key].append((t, text))
    return buckets

openai_segs = parse_segments(openai_file)
local_segs = parse_segments(local_file)
openai_buckets = bucket_segments(openai_segs)
local_buckets = bucket_segments(local_segs)
all_keys = sorted(set(openai_buckets.keys()) | set(local_buckets.keys()))

merged = []
stats = {"openai": 0, "local_coverage": 0, "openai_only": 0, "local_only": 0, "local_halluc": 0}

for key in all_keys:
    o = openai_buckets.get(key, [])
    l = local_buckets.get(key, [])

    if not o and not l:
        continue
    if not o:
        if not has_repetition(l):
            for t, text in l:
                merged.append((t, text))
        stats["local_only"] += 1
        continue
    if not l:
        for t, text in o:
            merged.append((t, text))
        stats["openai_only"] += 1
        continue

    # Both have content
    o_sub = sum(sub_count(text) for _, text in o)
    l_sub = sum(sub_count(text) for _, text in l)

    # Check local for hallucination in this window
    if has_repetition(l):
        for t, text in o:
            merged.append((t, text))
        stats["local_halluc"] += 1
        stats["openai"] += 1
        continue

    # Local wins only if it has >60% more substantive words (real coverage gap)
    if l_sub > 0 and o_sub > 0 and o_sub / l_sub < 0.6:
        for t, text in l:
            merged.append((t, text))
        stats["local_coverage"] += 1
    else:
        for t, text in o:
            merged.append((t, text))
        stats["openai"] += 1

# Sort by timestamp and write
merged.sort(key=lambda x: x[0])
lines = []
for t, text in merged:
    text = text.strip()
    if text:
        lines.append(f"[{int(t//60):02d}:{int(t%60):02d}] {text}")

with open(merged_file, "w") as f:
    f.write("\n".join(lines) + "\n")

total_words = sum(len(text.split()) for _, text in merged)
total_buckets = stats["openai"] + stats["local_coverage"] + stats["openai_only"] + stats["local_only"]
print(f"[merge] {len(lines)} lines, {total_words} words from {total_buckets} time windows (15s each)")
print(f"[merge] openai: {stats['openai']}x (accuracy), local: {stats['local_coverage']}x (coverage gap)")
if stats["openai_only"] or stats["local_only"]:
    print(f"[merge] exclusive: openai-only {stats['openai_only']}x, local-only {stats['local_only']}x")
if stats["local_halluc"]:
    print(f"[merge] local hallucination blocked: {stats['local_halluc']}x (used openai instead)")
MERGESCRIPT
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

    # Merge the best segments from both providers
    if $OPENAI_OK && $LOCAL_OK; then
        OPENAI_SCORE=$(score_repetition "$OPENAI_FILE")
        LOCAL_SCORE=$(score_repetition "$LOCAL_FILE")
        OPENAI_LINES=$(wc -l < "$OPENAI_FILE" | tr -d ' ')
        LOCAL_LINES=$(wc -l < "$LOCAL_FILE" | tr -d ' ')
        OPENAI_WORDS=$(wc -w < "$OPENAI_FILE" | tr -d ' ')
        LOCAL_WORDS=$(wc -w < "$LOCAL_FILE" | tr -d ' ')
        echo "Provider stats — openai: $OPENAI_SCORE rep ($OPENAI_LINES lines, $OPENAI_WORDS words), local: $LOCAL_SCORE rep ($LOCAL_LINES lines, $LOCAL_WORDS words)"

        # Check if one provider has bad repetition — if so, don't merge, just use the clean one
        OPENAI_REP_BAD=$(python3 -c "print('yes' if $OPENAI_SCORE > 0.3 else 'no')")
        LOCAL_REP_BAD=$(python3 -c "print('yes' if $LOCAL_SCORE > 0.3 else 'no')")

        if [[ "$OPENAI_REP_BAD" == "yes" && "$LOCAL_REP_BAD" == "yes" ]]; then
            # Both have repetition — pick lesser evil, don't merge garbage
            if python3 -c "exit(0 if $OPENAI_SCORE <= $LOCAL_SCORE else 1)"; then
                echo "Both have repetition — using openai (less: $OPENAI_SCORE vs $LOCAL_SCORE)"
                cp "$OPENAI_FILE" "$OUTPUT_FILE"
            else
                echo "Both have repetition — using local (less: $LOCAL_SCORE vs $OPENAI_SCORE)"
                cp "$LOCAL_FILE" "$OUTPUT_FILE"
            fi
        elif [[ "$OPENAI_REP_BAD" == "yes" ]]; then
            echo "OpenAI has repetition ($OPENAI_SCORE) — using local only"
            cp "$LOCAL_FILE" "$OUTPUT_FILE"
        elif [[ "$LOCAL_REP_BAD" == "yes" ]]; then
            echo "Local has repetition ($LOCAL_SCORE) — using openai only"
            cp "$OPENAI_FILE" "$OUTPUT_FILE"
        else
            # Both clean — merge best segments from each
            echo ""
            merge_transcripts "$OPENAI_FILE" "$LOCAL_FILE" "$OUTPUT_FILE"
        fi

        # Save individual provider outputs if requested
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