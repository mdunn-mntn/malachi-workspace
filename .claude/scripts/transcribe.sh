#!/bin/bash
# transcribe.sh — Transcribe Zoom meeting recordings using faster-whisper (local)
#
# Usage:
#   bash .claude/scripts/transcribe.sh "2026-03-30 11.33.01 Discuss Experiment with Causal Impact Meeting"
#   bash .claude/scripts/transcribe.sh "2026-03-30 11.33.01 Discuss Experiment with Causal Impact Meeting" --ticket ti_504
#   bash .claude/scripts/transcribe.sh /path/to/audio.m4a
#   bash .claude/scripts/transcribe.sh /path/to/audio.m4a --ticket ti_504
#
# Options:
#   --ticket TI_XXX   Save output to tickets/TI_XXX/meetings/ instead of current directory
#   --model MODEL     Whisper model size (default: large-v3). Options: tiny, base, small, medium, large-v3
#   --output FILE     Custom output filename (without extension)
#
# Output: .txt transcript with timestamps and speaker segments

set -euo pipefail

ZOOM_DIR="$HOME/Documents/Zoom"
WORKSPACE="/Users/malachi/Developer/work/mntn/workspace"
MODEL="large-v3"
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
    echo "Usage: bash transcribe.sh <zoom-folder-name|audio-file> [--ticket ti_xxx] [--model large-v3]"
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
echo "Model: $MODEL"

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

# Run transcription using mlx-whisper (Apple Silicon native)
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

echo ""
echo "Transcription complete: $OUTPUT_FILE"
