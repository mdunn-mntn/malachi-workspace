"""LLM-based knowledge extraction from daily Slack message dumps."""

import json
import logging
import os
from pathlib import Path

import anthropic
import yaml

from utils.doc_parser import get_all_doc_summaries

logger = logging.getLogger(__name__)

WORKSPACE = Path(__file__).resolve().parent.parent
CONFIG_PATH = WORKSPACE / "slack_bot" / "config.yaml"


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


EXTRACTION_PROMPT = """\
You are a knowledge extraction system for MNTN, an adtech/CTV company. You are analyzing \
Slack messages to extract valuable institutional knowledge that should be documented.

## Your Task

Analyze the Slack messages below and extract ONLY high-value institutional knowledge. \
Each extracted item should be a clean, well-written documentation entry — NOT a Slack quote.

## What to Extract

- **data_catalog**: Table schemas, column meanings, join keys, partitions, data quality issues, \
SQL tips, performance gotchas, new tables discovered
- **data_knowledge**: Business logic explanations, why things work certain ways, edge cases, \
architecture patterns, pipeline behavior, how fields are used
- **mntn_business**: Org changes, product updates, strategy shifts, team responsibilities, \
terminology, industry context
- **experimentation**: Experiment results, methodology discussions, statistical approaches, \
test design lessons
- **strategic**: Priority shifts, new initiatives, leadership direction changes, OKR updates

## What to IGNORE

- Personal conversations, greetings, social chat, emoji reactions
- Ticket-specific discussions only relevant to that ticket
- Meeting scheduling, time-off notices, HR/admin stuff
- Questions without answers (no knowledge to extract)
- Bot join/leave messages
- Messages that are just acknowledgements ("ok", "thanks", "got it")
- Vague or speculative statements without concrete information

## Existing Knowledge (for deduplication)

Below are summaries of what's already documented. Do NOT extract items that duplicate existing knowledge.

{doc_summaries}

## Output Format

Return a JSON array. Each item:
```json
{{
  "category": "data_catalog|data_knowledge|mntn_business|experimentation|strategic",
  "content": "Clean documentation entry. Write as if adding to a reference doc — no Slack tone.",
  "confidence": "high|medium",
  "source_channel": "#channel-name",
  "source_date": "YYYY-MM-DD",
  "existing_section": "Section header in the target doc to append under, or null if new"
}}
```

If there is NO extractable knowledge, return an empty array: `[]`

Only include items with "high" or "medium" confidence. When in doubt, leave it out.

## Messages

{messages}
"""


def format_messages_for_prompt(raw_data: dict) -> str:
    """Format raw scraped messages into a readable block for the LLM."""
    parts = []
    for channel_data in raw_data.get("channels", []):
        ch_name = channel_data["channel"]
        messages = channel_data.get("messages", [])
        if not messages:
            continue

        parts.append(f"\n### #{ch_name}\n")
        for msg in messages:
            user = msg.get("user", "unknown")
            text = msg.get("text", "").strip()
            if not text:
                continue
            parts.append(f"**{user}**: {text}")

            for reply in msg.get("replies", []):
                r_user = reply.get("user", "unknown")
                r_text = reply.get("text", "").strip()
                if r_text:
                    parts.append(f"  ↳ **{r_user}**: {r_text}")

    return "\n".join(parts)


def estimate_tokens(text: str) -> int:
    """Rough token estimate (1 token ≈ 4 chars for English text)."""
    return len(text) // 4


def extract(raw_path: Path) -> list[dict]:
    """Extract knowledge from a daily message dump.

    Returns list of extraction items and saves to _extracted.json alongside the raw file.
    """
    config = load_config()
    extraction_config = config.get("extraction", {})
    model = extraction_config.get("model", "claude-sonnet-4-6-20250514")
    max_tokens = extraction_config.get("max_tokens_per_batch", 100000)

    with open(raw_path) as f:
        raw_data = json.load(f)

    messages_text = format_messages_for_prompt(raw_data)
    if not messages_text.strip():
        logger.info("No messages to extract from")
        return []

    # Build doc summaries for dedup
    doc_summaries = get_all_doc_summaries(max_chars_per_doc=2000)
    doc_summary_text = "\n\n".join(
        f"### {cat}\n{summary}" for cat, summary in doc_summaries.items()
    )

    prompt = EXTRACTION_PROMPT.format(
        doc_summaries=doc_summary_text, messages=messages_text
    )

    prompt_tokens = estimate_tokens(prompt)
    logger.info("Prompt is ~%d tokens (model: %s)", prompt_tokens, model)

    # Split into batches if too large
    if prompt_tokens > max_tokens:
        logger.warning(
            "Prompt exceeds %d tokens — splitting by channel", max_tokens
        )
        return _extract_by_channel(raw_data, doc_summary_text, model, max_tokens)

    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    response = client.messages.create(
        model=model,
        max_tokens=16384,
        messages=[{"role": "user", "content": prompt}],
    )

    # Parse the response
    response_text = response.content[0].text
    items = _parse_extraction_response(response_text)
    logger.info("Extracted %d items", len(items))

    # Save extraction results
    out_path = raw_path.with_name(raw_path.stem + "_extracted.json")
    with open(out_path, "w") as f:
        json.dump(items, f, indent=2, ensure_ascii=False)
    logger.info("Saved extractions → %s", out_path)

    return items


def _extract_by_channel(
    raw_data: dict, doc_summary_text: str, model: str, max_tokens: int
) -> list[dict]:
    """Split extraction by channel when the full prompt is too large."""
    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    all_items = []

    for channel_data in raw_data.get("channels", []):
        single_channel_data = {
            "channels": [channel_data],
            "date": raw_data["date"],
        }
        messages_text = format_messages_for_prompt(single_channel_data)
        if not messages_text.strip():
            continue

        prompt = EXTRACTION_PROMPT.format(
            doc_summaries=doc_summary_text, messages=messages_text
        )

        if estimate_tokens(prompt) > max_tokens:
            logger.warning(
                "Channel #%s alone exceeds token limit — skipping",
                channel_data["channel"],
            )
            continue

        response = client.messages.create(
            model=model,
            max_tokens=16384,
            messages=[{"role": "user", "content": prompt}],
        )
        items = _parse_extraction_response(response.content[0].text)
        all_items.extend(items)
        logger.info(
            "  #%s: %d items extracted", channel_data["channel"], len(items)
        )

    return all_items


def _parse_extraction_response(text: str) -> list[dict]:
    """Parse the LLM response into a list of extraction items."""
    text = text.strip()

    # Handle markdown code blocks
    if "```json" in text:
        start = text.index("```json") + 7
        # Find closing ``` — may not exist if truncated
        try:
            end = text.index("```", start)
            text = text[start:end].strip()
        except ValueError:
            text = text[start:].strip()
    elif text.startswith("```"):
        text = text[3:].strip()
        if text.endswith("```"):
            text = text[:-3].strip()

    # Try direct parse first
    try:
        items = json.loads(text)
    except json.JSONDecodeError:
        # Truncated JSON: try to salvage by closing the array
        # Find the last complete object (ends with })
        last_brace = text.rfind("}")
        if last_brace > 0:
            truncated = text[: last_brace + 1]
            if not truncated.rstrip().endswith("]"):
                truncated = truncated.rstrip().rstrip(",") + "\n]"
            try:
                items = json.loads(truncated)
            except json.JSONDecodeError:
                logger.error("Failed to parse extraction response as JSON (even after truncation fix)")
                logger.debug("Response text (first 500): %s", text[:500])
                return []
        else:
            logger.error("Failed to parse extraction response as JSON")
            logger.debug("Response text (first 500): %s", text[:500])
            return []

    if isinstance(items, list):
        valid = []
        for item in items:
            if all(k in item for k in ("category", "content", "confidence")):
                if item["confidence"] in ("high", "medium"):
                    valid.append(item)
        return valid
    return []


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(description="Extract knowledge from Slack dump")
    parser.add_argument("raw_file", type=Path, help="Path to raw JSON dump")
    args = parser.parse_args()

    items = extract(args.raw_file)
    print(f"Extracted {len(items)} items")
    for item in items:
        print(f"  [{item['confidence']}] {item['category']}: {item['content'][:80]}...")
