"""Apply extracted knowledge items to the appropriate knowledge docs."""

import logging
import re
from datetime import date
from pathlib import Path

from utils.doc_parser import get_doc_path

logger = logging.getLogger(__name__)

WORKSPACE = Path(__file__).resolve().parent.parent
REVIEW_QUEUE = WORKSPACE / "knowledge" / "slack_review_queue.md"


def update_docs(items: list[dict], date_label: str | None = None) -> dict:
    """Apply extraction items to knowledge docs.

    Returns a summary: {updated_files: [...], review_queue_count: N, skipped: N}
    """
    date_label = date_label or date.today().isoformat()
    marker = f"<!-- slack-extracted: {date_label} -->"

    updated_files = set()
    skipped = 0
    unknown_category = 0

    # Group items by target file
    by_file: dict[str, list[dict]] = {}
    for item in items:
        category = item.get("category", "")
        doc_path = get_doc_path(category)
        if not doc_path:
            logger.warning("Unknown category '%s' — skipping item", category)
            unknown_category += 1
            continue
        key = str(doc_path)
        by_file.setdefault(key, []).append(item)

    for file_path_str, file_items in by_file.items():
        file_path = Path(file_path_str)
        if not file_path.exists():
            logger.warning("Target file does not exist: %s — skipping", file_path)
            skipped += len(file_items)
            continue

        content = file_path.read_text()

        # Check for idempotency — skip items already added today
        if marker in content:
            logger.info("Already updated %s for %s — skipping", file_path.name, date_label)
            skipped += len(file_items)
            continue

        # Apply all items directly — no review queue gating
        append_block = _build_append_block(file_items, marker)

        # Try to insert under existing section if specified
        inserted = False
        for item in file_items:
            section = item.get("existing_section")
            if section:
                updated_content = _insert_under_section(content, section, append_block)
                if updated_content != content:
                    content = updated_content
                    inserted = True
                    break

        if not inserted:
            # Append to end of file
            content = content.rstrip() + "\n\n" + append_block + "\n"

        file_path.write_text(content)
        updated_files.add(file_path.name)
        logger.info("Updated %s with %d items", file_path.name, len(file_items))

    return {
        "updated_files": sorted(updated_files),
        "skipped": skipped,
        "items_applied": len(items) - skipped - unknown_category,
    }


def _build_append_block(items: list[dict], marker: str) -> str:
    """Build a formatted block of knowledge entries."""
    lines = [marker]
    for item in items:
        content = item["content"].strip()
        source = item.get("source_channel", "slack")
        lines.append(f"- {content}")
    return "\n".join(lines)


def _insert_under_section(content: str, section_header: str, block: str) -> str:
    """Try to insert a block under a specific markdown section header."""
    # Find the section header
    pattern = re.compile(
        rf"^(#{1,4}\s+.*{re.escape(section_header)}.*$)", re.MULTILINE | re.IGNORECASE
    )
    match = pattern.search(content)
    if not match:
        return content  # Section not found — caller will append to end

    # Find the next section header (same or higher level) to know where this section ends
    header_end = match.end()
    header_level = len(match.group(1).split()[0])  # Count #'s
    next_section = re.search(
        rf"^#{{{1},{header_level}}}\s", content[header_end:], re.MULTILINE
    )

    if next_section:
        insert_pos = header_end + next_section.start()
    else:
        insert_pos = len(content)

    # Insert before the next section (with spacing)
    return content[:insert_pos].rstrip() + "\n\n" + block + "\n\n" + content[insert_pos:]


def _might_contradict(new_content: str, existing_content: str) -> bool:
    """Simple heuristic to detect potential contradictions.

    Flags items that reference the same table/column but with different assertions.
    Conservative — only flags obvious cases.
    """
    # Extract table-like references from new content (e.g., "table_name.column_name")
    table_refs = re.findall(r"\b(\w+\.\w+)\b", new_content)
    if not table_refs:
        return False

    # Check if any of these references appear in existing content with negating language
    negators = ["not", "never", "don't", "doesn't", "no longer", "deprecated", "removed"]
    for ref in table_refs:
        if ref not in existing_content:
            continue
        # Check surrounding context in new content for contradiction signals
        new_lower = new_content.lower()
        for neg in negators:
            if neg in new_lower and ref.lower() in new_lower:
                return True
    return False


def _append_review_queue(items: list[dict], date_label: str):
    """Append items to the review queue file."""
    REVIEW_QUEUE.parent.mkdir(parents=True, exist_ok=True)

    if not REVIEW_QUEUE.exists():
        header = "# Slack Knowledge Review Queue\n\nItems below need human review before being added to knowledge docs.\n\n---\n"
        REVIEW_QUEUE.write_text(header)

    lines = [f"\n## {date_label}\n"]
    for item in items:
        reason = item.get("review_reason", "Needs review")
        category = item.get("category", "unknown")
        source = item.get("source_channel", "slack")
        content = item["content"].strip()
        lines.append(f"### [{category}] from {source}")
        lines.append(f"**Reason:** {reason}")
        lines.append(f"**Confidence:** {item.get('confidence', 'unknown')}")
        lines.append(f"\n{content}\n")

    with open(REVIEW_QUEUE, "a") as f:
        f.write("\n".join(lines))

    logger.info("Added %d items to review queue", len(items))


if __name__ == "__main__":
    import argparse
    import json

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(description="Update knowledge docs from extractions")
    parser.add_argument("extracted_file", type=Path, help="Path to extracted JSON")
    parser.add_argument("--date", help="Date label for marker (YYYY-MM-DD)")
    args = parser.parse_args()

    with open(args.extracted_file) as f:
        items = json.load(f)

    result = update_docs(items, date_label=args.date)
    print(f"Result: {result}")
