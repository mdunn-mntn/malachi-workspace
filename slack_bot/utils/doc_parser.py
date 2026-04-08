"""Parse existing knowledge docs to build context summaries for deduplication."""

import re
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent.parent.parent

# Map category names to knowledge doc paths
CATEGORY_TO_FILE = {
    "data_catalog": WORKSPACE / "knowledge" / "data_catalog.md",
    "data_knowledge": WORKSPACE / "knowledge" / "data_knowledge.md",
    "mntn_business": WORKSPACE / "knowledge" / "mntn_business.md",
    "experimentation": WORKSPACE / "knowledge" / "experimentation.md",
    "strategic": WORKSPACE / "knowledge" / "strategic_north_star.md",
}


def get_doc_summary(path: Path, max_chars: int = 3000) -> str:
    """Extract section headers and first lines to summarize a doc's contents.

    Returns a condensed summary suitable for inclusion in an LLM prompt
    to help it avoid duplicating existing knowledge.
    """
    if not path.exists():
        return "(file not found)"

    text = path.read_text()
    lines = text.split("\n")

    summary_parts = []
    for line in lines:
        # Capture all headers
        if re.match(r"^#{1,4}\s", line):
            summary_parts.append(line.strip())
        # Capture bullet points that look like key facts (gotchas, notes, etc.)
        elif re.match(r"^[-*]\s\*\*", line):
            # Bold-prefixed bullets are usually key entries
            truncated = line[:150] + "..." if len(line) > 150 else line
            summary_parts.append(truncated)

    summary = "\n".join(summary_parts)
    if len(summary) > max_chars:
        summary = summary[:max_chars] + "\n... (truncated)"
    return summary


def get_all_doc_summaries(max_chars_per_doc: int = 3000) -> dict[str, str]:
    """Get summaries of all knowledge docs for dedup context."""
    return {
        category: get_doc_summary(path, max_chars_per_doc)
        for category, path in CATEGORY_TO_FILE.items()
    }


def get_doc_path(category: str) -> Path | None:
    """Get the file path for a knowledge category."""
    return CATEGORY_TO_FILE.get(category)
