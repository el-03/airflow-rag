from __future__ import annotations

import html
import re
from typing import Any


BLOCK_TAG_PATTERN = re.compile(r"</?(?:p|div|br|li|ul|ol|h[1-6]|tr|table|blockquote)[^>]*>", re.IGNORECASE)
TAG_PATTERN = re.compile(r"<[^>]+>")
WHITESPACE_PATTERN = re.compile(r"\s+")


def clean_content_text(content: str | None) -> str:
    if not content:
        return ""

    cleaned = BLOCK_TAG_PATTERN.sub("\n", content)
    cleaned = TAG_PATTERN.sub(" ", cleaned)
    cleaned = html.unescape(cleaned)
    cleaned = cleaned.replace("\xa0", " ")

    lines = [WHITESPACE_PATTERN.sub(" ", line).strip() for line in cleaned.splitlines()]
    non_empty_lines = [line for line in lines if line]

    return "\n".join(non_empty_lines)


def split_text(
    documents: dict[str, dict[str, Any]],
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
    clean_first: bool = True,
) -> dict[str, dict[str, Any]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than 0.")
    if chunk_overlap < 0:
        raise ValueError("chunk_overlap cannot be negative.")
    if chunk_overlap >= chunk_size:
        raise ValueError("chunk_overlap must be smaller than chunk_size.")

    transformed_documents: dict[str, dict[str, Any]] = {}
    step = chunk_size - chunk_overlap

    for page_id, document in documents.items():
        raw_content = document.get("content")
        content_text = clean_content_text(raw_content) if clean_first else (raw_content or "")

        chunks: list[dict[str, Any]] = []
        if content_text:
            for index, start in enumerate(range(0, len(content_text), step)):
                chunk_text = content_text[start:start + chunk_size].strip()
                if not chunk_text:
                    continue

                chunks.append(
                    {
                        "chunk_index": index,
                        "text": chunk_text,
                    }
                )

        transformed_document = dict(document)
        transformed_document["content"] = content_text
        transformed_document["chunks"] = chunks
        transformed_documents[page_id] = transformed_document

    return transformed_documents
