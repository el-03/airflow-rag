from __future__ import annotations

import hashlib
import os
from typing import Any

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import chain, dag, task, TriggerRule
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams

from include.tasks.extract import extract_confluence_documents
from include.tasks.transform import split_text

CONFLUENCE_COLLECTION_NAME = "confluence_vector_db"
EMBEDDING_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
VECTOR_SIZE = 384
QDRANT_URL = "http://localhost:6333"
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200


def build_point_id(page_id: str, chunk_index: int) -> int:
    digest = hashlib.sha256(f"{page_id}:{chunk_index}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False)


@dag(
    dag_id="load_confluence_documents",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 3, 5, tz="UTC"),
    catchup=False,
    tags=["telia"],
)
def load_confluence_documents():
    @task.branch
    def check_schema(schema_name: str) -> str:
        client = QdrantClient(QDRANT_URL)

        if client.collection_exists(schema_name):
            print(f"Schema '{schema_name}' already exists.")
            return "schema_already_exists"

        print(f"Schema '{schema_name}' does not exist yet.")
        return "create_schema"

    @task
    def create_schema(schema_name: str) -> None:
        client = QdrantClient(QDRANT_URL)
        client.create_collection(
            collection_name=schema_name,
            vectors_config=VectorParams(size=VECTOR_SIZE, distance=Distance.COSINE),
        )

    @task
    def extract() -> dict[str, dict[str, Any]]:
        return extract_confluence_documents()

    @task
    def transform(documents: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
        return split_text(
            documents,
            chunk_size=CHUNK_SIZE,
            chunk_overlap=CHUNK_OVERLAP,
        )

    @task
    def load(
            documents: dict[str, dict[str, Any]],
            schema_name: str,
    ) -> int:
        # Import in-task to avoid macOS fork-safety crashes from Objective-C init in parent.
        from sentence_transformers import SentenceTransformer

        client = QdrantClient(QDRANT_URL)
        embedding_model = SentenceTransformer(EMBEDDING_MODEL_NAME, device="cpu")

        chunk_records: list[dict[str, Any]] = []
        for page_id, document in documents.items():
            for chunk in document.get("chunks", []):
                text = chunk.get("text", "").strip()
                if not text:
                    continue

                chunk_index = chunk["chunk_index"]
                chunk_records.append(
                    {
                        "id": build_point_id(page_id, chunk_index),
                        "text": text,
                        "metadata": {
                            "page_id": page_id,
                            "title": document.get("title"),
                            "created_date": document.get("created_date"),
                            "last_modified_date": document.get("last_modified_date"),
                            "chunk_index": chunk_index,
                        },
                    }
                )

        if not chunk_records:
            print("No document chunks to load.")
            return 0

        vectors = embedding_model.encode(
            [record["text"] for record in chunk_records],
            normalize_embeddings=True,
        )

        points = [
            PointStruct(
                id=record["id"],
                vector=vectors[index].tolist(),
                payload={
                    "information": record["text"],
                    "metadata": record["metadata"],
                },
            )
            for index, record in enumerate(chunk_records)
        ]

        client.upsert(collection_name=schema_name, points=points)
        print(f"Loaded {len(points)} chunks into '{schema_name}'.")
        return len(points)

    schema_already_exists = EmptyOperator(task_id="schema_already_exists")

    ingest_source = EmptyOperator(
        task_id="ingest_source",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    extract = extract()
    transform = transform(extract)
    load = load(documents=transform, schema_name=CONFLUENCE_COLLECTION_NAME)

    chain(
        check_schema(schema_name=CONFLUENCE_COLLECTION_NAME),
        [schema_already_exists, create_schema(schema_name=CONFLUENCE_COLLECTION_NAME)],
        ingest_source,
        extract,
        transform,
        load,
    )


load_confluence_documents()
