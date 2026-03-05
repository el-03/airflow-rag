from airflow.sdk import dag, task
import pendulum
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance

DELETE_ALL_SCHEMAS = True

@dag(
    dag_id="create_schema",
    schedule=None,
    start_date=pendulum.datetime(2026, 3, 4, tz="UTC"),
    catchup=False,
    tags=["telia"],
)
def create_schema():
    @task
    def delete_all_qdrant_schemas():
        client = QdrantClient("http://localhost:6333")

        if DELETE_ALL_SCHEMAS and client.collection_exists("confluence_vector_db"):
            client.delete_collection("confluence_vector_db")

    @task
    def create_schema_if_not_exist():
        client = QdrantClient("http://localhost:6333")

        if not client.collection_exists("confluence_vector_db"):
            client.create_collection(
                collection_name="confluence_vector_db",
                vectors_config=VectorParams(size=384, distance=Distance.COSINE),
            )

    delete_all_qdrant_schemas()
    create_schema_if_not_exist()

create_schema()
