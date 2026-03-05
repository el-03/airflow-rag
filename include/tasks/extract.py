from __future__ import annotations

import base64
import json
import os
import ssl
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, parse, request


DEFAULT_PARENT_PAGE_ID = "720918"


def load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))


def build_auth_header() -> str:
    access_token = os.getenv("ATLASSIAN_ACCESS_TOKEN")
    if access_token:
        return f"Bearer {access_token}"

    email = os.getenv("ATLASSIAN_EMAIL")
    api_token = os.getenv("ATLASSIAN_API_TOKEN")
    if email and api_token:
        token_bytes = f"{email}:{api_token}".encode("utf-8")
        return f"Basic {base64.b64encode(token_bytes).decode('ascii')}"

    raise RuntimeError(
        "Missing Confluence credentials."
    )


def build_ssl_context() -> ssl.SSLContext:
    ca_bundle = os.getenv("SSL_CERT_FILE")
    allow_insecure = os.getenv("CONFLUENCE_ALLOW_INSECURE", "").lower() in {
        "1",
        "true",
        "yes",
    }

    if allow_insecure:
        return ssl._create_unverified_context()

    if ca_bundle:
        return ssl.create_default_context(cafile=ca_bundle)

    return ssl.create_default_context()


def normalize_base_url(base_url: str) -> str:
    return base_url.rstrip("/")


def parse_confluence_datetime(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def should_include_page(
    last_modified_date: str | None,
    modified_since: datetime | None,
) -> bool:
    if modified_since is None:
        return True
    if not last_modified_date:
        return False
    return parse_confluence_datetime(last_modified_date) >= modified_since


def confluence_get(
    base_url: str,
    path: str,
    auth_header: str,
    ssl_context: ssl.SSLContext,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    query = parse.urlencode(params or {})
    url = f"{normalize_base_url(base_url)}{path}"
    if query:
        url = f"{url}?{query}"

    req = request.Request(
        url,
        headers={
            "Accept": "application/json",
            "Authorization": auth_header,
        },
    )

    try:
        with request.urlopen(req, context=ssl_context) as response:
            return json.load(response)
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Confluence API request failed with {exc.code}: {body}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Could not reach Confluence: {exc.reason}") from exc


def collect_child_pages(
    base_url: str,
    parent_page_id: str,
    auth_header: str,
    ssl_context: ssl.SSLContext,
    modified_since: datetime | None,
) -> dict[str, dict[str, Any]]:
    documents: dict[str, dict[str, Any]] = {}
    start = 0
    limit = 100

    while True:
        payload = confluence_get(
            base_url=base_url,
            path=f"/wiki/rest/api/content/{parent_page_id}/child/page",
            auth_header=auth_header,
            ssl_context=ssl_context,
            params={
                "expand": "history,version,body.storage",
                "limit": limit,
                "start": start,
            },
        )

        pages = payload.get("results", [])
        for page in pages:
            page_id = page.get("id")
            if not page_id:
                continue

            history = page.get("history", {})
            version = page.get("version", {})
            body = page.get("body", {}).get("storage", {})
            last_modified_date = version.get("when")

            if should_include_page(last_modified_date, modified_since):
                documents[page_id] = {
                    "id": page_id,
                    "title": page.get("title"),
                    "created_date": history.get("createdDate"),
                    "last_modified_date": last_modified_date,
                    "content": body.get("value"),
                }

            documents.update(
                collect_child_pages(
                    base_url=base_url,
                    parent_page_id=page_id,
                    auth_header=auth_header,
                    ssl_context=ssl_context,
                    modified_since=modified_since,
                )
            )

        if len(pages) < limit:
            break

        start += limit

    return documents


def extract_confluence_documents(
    parent_page_id: str = DEFAULT_PARENT_PAGE_ID,
    modified_since: str | None = None,
) -> dict[str, dict[str, Any]]:
    load_dotenv(Path(".env"))

    base_url = os.getenv("CONFLUENCE_BASE_URL")
    if not base_url:
        raise RuntimeError(
            "Missing CONFLUENCE_BASE_URL. Set it in the environment or .env."
        )

    auth_header = build_auth_header()
    ssl_context = build_ssl_context()
    modified_since_dt = (
        parse_confluence_datetime(modified_since) if modified_since else None
    )

    return collect_child_pages(
        base_url=base_url,
        parent_page_id=parent_page_id,
        auth_header=auth_header,
        ssl_context=ssl_context,
        modified_since=modified_since_dt,
    )
