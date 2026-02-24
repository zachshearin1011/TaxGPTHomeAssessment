from __future__ import annotations

import logging
from typing import Any

from core.storage.vector_store import VectorStore

logger = logging.getLogger(__name__)


class VectorRetriever:

    def __init__(self, vector_store: VectorStore):
        self.store = vector_store

    def retrieve(self, query: str, top_k: int = 8) -> list[dict[str, Any]]:
        results = self.store.search(query, top_k=top_k)
        logger.debug("Vector retrieval returned %d results for: %s", len(results), query[:80])
        return results