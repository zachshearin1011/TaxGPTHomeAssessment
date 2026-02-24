from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any

import chromadb
import pandas as pd
from chromadb.config import Settings as ChromaSettings
from django.conf import settings

logger = logging.getLogger(__name__)


class VectorStore:

    COLLECTION_NAME = "taxgpt_docs"

    def __init__(self, persist_dir: str | None = None):
        persist = persist_dir or str(Path(settings.BASE_DIR) / settings.CHROMA_PERSIST_DIR)
        self.client = chromadb.PersistentClient(
            path=persist,
            settings=ChromaSettings(anonymized_telemetry=False),
        )
        self._embedding_fn = self._build_embedding_fn()
        self.collection = self.client.get_or_create_collection(
            name=self.COLLECTION_NAME,
            embedding_function=self._embedding_fn,
            metadata={"hnsw:space": "cosine"},
        )
        self.csv_dataframes: dict[str, pd.DataFrame] = {}

    def add_documents(
        self,
        texts: list[str],
        metadatas: list[dict[str, Any]] | None = None,
    ) -> None:
        if not texts:
            return

        batch_size = 256
        for start in range(0, len(texts), batch_size):
            end = min(start + batch_size, len(texts))
            batch_texts = texts[start:end]
            batch_meta = metadatas[start:end] if metadatas else None

            ids = [self._make_id(t, i + start) for i, t in enumerate(batch_texts)]

            clean_meta = None
            if batch_meta:
                clean_meta = [self._clean_metadata(m) for m in batch_meta]

            self.collection.upsert(
                ids=ids,
                documents=batch_texts,
                metadatas=clean_meta,
            )

        logger.info("Stored %d documents in vector store (total: %d)", len(texts), self.collection.count())

    def search(
        self,
        query: str,
        top_k: int | None = None,
        where: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        k = top_k or settings.TOP_K
        kwargs: dict[str, Any] = {
            "query_texts": [query],
            "n_results": min(k, self.collection.count()) if self.collection.count() > 0 else k,
        }
        if where:
            kwargs["where"] = where

        try:
            results = self.collection.query(**kwargs)
        except Exception as e:
            logger.error("Vector search failed: %s", e)
            return []

        docs: list[dict[str, Any]] = []
        if results and results["documents"]:
            for i, doc_text in enumerate(results["documents"][0]):
                entry: dict[str, Any] = {"text": doc_text, "score": 0.0}
                if results["distances"]:
                    entry["score"] = 1.0 - results["distances"][0][i]
                if results["metadatas"]:
                    entry["metadata"] = results["metadatas"][0][i]
                docs.append(entry)

        return docs

    def count(self) -> int:
        return self.collection.count()

    @staticmethod
    def _make_id(text: str, index: int) -> str:
        h = hashlib.md5(text.encode()).hexdigest()[:12]
        return f"doc_{index}_{h}"

    @staticmethod
    def _clean_metadata(meta: dict[str, Any]) -> dict[str, Any]:
        clean: dict[str, Any] = {}
        for k, v in meta.items():
            if isinstance(v, (str, int, float, bool)):
                clean[k] = v
            elif v is None:
                continue
            else:
                clean[k] = str(v)
        return clean

    @staticmethod
    def _build_embedding_fn() -> chromadb.EmbeddingFunction:
        from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
        return SentenceTransformerEmbeddingFunction(
            model_name=settings.EMBEDDING_MODEL,
        )