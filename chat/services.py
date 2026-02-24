from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from django.conf import settings

from core.chat.engine import ChatEngine
from core.ingestion.pipeline import IngestionPipeline
from core.retrieval.graph_retriever import GraphRetriever
from core.retrieval.hybrid_retriever import HybridRetriever
from core.retrieval.vector_retriever import VectorRetriever
from core.storage.graph_store import KnowledgeGraph
from core.storage.vector_store import VectorStore

logger = logging.getLogger(__name__)

_vector_store: Optional[VectorStore] = None
_knowledge_graph: Optional[KnowledgeGraph] = None
_chat_engine: Optional[ChatEngine] = None
_initialized = False


def initialize():
    global _vector_store, _knowledge_graph, _chat_engine, _initialized

    if _initialized:
        return

    logger.info("Initializing TaxGPT services...")

    _vector_store = VectorStore()
    _knowledge_graph = KnowledgeGraph()

    graph_loaded = _knowledge_graph.load()
    docs_exist = _vector_store.count() > 0

    if not docs_exist or not graph_loaded:
        logger.info("Running ingestion pipeline...")
        pipeline = IngestionPipeline(_vector_store, _knowledge_graph)
        stats = pipeline.run()
        logger.info("Ingestion stats: %s", stats)
    else:
        logger.info(
            "Using existing data: %d vectors, graph %s",
            _vector_store.count(),
            _knowledge_graph.stats(),
        )

    if not _vector_store.csv_dataframes:
        data_dir = Path(settings.BASE_DIR) / settings.DATA_DIR
        for csv_path in data_dir.glob("*.csv"):
            _vector_store.csv_dataframes[csv_path.name] = pd.read_csv(csv_path)

    vector_retriever = VectorRetriever(_vector_store)
    graph_retriever = GraphRetriever(_knowledge_graph)
    hybrid_retriever = HybridRetriever(vector_retriever, graph_retriever, _vector_store)

    _chat_engine = ChatEngine(hybrid_retriever)

    _initialized = True
    logger.info("TaxGPT services ready!")


def get_chat_engine() -> Optional[ChatEngine]:
    if not _initialized:
        initialize()
    return _chat_engine


def get_ingestion_deps() -> tuple[Optional[VectorStore], Optional[KnowledgeGraph]]:
    if not _initialized:
        initialize()
    return _vector_store, _knowledge_graph