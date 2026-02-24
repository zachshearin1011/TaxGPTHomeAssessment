#!/usr/bin/env python3
import os
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django
django.setup()

from django.conf import settings

from core.ingestion.pipeline import IngestionPipeline
from core.storage.graph_store import KnowledgeGraph
from core.storage.vector_store import VectorStore


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
    logger = logging.getLogger("ingest")

    data_dir = Path(settings.BASE_DIR) / settings.DATA_DIR
    logger.info("Data directory: %s", data_dir)

    vector_store = VectorStore()
    knowledge_graph = KnowledgeGraph()

    pipeline = IngestionPipeline(vector_store, knowledge_graph)
    stats = pipeline.run()

    logger.info("Ingestion complete!")
    logger.info("Documents indexed: %d", vector_store.count())
    logger.info("Graph stats: %s", knowledge_graph.stats())
    for source, count in stats.items():
        logger.info("  %s: %d chunks", source, count)


if __name__ == "__main__":
    main()