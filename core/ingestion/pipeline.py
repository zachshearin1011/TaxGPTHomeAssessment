from __future__ import annotations

import logging
from pathlib import Path

from django.conf import settings

from core.ingestion.csv_processor import CSVProcessor, Document, GraphTriple
from core.ingestion.pdf_processor import PDFProcessor
from core.ingestion.ppt_processor import PPTProcessor
from core.storage.graph_store import KnowledgeGraph
from core.storage.vector_store import VectorStore

logger = logging.getLogger(__name__)


class IngestionPipeline:

    def __init__(
        self,
        vector_store: VectorStore,
        knowledge_graph: KnowledgeGraph,
        data_dir: Path | None = None,
    ):
        self.vector_store = vector_store
        self.graph = knowledge_graph
        self.data_dir = data_dir or Path(settings.BASE_DIR) / settings.DATA_DIR

    def run(self) -> dict[str, int]:
        stats: dict[str, int] = {}

        csv_files = list(self.data_dir.glob("*.csv"))
        pdf_files = list(self.data_dir.glob("*.pdf"))
        ppt_files = list(self.data_dir.glob("*.ppt")) + list(self.data_dir.glob("*.pptx"))

        for f in csv_files:
            count = self._ingest_csv(f)
            stats[f.name] = count

        for f in pdf_files:
            count = self._ingest_pdf(f)
            stats[f.name] = count

        for f in ppt_files:
            count = self._ingest_ppt(f)
            stats[f.name] = count

        self.graph.save()
        logger.info("Ingestion complete. Stats: %s", stats)
        return stats

    def _ingest_csv(self, path: Path) -> int:
        logger.info("Ingesting CSV: %s", path.name)
        proc = CSVProcessor(path)
        proc.load()

        docs = proc.to_documents()
        triples = proc.to_graph_triples()

        self._store_documents(docs)
        self._store_triples(triples)

        self.vector_store.csv_dataframes[path.name] = proc.df

        return len(docs)

    def _ingest_pdf(self, path: Path) -> int:
        logger.info("Ingesting PDF: %s", path.name)
        proc = PDFProcessor(path, chunk_size=settings.CHUNK_SIZE, chunk_overlap=settings.CHUNK_OVERLAP)

        docs = proc.to_documents()
        triples = proc.to_graph_triples()

        self._store_documents(docs)
        self._store_triples(triples)

        return len(docs)

    def _ingest_ppt(self, path: Path) -> int:
        logger.info("Ingesting PPT: %s", path.name)
        proc = PPTProcessor(path, chunk_size=settings.CHUNK_SIZE, chunk_overlap=settings.CHUNK_OVERLAP)

        docs = proc.to_documents()
        triples = proc.to_graph_triples()

        self._store_documents(docs)
        self._store_triples(triples)

        return len(docs)

    def _store_documents(self, docs: list[Document]) -> None:
        texts = [d.text for d in docs]
        metadatas = [d.metadata for d in docs]
        self.vector_store.add_documents(texts, metadatas)

    def _store_triples(self, triples: list[GraphTriple]) -> None:
        for t in triples:
            self.graph.add_triple(t)