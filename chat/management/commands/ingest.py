from __future__ import annotations

import logging
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand

from chat.models import IngestedFile
from chat.services import get_ingestion_deps
from core.ingestion.pipeline import IngestionPipeline

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Ingest all datasets into vector store and knowledge graph"

    def add_arguments(self, parser):
        parser.add_argument("--data-dir", type=str, default=None)

    def handle(self, *args, **options):
        data_dir = options["data_dir"]
        path = Path(data_dir) if data_dir else Path(settings.BASE_DIR) / settings.DATA_DIR

        self.stdout.write(f"Data directory: {path}")

        vector_store, knowledge_graph = get_ingestion_deps()
        pipeline = IngestionPipeline(vector_store, knowledge_graph, data_dir=path)
        stats = pipeline.run()

        for filename, count in stats.items():
            suffix = Path(filename).suffix.lstrip(".").lower()
            file_type = "ppt" if suffix in ("ppt", "pptx") else suffix
            IngestedFile.objects.update_or_create(
                file_name=filename,
                defaults={"file_type": file_type, "chunk_count": count, "status": "completed"},
            )

        self.stdout.write(self.style.SUCCESS(f"Ingestion complete: {stats}"))