from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from core.ingestion.csv_processor import GraphTriple
from core.retrieval.graph_retriever import GraphRetriever
from core.retrieval.hybrid_retriever import HybridRetriever
from core.retrieval.vector_retriever import VectorRetriever
from core.storage.graph_store import KnowledgeGraph
from core.storage.vector_store import VectorStore


class TestKnowledgeGraph:
    def test_add_and_query(self, knowledge_graph: KnowledgeGraph):
        triple = GraphTriple(
            subject="Individual",
            subject_type="TaxpayerType",
            predicate="files_in",
            object="CA",
            object_type="State",
        )
        knowledge_graph.add_triple(triple)

        neighbors = knowledge_graph.get_neighbors("Individual")
        assert len(neighbors) > 0
        assert any(n["target"] == "CA" for n in neighbors)

    def test_save_and_load(self, knowledge_graph: KnowledgeGraph):
        triple = GraphTriple(
            subject="Corporation",
            subject_type="TaxpayerType",
            predicate="files_in",
            object="NY",
            object_type="State",
        )
        knowledge_graph.add_triple(triple)
        knowledge_graph.save()

        new_graph = KnowledgeGraph(persist_dir=knowledge_graph.persist_dir)
        assert new_graph.load()
        assert new_graph.graph.number_of_nodes() == 2

    def test_entity_context(self, knowledge_graph: KnowledgeGraph):
        knowledge_graph.add_triple(GraphTriple("Individual", "TaxpayerType", "files_in", "CA", "State"))
        knowledge_graph.add_triple(GraphTriple("Individual", "TaxpayerType", "earns_from", "Salary", "IncomeSource"))

        context = knowledge_graph.get_entity_context("Individual")
        assert "Individual" in context
        assert "CA" in context or "Salary" in context

    def test_query_by_type(self, knowledge_graph: KnowledgeGraph):
        knowledge_graph.add_triple(GraphTriple("Individual", "TaxpayerType", "files_in", "CA", "State"))
        knowledge_graph.add_triple(GraphTriple("Corporation", "TaxpayerType", "files_in", "NY", "State"))

        types = knowledge_graph.query_by_type("TaxpayerType")
        assert len(types) == 2

    def test_stats(self, knowledge_graph: KnowledgeGraph):
        knowledge_graph.add_triple(GraphTriple("Individual", "TaxpayerType", "files_in", "CA", "State"))
        stats = knowledge_graph.stats()
        assert stats["nodes"] == 2
        assert stats["edges"] == 1


class TestGraphRetriever:
    def test_extract_entities(self, knowledge_graph: KnowledgeGraph):
        retriever = GraphRetriever(knowledge_graph)
        entities = retriever.extract_entities("What is the average income for individuals in CA?")
        assert "individual" in [e.lower() for e in entities]
        assert "CA" in entities

    def test_extract_year(self, knowledge_graph: KnowledgeGraph):
        retriever = GraphRetriever(knowledge_graph)
        entities = retriever.extract_entities("Tax owed in 2023")
        assert "2023" in entities

    def test_retrieve_with_graph(self, knowledge_graph: KnowledgeGraph):
        knowledge_graph.add_triple(GraphTriple("Individual", "TaxpayerType", "files_in", "CA", "State"))
        knowledge_graph.add_triple(GraphTriple("CA", "State", "has_income_source", "Salary", "IncomeSource"))

        retriever = GraphRetriever(knowledge_graph)
        result = retriever.retrieve("individual taxpayers in CA")
        assert len(result["entities"]) > 0
        assert result["context"] != ""


def _mock_vector_store() -> MagicMock:
    mock = MagicMock(spec=VectorStore)
    mock.csv_dataframes = {}
    mock.search.return_value = []
    mock.count.return_value = 0
    return mock


class TestHybridRetrieverLogic:

    @pytest.fixture
    def hr(self, knowledge_graph: KnowledgeGraph) -> HybridRetriever:
        mock_vs = _mock_vector_store()
        vr = VectorRetriever(mock_vs)
        gr = GraphRetriever(knowledge_graph)
        return HybridRetriever(vr, gr, mock_vs)

    def test_classify_structured_query(self, hr: HybridRetriever):
        assert hr._classify_query("What is the total income for corporations?") == "structured"
        assert hr._classify_query("How many individual taxpayers are there?") == "structured"

    def test_classify_unstructured_query(self, hr: HybridRetriever):
        assert hr._classify_query("What is the standard deduction?") == "unstructured"
        assert hr._classify_query("Explain the filing requirements for Form 1040") == "unstructured"

    def test_classify_hybrid_query(self, hr: HybridRetriever):
        q = "general question about taxes"
        result = hr._classify_query(q)
        assert result in ("structured", "unstructured", "hybrid")

    def test_detect_aggregation(self, hr: HybridRetriever):
        assert hr._detect_aggregation("how many records") == "count"
        assert hr._detect_aggregation("total tax owed") == "total"
        assert hr._detect_aggregation("average income") == "average"
        assert hr._detect_aggregation("highest tax owed") == "max"
        assert hr._detect_aggregation("lowest income") == "min"

    def test_structured_query_with_data(self, hr: HybridRetriever, sample_csv: Path):
        df = pd.read_csv(sample_csv)
        hr.vector_store.csv_dataframes = {"test.csv": df}

        result = hr._structured_query("total income for individuals")
        assert "150,000" in result

    def test_extract_filters(self, hr: HybridRetriever, sample_csv: Path):
        df = pd.read_csv(sample_csv)
        filters = hr._extract_filters("individual taxpayers in ca for 2023", df)
        assert filters.get("Taxpayer Type") == "Individual"
        assert filters.get("State") == "CA"
        assert filters.get("Tax Year") == "2023"

    def test_retrieve_returns_context(self, hr: HybridRetriever):
        result = hr.retrieve("What is the total income?")
        assert "context" in result
        assert "sources" in result
        assert "query_type" in result