from __future__ import annotations

from pathlib import Path

import pytest

from core.ingestion.csv_processor import CSVProcessor, Document, GraphTriple


class TestCSVProcessor:
    def test_load(self, csv_processor: CSVProcessor):
        assert csv_processor.df is not None
        assert len(csv_processor.df) == 5

    def test_to_documents_produces_row_docs(self, csv_processor: CSVProcessor):
        docs = csv_processor.to_documents()
        row_docs = [d for d in docs if d.doc_type == "csv_row"]
        assert len(row_docs) == 5

    def test_to_documents_produces_aggregates(self, csv_processor: CSVProcessor):
        docs = csv_processor.to_documents()
        agg_docs = [d for d in docs if d.doc_type == "csv_aggregate"]
        assert len(agg_docs) > 0

    def test_document_text_contains_financial_info(self, csv_processor: CSVProcessor):
        docs = csv_processor.to_documents()
        row_docs = [d for d in docs if d.doc_type == "csv_row"]

        ca_doc = next(d for d in row_docs if "CA" in d.text)
        assert "$150,000.00" in ca_doc.text
        assert "Individual" in ca_doc.text
        assert "Salary" in ca_doc.text

    def test_document_metadata(self, csv_processor: CSVProcessor):
        docs = csv_processor.to_documents()
        row_docs = [d for d in docs if d.doc_type == "csv_row"]
        assert all("source_file" in d.metadata for d in row_docs)

    def test_to_graph_triples(self, csv_processor: CSVProcessor):
        triples = csv_processor.to_graph_triples()
        assert len(triples) > 0

        predicates = {t.predicate for t in triples}
        assert "files_in" in predicates
        assert "earns_from" in predicates
        assert "claims" in predicates

    def test_graph_triples_unique(self, csv_processor: CSVProcessor):
        triples = csv_processor.to_graph_triples()
        non_stat = [t for t in triples if t.predicate != "stats_in_state"]
        keys = [(t.subject, t.predicate, t.object) for t in non_stat]
        assert len(keys) == len(set(keys))

    def test_aggregate_stats_triples(self, csv_processor: CSVProcessor):
        triples = csv_processor.to_graph_triples()
        stat_triples = [t for t in triples if t.predicate == "stats_in_state"]
        assert len(stat_triples) > 0
        assert all("avg_income" in t.properties for t in stat_triples)


class TestDocumentDataclass:
    def test_default_values(self):
        doc = Document(text="test")
        assert doc.metadata == {}
        assert doc.source == ""
        assert doc.doc_type == ""

    def test_with_metadata(self):
        doc = Document(text="hello", metadata={"key": "val"}, source="test.csv", doc_type="csv_row")
        assert doc.metadata["key"] == "val"
        assert doc.source == "test.csv"


class TestGraphTriple:
    def test_creation(self):
        t = GraphTriple(
            subject="Individual",
            subject_type="TaxpayerType",
            predicate="files_in",
            object="CA",
            object_type="State",
        )
        assert t.subject == "Individual"
        assert t.properties == {}