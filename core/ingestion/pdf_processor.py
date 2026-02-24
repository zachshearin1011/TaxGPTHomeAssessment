from __future__ import annotations

import logging
import re
from pathlib import Path

import fitz

from core.ingestion.csv_processor import Document, GraphTriple

logger = logging.getLogger(__name__)

TAX_CONCEPTS = [
    "adjusted gross income", "standard deduction", "itemized deduction",
    "taxable income", "filing status", "dependent", "tax credit",
    "earned income", "capital gains", "tax bracket", "withholding",
    "estimated tax", "self-employment tax", "alternative minimum tax",
    "child tax credit", "education credit", "retirement",
    "social security", "medicare", "head of household",
    "married filing jointly", "married filing separately", "single",
    "qualifying widow", "form 1040", "schedule a", "schedule b",
    "schedule c", "schedule d", "schedule e", "schedule se",
    "w-2", "1099", "charitable contribution", "mortgage interest",
    "medical expenses", "business expenses", "depreciation",
    "section 179", "net operating loss", "partnership", "corporation",
    "s corporation", "trust", "estate", "non-profit", "exempt",
    "gross income", "above-the-line", "below-the-line",
]


class PDFProcessor:

    def __init__(self, path: Path, chunk_size: int = 512, chunk_overlap: int = 64):
        self.path = path
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

    def extract_text(self) -> list[tuple[int, str]]:
        doc = fitz.open(str(self.path))
        pages: list[tuple[int, str]] = []
        for page_num in range(len(doc)):
            page = doc[page_num]
            text = page.get_text("text")
            if text.strip():
                pages.append((page_num + 1, text))
        doc.close()
        logger.info("Extracted text from %d pages of %s", len(pages), self.path.name)
        return pages

    def to_documents(self) -> list[Document]:
        pages = self.extract_text()
        docs: list[Document] = []

        for page_num, page_text in pages:
            chunks = self._chunk_text(page_text)
            for i, chunk in enumerate(chunks):
                if len(chunk.strip()) < 30:
                    continue
                docs.append(Document(
                    text=chunk,
                    metadata={
                        "source_file": self.path.name,
                        "page": page_num,
                        "chunk_index": i,
                    },
                    source=self.path.name,
                    doc_type="pdf",
                ))

        logger.info("Produced %d document chunks from %s", len(docs), self.path.name)
        return docs

    def to_graph_triples(self) -> list[GraphTriple]:
        pages = self.extract_text()
        full_text = " ".join(t for _, t in pages).lower()

        triples: list[GraphTriple] = []

        found_concepts: list[str] = []
        for concept in TAX_CONCEPTS:
            if concept in full_text:
                found_concepts.append(concept)
                triples.append(GraphTriple(
                    subject=self.path.stem,
                    subject_type="Document",
                    predicate="discusses",
                    object=concept,
                    object_type="TaxConcept",
                ))

        for i, c1 in enumerate(found_concepts):
            for c2 in found_concepts[i + 1:]:
                triples.append(GraphTriple(
                    subject=c1, subject_type="TaxConcept",
                    predicate="related_to",
                    object=c2, object_type="TaxConcept",
                ))

        section_pattern = r'(?:section|§)\s*(\d+[a-zA-Z]?(?:\([a-z0-9]+\))*)'
        sections = set(re.findall(section_pattern, full_text, re.IGNORECASE))
        for sec in sections:
            sec_label = f"Section {sec}"
            triples.append(GraphTriple(
                subject=self.path.stem,
                subject_type="Document",
                predicate="references_section",
                object=sec_label,
                object_type="TaxSection",
            ))

        logger.info("Extracted %d graph triples from %s", len(triples), self.path.name)
        return triples

    def _chunk_text(self, text: str) -> list[str]:
        words = text.split()
        chunks: list[str] = []
        start = 0
        while start < len(words):
            end = start + self.chunk_size
            chunk = " ".join(words[start:end])
            chunks.append(chunk)
            start += self.chunk_size - self.chunk_overlap
        return chunks