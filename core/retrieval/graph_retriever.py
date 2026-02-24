from __future__ import annotations

import logging
import re
from typing import Any

from core.storage.graph_store import KnowledgeGraph

logger = logging.getLogger(__name__)

ENTITY_KEYWORDS: dict[str, list[str]] = {
    "TaxpayerType": ["individual", "corporation", "partnership", "trust", "non-profit", "nonprofit"],
    "State": [
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
        "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    ],
    "IncomeSource": ["salary", "capital gains", "rental", "investment", "royalties", "business income"],
    "DeductionType": [
        "charitable contributions", "mortgage interest", "medical expenses",
        "education expenses", "business expenses",
    ],
    "TaxYear": ["2019", "2020", "2021", "2022", "2023"],
    "TaxConcept": [
        "standard deduction", "itemized deduction", "filing status",
        "tax credit", "tax bracket", "withholding", "estimated tax",
        "self-employment tax", "alternative minimum tax", "child tax credit",
        "earned income", "adjusted gross income", "taxable income",
        "capital gains", "depreciation", "section 179",
    ],
}


class GraphRetriever:

    def __init__(self, knowledge_graph: KnowledgeGraph):
        self.graph = knowledge_graph

    def retrieve(self, query: str, depth: int = 2) -> dict[str, Any]:
        entities = self.extract_entities(query)
        if not entities:
            return {"entities": [], "context": "", "relationships": []}

        all_relationships: list[dict[str, Any]] = []
        for entity in entities:
            neighbors = self.graph.get_neighbors(entity, depth=depth)
            all_relationships.extend(neighbors)

        seen: set[str] = set()
        unique: list[dict[str, Any]] = []
        for r in all_relationships:
            key = f"{r['source']}|{r['relation']}|{r['target']}"
            if key not in seen:
                seen.add(key)
                unique.append(r)

        context = self._relationships_to_text(unique[:30])
        subgraph_summary = self.graph.get_subgraph_summary(entities) if len(entities) > 1 else ""

        return {
            "entities": entities,
            "context": context,
            "subgraph_summary": subgraph_summary,
            "relationships": unique[:30],
        }

    def extract_entities(self, query: str) -> list[str]:
        query_lower = query.lower()
        found: list[str] = []

        for entity_type, keywords in ENTITY_KEYWORDS.items():
            for kw in keywords:
                if kw.lower() in query_lower:
                    found.append(kw)

        years = re.findall(r'\b(20\d{2})\b', query)
        for y in years:
            if y not in found:
                found.append(y)

        return list(dict.fromkeys(found))

    @staticmethod
    def _relationships_to_text(relationships: list[dict[str, Any]]) -> str:
        if not relationships:
            return ""
        lines = []
        for r in relationships:
            line = f"- {r['source']} --[{r['relation']}]--> {r['target']}"
            props = r.get("properties", {})
            if props:
                prop_str = ", ".join(f"{k}: {v}" for k, v in props.items())
                line += f" ({prop_str})"
            lines.append(line)
        return "Graph-derived relationships:\n" + "\n".join(lines)