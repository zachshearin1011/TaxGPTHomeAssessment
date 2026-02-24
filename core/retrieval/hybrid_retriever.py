from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd

from core.retrieval.graph_retriever import GraphRetriever
from core.retrieval.vector_retriever import VectorRetriever
from core.storage.vector_store import VectorStore

logger = logging.getLogger(__name__)


class HybridRetriever:

    def __init__(
        self,
        vector_retriever: VectorRetriever,
        graph_retriever: GraphRetriever,
        vector_store: VectorStore,
    ):
        self.vector = vector_retriever
        self.graph = graph_retriever
        self.vector_store = vector_store

    def retrieve(self, query: str, top_k: int = 8) -> dict[str, Any]:
        query_type = self._classify_query(query)

        vector_results = self.vector.retrieve(query, top_k=top_k)
        graph_results = self.graph.retrieve(query)
        structured_result = self._structured_query(query) if query_type == "structured" else ""

        context = self._build_context(
            query=query,
            vector_results=vector_results,
            graph_results=graph_results,
            structured_result=structured_result,
            query_type=query_type,
        )

        sources = self._extract_sources(vector_results)

        return {
            "context": context,
            "sources": sources,
            "query_type": query_type,
            "vector_count": len(vector_results),
            "graph_entities": graph_results.get("entities", []),
        }

    def _classify_query(self, query: str) -> str:
        query_lower = query.lower()

        structured_signals = [
            r'\b(total|sum|average|mean|count|how many|highest|lowest|max|min|median)\b',
            r'\b(compare|comparison|between|versus|vs\.?)\b',
            r'\b(tax owed|income|deductions?|taxable income|tax rate)\b',
            r'\b(by state|per state|each state|by year|per year|each year)\b',
            r'\b(individual|corporation|partnership|trust|non-?profit)\b',
            r'\b(taxpayer|filer)s?\b',
        ]
        struct_score = sum(1 for p in structured_signals if re.search(p, query_lower))

        unstructured_signals = [
            r'\b(what is|what are|define|explain|describe|how does|how do|how to|when should|who can|who is)\b',
            r'\b(form|schedule|section|rule|regulation|law|code|irs|instruction)\b',
            r'\b(requirement|eligibility|qualify|filing|deadline|penalty)\b',
            r'\b(standard deduction|itemized|credit|bracket|withholding|exemption)\b',
        ]
        unstruct_score = sum(1 for p in unstructured_signals if re.search(p, query_lower))

        if struct_score >= 2 and struct_score > unstruct_score:
            return "structured"
        if unstruct_score >= 2 and unstruct_score > struct_score:
            return "unstructured"
        if struct_score >= 2:
            return "structured"
        if unstruct_score >= 2:
            return "unstructured"

        return "hybrid"

    def _structured_query(self, query: str) -> str:
        if not self.vector_store.csv_dataframes:
            return ""

        df = next(iter(self.vector_store.csv_dataframes.values()))
        query_lower = query.lower()

        try:
            return self._run_structured_analysis(df, query_lower)
        except Exception as e:
            logger.warning("Structured query failed: %s", e)
            return ""

    def _run_structured_analysis(self, df: pd.DataFrame, query: str) -> str:
        results: list[str] = []

        filters = self._extract_filters(query, df)
        filtered = df.copy()
        for col, val in filters.items():
            if col in filtered.columns:
                filtered = filtered[filtered[col].astype(str).str.lower() == val.lower()]

        if len(filtered) == 0:
            filtered = df

        agg_type = self._detect_aggregation(query)

        if agg_type == "count":
            results.append(f"Record count: {len(filtered)}")
        elif agg_type == "total":
            for col in ["Income", "Deductions", "Taxable Income", "Tax Owed"]:
                if col.lower() in query or "all" in query:
                    results.append(f"Total {col}: ${filtered[col].sum():,.2f}")
            if not results:
                results.append(f"Total Income: ${filtered['Income'].sum():,.2f}")
                results.append(f"Total Tax Owed: ${filtered['Tax Owed'].sum():,.2f}")
        elif agg_type == "average":
            for col in ["Income", "Deductions", "Taxable Income", "Tax Owed", "Tax Rate"]:
                if col.lower() in query:
                    if col == "Tax Rate":
                        results.append(f"Average {col}: {filtered[col].mean():.2%}")
                    else:
                        results.append(f"Average {col}: ${filtered[col].mean():,.2f}")
            if not results:
                results.append(f"Average Income: ${filtered['Income'].mean():,.2f}")
                results.append(f"Average Tax Owed: ${filtered['Tax Owed'].mean():,.2f}")
        elif agg_type in ("max", "min"):
            fn = filtered.nlargest if agg_type == "max" else filtered.nsmallest
            for col in ["Income", "Deductions", "Tax Owed"]:
                if col.lower() in query:
                    top = fn(5, col)
                    results.append(f"{'Highest' if agg_type == 'max' else 'Lowest'} {col}:")
                    for _, r in top.iterrows():
                        results.append(
                            f"  {r['Taxpayer Type']} in {r['State']} ({r['Tax Year']}): ${r[col]:,.2f}"
                        )
            if not results:
                top = fn(5, "Tax Owed")
                label = "Highest" if agg_type == "max" else "Lowest"
                results.append(f"{label} Tax Owed records:")
                for _, r in top.iterrows():
                    results.append(
                        f"  {r['Taxpayer Type']} in {r['State']} ({r['Tax Year']}): ${r['Tax Owed']:,.2f}"
                    )
        else:
            results.append(f"Records: {len(filtered)}")
            results.append(f"Total Income: ${filtered['Income'].sum():,.2f}")
            results.append(f"Average Income: ${filtered['Income'].mean():,.2f}")
            results.append(f"Total Tax Owed: ${filtered['Tax Owed'].sum():,.2f}")
            results.append(f"Average Tax Rate: {filtered['Tax Rate'].mean():.2%}")

        if filters:
            filter_desc = ", ".join(f"{k}={v}" for k, v in filters.items())
            results.insert(0, f"[Filtered by: {filter_desc}]")

        return "Structured data analysis:\n" + "\n".join(results)

    def _extract_filters(self, query: str, df: pd.DataFrame) -> dict[str, str]:
        filters: dict[str, str] = {}
        query_lower = query.lower()

        for col in ["Taxpayer Type", "State", "Income Source", "Deduction Type"]:
            for val in df[col].unique():
                if str(val).lower() in query_lower:
                    filters[col] = str(val)
                    break

        year_match = re.search(r'\b(20\d{2})\b', query)
        if year_match:
            year = int(year_match.group(1))
            if year in df["Tax Year"].unique():
                filters["Tax Year"] = str(year)

        return filters

    @staticmethod
    def _detect_aggregation(query: str) -> str:
        query_lower = query.lower()
        if any(w in query_lower for w in ("how many", "count", "number of")):
            return "count"
        if any(w in query_lower for w in ("total", "sum")):
            return "total"
        if any(w in query_lower for w in ("average", "mean", "avg")):
            return "average"
        if any(w in query_lower for w in ("highest", "maximum", "max", "most", "largest", "top")):
            return "max"
        if any(w in query_lower for w in ("lowest", "minimum", "min", "least", "smallest", "bottom")):
            return "min"
        return "summary"

    def _build_context(
        self,
        query: str,
        vector_results: list[dict[str, Any]],
        graph_results: dict[str, Any],
        structured_result: str,
        query_type: str,
    ) -> str:
        sections: list[str] = []

        if structured_result:
            sections.append(structured_result)

        if vector_results:
            chunks = []
            for i, r in enumerate(vector_results, 1):
                source = r.get("metadata", {}).get("source_file", "unknown")
                score = r.get("score", 0)
                chunks.append(f"[{i}] (source: {source}, relevance: {score:.2f})\n{r['text']}")
            sections.append("Semantic search results:\n" + "\n\n".join(chunks))

        graph_ctx = graph_results.get("context", "")
        if graph_ctx:
            sections.append(graph_ctx)

        subgraph = graph_results.get("subgraph_summary", "")
        if subgraph:
            sections.append(f"Entity relationships:\n{subgraph}")

        return "\n\n---\n\n".join(sections)

    @staticmethod
    def _extract_sources(vector_results: list[dict[str, Any]]) -> list[str]:
        sources: set[str] = set()
        for r in vector_results:
            src = r.get("metadata", {}).get("source_file")
            if src:
                sources.add(src)
        return sorted(sources)