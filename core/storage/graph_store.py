from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import networkx as nx
from django.conf import settings

from core.ingestion.csv_processor import GraphTriple

logger = logging.getLogger(__name__)


class KnowledgeGraph:

    def __init__(self, persist_dir: Path | None = None):
        self.graph = nx.MultiDiGraph()
        self.persist_dir = persist_dir or Path(settings.BASE_DIR) / settings.GRAPH_PERSIST_DIR
        self.persist_dir.mkdir(parents=True, exist_ok=True)
        self._path = self.persist_dir / "knowledge_graph.json"

    def add_triple(self, triple: GraphTriple) -> None:
        subj_id = self._node_id(triple.subject, triple.subject_type)
        obj_id = self._node_id(triple.object, triple.object_type)

        if not self.graph.has_node(subj_id):
            self.graph.add_node(subj_id, label=triple.subject, node_type=triple.subject_type)
        if not self.graph.has_node(obj_id):
            self.graph.add_node(obj_id, label=triple.object, node_type=triple.object_type)

        self.graph.add_edge(
            subj_id, obj_id,
            relation=triple.predicate,
            **triple.properties,
        )

    def get_neighbors(self, entity: str, depth: int = 1) -> list[dict[str, Any]]:
        matches = self._find_nodes(entity)
        if not matches:
            return []

        results: list[dict[str, Any]] = []
        visited: set[str] = set()

        for node_id in matches:
            self._traverse(node_id, depth, visited, results)

        return results

    def get_entity_context(self, entity: str) -> str:
        neighbors = self.get_neighbors(entity, depth=settings.GRAPH_TRAVERSAL_DEPTH)
        if not neighbors:
            return ""

        lines: list[str] = []
        for n in neighbors:
            line = f"{n['source']} --[{n['relation']}]--> {n['target']}"
            if n.get("properties"):
                props = ", ".join(f"{k}={v}" for k, v in n["properties"].items())
                line += f" ({props})"
            lines.append(line)

        return "Knowledge graph relationships:\n" + "\n".join(lines)

    def query_by_type(self, node_type: str) -> list[dict[str, Any]]:
        results = []
        for nid, data in self.graph.nodes(data=True):
            if data.get("node_type") == node_type:
                results.append({"id": nid, **data})
        return results

    def get_subgraph_summary(self, entities: list[str]) -> str:
        node_ids: set[str] = set()
        for e in entities:
            node_ids.update(self._find_nodes(e))

        if len(node_ids) < 2:
            return ""

        lines: list[str] = []
        for u, v, data in self.graph.edges(data=True):
            if u in node_ids or v in node_ids:
                u_label = self.graph.nodes[u].get("label", u)
                v_label = self.graph.nodes[v].get("label", v)
                rel = data.get("relation", "related_to")
                lines.append(f"{u_label} --[{rel}]--> {v_label}")

        return "\n".join(lines[:50])

    def save(self) -> None:
        data = nx.node_link_data(self.graph)
        with open(self._path, "w") as f:
            json.dump(data, f, default=str)
        logger.info("Saved knowledge graph (%d nodes, %d edges)", self.graph.number_of_nodes(), self.graph.number_of_edges())

    def load(self) -> bool:
        if not self._path.exists():
            return False
        with open(self._path) as f:
            data = json.load(f)
        self.graph = nx.node_link_graph(data, directed=True, multigraph=True)
        logger.info("Loaded knowledge graph (%d nodes, %d edges)", self.graph.number_of_nodes(), self.graph.number_of_edges())
        return True

    def stats(self) -> dict[str, Any]:
        type_counts: dict[str, int] = {}
        for _, data in self.graph.nodes(data=True):
            nt = data.get("node_type", "unknown")
            type_counts[nt] = type_counts.get(nt, 0) + 1
        return {
            "nodes": self.graph.number_of_nodes(),
            "edges": self.graph.number_of_edges(),
            "node_types": type_counts,
        }

    @staticmethod
    def _node_id(label: str, node_type: str) -> str:
        return f"{node_type}::{label}".lower().replace(" ", "_")

    def _find_nodes(self, entity: str) -> list[str]:
        entity_lower = entity.lower().strip()
        matches: list[str] = []
        for nid, data in self.graph.nodes(data=True):
            label = data.get("label", "").lower()
            if entity_lower == label or entity_lower in label or entity_lower in nid:
                matches.append(nid)
        return matches

    def _traverse(
        self,
        node_id: str,
        depth: int,
        visited: set[str],
        results: list[dict[str, Any]],
    ) -> None:
        if depth <= 0 or node_id in visited:
            return
        visited.add(node_id)

        for _, target, data in self.graph.edges(node_id, data=True):
            source_label = self.graph.nodes[node_id].get("label", node_id)
            target_label = self.graph.nodes[target].get("label", target)
            props = {k: v for k, v in data.items() if k != "relation"}
            results.append({
                "source": source_label,
                "target": target_label,
                "relation": data.get("relation", "related_to"),
                "properties": props,
            })
            self._traverse(target, depth - 1, visited, results)

        for source, _, data in self.graph.in_edges(node_id, data=True):
            source_label = self.graph.nodes[source].get("label", source)
            target_label = self.graph.nodes[node_id].get("label", node_id)
            results.append({
                "source": source_label,
                "target": target_label,
                "relation": data.get("relation", "related_to"),
                "properties": {k: v for k, v in data.items() if k != "relation"},
            })
            self._traverse(source, depth - 1, visited, results)