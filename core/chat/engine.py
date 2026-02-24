from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Generator

from django.conf import settings
from openai import OpenAI

from core.retrieval.hybrid_retriever import HybridRetriever

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are TaxGPT, an expert financial and tax assistant. You answer questions accurately using the provided context from tax datasets, IRS documents, tax code, and financial presentations.

Guidelines:
- Base your answers strictly on the provided context. If the context doesn't contain enough information, say so clearly.
- When citing financial figures, use exact numbers from the data.
- When discussing tax rules or regulations, reference the specific source (e.g., IRS Form 1040 instructions, US Tax Code).
- For structured data queries (aggregations, comparisons), prioritize the structured analysis results.
- Be concise but thorough. Format numbers with proper commas and currency symbols.
- If asked about relationships between entities, leverage the graph-derived relationships in the context.
- Do not fabricate information not present in the context."""


@dataclass
class ChatMessage:
    role: str
    content: str


@dataclass
class ChatResponse:
    answer: str
    sources: list[str] = field(default_factory=list)
    query_type: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


class ChatEngine:
    def __init__(self, retriever: HybridRetriever, model: str | None = None):
        self.retriever = retriever
        self.model = model or settings.LLM_MODEL
        self.client = OpenAI(api_key=settings.OPENAI_API_KEY)
        self.conversation_history: list[ChatMessage] = []

    def chat(self, user_message: str) -> ChatResponse:
        retrieval = self.retriever.retrieve(user_message)
        context = retrieval["context"]

        messages = self._build_messages(user_message, context)

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": m.role, "content": m.content} for m in messages],
                temperature=0.1,
                max_tokens=2048,
            )
            answer = response.choices[0].message.content or ""
        except Exception as e:
            logger.error("LLM call failed: %s", e)
            answer = f"I encountered an error generating a response: {e}"

        self.conversation_history.append(ChatMessage(role="user", content=user_message))
        self.conversation_history.append(ChatMessage(role="assistant", content=answer))

        return ChatResponse(
            answer=answer,
            sources=retrieval.get("sources", []),
            query_type=retrieval.get("query_type", ""),
            metadata={
                "vector_results": retrieval.get("vector_count", 0),
                "graph_entities": retrieval.get("graph_entities", []),
            },
        )

    def chat_stream(self, user_message: str) -> Generator[str, None, None]:
        retrieval = self.retriever.retrieve(user_message)
        context = retrieval["context"]
        messages = self._build_messages(user_message, context)

        meta_event = {
            "type": "meta",
            "sources": retrieval.get("sources", []),
            "query_type": retrieval.get("query_type", ""),
            "metadata": {
                "vector_results": retrieval.get("vector_count", 0),
                "graph_entities": retrieval.get("graph_entities", []),
            },
        }
        yield f"data: {json.dumps(meta_event)}\n\n"

        full_answer = ""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": m.role, "content": m.content} for m in messages],
                temperature=0.1,
                max_tokens=2048,
                stream=True,
            )
            for chunk in response:
                delta = chunk.choices[0].delta
                if delta.content:
                    full_answer += delta.content
                    yield f"data: {json.dumps({'type': 'token', 'content': delta.content})}\n\n"
        except Exception as e:
            logger.error("LLM streaming failed: %s", e)
            error_msg = f"I encountered an error generating a response: {e}"
            full_answer = error_msg
            yield f"data: {json.dumps({'type': 'token', 'content': error_msg})}\n\n"

        self.conversation_history.append(ChatMessage(role="user", content=user_message))
        self.conversation_history.append(ChatMessage(role="assistant", content=full_answer))

        yield f"data: {json.dumps({'type': 'done'})}\n\n"

    def reset_conversation(self) -> None:
        self.conversation_history.clear()

    def _build_messages(self, user_message: str, context: str) -> list[ChatMessage]:
        messages = [ChatMessage(role="system", content=SYSTEM_PROMPT)]

        recent = self.conversation_history[-12:]
        messages.extend(recent)

        augmented_message = f"""Question: {user_message}

Context (retrieved from tax datasets, documents, and knowledge graph):
{context}

Please answer the question based on the context above."""

        messages.append(ChatMessage(role="user", content=augmented_message))
        return messages