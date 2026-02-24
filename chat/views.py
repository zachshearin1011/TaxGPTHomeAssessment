from __future__ import annotations

import json
import logging
from pathlib import Path

from django.conf import settings
from django.http import JsonResponse, StreamingHttpResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from rest_framework import status, viewsets
from rest_framework.decorators import api_view
from rest_framework.response import Response

from chat.models import Conversation, IngestedFile, Message
from chat.serializers import (
    ChatRequestSerializer,
    ChatResponseSerializer,
    ConversationSerializer,
)
from chat.services import get_chat_engine, get_ingestion_deps

logger = logging.getLogger(__name__)


def index(request):
    return render(request, "index.html")


@api_view(["POST"])
def chat_view(request):
    serializer = ChatRequestSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)
    data = serializer.validated_data

    message_text = data["message"]
    conversation_id = data.get("conversation_id")
    reset = data.get("reset", False)

    engine = get_chat_engine()
    if engine is None:
        return Response(
            {"detail": "Chat engine not initialized"},
            status=status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    if reset:
        engine.reset_conversation()
        if not message_text.strip():
            return Response({"detail": "Conversation reset"})

    if not message_text.strip():
        return Response(
            {"detail": "Message cannot be empty"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    if conversation_id:
        conversation, _ = Conversation.objects.get_or_create(id=conversation_id)
    else:
        conversation = Conversation.objects.create(title=message_text[:100])

    Message.objects.create(
        conversation=conversation,
        role="user",
        content=message_text,
    )

    result = engine.chat(message_text)

    Message.objects.create(
        conversation=conversation,
        role="assistant",
        content=result.answer,
        sources=result.sources,
        query_type=result.query_type,
        metadata=result.metadata,
    )

    response_data = {
        "answer": result.answer,
        "sources": result.sources,
        "query_type": result.query_type,
        "conversation_id": str(conversation.id),
        "metadata": result.metadata,
    }
    return Response(ChatResponseSerializer(response_data).data)


def chat_stream_view(request):
    if request.method != "POST":
        return JsonResponse({"detail": "Method not allowed"}, status=405)

    try:
        body = json.loads(request.body)
    except (json.JSONDecodeError, ValueError):
        return JsonResponse({"detail": "Invalid JSON"}, status=400)

    message_text = body.get("message", "")
    conversation_id = body.get("conversation_id")
    reset = body.get("reset", False)

    engine = get_chat_engine()
    if engine is None:
        return JsonResponse({"detail": "Chat engine not initialized"}, status=503)

    if reset:
        engine.reset_conversation()
        if not message_text.strip():
            return JsonResponse({"detail": "Conversation reset"})

    if not message_text.strip():
        return JsonResponse({"detail": "Message cannot be empty"}, status=400)

    if conversation_id:
        conversation, _ = Conversation.objects.get_or_create(id=conversation_id)
    else:
        conversation = Conversation.objects.create(title=message_text[:100])

    Message.objects.create(
        conversation=conversation,
        role="user",
        content=message_text,
    )

    def event_stream():
        full_answer = ""
        sources = []
        query_type = ""
        metadata = {}

        for sse_chunk in engine.chat_stream(message_text):
            if not sse_chunk.startswith("data: "):
                yield sse_chunk
                continue

            try:
                payload = json.loads(sse_chunk[6:].strip())
            except (json.JSONDecodeError, ValueError):
                yield sse_chunk
                continue

            if payload.get("type") == "meta":
                sources = payload.get("sources", [])
                query_type = payload.get("query_type", "")
                metadata = payload.get("metadata", {})
                payload["conversation_id"] = str(conversation.id)
                yield f"data: {json.dumps(payload)}\n\n"
            elif payload.get("type") == "token":
                full_answer += payload.get("content", "")
                yield sse_chunk
            else:
                yield sse_chunk

        Message.objects.create(
            conversation=conversation,
            role="assistant",
            content=full_answer,
            sources=sources,
            query_type=query_type,
            metadata=metadata,
        )

    response = StreamingHttpResponse(event_stream(), content_type="text/event-stream")
    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"
    return response


@api_view(["POST"])
def ingest_view(request):
    data_dir = request.data.get("data_dir")
    path = Path(data_dir) if data_dir else Path(settings.BASE_DIR) / settings.DATA_DIR
    if not path.exists():
        return Response(
            {"detail": f"Data directory not found: {path}"},
            status=status.HTTP_404_NOT_FOUND,
        )

    vector_store, knowledge_graph = get_ingestion_deps()
    from core.ingestion.pipeline import IngestionPipeline
    pipeline = IngestionPipeline(vector_store, knowledge_graph, data_dir=path)
    stats = pipeline.run()

    for filename, count in stats.items():
        suffix = Path(filename).suffix.lstrip(".").lower()
        file_type = "ppt" if suffix in ("ppt", "pptx") else suffix
        IngestedFile.objects.update_or_create(
            file_name=filename,
            defaults={"file_type": file_type, "chunk_count": count, "status": "completed"},
        )

    return Response({"status": "ok", "stats": stats})


@api_view(["GET"])
def stats_view(request):
    vector_store, knowledge_graph = get_ingestion_deps()
    return Response({
        "vector_store_documents": vector_store.count() if vector_store else 0,
        "knowledge_graph": knowledge_graph.stats() if knowledge_graph else {},
        "ingested_files": list(IngestedFile.objects.values("file_name", "file_type", "chunk_count", "status")),
    })


@api_view(["GET"])
def health_view(request):
    engine = get_chat_engine()
    return Response({"status": "healthy", "engine_ready": engine is not None})


class ConversationViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Conversation.objects.prefetch_related("messages").all()
    serializer_class = ConversationSerializer