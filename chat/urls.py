from django.urls import include, path
from rest_framework.routers import DefaultRouter

from chat.views import (
    chat_stream_view,
    chat_view,
    health_view,
    ingest_view,
    stats_view,
    ConversationViewSet,
)

router = DefaultRouter()
router.register(r"conversations", ConversationViewSet)

urlpatterns = [
    path("chat", chat_view),
    path("chat/stream", chat_stream_view),
    path("ingest", ingest_view),
    path("stats", stats_view),
    path("health", health_view),
    path("", include(router.urls)),
]