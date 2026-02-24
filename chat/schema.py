from __future__ import annotations

import graphene
from graphene_django import DjangoObjectType

from chat.models import Conversation, Message
from chat.services import get_chat_engine


class MessageType(DjangoObjectType):
    class Meta:
        model = Message
        fields = ("id", "role", "content", "sources", "query_type", "metadata", "created_at")


class ConversationType(DjangoObjectType):
    messages = graphene.List(MessageType)

    class Meta:
        model = Conversation
        fields = ("id", "title", "created_at", "updated_at")

    def resolve_messages(self, info):
        return self.messages.all()


class ChatResponseType(graphene.ObjectType):
    answer = graphene.String()
    sources = graphene.List(graphene.String)
    query_type = graphene.String()
    conversation_id = graphene.String()


class Query(graphene.ObjectType):
    conversations = graphene.List(ConversationType, limit=graphene.Int(default_value=20))
    conversation = graphene.Field(ConversationType, id=graphene.UUID(required=True))

    def resolve_conversations(self, info, limit):
        return Conversation.objects.prefetch_related("messages").all()[:limit]

    def resolve_conversation(self, info, id):
        return Conversation.objects.prefetch_related("messages").get(id=id)


class SendMessage(graphene.Mutation):
    class Arguments:
        message = graphene.String(required=True)
        conversation_id = graphene.UUID(required=False)

    answer = graphene.String()
    sources = graphene.List(graphene.String)
    query_type = graphene.String()
    conversation_id = graphene.UUID()

    def mutate(self, info, message, conversation_id=None):
        engine = get_chat_engine()
        if engine is None:
            raise Exception("Chat engine not initialized")

        if conversation_id:
            conversation, _ = Conversation.objects.get_or_create(id=conversation_id)
        else:
            conversation = Conversation.objects.create(title=message[:100])

        Message.objects.create(conversation=conversation, role="user", content=message)

        result = engine.chat(message)

        Message.objects.create(
            conversation=conversation,
            role="assistant",
            content=result.answer,
            sources=result.sources,
            query_type=result.query_type,
            metadata=result.metadata,
        )

        return SendMessage(
            answer=result.answer,
            sources=result.sources,
            query_type=result.query_type,
            conversation_id=conversation.id,
        )


class Mutation(graphene.ObjectType):
    send_message = SendMessage.Field()


schema = graphene.Schema(query=Query, mutation=Mutation)