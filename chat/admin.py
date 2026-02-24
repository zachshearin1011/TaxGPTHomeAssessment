from django.contrib import admin

from chat.models import Conversation, IngestedFile, Message

admin.site.register(Conversation)
admin.site.register(Message)
admin.site.register(IngestedFile)