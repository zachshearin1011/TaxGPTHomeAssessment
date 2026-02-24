from django.contrib import admin
from django.urls import include, path
from graphene_django.views import GraphQLView

from chat.views import index

urlpatterns = [
    path("", index),
    path("admin/", admin.site.urls),
    path("api/", include("chat.urls")),
    path("graphql/", GraphQLView.as_view(graphiql=True)),
]