from __future__ import annotations

import pytest
from django.test import TestCase, Client


class TestHealthEndpoint(TestCase):
    def test_health(self):
        client = Client()
        response = client.get("/api/health")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("status", data)


class TestChatEndpoint(TestCase):
    def test_empty_message(self):
        client = Client()
        response = client.post(
            "/api/chat",
            data={"message": "", "reset": False},
            content_type="application/json",
        )
        self.assertIn(response.status_code, (400, 503))