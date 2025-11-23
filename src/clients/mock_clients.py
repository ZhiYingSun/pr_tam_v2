from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from src.core.api_models import ZyteHttpResponse


class MockOpenAIClient:
    """Lightweight mock that mimics OpenAIClient's async interface."""

    def __init__(
        self,
        responses: Optional[List[Dict[str, Any]]] = None,
        error: Optional[Exception] = None,
    ) -> None:
        self._responses = list(responses or [])
        self._error = error
        self.calls: list[Dict[str, Any]] = []

    async def chat_completion(
        self,
        model: str,
        messages: list,
        temperature: float = 0.2,
        response_format: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        self.calls.append(
            {
                "model": model,
                "messages": messages,
                "temperature": temperature,
                "response_format": response_format,
            }
        )
        if self._error:
            raise self._error
        if not self._responses:
            return None
        return self._responses.pop(0)


class MockZyteClient:
    """Mock Zyte client with async context manager interface."""

    def __init__(
        self,
        post_responses: Optional[List[ZyteHttpResponse]] = None,
        get_responses: Optional[List[ZyteHttpResponse]] = None,
        post_error: Optional[Exception] = None,
        get_error: Optional[Exception] = None,
    ) -> None:
        self.post_responses = list(post_responses or [])
        self.get_responses = list(get_responses or [])
        self.post_error = post_error
        self.get_error = get_error
        self.post_calls: list[Dict[str, Any]] = []
        self.get_calls: list[Dict[str, Any]] = []

    async def __aenter__(self) -> MockZyteClient:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        return None

    async def post_request(
        self,
        url: str,
        request_body: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
    ) -> ZyteHttpResponse:
        self.post_calls.append(
            {"url": url, "request_body": request_body, "headers": headers}
        )
        if self.post_error:
            raise self.post_error
        if not self.post_responses:
            raise RuntimeError("No mock POST responses remaining")
        return self.post_responses.pop(0)

    async def get_request(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
    ) -> ZyteHttpResponse:
        self.get_calls.append({"url": url, "headers": headers})
        if self.get_error:
            raise self.get_error
        if not self.get_responses:
            raise RuntimeError("No mock GET responses remaining")
        return self.get_responses.pop(0)

