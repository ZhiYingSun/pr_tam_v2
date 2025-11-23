from __future__ import annotations

import asyncio
import base64
import json

import pytest

from src.clients.mock_clients import MockOpenAIClient, MockZyteClient
from src.core.api_model import ZyteHttpResponse


def _run(coro):
    return asyncio.run(coro)


def _make_zyte_response(payload: dict) -> ZyteHttpResponse:
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")
    return ZyteHttpResponse(httpResponseBody=encoded)


def test_mock_openai_client_returns_scripted_response() -> None:
    client = MockOpenAIClient(responses=[{"content": "hi"}])
    result = _run(
        client.chat_completion(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "hello"}],
            temperature=0.3,
        )
    )

    assert result == {"content": "hi"}
    assert client.calls[0]["model"] == "gpt-4o-mini"
    assert client.calls[0]["messages"][0]["content"] == "hello"


def test_mock_openai_client_can_raise_errors() -> None:
    client = MockOpenAIClient(error=RuntimeError("boom"))
    with pytest.raises(RuntimeError):
        _run(
            client.chat_completion(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": "hello"}],
            )
        )


def test_mock_zyte_client_post_and_get_requests() -> None:
    post_payload = {"status": "created"}
    get_payload = {"status": "fetched"}
    client = MockZyteClient(
        post_responses=[_make_zyte_response(post_payload)],
        get_responses=[_make_zyte_response(get_payload)],
    )

    post_resp = _run(
        client.post_request(
            url="https://example.com",
            request_body={"query": "foo"},
            headers={"X-Test": "1"},
        )
    )
    get_resp = _run(client.get_request(url="https://example.com/1"))

    assert post_resp.decode_body() == post_payload
    assert get_resp.decode_body() == get_payload
    assert client.post_calls[0]["headers"] == {"X-Test": "1"}
    assert client.get_calls[0]["url"] == "https://example.com/1"

