from typing import Protocol, Dict, Any, Optional
from src.core.api_models import ZyteHttpResponse


class ZyteClientProtocol(Protocol):
    async def __aenter__(self):
        """Async context manager entry."""
        ...

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        ...

    async def close(self):
        """Close the client session."""
        ...

    async def post_request(
            self,
            url: str,
            request_body: Dict[str, Any],
            headers: Optional[Dict[str, str]] = None
    ) -> ZyteHttpResponse:
        ...

    async def get_request(
            self,
            url: str,
            headers: Optional[Dict[str, str]] = None
    ) -> ZyteHttpResponse:
        ...


class OpenAIClientProtocol(Protocol):

    async def chat_completion(
            self,
            model: str,
            messages: list,
            temperature: float = 0.2,
            response_format: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        ...

