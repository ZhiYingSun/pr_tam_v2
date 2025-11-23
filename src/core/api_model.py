import json
import base64
import binascii
from typing import Optional, Any, List, Dict
from pydantic import BaseModel, Field

class ZyteHttpResponse(BaseModel):
    httpResponseBody: Optional[str] = Field(None, description="Base64 encoded response body")

    def decode_body(self) -> Dict[str, Any]:
        """Decode base64 response body and parse JSON."""
        if not self.httpResponseBody:
            raise ValueError("httpResponseBody is missing or empty")

        try:
            decoded_body = base64.b64decode(self.httpResponseBody).decode('utf-8')
            return json.loads(decoded_body)
        except (binascii.Error, UnicodeDecodeError) as e:
            raise ValueError(f"Failed to decode base64 response body: {e}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON from decoded body: {e}")