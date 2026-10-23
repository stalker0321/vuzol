"""OAuth resource server verification; authorization is an external IdP's job."""

import asyncio
from urllib.parse import urlsplit

import jwt
from mcp.server.auth.provider import AccessToken
from pydantic import BaseModel, ConfigDict, Field, model_validator

from vuzol.leadership.access import ProjectAccess


class MCPConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    resource_url: str
    issuer_url: str
    jwks_url: str
    # Keys are verified OAuth sub values, never model-provided identities.
    subjects: dict[str, ProjectAccess] = Field(min_length=1)
    clients: dict[str, str] = Field(default_factory=dict)
    port: int = Field(default=8093, ge=1024, le=65535)

    @model_validator(mode="after")
    def secure_urls(self) -> "MCPConfig":
        for value in (self.resource_url, self.issuer_url, self.jwks_url):
            parsed = urlsplit(value)
            if (
                parsed.scheme != "https"
                or not parsed.hostname
                or parsed.username is not None
                or parsed.password is not None
                or parsed.query
                or parsed.fragment
            ):
                raise ValueError("MCP URLs must be absolute HTTPS URLs without credentials/query")
        if urlsplit(self.resource_url).path != "/mcp":
            raise ValueError("resource_url must end in /mcp")
        if any(not subject.strip() for subject in self.subjects):
            raise ValueError("OAuth subject must not be empty")
        return self


class OAuthVerifier:
    def __init__(self, config: MCPConfig) -> None:
        self.config = config
        # Never follow a token-supplied jku/x5u. Keys come only from operator config.
        self.keys = jwt.PyJWKClient(config.jwks_url, timeout=5, lifespan=300)

    async def verify_token(self, token: str) -> AccessToken | None:
        if len(token) > 16_384:
            return None
        try:
            return await asyncio.to_thread(self._verify, token)
        except (jwt.PyJWTError, ValueError, TypeError, OSError):
            return None

    def _verify(self, token: str) -> AccessToken | None:
        key = self.keys.get_signing_key_from_jwt(token).key
        claims = jwt.decode(
            token,
            key,
            algorithms=["RS256"],
            audience=self.config.resource_url,
            issuer=self.config.issuer_url,
            options={"require": ["exp", "iat", "sub", "iss", "aud"]},
        )
        subject, scope = claims.get("sub"), claims.get("scope", "")
        if not isinstance(subject, str) or subject not in self.config.subjects:
            return None
        if not isinstance(scope, str) or "vuzol:read" not in scope.split():
            return None
        return AccessToken(
            token=token,
            client_id=str(claims.get("client_id") or claims.get("azp") or subject),
            subject=subject,
            scopes=scope.split(),
            expires_at=int(claims["exp"]),
            resource=self.config.resource_url,
        )
