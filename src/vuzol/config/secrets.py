"""Consumer-scoped resolution of secret references."""

import os
from collections.abc import Mapping
from pathlib import Path

from pydantic import SecretStr


class SecretResolutionError(ValueError):
    """Secret is unavailable or outside the caller's scope."""


class ScopedSecretResolver:
    def __init__(
        self,
        *,
        access_policy: Mapping[str, frozenset[str]],
        secret_file_root: Path,
        environment: Mapping[str, str] | None = None,
    ) -> None:
        self._access_policy = dict(access_policy)
        self._secret_file_root = secret_file_root.resolve()
        self._environment = environment if environment is not None else os.environ

    def get(self, reference: str, consumer_scope: str) -> SecretStr:
        allowed_consumers = self._access_policy.get(reference, frozenset())
        if consumer_scope not in allowed_consumers:
            raise SecretResolutionError(
                f"consumer {consumer_scope} is not allowed to resolve secret reference"
            )
        scheme, separator, location = reference.partition(":")
        if not separator or not location:
            raise SecretResolutionError("invalid secret reference")
        if scheme == "env":
            value = self._environment.get(location)
            if not value:
                raise SecretResolutionError(f"missing environment secret: {location}")
            return SecretStr(value)
        if scheme == "file":
            requested = Path(location)
            path = requested if requested.is_absolute() else self._secret_file_root / requested
            path = path.resolve()
            try:
                path.relative_to(self._secret_file_root)
            except ValueError as error:
                raise SecretResolutionError("secret file escapes configured root") from error
            try:
                value = path.read_text().rstrip("\n")
                if not value:
                    raise SecretResolutionError(f"empty secret file: {path.name}")
                return SecretStr(value)
            except OSError as error:
                raise SecretResolutionError(f"cannot read secret file: {path.name}") from error
        raise SecretResolutionError(f"unsupported secret reference scheme: {scheme}")
