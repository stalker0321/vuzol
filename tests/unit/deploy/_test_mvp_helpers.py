import hashlib
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).parents[3]

__all__ = [
    "ROOT",
    "MagicMock",
    "ModuleType",
    "Path",
    "_module",
    "hashlib",
    "importlib",
    "json",
    "os",
    "pytest",
    "subprocess",
    "sys",
]


def _module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module
