"""Small, deliberately constrained configuration template interpolation."""

from __future__ import annotations

import re
from collections.abc import Mapping


class TemplateError(ValueError):
    """A template is malformed or references an unknown value."""


_TOKEN = re.compile(r"^(context|task|target)\.([A-Za-z_][A-Za-z0-9_.-]*)$")


def expand_template(
    value: str,
    scopes: Mapping[str, Mapping[str, object]],
    *,
    deferred: frozenset[str] = frozenset(),
) -> str:
    """Expand `${scope.name}` values while optionally preserving known late values."""
    pieces: list[str] = []
    position = 0
    while True:
        start = value.find("${", position)
        if start < 0:
            pieces.append(value[position:])
            break
        pieces.append(value[position:start])
        end = value.find("}", start + 2)
        if end < 0:
            raise TemplateError(f"unterminated template in {value!r}")
        token = value[start + 2 : end]
        match = _TOKEN.fullmatch(token)
        if match is None:
            raise TemplateError(f"invalid template ${{{token}}}")
        scope_name, key = match.groups()
        full_name = f"{scope_name}.{key}"
        if full_name in deferred:
            pieces.append(value[start : end + 1])
        else:
            scope = scopes.get(scope_name, {})
            if key not in scope:
                raise TemplateError(f"missing template value: {full_name}")
            replacement = scope[key]
            if replacement is None:
                raise TemplateError(f"missing template value: {full_name}")
            pieces.append(str(replacement))
        position = end + 1
    return "".join(pieces)
