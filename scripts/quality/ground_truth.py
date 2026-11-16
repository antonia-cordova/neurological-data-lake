from __future__ import annotations

from decimal import Decimal


def same_exported_value(actual, expected: str) -> bool:
    """Compara un valor PostgreSQL con su representación en el CSV de cambios."""
    if expected == "":
        expected = None
    if actual is None:
        return expected is None
    if expected is None:
        return False
    if isinstance(actual, (int, float, Decimal)):
        try:
            return Decimal(str(actual)) == Decimal(str(expected))
        except Exception:
            return False
    return str(actual) == str(expected)
