from __future__ import annotations


def classify_geography_counts(states: int, counties: int) -> str:
    """Reconoce el seed de 52 jurisdicciones y distingue vacío/completo/parcial."""
    if (states, counties) == (52, 0):
        return "pending"
    if (states, counties) == (52, 83):
        return "complete"
    raise RuntimeError(
        "Gold Geography está en un estado parcial o inesperado: "
        f"estados={states}/52, condados={counties}/83."
    )
