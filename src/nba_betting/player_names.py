from __future__ import annotations

from typing import Any

import unicodedata


_SUFFIX_TOKENS = frozenset({"JR", "SR", "II", "III", "IV", "V"})
_CANONICAL_PLAYER_NAME_ALIASES = {
    "CARLTON CARRINGTON": "BUB CARRINGTON",
    "HERB JONES": "HERBERT JONES",
    "MOE WAGNER": "MORITZ WAGNER",
}


def normalize_player_name_key(value: Any, *, case: str = "upper") -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "(" in text:
        text = text.split("(", 1)[0]
    text = text.replace("-", " ")
    text = text.replace(".", "").replace("'", "").replace(",", " ")
    text = " ".join(text.split())
    if not text:
        return ""
    try:
        text = unicodedata.normalize("NFKD", text)
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        text = text.encode("ascii", "ignore").decode("ascii")
    except Exception:
        pass
    tokens = [token for token in text.upper().split() if token not in _SUFFIX_TOKENS]
    if not tokens:
        return ""
    normalized = " ".join(tokens)
    normalized = _CANONICAL_PLAYER_NAME_ALIASES.get(normalized, normalized)
    if case == "lower":
        return normalized.lower()
    return normalized


def short_player_key(value: Any, *, case: str = "upper") -> str:
    normalized = normalize_player_name_key(value, case="upper")
    if not normalized:
        return ""
    parts = normalized.split()
    key = f"{parts[-1]}{parts[0][0]}"
    if case == "lower":
        return key.lower()
    return key