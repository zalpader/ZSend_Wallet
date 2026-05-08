from __future__ import annotations

import json
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any


_MODULE_ROOT = Path(__file__).resolve().parent
_APP_ROOT = _MODULE_ROOT.parent
_DEFAULT_LANG = "en"


def _locales_dir() -> Path:
    if hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS) / "locales"
    return _MODULE_ROOT / "locales"


@lru_cache(maxsize=8)
def load_locale(lang: str = _DEFAULT_LANG) -> dict[str, Any]:
    path = _locales_dir() / f"{lang}.json"
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def tr(key: str, default: str | None = None, lang: str = _DEFAULT_LANG, **kwargs: Any) -> str:
    data: Any = load_locale(lang)
    for part in key.split("."):
        if not isinstance(data, dict) or part not in data:
            text = default if default is not None else key
            break
        data = data[part]
    else:
        text = data if isinstance(data, str) else (default if default is not None else key)

    if kwargs:
        try:
            return text.format(**kwargs)
        except Exception:
            return text
    return text
