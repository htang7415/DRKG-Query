from __future__ import annotations

import hashlib
import re


NON_ALNUM = re.compile(r"[^A-Za-z0-9]+")


def sanitize_relation_type(raw_relation: str, *, prefix: str = "REL", hash_chars: int = 8) -> str:
    sanitized = NON_ALNUM.sub("_", raw_relation).strip("_").upper()
    digest = hashlib.sha1(raw_relation.encode("utf-8")).hexdigest()[:hash_chars].upper()
    return f"{prefix}_{sanitized}__{digest}"

