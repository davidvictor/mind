"""Build template variables from the user config for prompt injection.

Every prompt that mentions who the user is should call ``build_identity_vars()``
and substitute the returned dict into its template strings. This removes all
hardcoded identity from prompts and makes the system configurable for any user.
"""
from __future__ import annotations

import functools
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scripts.common.config import BrainConfig


@functools.lru_cache(maxsize=1)
def _cached_vars(
    name: str,
    role: str,
    business: tuple[str, ...],
    personal: tuple[str, ...],
    exclude: tuple[str, ...],
    rules: tuple[str, ...],
) -> dict[str, str]:
    """Inner cache keyed on immutable tuple copies of config lists."""
    return {
        "user_name": name,
        "user_role": role,
        "business_description": ", ".join(business),
        "personal_description": ", ".join(personal),
        "exclude_description": ", ".join(exclude),
        "classification_rules": "\n".join(f"- {r}" for r in rules),
    }


def build_identity_vars(cfg: BrainConfig | None = None) -> dict[str, str]:
    """Build template variables from user config. Cached per process.

    Returns a dict with keys:
        user_name, user_role, business_description,
        personal_description, exclude_description, classification_rules
    """
    if cfg is None:
        from scripts.common.config import BrainConfig as _BC
        cfg = _BC.defaults()
    u = cfg.user
    return _cached_vars(
        name=u.name,
        role=u.role,
        business=tuple(u.business_interests),
        personal=tuple(u.personal_interests),
        exclude=tuple(u.exclude_always),
        rules=tuple(u.classification_rules),
    )
