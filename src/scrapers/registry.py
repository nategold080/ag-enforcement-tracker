"""Scraper registry and factory.

Loads state configurations from config/states.yaml and instantiates
the appropriate scraper class for each state.

States that need custom scraper code register their class here.
States without custom code use the BaseScraper with config-only operation.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Type

import yaml

from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "states.yaml"

_states_imported = False


def _ensure_states_imported():
    """Import all state scraper modules to trigger @register_scraper decorators."""
    global _states_imported
    if _states_imported:
        return
    _states_imported = True
    import importlib
    import pkgutil
    import src.scrapers.states as states_pkg
    for importer, modname, ispkg in pkgutil.iter_modules(states_pkg.__path__):
        importlib.import_module(f"src.scrapers.states.{modname}")

# Registry of custom scraper classes keyed by state config key (e.g., "california")
_CUSTOM_SCRAPERS: dict[str, Type[BaseScraper]] = {}


def register_scraper(state_key: str):
    """Decorator to register a custom scraper class for a state."""
    def decorator(cls: Type[BaseScraper]):
        _CUSTOM_SCRAPERS[state_key] = cls
        return cls
    return decorator


def load_state_configs(config_path: Path | None = None) -> dict[str, dict]:
    """Load all state configs from the YAML file."""
    path = config_path or CONFIG_PATH
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def get_scraper(state_key: str, config_path: Path | None = None) -> BaseScraper:
    """Create and return a scraper instance for the given state.

    Uses a custom scraper class if one is registered, otherwise uses
    BaseScraper with the YAML config.

    Args:
        state_key: The key in states.yaml (e.g., "california", "new_york")
        config_path: Optional override for the config file path.
    """
    configs = load_state_configs(config_path)

    if state_key not in configs:
        available = ", ".join(sorted(configs.keys()))
        raise ValueError(f"Unknown state: {state_key!r}. Available: {available}")

    config = configs[state_key]
    if not config.get("active", False):
        logger.warning("State %s is not active. Creating scraper anyway.", state_key)

    # Ensure state modules are imported so @register_scraper decorators fire
    _ensure_states_imported()

    # Use custom class if registered, otherwise use BaseScraper
    cls = _CUSTOM_SCRAPERS.get(state_key, BaseScraper)
    return cls(config)


def get_active_states(config_path: Path | None = None) -> list[str]:
    """Return list of state keys that have active=true in config."""
    configs = load_state_configs(config_path)
    return [key for key, cfg in configs.items() if cfg.get("active", False)]


def get_state_code(state_key: str, config_path: Path | None = None) -> str:
    """Return the two-letter state code for a state key."""
    configs = load_state_configs(config_path)
    if state_key not in configs:
        raise ValueError(f"Unknown state: {state_key!r}")
    return configs[state_key]["code"]


def state_key_from_code(code: str, config_path: Path | None = None) -> str | None:
    """Look up state key from a two-letter code (e.g., 'CA' → 'california')."""
    configs = load_state_configs(config_path)
    code = code.upper()
    for key, cfg in configs.items():
        if cfg.get("code", "").upper() == code:
            return key
    return None
