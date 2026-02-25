# Import state scrapers to trigger registration via @register_scraper decorator.
# Note: The registry also auto-discovers modules via pkgutil.iter_modules.
# California uses BaseScraper directly (config-only, no custom code needed).
from src.scrapers.states import new_york  # noqa: F401
from src.scrapers.states import ohio  # noqa: F401
from src.scrapers.states import oregon  # noqa: F401
from src.scrapers.states import pennsylvania  # noqa: F401
from src.scrapers.states import texas  # noqa: F401
from src.scrapers.states import virginia  # noqa: F401
