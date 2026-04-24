from app.scrapers.ny_surrogate import NYSurrogateScraper

SCRAPER_REGISTRY = {
    "ny_surrogate": NYSurrogateScraper,
}


def get_scraper(name):
    """Get a scraper instance by name."""
    cls = SCRAPER_REGISTRY.get(name)
    if not cls:
        raise ValueError(f"Unknown scraper: {name}. Available: {list(SCRAPER_REGISTRY.keys())}")
    return cls()


def list_scrapers():
    """Return info about all registered scrapers."""
    return [
        {"name": name, "description": cls.description}
        for name, cls in SCRAPER_REGISTRY.items()
    ]
