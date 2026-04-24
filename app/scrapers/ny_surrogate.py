from app.scrapers.base import BaseScraper


class NYSurrogateScraper(BaseScraper):
    """Scraper for NY Surrogate Court (websurrogates.nycourts.gov).

    Uses Scrapling's StealthyFetcher to handle CAPTCHA/anti-bot protection.
    Searches probate filings and extracts property addresses, decedent info.
    """

    name = "ny_surrogate"
    description = "NY Surrogate Courts - Probate records"

    BASE_URL = "https://websurrogates.nycourts.gov"

    # NY county surrogate court codes
    COUNTIES = {
        "New York": "31",
        "Kings": "24",
        "Queens": "41",
        "Bronx": "02",
        "Richmond": "43",
        "Nassau": "30",
        "Suffolk": "52",
        "Westchester": "60",
        "Erie": "15",
        "Monroe": "28",
    }

    def scrape(self, counties=None, date_range=None, max_pages=10) -> list[dict]:
        """Scrape NY Surrogate Court probate records.

        Args:
            counties: list of county names to search (default: all)
            date_range: tuple of (start_date, end_date) strings
            max_pages: max result pages to scrape per county
        """
        from scrapling.fetchers import StealthyFetcher

        target_counties = counties or list(self.COUNTIES.keys())
        results = []

        for county in target_counties:
            county_code = self.COUNTIES.get(county)
            if not county_code:
                continue

            try:
                # Navigate to the search page
                page = StealthyFetcher.fetch(
                    f"{self.BASE_URL}/Home/Welcome",
                    headless=True,
                    network_idle=True,
                    block_images=True,
                )

                if not page or not page.status == 200:
                    continue

                # Look for case search forms and extract data
                # The actual form structure will need to be mapped once
                # we can access the site through StealthyFetcher
                records = self._parse_search_results(page, county)
                results.extend(records)

            except Exception as e:
                # Log but continue with other counties
                print(f"Error scraping {county}: {e}")
                continue

        return results

    def _parse_search_results(self, page, county):
        """Parse search result rows into lead dicts."""
        records = []

        # Look for table rows with case data
        rows = page.css("table.results tr, table tbody tr, .case-row")
        for row in rows:
            try:
                cells = row.css("td")
                if len(cells) < 3:
                    continue

                record = {
                    "county": county,
                    "state": "NY",
                    "source_id": self._extract_text(cells, 0),  # case number
                    "case_number": self._extract_text(cells, 0),
                    "decedent_name": self._extract_text(cells, 1),
                    "filing_date": self._extract_text(cells, 2),
                }

                # Try to get address from case detail page
                detail_link = row.css("a[href]")
                if detail_link:
                    href = detail_link[0].attrib.get("href", "")
                    if href:
                        record["source_url"] = f"{self.BASE_URL}{href}"

                # Only include if we have meaningful data
                if record.get("case_number") and record.get("decedent_name"):
                    records.append(record)

            except Exception:
                continue

        return records

    def _extract_text(self, cells, index):
        if index < len(cells):
            text = cells[index].text
            return text.strip() if text else None
        return None
