"""
Single source-of-truth registry for every dataset/metric the dashboard renders.

Every metric exposed by `app.ingest.market_data` and every aggregate computed
in `dump_static.py` should appear here. The dashboard reads `sources.json`
(produced by `dump_static.py`) and renders hyperlinks next to each metric.

Each entry has:
  publisher  — "Realtor.com", "Zillow", etc.
  dataset    — human label of the file/feed
  landing    — public landing page (the canonical "source of truth" link)
  file_url   — direct download (None if not redistributable)
  cadence    — refresh frequency
"""

PUBLISHERS = {
    "realtor": {
        "name": "Realtor.com",
        "landing": "https://www.realtor.com/research/data/",
    },
    "zillow": {
        "name": "Zillow",
        "landing": "https://www.zillow.com/research/data/",
    },
    "redfin": {
        "name": "Redfin",
        "landing": "https://www.redfin.com/news/data-center/",
    },
    "census": {
        "name": "U.S. Census ACS",
        "landing": "https://www.census.gov/programs-surveys/acs",
    },
    "fhfa": {
        "name": "FHFA HPI",
        "landing": "https://www.fhfa.gov/data/hpi",
    },
}

DATASETS = {
    "realtor_county": {
        "publisher": "realtor",
        "dataset": "Inventory Core Metrics — County History",
        "landing": "https://www.realtor.com/research/data/",
        "file_url": "https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_County_History.csv",
        "cadence": "monthly",
    },
    "realtor_county_current": {
        "publisher": "realtor",
        "dataset": "Inventory Core Metrics — County (current month)",
        "landing": "https://www.realtor.com/research/data/",
        "file_url": "https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_County.csv",
        "cadence": "monthly",
    },
    "realtor_zip": {
        "publisher": "realtor",
        "dataset": "Inventory Core Metrics — ZIP History",
        "landing": "https://www.realtor.com/research/data/",
        "file_url": "https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_Zip_History.csv",
        "cadence": "monthly",
    },
    "zillow_zhvi_county": {
        "publisher": "zillow",
        "dataset": "ZHVI — All Homes (county, smoothed, seasonally adjusted)",
        "landing": "https://www.zillow.com/research/data/",
        "file_url": "https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv",
        "cadence": "monthly",
    },
    "zillow_zhvi_zip": {
        "publisher": "zillow",
        "dataset": "ZHVI — All Homes (ZIP, smoothed, seasonally adjusted)",
        "landing": "https://www.zillow.com/research/data/",
        "file_url": "https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv",
        "cadence": "monthly",
    },
    "zillow_heat_county": {
        "publisher": "zillow",
        "dataset": "Market Heat Index (county) — buyer/seller market score",
        "landing": "https://www.zillow.com/research/data/",
        "file_url": "https://files.zillowstatic.com/research/public_csvs/market_temp_index/County_market_temp_index_uc_sfrcondo_month.csv",
        "cadence": "monthly",
    },
    "zillow_heat_zip": {
        "publisher": "zillow",
        "dataset": "Market Heat Index (ZIP) — buyer/seller market score",
        "landing": "https://www.zillow.com/research/data/",
        "file_url": "https://files.zillowstatic.com/research/public_csvs/market_temp_index/Zip_market_temp_index_uc_sfrcondo_month.csv",
        "cadence": "monthly",
    },
    "redfin_county": {
        "publisher": "redfin",
        "dataset": "Market Tracker — County (median sale, DOM, sale-to-list)",
        "landing": "https://www.redfin.com/news/data-center/",
        "file_url": "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/county_market_tracker.tsv000.gz",
        "cadence": "monthly",
    },
    "fhfa_hpi_state": {
        "publisher": "fhfa",
        "dataset": "House Price Index — State, quarterly",
        "landing": "https://www.fhfa.gov/data/hpi",
        "file_url": None,
        "cadence": "quarterly",
    },
    "census_income": {
        "publisher": "census",
        "dataset": "ACS 5-year — Median Household Income (B19013)",
        "landing": "https://www.census.gov/programs-surveys/acs",
        "file_url": "https://api.census.gov/data/2022/acs/acs5?get=B19013_001E,NAME&for=county:*",
        "cadence": "annual",
    },
}

# Per-metric source mapping. Keys match field names produced by market_data
# and rendered by the dashboard. UI uses this to add a hyperlink per metric.
METRICS = {
    # Composite scores (computed locally)
    "golden_score":      {"label": "Golden Zone Score",   "datasets": ["realtor_county", "zillow_zhvi_county", "redfin_county"]},
    "buy_score":         {"label": "Buy Opportunity",     "datasets": ["realtor_county", "redfin_county", "zillow_zhvi_county"]},
    "exit_score":        {"label": "Exit Speed",          "datasets": ["realtor_county", "redfin_county"]},

    # Realtor.com metrics
    "realtor_dom":           {"label": "Days on Market",        "datasets": ["realtor_county"]},
    "realtor_list_price":    {"label": "Median List Price",     "datasets": ["realtor_county"]},
    "realtor_list_price_yy": {"label": "List Price YoY",        "datasets": ["realtor_county"]},
    "realtor_active":        {"label": "Active Listings",       "datasets": ["realtor_county"]},
    "realtor_new_listings":  {"label": "New Listings",          "datasets": ["realtor_county"]},
    "realtor_pending":       {"label": "Pending Listings",      "datasets": ["realtor_county"]},
    "realtor_pending_ratio": {"label": "Pending Ratio",         "datasets": ["realtor_county"]},
    "realtor_price_reduced": {"label": "Price Reduction Share", "datasets": ["realtor_county"]},
    "realtor_ppsf":          {"label": "Price / Sq Ft",         "datasets": ["realtor_county"]},

    # Zillow metrics
    "zillow_zhvi":      {"label": "Home Value (ZHVI)",          "datasets": ["zillow_zhvi_county"]},
    "zillow_yoy":       {"label": "Home Value YoY",             "datasets": ["zillow_zhvi_county"]},
    "zillow_5yr":       {"label": "5-yr Appreciation",          "datasets": ["zillow_zhvi_county"]},
    "zillow_heat":      {"label": "Market Heat Index",          "datasets": ["zillow_heat_county"]},
    "zillow_heat_class":{"label": "Market Classification",      "datasets": ["zillow_heat_county"]},

    # Redfin metrics
    "redfin_sale_price":   {"label": "Median Sale Price",  "datasets": ["redfin_county"]},
    "redfin_dom":          {"label": "Days on Market",     "datasets": ["redfin_county"]},
    "redfin_supply":       {"label": "Months of Supply",   "datasets": ["redfin_county"]},
    "redfin_sale_to_list": {"label": "Sale-to-List Ratio", "datasets": ["redfin_county"]},
    "redfin_sold_above":   {"label": "Sold Above List %",  "datasets": ["redfin_county"]},
    "redfin_homes_sold":   {"label": "Homes Sold",         "datasets": ["redfin_county"]},
    "redfin_inventory":    {"label": "Inventory",          "datasets": ["redfin_county"]},

    # External
    "income": {"label": "Median Household Income", "datasets": ["census_income"]},
}


def metric_source(metric_key):
    """Return primary source dict for a metric, or None."""
    m = METRICS.get(metric_key)
    if not m or not m["datasets"]:
        return None
    ds_key = m["datasets"][0]
    ds = DATASETS.get(ds_key)
    if not ds:
        return None
    pub = PUBLISHERS.get(ds["publisher"], {})
    return {
        "publisher": pub.get("name", ds["publisher"]),
        "dataset": ds["dataset"],
        "url": ds["landing"],
    }


def build_index():
    """Return JSON-serializable dict for the dashboard."""
    return {
        "publishers": PUBLISHERS,
        "datasets": DATASETS,
        "metrics": {
            k: {
                "label": v["label"],
                "datasets": v["datasets"],
                "primary": metric_source(k),
            }
            for k, v in METRICS.items()
        },
    }
