"""
Idempotent downloader for all upstream market datasets.

Usage:
  python scripts/fetch_data.py             # fetch missing or stale (>7 days)
  python scripts/fetch_data.py --force     # re-download everything
  python scripts/fetch_data.py --only zillow_heat_zip realtor_zip

Files land in data/. URLs come from `app.ingest.sources.DATASETS` so the
dashboard's "source of truth" hyperlinks and what we actually download stay
in lockstep.

If a publisher renames a file, edit `app/ingest/sources.py` — the script
fails loudly on 404 rather than silently writing nothing.
"""
import argparse
import os
import sys
import time
import urllib.request
import urllib.error

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.ingest.sources import DATASETS  # noqa: E402

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
STALE_AFTER_SECONDS = 7 * 24 * 3600

# Map dataset key → on-disk filename in data/.
# Match existing filenames where they exist so we don't break market_data.py.
LOCAL_FILES = {
    "realtor_county":          "realtor_county.csv",
    "realtor_county_current":  "realtor_county_current.csv",
    "realtor_zip":             "realtor_zip.csv",
    "zillow_zhvi_county":      "zillow_zhvi_county.csv",
    "zillow_zhvi_zip":         "zillow_zhvi_zip.csv",
    "zillow_heat_county":      "zillow_heat_county.csv",
    "zillow_heat_zip":         "zillow_heat_zip.csv",
    "redfin_county":           "redfin_county_full.tsv.gz",
}


def _is_fresh(path):
    if not os.path.exists(path):
        return False
    age = time.time() - os.path.getmtime(path)
    return age < STALE_AFTER_SECONDS


def _download(url, dest):
    """Stream URL → temp file → atomic rename. Raises on HTTP error."""
    tmp = dest + ".part"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 mailer-fetch"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        if resp.status >= 400:
            raise RuntimeError(f"HTTP {resp.status} for {url}")
        with open(tmp, "wb") as f:
            while True:
                chunk = resp.read(64 * 1024)
                if not chunk:
                    break
                f.write(chunk)
    os.replace(tmp, dest)
    return os.path.getsize(dest)


def fetch(key, force=False):
    ds = DATASETS.get(key)
    if not ds:
        print(f"  [skip] unknown dataset key: {key}")
        return False
    if not ds.get("file_url"):
        print(f"  [skip] {key} — no direct file_url (manual download required)")
        return False
    fname = LOCAL_FILES.get(key)
    if not fname:
        print(f"  [skip] {key} — no local filename mapped")
        return False
    dest = os.path.join(DATA_DIR, fname)
    if not force and _is_fresh(dest):
        print(f"  [fresh] {fname}  (age < 7d, use --force to redownload)")
        return True
    print(f"  [get]  {fname}  ←  {ds['file_url']}")
    try:
        size = _download(ds["file_url"], dest)
        print(f"         wrote {size/1024/1024:.1f} MB")
        return True
    except urllib.error.HTTPError as e:
        print(f"  [FAIL] {key}: HTTP {e.code} — file may have been renamed; check {ds['landing']}")
        return False
    except Exception as e:
        print(f"  [FAIL] {key}: {e}")
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="redownload even if fresh")
    ap.add_argument("--only", nargs="+", help="dataset keys to fetch (default: all)")
    args = ap.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)
    keys = args.only or list(LOCAL_FILES.keys())

    print(f"Fetching into {DATA_DIR}")
    ok = 0
    for k in keys:
        if fetch(k, force=args.force):
            ok += 1
    print(f"\nDone. {ok}/{len(keys)} datasets present and fresh.")


if __name__ == "__main__":
    main()
