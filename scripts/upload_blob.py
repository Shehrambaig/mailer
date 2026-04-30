"""
Upload a file to Vercel Blob, replacing the existing object at the same
pathname (so the public URL stays stable). Reads BLOB_READ_WRITE_TOKEN
from .env / .env.local.

Usage:
  python3 scripts/upload_blob.py app/static/data/zip-detail.json data/zip-detail.json

The second argument is the pathname inside the blob store (no leading slash).
"""
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
load_dotenv(ROOT / ".env.local", override=True)

TOKEN = os.getenv("BLOB_READ_WRITE_TOKEN")
if not TOKEN:
    sys.exit("BLOB_READ_WRITE_TOKEN not set (check .env.local)")

if len(sys.argv) != 3:
    sys.exit(f"usage: python3 {sys.argv[0]} <local-file> <blob-pathname>")

local = Path(sys.argv[1]).resolve()
pathname = sys.argv[2].lstrip("/")
if not local.exists():
    sys.exit(f"file not found: {local}")

size_mb = local.stat().st_size / (1024 * 1024)
print(f"Uploading {local} ({size_mb:.1f} MB) → blob://{pathname}")

# Vercel Blob HTTP API — PUT to https://blob.vercel-storage.com/<pathname>
# Headers:
#   Authorization: Bearer <BLOB_READ_WRITE_TOKEN>
#   x-content-type: <mime>
#   x-add-random-suffix: 0     (overwrite at fixed pathname)
#   x-cache-control-max-age: <seconds>
url = f"https://blob.vercel-storage.com/{pathname}"
headers = {
    "Authorization": f"Bearer {TOKEN}",
    "x-api-version": "7",
    "x-content-type": "application/json",
    "x-add-random-suffix": "0",
    "x-cache-control-max-age": "300",
    "access": "public",
}

with local.open("rb") as f:
    r = requests.put(url, headers=headers, data=f, timeout=600)

if not r.ok:
    print(f"ERROR {r.status_code}: {r.text[:500]}")
    sys.exit(1)

result = r.json()
print(f"OK — uploaded.")
print(f"  url:         {result.get('url')}")
print(f"  downloadUrl: {result.get('downloadUrl')}")
print(f"  pathname:    {result.get('pathname')}")
