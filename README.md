# Open Access Art Bulk Downloader (Python)

A small collection of Python utilities for **searching metadata and bulk-downloading high-resolution images** from multiple open-access / public museum sources and Wikimedia Commons, with **polite rate limiting**, **retries**, and **structured outputs** (images + per-item metadata JSON/CSV). :contentReference[oaicite:0]{index=0}

## What this does

This module provides download helpers for:

- **The Metropolitan Museum of Art (The Met)**  
  - Enrich a Met metadata DataFrame by hitting the Met object endpoint (adds primary image URLs, additional image URLs, and public-domain flag).  
  - Download images from the enriched URLs without further API calls.
- **Art Institute of Chicago (AIC)**  
  - Uses an **already-downloaded AIC JSON dump** (`artic-api-data`) and IIIF to fetch max-res images.
- **Cleveland Museum of Art (CMA)**  
  - Uses the CMA Open Access API; selects **only the best single image URL per record** (prefers “print” when available).
- **Smithsonian Open Access (SI)**  
  - Uses the SI Open Access API (requires an API key).  
  - Deduplicates “same asset different sizes” and keeps the **best resolution** variant.
- **Wikimedia Commons**
  - Searches and downloads images with robust filtering:
    - optional public domain constraint
    - allowed MIME/ext
    - max file size cap
    - throttled download speed
    - requires a Wikimedia-policy-friendly User-Agent with contact info

There’s also a convenience orchestrator:

- `multi_download(...)` — runs Met + AIC + CMA + Smithsonian + Commons for one search term.

---

## Repository layout (suggested)

```text
.
├── utils.py                  # this module (the download utilities)
├── README.md                 # this file
├── requirements.txt          # dependencies
└── (optional)
    ├── data/
    │   └── artic-api-data/   # AIC dump root
    └── outputs/
        └── <search_term>/
            ├── images/
            └── metadata/
```

## Configuration

pip install requests pandas


A) Smithsonian API key (required for Smithsonian)

You need an API key for the Smithsonian Open Access API.

Either pass it as api_key=... to download_smithsonian_by_search(...)
Or set an environment variable.

export smithsonian_key="YOUR_KEY"

B) Wikimedia Commons User-Agent (required)

Wikimedia strongly prefers that automated tools use a descriptive User-Agent including contact info.

Example:

ua = "MyArtDownloader/1.0 (https://example.com; you@example.com)"

If your UA doesn’t contain a URL or an email, the Commons downloader will raise a ValueError.

C) Art Institute of Chicago dump root (required for AIC)
AIC downloading expects a local JSON dump root containing:
```
artic-api-data/
└── json/
    ├── config.json
    └── artworks/
        ├── 1.json
        ├── 2.json
        └── ...
```

The module has a constant AIC_DUMP_ROOT pointing to a local path. You can:

edit that constant, or

pass dump_root=Path("...") into download_aic_by_search(...).



## Example usage

import pandas as pd
from utils import multi_download

met_df = pd.read_csv("met_objects.csv")
ua = "MyArtDownloader/1.0 (https://example.com; you@example.com)"

multi_download(
    multi_search_term="rembrandt",
    multi_save_directory="outputs",
    require_public_domain=True,
    ua=ua,
    met_df=met_df,
)












