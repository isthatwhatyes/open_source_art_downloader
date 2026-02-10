from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import ReadTimeout, ConnectTimeout, Timeout



import html
import requests
import json
import re
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple

import os
from urllib.parse import urlparse
import pandas as pd

from motifs import MOTIF_BANK
from animals import animal_bank
from utils import *

import csv
import pathlib
import hashlib
import mimetypes
import threading
import random
import urllib.parse





# YOU set this to your extracted dump root, e.g. Path("/data/aic/artic_dump")
AIC_DUMP_ROOT = Path("/home/boul/github/download_art/artic-api-data")

MET_API_BASE = "https://collectionapi.metmuseum.org/public/collection/v1"

_slug_re = re.compile(r"[^-\w]+", re.UNICODE)
UA = "MetDownloader/EnrichThenDownload/1.0 (non-commercial; research/education)"

# -----------------------------
# Helpers: normalization + text extraction
# -----------------------------

def _normalize_text(s: str) -> str:
    """
    Robust normalization:
      - Unicode NFKD (strip accents)
      - lower-case
      - convert any non-alnum to spaces
      - collapse whitespace
    """
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _safe_path_component(s: str, max_len: int = 80) -> str:
    """
    Safe folder/file component for most filesystems.
    Also strips Windows-forbidden trailing dots/spaces and forbidden characters.
    """
    s_norm = _normalize_text(s)
    if not s_norm:
        return "unknown"
    s_norm = s_norm.replace(" ", "_")

    # Windows forbidden characters: < > : " / \ | ? *
    s_norm = re.sub(r'[<>:"/\\|?*]+', "_", s_norm)

    # Avoid trailing dots/spaces (Windows)
    s_norm = s_norm.strip(" ._")
    if not s_norm:
        s_norm = "unknown"

    return s_norm[:max_len]


def _flatten_strings(obj: Any) -> Iterator[str]:
    """
    Recursively yield string representations of primitive values in a JSON-like structure.
    This makes the search term apply to "any of the tags or metadata".
    """
    if obj is None:
        return
    if isinstance(obj, str):
        yield obj
        return
    if isinstance(obj, (int, float, bool)):
        yield str(obj)
        return
    if isinstance(obj, list):
        for v in obj:
            yield from _flatten_strings(v)
        return
    if isinstance(obj, dict):
        for v in obj.values():
            yield from _flatten_strings(v)
        return
    yield str(obj)


def _matches_query(flat_text_norm: str, query_norm: str) -> bool:
    """
    Robust partial matching:
    - Split query into tokens; require each token appear as substring in normalized text.
    """
    if not query_norm:
        return False
    tokens = query_norm.split()
    return all(tok in flat_text_norm for tok in tokens)


# -----------------------------
# Helpers: rate limiting + download
# -----------------------------

@dataclass
class _RateLimiter:
    min_interval_s: float
    _last_t: float = 0.0

    def wait(self) -> None:
        if self.min_interval_s <= 0:
            return
        now = time.monotonic()
        dt = now - self._last_t
        if dt < self.min_interval_s:
            time.sleep(self.min_interval_s - dt)
        self._last_t = time.monotonic()


def _load_aic_config(dump_root: Path) -> Dict[str, Any]:
    cfg_path = dump_root / "json" / "config.json"
    if not cfg_path.exists():
        return {}
    return json.loads(cfg_path.read_text(encoding="utf-8"))


def _iter_artwork_files(dump_root: Path) -> Iterable[Path]:
    artworks_dir = dump_root / "json" / "artworks"
    if not artworks_dir.exists():
        raise FileNotFoundError(f"Expected artworks directory at: {artworks_dir}")
    return sorted(artworks_dir.glob("*.json"))


def _read_artwork_data(fp: Path) -> Dict[str, Any]:
    obj = json.loads(fp.read_text(encoding="utf-8"))
    return obj.get("data", obj)


def _iiif_full_url(iiif_base: str, image_id: str) -> str:
    """
    IIIF max-res fetch (AIC uses IIIF Image API):
      /{identifier}/full/full/0/default.jpg
    """
    return f"{iiif_base.rstrip('/')}/{image_id}/full/full/0/default.jpg"


def _download_stream(
    session: requests.Session,
    url: str,
    out_path: Path,
    *,
    timeout_s: int = 60,
    max_retries: int = 4,
) -> bool:
    """
    Streaming download with retries; writes to .part then renames.
    Returns True if downloaded (or already existed), False if failed.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and out_path.stat().st_size > 0:
        return True

    tmp_path = out_path.with_suffix(out_path.suffix + ".part")

    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            with session.get(url, stream=True, timeout=timeout_s) as r:
                if r.status_code == 404:
                    return False
                r.raise_for_status()

                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            f.write(chunk)

            tmp_path.replace(out_path)
            return True

        except (requests.RequestException, OSError):
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except OSError:
                pass

            if attempt < max_retries:
                time.sleep(backoff)
                backoff *= 2.0

    return False



# -----------------------------
# Cleveland Museum of Art (CMA) + Smithsonian (SI) helpers
# -----------------------------

def _guess_ext_from_url(url: str, default: str = ".jpg") -> str:
    try:
        path = urlparse(url).path
        for ext in (".jpg", ".jpeg", ".png", ".tif", ".tiff", ".webp", ".gif"):
            if path.lower().endswith(ext):
                return ext if ext.startswith(".") else "." + ext
    except Exception:
        pass
    return default


def _looks_like_http_url(s: str) -> bool:
    return isinstance(s, str) and (s.startswith("http://") or s.startswith("https://"))


def _pick_cma_image_urls(record: Dict[str, Any], *, prefer_print: bool = True) -> List[str]:
    """
    CMA record: pick ONLY the best single image URL.
    Prefer print when available (usually higher res), otherwise web.
    """
    images = record.get("images") or {}

    def get_url(key: str) -> str | None:
        v = images.get(key) or {}
        u = v.get("url")
        return u if _looks_like_http_url(u) else None

    u_print = get_url("print")
    u_web = get_url("web")

    if prefer_print and u_print:
        return [u_print]
    if u_web:
        return [u_web]
    if u_print:
        return [u_print]
    return []



def _smithsonian_is_cc0(row: Dict[str, Any]) -> bool:
    """
    Smithsonian: CC0 is commonly reflected in descriptiveNonRepeating.metadata_usage.access. :contentReference[oaicite:9]{index=9}
    Some records also include usage in online_media.media[].usage.access. :contentReference[oaicite:10]{index=10}
    """
    content = row.get("content") or {}
    dnr = content.get("descriptiveNonRepeating") or {}
    mu = dnr.get("metadata_usage") or {}
    if isinstance(mu, dict) and (mu.get("access") == "CC0"):
        return True

    # Fallback: check online_media usage
    om = dnr.get("online_media") or {}
    media = om.get("media") or []
    if isinstance(media, list):
        for m in media:
            if not isinstance(m, dict):
                continue
            usage = m.get("usage")
            if isinstance(usage, dict) and usage.get("access") == "CC0":
                return True
    return False


def _pick_smithsonian_image_urls(row: Dict[str, Any], *, allow_thumbnails: bool = False) -> List[str]:
    """
    Smithsonian: choose only the best-resolution variant per underlying asset.

    We gather candidates from:
      descriptiveNonRepeating.online_media.media[].resources[].url
      media[].content (sometimes a direct URL)
      media[].thumbnail (optional)
    Then canonicalize by underlying asset and keep the highest score.
    """
    content = row.get("content") or {}
    dnr = content.get("descriptiveNonRepeating") or {}
    om = dnr.get("online_media") or {}
    media = om.get("media") or []

    candidates: list[tuple[str, int, str]] = []

    if isinstance(media, list):
        for m in media:
            if not isinstance(m, dict):
                continue

            # Sometimes media has a label or type-ish field
            media_label = m.get("label") or m.get("type") or ""

            # resources[].url (+ optional label)
            resources = m.get("resources") or []
            if isinstance(resources, list):
                for r in resources:
                    if not isinstance(r, dict):
                        continue
                    u = r.get("url")
                    if _looks_like_http_url(u):
                        lbl = r.get("label") or r.get("type") or media_label or "resource"
                        key = _si_canonical_asset_key(u)
                        score = _si_url_size_score(u, label=str(lbl))
                        candidates.append((key, score, u))

            # sometimes "content" is itself a URL
            u_content = m.get("content")
            if _looks_like_http_url(u_content):
                key = _si_canonical_asset_key(u_content)
                score = _si_url_size_score(u_content, label=str(media_label or "content"))
                candidates.append((key, score, u_content))

            # thumbnail (optional)
            if allow_thumbnails:
                u_thumb = m.get("thumbnail")
                if _looks_like_http_url(u_thumb):
                    key = _si_canonical_asset_key(u_thumb)
                    score = _si_url_size_score(u_thumb, label="thumbnail")
                    candidates.append((key, score, u_thumb))

    if not candidates:
        return []

    return _pick_best_per_canonical_asset(candidates)


# -----------------------------
# Cleveland Museum of Art downloader
# -----------------------------

def download_cma_by_search(
    save_dir: str | Path,
    search_term: str,
    public_domain: bool,
    *,
    requests_per_second: float = 1.0,
    write_metadata_json: bool = True,
    group_by_medium: bool = False,
    prefer_print_images: bool = True,
    exhaustive_metadata_scan: bool = False,
    user_agent: str = "openaccess-bulk-downloader/1.0 (polite)",
) -> Dict[str, Any]:
    """
    Download CMA Open Access images for records matching `search_term`.

    CMA API:
      https://openaccess-api.clevelandart.org/api/artworks?q=...&cc0=true&has_image=1&skip=0&limit=1000 :contentReference[oaicite:12]{index=12}

    Note on "exhaustive_metadata_scan":
      - False (default): uses CMA server-side q=search_term, then also verifies match by flattening.
      - True: ignores server-side q narrowing and scans *all* returned records (still paged),
              applying your flatten+token match locally. This is slower but closer to your AIC
              “search any metadata field” semantics.
    """
    save_dir = Path(save_dir)
    if not search_term or not search_term.strip():
        raise ValueError("search_term must be non-empty")

    term_folder = _safe_path_component(search_term, max_len=80)
    out_base = (save_dir / term_folder / "mediums") if group_by_medium else (save_dir / term_folder)
    out_base.mkdir(parents=True, exist_ok=True)

    query_norm = _normalize_text(search_term)
    limiter = _RateLimiter((1.0 / requests_per_second) if requests_per_second > 0 else 0.0)

    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})

    base_url = "https://openaccess-api.clevelandart.org/api/artworks"
    limit = 1000  # doc says max can be large; examples show 1000-scale limits. :contentReference[oaicite:13]{index=13}

    scanned = 0
    matched = 0
    downloaded_images = 0
    failed_images = 0
    skipped_no_image = 0
    skipped_rights = 0

    skip = 0
    total = None

    while total is None or skip < total:
        params = {
            "has_image": 1,
            "limit": limit,
            "skip": skip,
        }
        if public_domain:
            params["cc0"] = "true"  # Filters by CC0. :contentReference[oaicite:14]{index=14}

        # If not exhaustive, use API q=... to narrow set; otherwise omit q and scan locally.
        if not exhaustive_metadata_scan:
            params["q"] = search_term  # searches across multiple meaningful fields. :contentReference[oaicite:15]{index=15}

        limiter.wait()
        r = session.get(base_url, params=params, timeout=60)
        r.raise_for_status()
        payload = r.json()

        info = payload.get("info") or {}
        if total is None:
            total = info.get("total") if isinstance(info, dict) else None
            if not isinstance(total, int):
                # if API changes, just stop when we get empty pages
                total = 10**18

        data = payload.get("data") or []
        if not data:
            break

        for rec in data:
            scanned += 1
            if not isinstance(rec, dict):
                continue

            # Rights filter (belt + suspenders): share_license_status appears as "CC0". :contentReference[oaicite:16]{index=16}
            if public_domain:
                if (rec.get("share_license_status") != "CC0"):
                    skipped_rights += 1
                    continue

            # Local "search any metadata" verification (your same flatten/token semantics)
            flat_text = " ".join(_flatten_strings(rec))
            if not _matches_query(_normalize_text(flat_text), query_norm):
                continue

            image_urls = _pick_cma_image_urls(rec, prefer_print=prefer_print_images)
            if not image_urls:
                skipped_no_image += 1
                continue

            matched += 1
            artwork_id = rec.get("id", "unknown_id")
            title = rec.get("title") or rec.get("tombstone") or ""
            title_part = _safe_path_component(str(title), max_len=80)

            if group_by_medium:
                medium = rec.get("medium") or rec.get("type") or "unknown medium"
                medium_norm = _safe_path_component(str(medium), max_len=80)
                img_dir = out_base / medium_norm / "images"
                meta_dir = out_base / medium_norm / "metadata"
            else:
                img_dir = out_base / "images"
                meta_dir = out_base / "metadata"

            for idx, url in enumerate(image_urls):
                limiter.wait()
                ext = _guess_ext_from_url(url, default=".jpg")
                fname = f"{artwork_id}__{idx:02d}__{title_part}{ext}"
                out_path = img_dir / fname

                ok = _download_stream(session, url, out_path)
                if ok:
                    downloaded_images += 1
                    if write_metadata_json:
                        meta_dir.mkdir(parents=True, exist_ok=True)
                        meta_path = meta_dir / f"{artwork_id}__{idx:02d}__{title_part}.json"
                        if not meta_path.exists():
                            meta_payload = {
                                "source": "cleveland",
                                "artwork_id": artwork_id,
                                "image_index": idx,
                                "image_url": url,
                                "search_term": search_term,
                                "public_domain_filter": public_domain,
                                "artwork_data": rec,
                            }
                            meta_path.write_text(json.dumps(meta_payload, ensure_ascii=False, indent=2), encoding="utf-8")
                else:
                    failed_images += 1

        skip += limit

    return {
        "source": "cleveland",
        "search_term": search_term,
        "public_domain_filter": public_domain,
        "requests_per_second": requests_per_second,
        "group_by_medium": group_by_medium,
        "prefer_print_images": prefer_print_images,
        "exhaustive_metadata_scan": exhaustive_metadata_scan,
        "scanned_records": scanned,
        "matched_records": matched,
        "downloaded_images": downloaded_images,
        "failed_images": failed_images,
        "skipped_no_image": skipped_no_image,
        "skipped_rights": skipped_rights,
        "output_base": str(out_base),
    }


# -----------------------------
# Smithsonian downloader
# -----------------------------

def download_smithsonian_by_search(
    save_dir: str | Path,
    search_term: str,
    public_domain: bool,
    *,
    api_key: str | None = None,
    requests_per_second: float = 1.0,
    rows_per_page: int = 100,
    fetch_full_content_if_missing_media: bool = True,
    write_metadata_json: bool = True,
    group_by_medium: bool = False,
    allow_thumbnails: bool = False,
    user_agent: str = "openaccess-bulk-downloader/1.0 (polite)",
) -> Dict[str, Any]:
    """
    Download Smithsonian Open Access images for records matching `search_term`.

    API docs:
      Search:   https://api.si.edu/openaccess/api/v1.0/search?api_key=&q=...&start=...&rows=... :contentReference[oaicite:17]{index=17}
      Content:  https://api.si.edu/openaccess/api/v1.0/content/:id?api_key=... :contentReference[oaicite:18]{index=18}
      API key required (api.data.gov). :contentReference[oaicite:19]{index=19}

    Metadata structure:
      content.{descriptiveNonRepeating,indexedStructured,freetext} and media in descriptiveNonRepeating.online_media. :contentReference[oaicite:20]{index=20}
    """
    save_dir = Path(save_dir)
    if not search_term or not search_term.strip():
        raise ValueError("search_term must be non-empty")

    api_key = api_key or os.environ.get("smithsonian_key")
    if not api_key:
        raise ValueError(
            "Smithsonian requires an API key. Pass api_key=... or set env SMITHSONIAN_API_KEY. "
            "API keys are issued via api.data.gov for the Smithsonian Open Access API."
        )

    term_folder = _safe_path_component(search_term, max_len=80)
    out_base = (save_dir / term_folder / "mediums") if group_by_medium else (save_dir / term_folder)
    out_base.mkdir(parents=True, exist_ok=True)

    query_norm = _normalize_text(search_term)
    limiter = _RateLimiter((1.0 / requests_per_second) if requests_per_second > 0 else 0.0)

    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})

    search_url = "https://api.si.edu/openaccess/api/v1.0/search"
    content_url_tmpl = "https://api.si.edu/openaccess/api/v1.0/content/{id}"

    scanned = 0
    matched = 0
    downloaded_images = 0
    failed_images = 0
    skipped_no_image = 0
    skipped_rights = 0

    start = 0
    row_count = None

    while row_count is None or start < row_count:
        params = {
            "api_key": api_key,
            "q": search_term,
            "start": start,
            "rows": rows_per_page,
        }

        limiter.wait()
        r = session.get(search_url, params=params, timeout=60)
        r.raise_for_status()
        payload = r.json()

        resp = payload.get("response") or {}
        rows = resp.get("rows") or []
        if row_count is None:
            row_count = resp.get("rowCount")
            if not isinstance(row_count, int):
                row_count = 0

        if not rows:
            break

        for row in rows:
            scanned += 1
            if not isinstance(row, dict):
                continue

            # Rights filter
            if public_domain and not _smithsonian_is_cc0(row):
                skipped_rights += 1
                continue

            # Local "search any metadata" verification on what we have
            flat_text = " ".join(_flatten_strings(row))
            if not _matches_query(_normalize_text(flat_text), query_norm):
                # The server already did q=search_term, but this keeps semantics consistent.
                continue

            # Extract image URLs
            image_urls = _pick_smithsonian_image_urls(row, allow_thumbnails=allow_thumbnails)

            # If no media in search result, optionally fetch full content record:
            if fetch_full_content_if_missing_media and not image_urls:
                row_id = row.get("id")  # id like "edanmdm-..." :contentReference[oaicite:21]{index=21}
                if isinstance(row_id, str) and row_id:
                    limiter.wait()
                    rr = session.get(content_url_tmpl.format(id=row_id), params={"api_key": api_key}, timeout=60)
                    if rr.ok:
                        full = rr.json()
                        # The content endpoint wraps actual record under response maybe; keep robust:
                        full_row = full.get("response") or full
                        if isinstance(full_row, dict):
                            # Re-run url extraction
                            image_urls = _pick_smithsonian_image_urls(full_row, allow_thumbnails=allow_thumbnails)
                            # Swap row used for metadata write
                            row = full_row

            if not image_urls:
                skipped_no_image += 1
                continue

            matched += 1
            row_id = row.get("id", "unknown_id")
            title = row.get("title", "") or ""
            title_part = _safe_path_component(str(title), max_len=80)

            # Medium-ish grouping: use indexedStructured.object_type if present (often exists). :contentReference[oaicite:22]{index=22}
            if group_by_medium:
                content = row.get("content") or {}
                idx = content.get("indexedStructured") or {}
                obj_types = idx.get("object_type") or []
                medium = obj_types[0] if isinstance(obj_types, list) and obj_types else "unknown medium"
                medium_norm = _safe_path_component(str(medium), max_len=80)
                img_dir = out_base / medium_norm / "images"
                meta_dir = out_base / medium_norm / "metadata"
            else:
                img_dir = out_base / "images"
                meta_dir = out_base / "metadata"

            for idx, url in enumerate(image_urls):
                limiter.wait()
                ext = _guess_ext_from_url(url, default=".jpg")
                fname = f"{row_id}__{idx:02d}__{title_part}{ext}"
                out_path = img_dir / fname

                ok = _download_stream(session, url, out_path)
                if ok:
                    downloaded_images += 1
                    if write_metadata_json:
                        meta_dir.mkdir(parents=True, exist_ok=True)
                        meta_path = meta_dir / f"{row_id}__{idx:02d}__{title_part}.json"
                        if not meta_path.exists():
                            meta_payload = {
                                "source": "smithsonian",
                                "row_id": row_id,
                                "image_index": idx,
                                "image_url": url,
                                "search_term": search_term,
                                "public_domain_filter": public_domain,
                                "row_data": row,
                            }
                            meta_path.write_text(json.dumps(meta_payload, ensure_ascii=False, indent=2), encoding="utf-8")
                else:
                    failed_images += 1

        start += rows_per_page

    return {
        "source": "smithsonian",
        "search_term": search_term,
        "public_domain_filter": public_domain,
        "requests_per_second": requests_per_second,
        "rows_per_page": rows_per_page,
        "group_by_medium": group_by_medium,
        "allow_thumbnails": allow_thumbnails,
        "fetch_full_content_if_missing_media": fetch_full_content_if_missing_media,
        "scanned_records": scanned,
        "matched_records": matched,
        "downloaded_images": downloaded_images,
        "failed_images": failed_images,
        "skipped_no_image": skipped_no_image,
        "skipped_rights": skipped_rights,
        "output_base": str(out_base),
    }


def download_aic_by_search(
    save_dir: str | Path,
    search_term: str,
    public_domain: bool,
    *,
    dump_root: Path = AIC_DUMP_ROOT,
    requests_per_second: float = 1.0,
    include_alt_images: bool = True,
    write_metadata_json: bool = True,
    group_by_medium: bool = False,   # <-- NEW: default puts all images together
    user_agent: str = "aic-bulk-downloader/1.0 (polite)",
) -> Dict[str, Any]:
    """
    Download highest-resolution AIC IIIF images for artworks matching `search_term`.

    Output structure (group_by_medium=False):
      save_dir/<search_term>/images/<artworkId>__<imageId>__<title>.jpg
      save_dir/<search_term>/metadata/<...>.json (optional)

    Output structure (group_by_medium=True):
      save_dir/<search_term>/mediums/<medium>/images/...
      save_dir/<search_term>/mediums/<medium>/metadata/...
    """
    dump_root = Path(dump_root)
    save_dir = Path(save_dir)

    if not dump_root.exists():
        raise FileNotFoundError(f"dump_root does not exist: {dump_root}")
    if not search_term or not search_term.strip():
        raise ValueError("search_term must be non-empty")

    cfg = _load_aic_config(dump_root)
    iiif_base = cfg.get("iiif_url") or "https://www.artic.edu/iiif/2"

    term_folder = _safe_path_component(search_term, max_len=80)

    # NEW: choose base output structure
    if group_by_medium:
        out_base = save_dir / term_folder / "mediums"
    else:
        out_base = save_dir / term_folder  # no mediums layer

    out_base.mkdir(parents=True, exist_ok=True)

    query_norm = _normalize_text(search_term)
    limiter = _RateLimiter((1.0 / requests_per_second) if requests_per_second > 0 else 0.0)

    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})

    scanned = 0
    matched_artworks = 0
    downloaded_images = 0
    failed_images = 0
    skipped_no_image = 0
    skipped_rights = 0

    raw_token_filter = query_norm.split()[0] if query_norm else ""

    for fp in _iter_artwork_files(dump_root):
        scanned += 1

        if raw_token_filter:
            try:
                raw = fp.read_text(encoding="utf-8", errors="ignore")
                if raw_token_filter not in _normalize_text(raw):
                    continue
            except OSError:
                pass

        data = _read_artwork_data(fp)

        if public_domain and not bool(data.get("is_public_domain")):
            skipped_rights += 1
            continue

        flat_text = " ".join(_flatten_strings(data))
        flat_text_norm = _normalize_text(flat_text)

        if not _matches_query(flat_text_norm, query_norm):
            continue

        matched_artworks += 1

        image_ids: List[str] = []
        main_image_id = data.get("image_id")
        if isinstance(main_image_id, str) and main_image_id:
            image_ids.append(main_image_id)

        if include_alt_images:
            alt = data.get("alt_image_ids") or []
            if isinstance(alt, list):
                image_ids.extend([x for x in alt if isinstance(x, str) and x])

        image_ids = list(dict.fromkeys(image_ids))
        if not image_ids:
            skipped_no_image += 1
            continue

        artwork_id = data.get("id", "unknown_id")
        title = data.get("title", "") or ""
        title_part = _safe_path_component(title, max_len=80)

        # NEW: choose output dirs
        if group_by_medium:
            medium = (data.get("medium_display") or "unknown medium")
            medium_norm = _safe_path_component(medium, max_len=80)
            img_dir = out_base / medium_norm / "images"
            meta_dir = out_base / medium_norm / "metadata"
        else:
            img_dir = out_base / "images"
            meta_dir = out_base / "metadata"

        for image_id in image_ids:
            limiter.wait()
            url = _iiif_full_url(iiif_base, image_id)

            fname = f"{artwork_id}__{image_id}__{title_part}.jpg"
            out_path = img_dir / fname

            ok = _download_stream(session, url, out_path)
            if ok:
                downloaded_images += 1

                if write_metadata_json:
                    meta_dir.mkdir(parents=True, exist_ok=True)
                    meta_path = meta_dir / f"{artwork_id}__{image_id}__{title_part}.json"
                    if not meta_path.exists():
                        payload = {
                            "artwork_id": artwork_id,
                            "image_id": image_id,
                            "iiif_full_url": url,
                            "search_term": search_term,
                            "public_domain_filter": public_domain,
                            "artwork_data": data,
                        }
                        meta_path.write_text(
                            json.dumps(payload, ensure_ascii=False, indent=2),
                            encoding="utf-8",
                        )
            else:
                failed_images += 1

    return {
        "dump_root": str(dump_root),
        "iiif_base": iiif_base,
        "save_dir": str(save_dir),
        "search_term": search_term,
        "public_domain_filter": public_domain,
        "requests_per_second": requests_per_second,
        "scanned_artwork_files": scanned,
        "matched_artworks": matched_artworks,
        "downloaded_images": downloaded_images,
        "failed_images": failed_images,
        "skipped_no_image": skipped_no_image,
        "skipped_rights": skipped_rights,
        "output_base": str(out_base),
        "group_by_medium": group_by_medium,
    }


from urllib.parse import parse_qs, urlparse

_SIZE_HINT_BONUS = [
    ("original", 10_000_000_000),
    ("master",   10_000_000_000),
    ("full",     5_000_000_000),
    ("print",    2_000_000_000),
    ("largest",  2_000_000_000),
    ("large",    1_000_000_000),
    ("medium",   100_000_000),
    ("small",    10_000_000),
    ("thumb",    1_000_000),
    ("thumbnail",1_000_000),
]

def _si_canonical_asset_key(url: str) -> str:
    """
    Canonicalize Smithsonian deliveryService URLs so different sizes map to the same key.
    - If URL is ids.si.edu/ids/deliveryService?id=XYZ..., key is (host+path+id=XYZ).
    - Otherwise key is host+path (and we ignore typical size params if present).
    """
    try:
        u = urlparse(url)
        qs = parse_qs(u.query or "")
        host_path = f"{u.netloc}{u.path}"
        if "ids.si.edu" in (u.netloc or "") and "deliveryService" in (u.path or "") and "id" in qs and qs["id"]:
            return f"{host_path}?id={qs['id'][0]}"
        # Generic fallback: drop query entirely (avoids treating ?width= as separate assets)
        return host_path
    except Exception:
        return url  # last resort


def _si_url_size_score(url: str, label: str | None = None) -> int:
    """
    Score candidate URLs by estimated resolution / desirability.
    - Prefer explicit width/height; else prefer 'max' param; else use label hints.
    """
    score = 0
    try:
        u = urlparse(url)
        qs = parse_qs(u.query or "")

        # Common patterns: &width=, &height=, &max=
        w = qs.get("width", [None])[0]
        h = qs.get("height", [None])[0]
        m = qs.get("max", [None])[0]

        def _to_int(x):
            try:
                return int(str(x))
            except Exception:
                return None

        wi = _to_int(w)
        hi = _to_int(h)
        mi = _to_int(m)

        if wi and hi:
            score += wi * hi
        elif mi:
            # max is usually a max dimension; approximate area ~ max^2
            score += mi * mi
    except Exception:
        pass

    # Label/content hint bonus (helps when no numeric params exist)
    hint = (label or "").lower()
    for key, bonus in _SIZE_HINT_BONUS:
        if key in hint:
            score += bonus
            break

    return score


def _pick_best_per_canonical_asset(candidates: list[tuple[str, int, str]]) -> list[str]:
    """
    candidates: list of (canonical_key, score, url) in discovery order.
    Returns best url per canonical_key, preserving first-seen asset order.
    """
    best: dict[str, tuple[int, str]] = {}
    order: list[str] = []

    for key, score, url in candidates:
        if key not in best:
            best[key] = (score, url)
            order.append(key)
        else:
            if score > best[key][0]:
                best[key] = (score, url)

    out: list[str] = []
    seen_urls: set[str] = set()
    for key in order:
        url = best[key][1]
        if url not in seen_urls:
            out.append(url)
            seen_urls.add(url)
    return out







# =========================
# Thread-safe token-bucket limiter (hard caps your RPS)
# =========================
class TokenBucketRateLimiter:
    def __init__(self, rate: float, capacity: Optional[float] = None):
        if rate <= 0:
            raise ValueError("rate must be > 0")
        self.rate = float(rate)
        self.capacity = float(capacity if capacity is not None else rate)
        self._tokens = self.capacity
        self._last = time.perf_counter()
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)

    def _refill(self) -> None:
        now = time.perf_counter()
        delta = now - self._last
        if delta > 0:
            self._tokens = min(self.capacity, self._tokens + delta * self.rate)
            self._last = now

    def acquire(self, tokens: float = 1.0) -> None:
        if tokens <= 0:
            return
        with self._cv:
            while True:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                needed = tokens - self._tokens
                wait_time = needed / self.rate
                self._cv.wait(timeout=max(wait_time, 0.001))

def _slugify(text: str, maxlen: int = 80) -> str:
    if not text:
        return "untitled"
    text = text.strip().replace("—", "-").replace("–", "-")
    return _slug_re.sub("_", text)[:maxlen].strip("_") or "untitled"

def _best_cols(df: pd.DataFrame, names: List[str]) -> Optional[str]:
    norm = {c.lower().replace(" ", ""): c for c in df.columns}
    for n in names:
        key = n.lower().replace(" ", "")
        if key in norm:
            return norm[key]
    return None

def _guess_ext_from_ct(ct: str) -> str:
    if not ct:
        return ".jpg"
    ext = mimetypes.guess_extension(ct.split(";")[0].strip()) or ".jpg"
    if ext in (".jpe", ".jpeg"):
        ext = ".jpg"
    return ext

def _sha256(path: pathlib.Path, chunk: int = 1 << 16) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(chunk), b""):
            h.update(b)
    return h.hexdigest()

# -----------------------------
# 1) Enrich your DataFrame via API (by Object ID)
# -----------------------------
def enrich_with_met_images(
    df: pd.DataFrame,
    *,
    object_id_col: str | None = None,
    title_col: str | None = None,
    max_workers: int = 5,
    connect_timeout: int = 5,
    read_timeout: int = 15,
    retries: int = 3,               # was 1 → more resilient runs
    retries_total: int = 6,         # upper bound including 429/5xx loops
    show_every: float = 2.0,
    api_rps_limit: float = 80.0,    # rate cap (RPS)
    skip_without_images: bool = False,  # NEW: keep only rows that got ≥1 image URL
) -> pd.DataFrame:
    """
    Returns a COPY of df with added columns:
      - primaryImage (str)
      - primaryImageSmall (str)
      - additionalImages (list[str])
      - Is Public Domain (bool) [overwrites/creates]
    Rate-limited and robust to 429/5xx with Retry-After.
    """
    if object_id_col is None:
        object_id_col = _best_cols(df, ["Object ID", "ObjectID"]) or "Object ID"
    if title_col is None:
        title_col = _best_cols(df, ["Title"]) or "Title"
    if object_id_col not in df.columns:
        raise ValueError("Object ID column not found in DataFrame.")

    df2 = df.copy()
    for c in ["primaryImage", "primaryImageSmall", "additionalImages", "Is Public Domain"]:
        if c not in df2.columns:
            df2[c] = None

    # Collect IDs to request
    ids_titles: List[Tuple[int, str]] = []
    for _, row in df2.iterrows():
        try:
            oid = int(row[object_id_col])
        except Exception:
            continue
        title = str(row.get(title_col) or f"object_{oid}")
        ids_titles.append((oid, title))

    limiter = TokenBucketRateLimiter(rate=api_rps_limit, capacity=api_rps_limit)

    # Counters for diagnostics
    diag = {
        "ok_200": 0,
        "with_images": 0,
        "no_images": 0,
        "http_429": 0,
        "http_5xx": 0,
        "http_404": 0,
        "http_other": 0,
        "json_errors": 0,
        "other_errors": 0,
    }

    sess = requests.Session()
    sess.headers.update({"User-Agent": UA})

    def _sleep_with_jitter(base: float) -> None:
        time.sleep(base + random.uniform(0, base * 0.2))

    def fetch_one(oid: int) -> Optional[Dict[str, Any]]:
        url = f"{MET_API_BASE}/objects/{oid}"
        attempts = 0
        backoff = 0.4
        while attempts < retries_total:
            attempts += 1
            try:
                limiter.acquire(1.0)
                r = sess.get(url, timeout=(connect_timeout, read_timeout))
                if r.status_code == 200:
                    try:
                        data = r.json()
                    except Exception:
                        diag["json_errors"] += 1
                        if attempts >= retries_total:
                            return None
                        _sleep_with_jitter(backoff)
                        backoff = min(backoff * 2, 8.0)
                        continue
                    diag["ok_200"] += 1
                    return data

                # Handle common statuses
                if r.status_code == 429:
                    diag["http_429"] += 1
                    ra = r.headers.get("Retry-After")
                    wait = float(ra) if (ra and ra.isdigit()) else backoff
                    _sleep_with_jitter(wait)
                    backoff = min(backoff * 2, 16.0)
                    continue

                if r.status_code == 404:
                    diag["http_404"] += 1
                    return None

                if 500 <= r.status_code < 600:
                    diag["http_5xx"] += 1
                    _sleep_with_jitter(backoff)
                    backoff = min(backoff * 2, 16.0)
                    continue

                diag["http_other"] += 1
                if attempts >= retries_total:
                    return None
                _sleep_with_jitter(backoff)
                backoff = min(backoff * 2, 8.0)

            except requests.RequestException:
                diag["other_errors"] += 1
                if attempts >= retries_total:
                    return None
                _sleep_with_jitter(backoff)
                backoff = min(backoff * 2, 8.0)
        return None

    last_print = time.time()
    results: Dict[int, Dict[str, Any]] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(fetch_one, oid): (oid, title) for oid, title in ids_titles}
        total = len(futs)
        done = 0
        for fut in as_completed(futs):
            oid, _ = futs[fut]
            data = fut.result()
            if data:
                prim = data.get("primaryImage") or ""
                prims = data.get("primaryImageSmall") or ""
                addl = data.get("additionalImages") or []
                results[oid] = {
                    "primaryImage": prim,
                    "primaryImageSmall": prims,
                    "additionalImages": addl if isinstance(addl, list) else [],
                    "Is Public Domain": bool(data.get("isPublicDomain")) if "isPublicDomain" in data else None,
                }
                if prim or prims or (isinstance(addl, list) and addl):
                    diag["with_images"] += 1
                else:
                    diag["no_images"] += 1
            else:
                # keep empty entry so we know it was attempted
                results[oid] = {
                    "primaryImage": "",
                    "primaryImageSmall": "",
                    "additionalImages": [],
                    "Is Public Domain": None,
                }
            done += 1
            now = time.time()
            if (now - last_print) >= show_every or done == total:
                print(f"[enrich] {done}/{total} API lookups complete")
                last_print = now

    # write back into df2
    pi_count = pis_count = addl_count = 0
    rows_to_drop = []
    for i, row in df2.iterrows():
        try:
            oid = int(row[object_id_col])
        except Exception:
            continue
        info = results.get(oid, {})
        if not info:
            continue
        if info.get("primaryImage"):
            df2.at[i, "primaryImage"] = info["primaryImage"]; pi_count += 1
        if info.get("primaryImageSmall"):
            df2.at[i, "primaryImageSmall"] = info["primaryImageSmall"]; pis_count += 1
        if info.get("additionalImages"):
            df2.at[i, "additionalImages"] = info["additionalImages"]; addl_count += 1
        if info.get("Is Public Domain") is not None:
            df2.at[i, "Is Public Domain"] = info["Is Public Domain"]

        if skip_without_images:
            has_any = bool(info.get("primaryImage") or info.get("primaryImageSmall") or info.get("additionalImages"))
            if not has_any:
                rows_to_drop.append(i)

    if skip_without_images and rows_to_drop:
        df2 = df2.drop(index=rows_to_drop)

    print(f"[enrich] filled primaryImage: {pi_count}, primaryImageSmall: {pis_count}, additionalImages: {addl_count}")
    print(
        "[enrich/diag] "
        f"200:{diag['ok_200']} | with_images:{diag['with_images']} | no_images:{diag['no_images']} | "
        f"429:{diag['http_429']} | 5xx:{diag['http_5xx']} | 404:{diag['http_404']} | otherHTTP:{diag['http_other']} | "
        f"jsonErr:{diag['json_errors']} | otherErr:{diag['other_errors']}"
    )

    # Sample a few rows to verify URLs are present
    try:
        sample = df2.head(3)[["Object ID" if object_id_col == "Object ID" else object_id_col,
                               "primaryImage","primaryImageSmall","additionalImages"]]
        print("[enrich/sample]\n", sample.to_string(index=False)[:800])
    except Exception:
        pass

    return df2

# -----------------------------
# 2) Download using the enriched columns (no further API calls)
# -----------------------------
def download_met_images_from_enriched_df(
    df_enriched: pd.DataFrame,
    out_dir: str | os.PathLike,
    *,
    require_public_domain: bool = True,
    include_additional: bool = True,
    filename_fmt: str = "{objectID}_{slug}",
    title_col: str | None = None,
    object_id_col: str | None = None,
    max_workers: int = 4,
    connect_timeout: int = 10,
    read_timeout: int = 60,
    show_every: float = 2.0,
    log_csv: str | None = "met_download_log.csv",
    download_rps_limit: float = 50.0,
) -> Dict[str, Any]:
    if object_id_col is None:
        object_id_col = _best_cols(df_enriched, ["Object ID", "ObjectID"]) or "Object ID"
    if title_col is None:
        title_col = _best_cols(df_enriched, ["Title"]) or "Title"

    out_dir = pathlib.Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    def iter_urls(row: pd.Series) -> List[str]:
        urls: List[str] = []
        p = row.get("primaryImage")
        ps = row.get("primaryImageSmall")
        if isinstance(p, str) and p.strip():
            urls.append(p.strip())
        if isinstance(ps, str) and ps.strip():
            urls.append(ps.strip())
        if include_additional:
            addl = row.get("additionalImages")
            if isinstance(addl, list):
                urls.extend([u for u in addl if isinstance(u, str) and u.strip()])
            elif isinstance(addl, str) and addl.strip():
                urls.extend([s.strip() for s in re.split(r"\s*\|\s*", addl) if s.strip()])
        seen, out = set(), []
        for u in urls:
            if u and u not in seen:
                seen.add(u); out.append(u)
        return out

    tasks: List[Tuple[int, str, str, pathlib.Path]] = []
    kept_rows = 0
    rows_with_urls = 0
    for _, row in df_enriched.iterrows():
        if require_public_domain and str(row.get("Is Public Domain")).strip().lower() in {"false", "0", "no"}:
            continue
        kept_rows += 1
        try:
            oid = int(row[object_id_col])
        except Exception:
            continue
        title = (str(row.get(title_col) or "")).strip() or f"object_{oid}"
        slug = _slugify(title, 80)
        base = filename_fmt.format(objectID=oid, slug=slug)
        urls = iter_urls(row)
        if urls:
            rows_with_urls += 1
        for i, url in enumerate(urls):
            stem = base if i == 0 else f"{base}_{i:02d}"
            url_path = pathlib.Path(url.split("?")[0])
            out_path = out_dir / (stem + (url_path.suffix if url_path.suffix else ""))
            tasks.append((oid, title, url, out_path))

    print(f"[download] rows kept: {kept_rows}  | rows with URLs: {rows_with_urls}  | files to fetch: {len(tasks)}")
    if not tasks:
        # Show a hint if there are zero URLs despite many kept rows.
        if kept_rows > 0:
            print("[download/hint] No URLs found. Ensure enrich step populated primaryImage/Small/additionalImages "
                  "(consider skip_without_images=True, and inspect [enrich/diag] counters).")
        return {"tasks": 0, "ok": 0, "skipped_exists": 0, "http_error": 0, "io_error": 0, "empty": 0}

    writer = None
    fobj = None
    log_path = None
    if log_csv:
        log_path = (out_dir / log_csv) if os.path.basename(log_csv) == log_csv else pathlib.Path(log_csv)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        newf = not log_path.exists()
        fobj = open(log_path, "a", newline="", encoding="utf-8")
        writer = csv.writer(fobj)
        if newf:
            writer.writerow(["object_id","title","url","saved_path","status","bytes","sec","sha256"])

    dl_limiter = TokenBucketRateLimiter(rate=download_rps_limit, capacity=download_rps_limit)

    def worker(t: Tuple[int, str, str, pathlib.Path]):
        oid, title, url, out_path = t
        try:
            if out_path.exists() and out_path.stat().st_size > 0:
                if writer: writer.writerow([oid, title, url, str(out_path), "skipped_exists", "", "0.000", ""])
                return "skipped_exists", 0.0, out_path, ""
            tmp = out_path.with_suffix(out_path.suffix + ".part")
            dl_limiter.acquire(1.0)
            t1 = time.time()
            with requests.get(url, stream=True, timeout=(connect_timeout, read_timeout),
                              headers={"User-Agent": UA}) as r:
                r.raise_for_status()
                if out_path.suffix == "":
                    ext = _guess_ext_from_ct(r.headers.get("Content-Type", ""))
                    out_path = out_path.with_suffix(ext)
                    tmp = out_path.with_suffix(out_path.suffix + ".part")
                n = 0
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 15):
                        if chunk:
                            f.write(chunk); n += len(chunk)
                if n == 0:
                    try: tmp.unlink(missing_ok=True)
                    except Exception: pass
                    if writer: writer.writerow([oid, title, url, str(out_path), "empty", "0", f"{time.time()-t1:.3f}", ""])
                    return "empty", time.time()-t1, out_path, ""
                tmp.replace(out_path)
            digest = ""
            try: digest = _sha256(out_path)
            except Exception: pass
            if writer:
                writer.writerow([oid, title, url, str(out_path), "ok", str(out_path.stat().st_size), f"{time.time()-t1:.3f}", digest])
            return "ok", time.time()-t1, out_path, digest
        except requests.HTTPError:
            if writer: writer.writerow([oid, title, url, str(out_path), "http_error", "", "0.000", ""])
            return "http_error", 0.0, out_path, ""
        except Exception:
            if writer: writer.writerow([oid, title, url, str(out_path), "io_error", "", "0.000", ""])
            return "io_error", 0.0, out_path, ""

    counters = {"ok": 0, "skipped_exists": 0, "http_error": 0, "io_error": 0, "empty": 0}
    last_print = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(worker, t) for t in tasks]
        total = len(futs)
        for i, fut in enumerate(as_completed(futs), 1):
            status, sec, out_path, digest = fut.result()
            counters[status] = counters.get(status, 0) + 1
            now = time.time()
            if (now - last_print) >= show_every or i == total:
                print(f"[download] {i}/{total}  ok:{counters['ok']}  exist:{counters['skipped_exists']}  "
                      f"http:{counters['http_error']}  io:{counters['io_error']}  empty:{counters['empty']}")
                last_print = now

    if fobj: fobj.close()
    return {**counters, "tasks": len(tasks), "log_path": str(log_path) if log_path else None}







def download_from_met(
        search_term,
        in_public_domain_bool,
        met_df,
        save_directory
):
    search_df = met_df[(met_df["Tags"].str.contains(search_term, case=False, na=False))
                       | (met_df["Title"].str.contains(search_term, case=False, na=False))
                       | (met_df["Artist Display Name"].str.contains(search_term, case=False, na=False))
                       ]



    total_entries = len(search_df)
    in_public_domain = len(search_df[search_df["Is Public Domain"]])
    if in_public_domain_bool:
        search_df = search_df[search_df["Is Public Domain"]]
        # in_public_domain = len(search_df)
    print(f"{in_public_domain} out of {total_entries} entries in public domain for {search_term}")

    # 1) Enrich your filtered df (e.g., elephant_df) using API by Object ID
    enriched = enrich_with_met_images(
        search_df,
        max_workers=8,          # parallel API lookups
        connect_timeout=5,      # short timeouts to avoid long hangs
        read_timeout=15,
        retries=1               # one quick retry
    )
    
    print("enriched")

    summary = download_met_images_from_enriched_df(
        enriched,
        out_dir=save_directory,
        require_public_domain=in_public_domain_bool,   # you can set True if you want only PD items
        include_additional=True,
        filename_fmt="{objectID}_{slug}",
        max_workers=15,
        connect_timeout=10,
        read_timeout=60,
        show_every=5.0,
        log_csv=f"{save_directory}/{search_term}_download_log.csv"
    )
    return enriched, summary
    

### wikimedia commons


_TAG_RE = re.compile(r"<[^>]+>")

def _strip_html(s: str) -> str:
    s = html.unescape(s or "")
    s = _TAG_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _slug(s: str) -> str:
    s = _normalize_text(s).replace(" ", "_")
    return s[:80] if s else "query"

def _best_filename_from_title(file_title: str) -> str:
    name = file_title[5:] if file_title.startswith("File:") else file_title
    return name.replace("/", "_").replace("\\", "_")

def _flatten_extmetadata(ext: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in (ext or {}).items():
        if isinstance(v, dict) and "value" in v:
            raw = v.get("value", "")
            out[f"ext_{k}"] = "" if raw is None else str(raw)
            out[f"ext_{k}_text"] = _strip_html("" if raw is None else str(raw))
        else:
            out[f"ext_{k}"] = "" if v is None else str(v)
            out[f"ext_{k}_text"] = _strip_html("" if v is None else str(v))
    return out

@dataclass
class _RateLimiter:
    requests_per_second: float
    _next_ok: float = 0.0

    def wait(self) -> None:
        if self.requests_per_second <= 0:
            return
        now = time.monotonic()
        if now < self._next_ok:
            time.sleep(self._next_ok - now)
        self._next_ok = max(self._next_ok, time.monotonic()) + (1.0 / self.requests_per_second)


def download_commons_images(
    search_word: str,
    save_path: str | Path,
    require_public_domain: bool,
    *,
    max_results: int = 300,
    requests_per_second: float = 0.5,

    # CHANGED: separate connect/read timeouts
    connect_timeout: float = 10.0,
    read_timeout: float = 90.0,

    user_agent: str = "YourCommonsDownloaderBot/1.0 (https://your.site; you@your.site)",
    verbose: bool = True,

    allowed_mime: Optional[Set[str]] = None,
    allowed_ext: Optional[Set[str]] = None,
    max_size_mb: Optional[float] = 200.0,

    max_download_mbps: float = 20.0,
    continue_on_error: bool = True,

    # NEW: reduce query payload per call to avoid slow API responses
    titles_per_query: int = 25,
) -> Tuple[List[Path], Path, List[Dict[str, Any]]]:
    """
    See prior docstring; patched for API robustness and Wikimedia robot policy friendliness.
    """

    if not search_word or not str(search_word).strip():
        raise ValueError("search_word must be non-empty")

    # Enforce a UA that includes contact info (URL or email)
    ua = (user_agent or "").strip()
    if not ua or ("http" not in ua.lower() and "@" not in ua):
        raise ValueError(
            "Wikimedia requires an informative User-Agent with contact info. "
            "Use: 'AppName/1.0 (https://...; email@...)'"
        )
    if "you@example.com" in ua or "example.com" in ua:
        raise ValueError(
            "Replace placeholder contact info in user_agent; Wikimedia blocks placeholder/generic UAs."
        )

    if allowed_mime is None:
        allowed_mime = {"image/jpeg", "image/png", "image/webp"}
    allowed_mime = {m.lower() for m in allowed_mime}

    if allowed_ext is None:
        allowed_ext = {".jpg", ".jpeg", ".png", ".webp"}
    allowed_ext = {e.lower() if e.startswith(".") else f".{e.lower()}" for e in allowed_ext}

    max_bytes = None if max_size_mb is None else int(max_size_mb * 1024 * 1024)

    save_dir = Path(save_path).expanduser().resolve()
    save_dir.mkdir(parents=True, exist_ok=True)

    api_url = "https://commons.wikimedia.org/w/api.php"
    sess = requests.Session()
    limiter = _RateLimiter(requests_per_second=requests_per_second)
    norm_q = _normalize_text(search_word)

    skip = {
        "no_imageinfo": 0,
        "not_image": 0,
        "pd_filtered": 0,
        "mime_not_allowed": 0,
        "ext_not_allowed": 0,
        "too_large": 0,
        "term_mismatch": 0,
        "no_url": 0,
        "download_failed": 0,
        "api_timeouts": 0,
    }

    def api_get(params: Dict[str, Any], retries: int = 6) -> Dict[str, Any]:
        backoff = 1.0
        headers = {"User-Agent": ua}
        for attempt in range(retries):
            limiter.wait()
            try:
                r = sess.get(
                    api_url,
                    params=params,
                    timeout=(connect_timeout, read_timeout),
                    headers=headers,
                )
            except (ReadTimeout, ConnectTimeout, Timeout):
                skip["api_timeouts"] += 1
                time.sleep(backoff)
                backoff = min(60.0, backoff * 2.0)
                continue

            if r.status_code in (429, 503):
                ra = r.headers.get("Retry-After")
                time.sleep(float(ra)) if ra and ra.isdigit() else time.sleep(backoff)
                backoff = min(60.0, backoff * 2.0)
                continue

            r.raise_for_status()
            return r.json()

        raise RuntimeError(f"API failed after {retries} retries (timeouts/limits). Last params keys={list(params.keys())}")

    def download(url: str, dest: Path, retries: int = 4) -> None:
        max_bytes_per_sec = (max_download_mbps * 1_000_000) / 8.0
        backoff = 1.0
        headers = {
            "User-Agent": ua,
            "Accept": "image/*,*/*;q=0.8",
            "Referer": "https://commons.wikimedia.org/",
        }

        for _ in range(retries):
            limiter.wait()
            try:
                r = sess.get(url, stream=True, timeout=(connect_timeout, read_timeout), headers=headers)
            except (ReadTimeout, ConnectTimeout, Timeout):
                time.sleep(backoff)
                backoff = min(60.0, backoff * 2.0)
                continue

            if r.status_code in (429, 503):
                ra = r.headers.get("Retry-After")
                time.sleep(float(ra)) if ra and ra.isdigit() else time.sleep(backoff)
                backoff = min(60.0, backoff * 2.0)
                continue

            if r.status_code == 403:
                snippet = (r.text or "")[:300]
                raise requests.HTTPError(
                    f"403 Forbidden for {url}\n{snippet}\nUA={headers['User-Agent']}",
                    response=r,
                )

            r.raise_for_status()

            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_suffix(dest.suffix + ".part")

            bytes_this_window = 0
            window_start = time.monotonic()

            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=256 * 1024):
                    if not chunk:
                        continue
                    f.write(chunk)

                    bytes_this_window += len(chunk)
                    elapsed = time.monotonic() - window_start
                    if elapsed > 0:
                        rate = bytes_this_window / elapsed
                        if rate > max_bytes_per_sec:
                            sleep_needed = (bytes_this_window / max_bytes_per_sec) - elapsed
                            if sleep_needed > 0:
                                time.sleep(sleep_needed)
                    if elapsed > 10.0:
                        bytes_this_window = 0
                        window_start = time.monotonic()

            os.replace(tmp, dest)
            return

        raise RuntimeError(f"Download failed after {retries} retries: {url}")

    # 1) Search
    search_hits: List[Dict[str, Any]] = []
    sroffset: Optional[int] = None
    while len(search_hits) < max_results:
        params = {
            "action": "query",
            "format": "json",
            "formatversion": 2,
            "list": "search",
            "srnamespace": 6,
            "srlimit": 50,
            "srsearch": search_word,
            "srprop": "snippet|titlesnippet|timestamp|size|wordcount",
        }
        if sroffset is not None:
            params["sroffset"] = sroffset

        data = api_get(params)
        batch = (data.get("query", {}) or {}).get("search", []) or []
        search_hits.extend(batch)

        cont = data.get("continue")
        if not cont or "sroffset" not in cont:
            break
        sroffset = cont["sroffset"]

    search_hits = search_hits[:max_results]
    if verbose:
        print(f"[commons] search hits: {len(search_hits)} (require_public_domain={require_public_domain})")

    snippet_by_title = {
        h.get("title"): _strip_html(h.get("snippet", "") or "")
        for h in search_hits
        if h.get("title")
    }
    titles = [h.get("title") for h in search_hits if h.get("title")]

    def chunked(xs: List[str], n: int) -> List[List[str]]:
        return [xs[i : i + n] for i in range(0, len(xs), n)]

    downloaded: List[Path] = []
    rows: List[Dict[str, Any]] = []

    titles_per_query = max(1, min(int(titles_per_query), 50))

    # 2) Fetch metadata + download
    for title_chunk in chunked(titles, titles_per_query):
        params = {
            "action": "query",
            "format": "json",
            "formatversion": 2,
            "titles": "|".join(title_chunk),
            "prop": "imageinfo",
            "iiprop": "url|size|mime|mediatype|sha1|timestamp|user|comment|extmetadata",
            "iilimit": 1,
        }
        data = api_get(params)
        pages = (data.get("query", {}) or {}).get("pages", []) or []

        for page in pages:
            title = page.get("title", "")
            ii_list = page.get("imageinfo") or []
            if not ii_list:
                skip["no_imageinfo"] += 1
                continue
            ii = ii_list[0] or {}

            mime = (ii.get("mime") or "").lower()
            mediatype = (ii.get("mediatype") or "").upper()
            if not (mime.startswith("image/") or mediatype in {"BITMAP", "DRAWING"}):
                skip["not_image"] += 1
                continue

            ext = Path(_best_filename_from_title(title)).suffix.lower()

            if mime and mime not in allowed_mime:
                skip["mime_not_allowed"] += 1
                continue
            if ext and ext not in allowed_ext:
                skip["ext_not_allowed"] += 1
                continue

            size_bytes = ii.get("size")
            if max_bytes is not None and isinstance(size_bytes, int) and size_bytes > max_bytes:
                skip["too_large"] += 1
                continue

            extmeta = ii.get("extmetadata") or {}
            flat_ext = _flatten_extmetadata(extmeta)

            if require_public_domain:
                copyrighted = _normalize_text(flat_ext.get("ext_Copyrighted_text", ""))
                if copyrighted != "false":
                    skip["pd_filtered"] += 1
                    continue

            fields = {
                "title": title,
                "categories": flat_ext.get("ext_Categories_text", ""),
                "description": flat_ext.get("ext_ImageDescription_text", ""),
                "credit": flat_ext.get("ext_Credit_text", ""),
                "snippet": snippet_by_title.get(title, ""),
            }
            if not any(norm_q in _normalize_text(v) for v in fields.values() if v):
                skip["term_mismatch"] += 1
                continue

            url = ii.get("url")
            if not url:
                skip["no_url"] += 1
                continue

            dest_name = _best_filename_from_title(title)
            dest = save_dir / dest_name
            if dest.exists():
                dest = save_dir / f"{dest.stem}__{page.get('pageid')}{dest.suffix}"

            error_msg = ""
            try:
                download(url, dest)
                downloaded.append(dest)
                dest_written = str(dest)
            except Exception as e:
                skip["download_failed"] += 1
                error_msg = f"{type(e).__name__}: {e}"
                if not continue_on_error:
                    raise
                dest_written = ""

            row: Dict[str, Any] = {
                "pageid": page.get("pageid"),
                "title": title,
                "matched_query": search_word,
                "download_path": dest_written,
                "url": url,
                "mime": ii.get("mime"),
                "mediatype": ii.get("mediatype"),
                "size_bytes": ii.get("size"),
                "width": ii.get("width"),
                "height": ii.get("height"),
                "sha1": ii.get("sha1"),
                "timestamp": ii.get("timestamp"),
                "user": ii.get("user"),
                "comment": ii.get("comment"),
                "search_snippet_text": snippet_by_title.get(title, ""),
                "download_error": error_msg,
            }
            row.update(flat_ext)
            rows.append(row)

    ts = time.strftime("%Y%m%d_%H%M%S")
    csv_path = save_dir / f"commons_{_slug(search_word)}_{ts}_metadata.csv"

    base_cols = [
        "pageid", "title", "matched_query", "download_path", "url", "mime", "mediatype",
        "size_bytes", "width", "height", "sha1", "timestamp", "user", "comment",
        "search_snippet_text", "download_error",
    ]
    ext_cols = sorted({k for r in rows for k in r.keys() if k.startswith("ext_")})
    cols = base_cols + ext_cols

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in cols})

    if verbose:
        print(f"[commons] downloaded: {len(downloaded)} | metadata rows: {len(rows)} | csv: {csv_path}")
        print(f"[commons] skip breakdown: {skip}")
        print(
            f"[commons] constraints: allowed_mime={sorted(allowed_mime)} "
            f"allowed_ext={sorted(allowed_ext)} max_size_mb={max_size_mb} "
            f"max_download_mbps={max_download_mbps} rps={requests_per_second} "
            f"connect_timeout={connect_timeout} read_timeout={read_timeout} "
            f"titles_per_query={titles_per_query}"
        )

    return downloaded, csv_path, rows




def multi_download(
        multi_search_term,
        multi_save_directory,
        require_public_domain,
        ua,
        met_df
):
    # download from MET
    enriched, summary = download_from_met(
        multi_search_term,
        in_public_domain_bool = require_public_domain,
        met_df = met_df,
        save_directory = f"{multi_save_directory}/{multi_search_term}/images",
    )
    print("finished downloading from MET")
    # download from Art Institute of Chicago
    summary = download_aic_by_search(
        save_dir=multi_save_directory,
        search_term=multi_search_term,
        public_domain=require_public_domain,          # recommended; avoids downloading non-PD images for reuse
        # optional knobs:
        requests_per_second=1.0,     # respectful throttling
        include_alt_images=True,     # download “differences” (alt imaging) too
        write_metadata_json=True,
    )
    print("finished downloading from AIC")

    # Cleveland Museum of Art (CC0 only)
    stats_cma = download_cma_by_search(
        save_dir=multi_save_directory,
        search_term=multi_search_term,
        public_domain=require_public_domain,
        group_by_medium=False,
        requests_per_second=1.0,
        # exhaustive_metadata_scan=True
    )
    print("finished downloading from CMA")

    # Smithsonian
    try:
        stats_si = download_smithsonian_by_search(
            save_dir=multi_save_directory,
            search_term=multi_search_term,
            public_domain=require_public_domain,
            api_key=os.environ["smithsonian_key"],
            group_by_medium=False,
            requests_per_second=1.0,
        )
        print("finished downloading from smithsonian")

    except:
        print(f"error downloading from smithsonian.  check key")

    # wikimedia commons
    try:
        downloaded, csv_path, rows = download_commons_images(
            search_word=multi_search_term,
            save_path=f"{multi_save_directory}/{multi_search_term}/images/",
            require_public_domain=require_public_domain,
            user_agent=ua,
            allowed_mime={"image/jpeg", "image/png"},
            allowed_ext={".jpg", ".jpeg", ".png"},
            max_size_mb=225,
            requests_per_second=0.5,
            max_download_mbps=20.0,
            connect_timeout=10.0,
            read_timeout=120.0,     # <-- key change
            titles_per_query=25,    # <-- key change
            verbose=True,
            max_results = 1000
        )
        print("finished downloading from commons")
    except:
        print(f"error downloading from wiki commons, check user_agent")