"""Google Maps Domain Finder - non-interactive city scraper.

Usage:
    python maps_domain_finder.py \
        --city jakarta \
        --data-dir ./data \
        --max-runtime-sec 17400

Exit codes:
    0 = kota selesai (semua keyword diproses)
    1 = error
    2 = partial (timeout tercapai, resume di job berikutnya)

Env vars:
    MAPS_API_KEYS (optional): string berisi baris API key (AIzaSy...).
        Kalau tidak di-set, fallback baca dari file Maps_Location.txt di cwd.
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import importlib.util
import json
import math
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import aiohttp

CONCURRENCY = 50
POINT_CONCURRENCY = 20
CONNECTION_LIMIT = 100
TIMEOUT = 15
MAX_PER_FILE = 1000
GRID_RADIUS_M = 25_000

TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
NEARBY_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
PLACE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"


def load_cities(data_dir: Path) -> list[dict]:
    """Import cities.py module dari data_dir dan return CITIES list."""
    cities_path = data_dir / "cities.py"
    if not cities_path.exists():
        raise FileNotFoundError(f"cities.py tidak ditemukan di {data_dir}")
    spec = importlib.util.spec_from_file_location("cities_module", cities_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.CITIES


def load_keywords(filepath: Path) -> list[str]:
    """Load keyword dari file. Skip blank lines + komentar '#'. Dedup case-insensitive."""
    if not filepath.exists():
        print(f"[WARN] File keyword tidak ditemukan: {filepath}")
        return []
    keywords: list[str] = []
    seen: set[str] = set()
    for line in filepath.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        lower = line.lower()
        if lower not in seen:
            seen.add(lower)
            keywords.append(line)
    return keywords


def load_keys_from_env_or_file() -> list[str]:
    """Prioritas: env MAPS_API_KEYS → fallback file lokal (apikeymaps.txt / Maps_Location.txt)."""
    env_val = os.environ.get("MAPS_API_KEYS", "").strip()
    source = None
    text = ""
    if env_val:
        source = "env MAPS_API_KEYS"
        text = env_val
    else:
        for candidate in ("apikeymaps.txt", "Maps_Location.txt"):
            fallback = Path(candidate)
            if fallback.exists():
                source = f"file {fallback}"
                text = fallback.read_text(encoding="utf-8")
                break

    keys: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("AIzaSy"):
            keys.append(stripped)
    if source:
        print(f"[INFO] API key dimuat dari {source}: {len(keys)} key")
    return keys


def extract_domain(url: str) -> str | None:
    if not url:
        return None
    try:
        parsed = urlparse(url)
        host = parsed.hostname or ""
        if host.startswith("www."):
            host = host[4:]
        return host if host else None
    except Exception:
        return None


def generate_grid_from_center(
    lat: float, lng: float, radius_km: float, step_m: int = GRID_RADIUS_M
) -> list[tuple[float, float]]:
    """Generate grid titik dalam radius_km dari center. Step antar titik ~ step_m * sqrt(2)."""
    step_deg_lat = (step_m * math.sqrt(2)) / 111_320
    radius_deg_lat = radius_km * 1000 / 111_320
    min_lat = lat - radius_deg_lat
    max_lat = lat + radius_deg_lat

    points: list[tuple[float, float]] = []
    cur_lat = min_lat
    while cur_lat <= max_lat:
        cos_factor = max(math.cos(math.radians(cur_lat)), 0.1)
        step_deg_lng = step_deg_lat / cos_factor
        radius_deg_lng = (radius_km * 1000) / (111_320 * cos_factor)
        min_lng = lng - radius_deg_lng
        max_lng = lng + radius_deg_lng
        cur_lng = min_lng
        while cur_lng <= max_lng:
            points.append((round(cur_lat, 5), round(cur_lng, 5)))
            cur_lng += step_deg_lng
        cur_lat += step_deg_lat
    if not points:
        points = [(round(lat, 5), round(lng, 5))]
    return points


class FileWriter:
    """Tulis domain ke file bernomor maps1.txt, maps2.txt, ... di output_dir.

    Saat resume, auto-advance file_index ke file berikutnya yang belum ada.
    """

    def __init__(self, location: str, max_per_file: int, output_dir: Path):
        self.location = location
        self.max_per_file = max_per_file
        self.output_dir = output_dir
        self._file_index = self._detect_next_index()
        self._buffer: list[str] = []
        self._files_written: list[str] = []

    def _detect_next_index(self) -> int:
        existing = sorted(self.output_dir.glob("maps*.txt"))
        if not existing:
            return 1
        max_idx = 0
        for p in existing:
            stem = p.stem
            if stem.startswith("maps") and stem[4:].isdigit():
                max_idx = max(max_idx, int(stem[4:]))
        return max_idx + 1

    def _filename(self, idx: int) -> str:
        return str(self.output_dir / f"maps{idx}.txt")

    def _write_buffer(self):
        if not self._buffer:
            return
        fname = self._filename(self._file_index)
        now_display = datetime.now().strftime("%d %B %Y, %H:%M:%S")
        with open(fname, "w", encoding="utf-8") as f:
            f.write(">> Hasil Pencarian Domain - Google Maps\n")
            f.write(f">> Tanggal: {now_display}\n")
            f.write(f">> Lokasi: {self.location}\n")
            f.write(f">> File: maps{self._file_index}.txt (bagian {self._file_index})\n")
            f.write(f">> Domain dalam file ini: {len(self._buffer)}\n")
            f.write("-" * 40 + "\n")
            for d in self._buffer:
                f.write(d + "\n")
        self._files_written.append(fname)
        print(f"  [+] Tersimpan: {fname} ({len(self._buffer)} domain)")
        self._file_index += 1
        self._buffer = []

    def add(self, domain: str):
        self._buffer.append(domain)
        if len(self._buffer) >= self.max_per_file:
            self._write_buffer()

    def flush(self):
        if self._buffer:
            self._write_buffer()

    @property
    def files_written(self) -> list[str]:
        return list(self._files_written)


class KeyRotator:
    """Round-robin key pool dengan bad-key tracking."""

    def __init__(self, keys: list[str]):
        self._keys = list(keys)
        self._exhausted: set[str] = set()
        self._counter = 0

    def next_key(self) -> str:
        total = len(self._keys)
        if total == 0 or len(self._exhausted) >= total:
            raise RuntimeError("Semua API key sudah habis/error!")
        for _ in range(total):
            idx = self._counter % total
            self._counter += 1
            key = self._keys[idx]
            if key not in self._exhausted:
                return key
        raise RuntimeError("Semua API key sudah habis/error!")

    def mark_bad(self, key: str):
        self._exhausted.add(key)

    @property
    def available_count(self) -> int:
        return len(self._keys) - len(self._exhausted)


async def nearby_search(
    session: aiohttp.ClientSession,
    rotator: KeyRotator,
    lat: float,
    lng: float,
    keyword: str,
    radius_m: int,
    page_token: str | None = None,
    sticky_key: str | None = None,
) -> tuple[dict, str | None]:
    key = sticky_key or rotator.next_key()
    if page_token:
        params = {"pagetoken": page_token, "key": key}
    else:
        params = {
            "location": f"{lat},{lng}",
            "radius": str(radius_m),
            "keyword": keyword,
            "key": key,
        }
    for attempt in range(3):
        try:
            async with session.get(
                NEARBY_SEARCH_URL,
                params=params,
                timeout=aiohttp.ClientTimeout(total=TIMEOUT),
            ) as resp:
                data = await resp.json()
                status = data.get("status", "")
                if status in ("OK", "ZERO_RESULTS"):
                    return data, key
                if status in ("REQUEST_DENIED", "OVER_QUERY_LIMIT"):
                    rotator.mark_bad(key)
                    try:
                        key = rotator.next_key()
                    except RuntimeError:
                        raise
                    params["key"] = key
                    continue
                return data, key
        except Exception:
            if attempt < 2:
                await asyncio.sleep(1)
    return {"status": "ERROR", "results": []}, key


async def get_website(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    rotator: KeyRotator,
    place_id: str,
) -> str | None:
    async with semaphore:
        try:
            key = rotator.next_key()
        except RuntimeError:
            return None
        params = {"place_id": place_id, "fields": "website", "key": key}
        for attempt in range(2):
            try:
                async with session.get(
                    PLACE_DETAILS_URL,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=TIMEOUT),
                ) as resp:
                    data = await resp.json()
                    status = data.get("status", "")
                    if status == "OK":
                        return data.get("result", {}).get("website")
                    if status in ("REQUEST_DENIED", "OVER_QUERY_LIMIT"):
                        rotator.mark_bad(key)
                        try:
                            key = rotator.next_key()
                        except RuntimeError:
                            return None
                        params["key"] = key
                        continue
                    return None
            except Exception:
                if attempt < 1:
                    await asyncio.sleep(0.5)
        return None


async def search_point(
    session: aiohttp.ClientSession,
    detail_sem: asyncio.Semaphore,
    rotator: KeyRotator,
    keyword: str,
    lat: float,
    lng: float,
    radius_m: int,
    seen_pids: set[str],
) -> list[tuple[str, str | None]]:
    new_places: list[tuple[str, str]] = []
    page_token = None
    sticky_key: str | None = None
    while True:
        if page_token:
            await asyncio.sleep(2)
        data, sticky_key = await nearby_search(
            session, rotator, lat, lng, keyword, radius_m,
            page_token, sticky_key=sticky_key,
        )
        for place in data.get("results", []):
            pid = place.get("place_id", "")
            name = place.get("name", "unknown")
            if pid and pid not in seen_pids:
                seen_pids.add(pid)
                new_places.append((pid, name))
        page_token = data.get("next_page_token")
        if not page_token:
            break
    if not new_places:
        return []
    tasks = [get_website(session, detail_sem, rotator, pid) for pid, _ in new_places]
    websites = await asyncio.gather(*tasks)
    return [(name, ws) for (_, name), ws in zip(new_places, websites)]


def load_existing_domains(run_dir: Path) -> set[str]:
    """Load semua domain yang sudah tersimpan di maps*.txt (untuk dedup saat resume)."""
    domains: set[str] = set()
    for file_path in sorted(run_dir.glob("maps*.txt")):
        try:
            for line in file_path.read_text(encoding="utf-8").splitlines():
                s = line.strip()
                if not s or s.startswith(">>") or s.startswith("-"):
                    continue
                domains.add(s)
        except OSError:
            continue
    return domains


def load_progress(run_dir: Path) -> dict:
    path = run_dir / "progress.json"
    if not path.exists():
        return {
            "keywords_done": 0,
            "domains_found": 0,
            "job_count": 0,
            "total_duration_sec": 0.0,
            "total_biz": 0,
            "total_with_website": 0,
        }
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {
            "keywords_done": 0,
            "domains_found": 0,
            "job_count": 0,
            "total_duration_sec": 0.0,
            "total_biz": 0,
            "total_with_website": 0,
        }


def save_progress(run_dir: Path, progress: dict):
    progress["last_updated"] = datetime.now(timezone.utc).isoformat()
    path = run_dir / "progress.json"
    path.write_text(json.dumps(progress, indent=2, ensure_ascii=False), encoding="utf-8")


def write_all_domains(run_dir: Path) -> tuple[Path | None, int]:
    domains: set[str] = set()
    for file_path in sorted(run_dir.glob("maps*.txt")):
        try:
            for line in file_path.read_text(encoding="utf-8").splitlines():
                s = line.strip()
                if not s or s.startswith(">>") or s.startswith("-"):
                    continue
                domains.add(s)
        except OSError:
            continue
    if not domains:
        return None, 0
    out_path = run_dir / "all_domains.txt"
    with open(out_path, "w", encoding="utf-8") as f:
        for d in sorted(domains):
            f.write(d + "\n")
    return out_path, len(domains)


def write_summary(
    run_dir: Path, city: dict, progress: dict,
    keywords_count: int, files_written: list[str], total_unique: int,
    grid_points_count: int, completed: bool,
):
    summary_path = run_dir / "summary.txt"
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(">> Ringkasan Run - Google Maps Domain Finder\n")
        f.write(f">> Status          : {'SELESAI' if completed else 'BELUM SELESAI (partial)'}\n")
        f.write(f">> Waktu update    : {datetime.now().strftime('%d %B %Y, %H:%M:%S')}\n")
        f.write(f">> Kota            : {city['name']}, {city['country']}\n")
        f.write(f">> Slug            : {city['slug']}\n")
        f.write(f">> Center          : ({city['lat']}, {city['lng']}) radius {city['radius_km']} km\n")
        f.write(f">> Grid points     : {grid_points_count}\n")
        f.write(f">> Keyword total   : {keywords_count}\n")
        f.write(f">> Keyword done    : {progress['keywords_done']}\n")
        f.write(f">> Total bisnis    : {progress['total_biz']}\n")
        f.write(f">> Bisnis+website  : {progress['total_with_website']}\n")
        f.write(f">> Domain unik     : {total_unique}\n")
        f.write(f">> Durasi total    : {progress['total_duration_sec']:.0f} detik"
                f" ({progress['total_duration_sec']/3600:.1f} jam)\n")
        f.write(f">> Jumlah job run  : {progress['job_count']}\n")
        f.write(f">> File output     : {len(files_written)}\n")
        for fname in files_written:
            f.write(f"   - {fname}\n")


async def scrape_city(
    city: dict,
    keywords: list[str],
    keys: list[str],
    run_dir: Path,
    max_runtime_sec: float,
) -> int:
    """Return exit code: 0 = selesai, 2 = partial (timeout)."""
    run_dir.mkdir(parents=True, exist_ok=True)
    progress = load_progress(run_dir)
    progress["job_count"] = progress.get("job_count", 0) + 1

    grid_points = generate_grid_from_center(
        city["lat"], city["lng"], city["radius_km"], step_m=GRID_RADIUS_M
    )

    existing_domains = load_existing_domains(run_dir)
    all_domains: set[str] = set(existing_domains)
    start_keyword_idx = progress.get("keywords_done", 0)
    total_biz = progress.get("total_biz", 0)
    total_with_website = progress.get("total_with_website", 0)

    print("=" * 70)
    print(f"  GOOGLE MAPS DOMAIN FINDER - {city['name']}, {city['country']}")
    print(f"  Center       : ({city['lat']}, {city['lng']}) radius {city['radius_km']} km")
    print(f"  Grid points  : {len(grid_points)}")
    print(f"  Keywords     : {len(keywords)} (resume dari index {start_keyword_idx})")
    print(f"  Domain aktif : {len(all_domains)} (loaded dari file existing)")
    print(f"  Job run ke   : {progress['job_count']}")
    print(f"  Max runtime  : {max_runtime_sec:.0f} detik")
    print("=" * 70)

    if start_keyword_idx >= len(keywords):
        print("[INFO] Semua keyword sudah diproses sebelumnya. Finalisasi.")
        _finalize(run_dir, city, progress, len(keywords), len(grid_points), completed=True)
        return 0

    rotator = KeyRotator(keys)
    connector = aiohttp.TCPConnector(
        limit=CONNECTION_LIMIT, limit_per_host=CONNECTION_LIMIT, ssl=False
    )
    writer = FileWriter(
        location=f"{city['name']}, {city['country']}",
        max_per_file=MAX_PER_FILE, output_dir=run_dir,
    )

    detail_sem = asyncio.Semaphore(CONCURRENCY)
    start = time.time()
    keys_exhausted = False
    timeout_reached = False
    current_idx = start_keyword_idx

    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            for i in range(start_keyword_idx, len(keywords)):
                keyword = keywords[i]
                current_idx = i

                elapsed = time.time() - start
                if elapsed > max_runtime_sec:
                    print(f"\n[!] Max runtime ({max_runtime_sec:.0f}s) tercapai sebelum keyword ke-{i+1}. Stop.")
                    timeout_reached = True
                    break

                seen_pids: set[str] = set()
                print(
                    f"\n[{i+1}/{len(keywords)}] keyword=\"{keyword}\""
                    f" | grid={len(grid_points)}"
                    f" | domain={len(all_domains)}"
                    f" | key={rotator.available_count}"
                    f" | {elapsed:.0f}s"
                )

                done_counter = {"n": 0, "new_domains": 0}
                exhausted_flag = asyncio.Event()
                queue: asyncio.Queue = asyncio.Queue()
                for pt in grid_points:
                    queue.put_nowait(pt)

                async def worker():
                    nonlocal total_biz, total_with_website
                    while not exhausted_flag.is_set():
                        try:
                            lat, lng = queue.get_nowait()
                        except asyncio.QueueEmpty:
                            return
                        try:
                            results = await search_point(
                                session, detail_sem, rotator,
                                keyword, lat, lng, GRID_RADIUS_M, seen_pids,
                            )
                        except RuntimeError:
                            exhausted_flag.set()
                            return
                        for name, website in results:
                            total_biz += 1
                            if website:
                                total_with_website += 1
                                dom = extract_domain(website)
                                if dom and dom not in all_domains:
                                    all_domains.add(dom)
                                    done_counter["new_domains"] += 1
                                    writer.add(dom)
                        results = None
                        done_counter["n"] += 1

                workers = [asyncio.create_task(worker()) for _ in range(POINT_CONCURRENCY)]
                await asyncio.gather(*workers)

                if exhausted_flag.is_set():
                    print("  !! Semua API key habis.")
                    keys_exhausted = True
                    break

                progress["keywords_done"] = i + 1
                progress["domains_found"] = len(all_domains)
                progress["total_biz"] = total_biz
                progress["total_with_website"] = total_with_website
                progress["total_duration_sec"] = progress.get("total_duration_sec", 0) + 0
                save_progress(run_dir, progress)

                print(
                    f"  -> +{done_counter['new_domains']} domain baru"
                    f" | total: {len(all_domains)}"
                )

                del seen_pids
                gc.collect()

    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\n[!] Interrupted.")
        timeout_reached = True
    finally:
        writer.flush()
        elapsed_this_job = time.time() - start
        progress["total_duration_sec"] = progress.get("total_duration_sec", 0) + elapsed_this_job
        progress["domains_found"] = len(all_domains)
        progress["total_biz"] = total_biz
        progress["total_with_website"] = total_with_website
        if not timeout_reached and not keys_exhausted:
            progress["keywords_done"] = max(progress.get("keywords_done", 0), current_idx + 1)
        save_progress(run_dir, progress)

    completed = (progress["keywords_done"] >= len(keywords)) and not keys_exhausted
    _finalize(run_dir, city, progress, len(keywords), len(grid_points), completed=completed)

    if keys_exhausted:
        print("\n[ERROR] Semua API key habis. Tambah key baru lalu re-run.")
        return 1
    if completed:
        print(f"\n[SUCCESS] {city['name']} SELESAI scraping.")
        return 0
    print(f"\n[PARTIAL] Timeout, akan di-resume di job berikutnya.")
    return 2


def _finalize(run_dir: Path, city: dict, progress: dict,
              total_keywords: int, grid_points_count: int, completed: bool):
    consolidated_path, consolidated_count = write_all_domains(run_dir)
    files_written = [str(p) for p in sorted(run_dir.glob("maps*.txt"))]
    write_summary(
        run_dir=run_dir, city=city, progress=progress,
        keywords_count=total_keywords, files_written=files_written,
        total_unique=consolidated_count, grid_points_count=grid_points_count,
        completed=completed,
    )
    print("\n" + "=" * 70)
    print("  RINGKASAN")
    print("=" * 70)
    print(f"  Kota               : {city['name']}, {city['country']}")
    print(f"  Keyword diproses   : {progress['keywords_done']}/{total_keywords}")
    print(f"  Total bisnis       : {progress['total_biz']}")
    print(f"  Bisnis + website   : {progress['total_with_website']}")
    print(f"  Domain unik        : {consolidated_count}")
    print(f"  File output        : {len(files_written)} file di {run_dir}/")
    if consolidated_path:
        print(f"  all_domains.txt    : {consolidated_path}")
    print(f"  Summary            : {run_dir / 'summary.txt'}")


def parse_args():
    parser = argparse.ArgumentParser(description="Maps Domain Finder - per city mode")
    parser.add_argument("--city", required=True, help="Slug kota (mis. jakarta, tokyo, london)")
    parser.add_argument("--data-dir", default="./data",
                        help="Path ke repo data (berisi cities.py, keywordsmaps_core.txt, results/)")
    parser.add_argument("--keywords", default=None,
                        help="Path file keyword. Default: <data-dir>/keywordsmaps_core.txt")
    parser.add_argument("--max-runtime-sec", type=int, default=17400,
                        help="Max runtime detik sebelum graceful stop (default 17400 = 4h50m)")
    return parser.parse_args()


async def async_main():
    args = parse_args()
    data_dir = Path(args.data_dir).resolve()
    if not data_dir.exists():
        print(f"[ERROR] data-dir tidak ada: {data_dir}")
        sys.exit(1)

    cities = load_cities(data_dir)
    city = next((c for c in cities if c["slug"] == args.city), None)
    if not city:
        print(f"[ERROR] Slug '{args.city}' tidak ditemukan di cities.py")
        sys.exit(1)

    keywords_path = Path(args.keywords) if args.keywords else (data_dir / "keywordsmaps_core.txt")
    keywords = load_keywords(keywords_path)
    if not keywords:
        print(f"[ERROR] Tidak ada keyword di {keywords_path}")
        sys.exit(1)

    keys = load_keys_from_env_or_file()
    if not keys:
        print("[ERROR] Tidak ada API key (set env MAPS_API_KEYS atau buat Maps_Location.txt)")
        sys.exit(1)

    run_dir = data_dir / "results" / city["slug"]
    code = await scrape_city(
        city=city, keywords=keywords, keys=keys,
        run_dir=run_dir, max_runtime_sec=args.max_runtime_sec,
    )
    sys.exit(code)


if __name__ == "__main__":
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print("\n[!] Dihentikan paksa.")
        sys.exit(2)
