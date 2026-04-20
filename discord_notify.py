"""Kirim notifikasi Discord via webhook setelah kota selesai di-scrape."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
from pathlib import Path

import requests


def send_webhook(webhook_url: str, payload: dict, timeout: int = 15) -> bool:
    """POST ke Discord webhook. Return True kalau sukses (2xx)."""
    try:
        resp = requests.post(webhook_url, json=payload, timeout=timeout)
        if 200 <= resp.status_code < 300:
            return True
        print(f"[WARN] Discord webhook gagal: HTTP {resp.status_code} | {resp.text[:200]}")
        return False
    except Exception as e:
        print(f"[WARN] Discord webhook exception: {e}")
        return False


def load_cities(data_dir: Path) -> list[dict]:
    cities_path = data_dir / "cities.py"
    spec = importlib.util.spec_from_file_location("cities_module", cities_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.CITIES


def count_files(run_dir: Path) -> int:
    return len(list(run_dir.glob("maps*.txt")))


def build_city_done_payload(city: dict, progress: dict, run_dir: Path,
                            total_keywords: int, next_city: dict | None) -> dict:
    """Build rich embed untuk Discord."""
    duration_sec = progress.get("total_duration_sec", 0)
    hours = duration_sec / 3600
    files = count_files(run_dir)
    domains = progress.get("domains_found", 0)
    jobs = progress.get("job_count", 1)

    next_line = (
        f"{next_city['name']}, {next_city['country']}" if next_city else "Tidak ada (antrian selesai)"
    )

    embed = {
        "title": f"Scraping Selesai: {city['name']}, {city['country']}",
        "color": 0x2ECC71,
        "fields": [
            {"name": "Domain unik", "value": f"{domains:,}", "inline": True},
            {"name": "File output", "value": f"{files} file", "inline": True},
            {"name": "Keyword", "value": f"{progress.get('keywords_done', 0)}/{total_keywords}", "inline": True},
            {"name": "Total bisnis", "value": f"{progress.get('total_biz', 0):,}", "inline": True},
            {"name": "Bisnis + website", "value": f"{progress.get('total_with_website', 0):,}", "inline": True},
            {"name": "Durasi total", "value": f"{hours:.1f} jam ({jobs} job)", "inline": True},
            {"name": "Folder hasil", "value": f"`results/{city['slug']}/`", "inline": False},
            {"name": "Kota berikutnya", "value": next_line, "inline": False},
        ],
        "footer": {"text": "Google Maps Domain Finder"},
    }

    return {
        "username": "Maps Scraper",
        "embeds": [embed],
    }


def build_partial_payload(city: dict, progress: dict, total_keywords: int) -> dict:
    embed = {
        "title": f"Progress: {city['name']}, {city['country']}",
        "description": "Job selesai (timeout tercapai), akan di-resume di cron berikutnya.",
        "color": 0xF1C40F,
        "fields": [
            {"name": "Keyword done", "value": f"{progress.get('keywords_done', 0)}/{total_keywords}", "inline": True},
            {"name": "Domain sementara", "value": f"{progress.get('domains_found', 0):,}", "inline": True},
            {"name": "Job run", "value": f"{progress.get('job_count', 1)}", "inline": True},
        ],
    }
    return {"username": "Maps Scraper", "embeds": [embed]}


def build_all_done_payload(completed_count: int) -> dict:
    embed = {
        "title": "SEMUA KOTA SELESAI",
        "description": f"Seluruh {completed_count} kota di antrian telah selesai di-scrape.",
        "color": 0x3498DB,
    }
    return {"username": "Maps Scraper", "embeds": [embed]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["city_done", "partial", "all_done"], required=True)
    parser.add_argument("--data-dir", default="./data")
    parser.add_argument("--city", help="Slug kota (untuk mode city_done & partial)")
    args = parser.parse_args()

    webhook = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
    if not webhook:
        print("[WARN] DISCORD_WEBHOOK_URL kosong, skip notifikasi.")
        return 0

    data_dir = Path(args.data_dir).resolve()

    if args.mode == "all_done":
        state_path = data_dir / "state.json"
        count = 0
        if state_path.exists():
            try:
                state = json.loads(state_path.read_text(encoding="utf-8"))
                count = len(state.get("completed", []))
            except Exception:
                pass
        payload = build_all_done_payload(count)
        send_webhook(webhook, payload)
        return 0

    if not args.city:
        print("[ERROR] --city wajib untuk mode ini")
        return 1

    cities = load_cities(data_dir)
    city = next((c for c in cities if c["slug"] == args.city), None)
    if not city:
        print(f"[ERROR] Kota '{args.city}' tidak ada di cities.py")
        return 1

    run_dir = data_dir / "results" / args.city
    progress_path = run_dir / "progress.json"
    progress = {}
    if progress_path.exists():
        try:
            progress = json.loads(progress_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    keywords_path = data_dir / "keywordsmaps_core.txt"
    total_keywords = 0
    if keywords_path.exists():
        for line in keywords_path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if s and not s.startswith("#"):
                total_keywords += 1

    if args.mode == "city_done":
        idx = next((i for i, c in enumerate(cities) if c["slug"] == args.city), -1)
        next_city = cities[idx + 1] if 0 <= idx < len(cities) - 1 else None
        payload = build_city_done_payload(city, progress, run_dir, total_keywords, next_city)
    else:
        payload = build_partial_payload(city, progress, total_keywords)

    send_webhook(webhook, payload)
    return 0


if __name__ == "__main__":
    sys.exit(main())
