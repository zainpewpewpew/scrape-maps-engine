# scrape-maps-engine

Auto scraper Google Maps Domain Finder dengan GitHub Actions.
Scan kota-kota besar dunia (urut negara A-Z, kota A-Z), kirim notifikasi Discord setiap kota selesai.

## Arsitektur 2-repo

- **Repo ini (public)** — engine: code + workflow. Pakai GitHub Actions unlimited minutes karena public.
- **Repo private data** (`scrape-maps-data`) — input (cities, keywords) & output (results). Workflow clone via PAT.

Hasil scraping tetap rahasia karena push ke repo private, kode generic bisa public.

## Flow

```
cron 5 jam -> workflow -> clone data-repo -> scraper -> push results -> discord notif
```

1. Cron trigger tiap 5 jam (atau manual via workflow_dispatch)
2. Checkout engine repo (ini)
3. Clone `scrape-maps-data` ke `./data` pakai `DATA_REPO_PAT`
4. Baca `data/state.json` → pilih kota aktif (urutan di `data/cities.py`)
5. Jalankan scraper max 4h50m (4h50m < 6h hard limit Actions) — checkpoint per keyword
6. Kalau kota SELESAI → update state + notif Discord
7. Kalau PARTIAL (timeout) → notif partial, cron berikut lanjut
8. Commit & push hasil ke `scrape-maps-data`

## Secrets yang harus di-set di repo ini

| Nama | Isi |
|------|-----|
| `MAPS_API_KEYS` | Seluruh isi `Maps_Location.txt` (104 API key Google Maps) |
| `DISCORD_WEBHOOK_URL` | URL Discord webhook |
| `DATA_REPO_PAT` | Personal Access Token (fine-grained) dengan akses Contents:RW ke `scrape-maps-data` |
| `DATA_REPO_URL` | `github.com/<user>/scrape-maps-data.git` (tanpa `https://`) |

## File di repo ini

```
maps_domain_finder.py      # Main scraper: non-interactive, checkpoint resume, graceful timeout
discord_notify.py          # Kirim embed ke Discord webhook
get_current_city.py        # Print slug kota aktif (dipakai di workflow shell)
update_state.py            # Advance state.json setelah kota selesai
requirements.txt           # aiohttp, requests
.github/workflows/scrape.yml
.gitignore                 # Exclude Maps_Location.txt & data/
```

## Testing lokal

Set env + sediakan folder `data/` yang meniru repo data (berisi `cities.py`, `keywordsmaps_core.txt`, `state.json`):

```bash
$env:MAPS_API_KEYS = (Get-Content Maps_Location.txt -Raw)
python maps_domain_finder.py --city jakarta --data-dir ../ScrapeMapsData --max-runtime-sec 600
```

Atau tanpa env (fallback ke `Maps_Location.txt` di cwd):

```bash
python maps_domain_finder.py --city singapore --data-dir ../ScrapeMapsData --max-runtime-sec 600
```

## Exit codes scraper

- `0` = kota selesai (semua keyword diproses)
- `2` = partial (timeout, resume di job berikutnya)
- `1` = error (mis. semua API key habis)

## Adjust schedule

Edit `.github/workflows/scrape.yml` baris `cron: '0 */5 * * *'`:

- `0 */5 * * *` — tiap 5 jam (default, hemat)
- `0 */3 * * *` — tiap 3 jam (lebih agresif)
- `*/30 * * * *` — tiap 30 menit (paling cepat, concurrency guard mencegah overlap)
