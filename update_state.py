"""Update state.json setelah 1 kota selesai di-scrape.

Usage: python update_state.py --data-dir ./data --city jakarta

Efek:
- Append slug ke completed[]
- Naikkan current_index
- Kalau current_index >= len(cities), set all_done=true
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


def load_cities(data_dir: Path) -> list[dict]:
    cities_path = data_dir / "cities.py"
    spec = importlib.util.spec_from_file_location("cities_module", cities_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.CITIES


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="./data")
    parser.add_argument("--city", required=True, help="Slug kota yang baru selesai")
    args = parser.parse_args()

    data_dir = Path(args.data_dir).resolve()
    state_path = data_dir / "state.json"
    cities = load_cities(data_dir)

    state = {"current_index": 0, "completed": [], "all_done": False,
             "started_at": None, "last_updated": None}
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    if not state.get("started_at"):
        state["started_at"] = datetime.now(timezone.utc).isoformat()

    completed = state.get("completed", [])
    if args.city not in completed:
        completed.append(args.city)
    state["completed"] = completed

    idx = next((i for i, c in enumerate(cities) if c["slug"] == args.city), -1)
    if idx >= 0:
        state["current_index"] = max(state.get("current_index", 0), idx + 1)

    if state["current_index"] >= len(cities):
        state["all_done"] = True

    state["last_updated"] = datetime.now(timezone.utc).isoformat()
    state_path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[OK] State updated. current_index={state['current_index']}/{len(cities)}"
          f" completed={len(completed)} all_done={state['all_done']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
