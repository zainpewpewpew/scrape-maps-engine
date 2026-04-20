"""Print slug kota yang sedang aktif (untuk dipakai di shell workflow).

Usage: python get_current_city.py --data-dir ./data
Output (stdout): slug kota, atau 'ALL_DONE' kalau semua kota sudah selesai.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
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
    args = parser.parse_args()

    data_dir = Path(args.data_dir).resolve()
    state_path = data_dir / "state.json"

    state = {"current_index": 0, "completed": [], "all_done": False}
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    if state.get("all_done"):
        print("ALL_DONE")
        return 0

    cities = load_cities(data_dir)
    idx = state.get("current_index", 0)
    if idx >= len(cities):
        print("ALL_DONE")
        return 0

    print(cities[idx]["slug"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
