import json
import os
import shutil
import subprocess
from pathlib import Path
from datetime import datetime, timezone

from service.config import (
    POPULARITY_PATH,
    ITEM_CF_PATH,
    SNAPSHOT_PATH,
    REGISTRY_PATH,
    VERSION_LOG_PATH,
    CONTAINER_IMAGE_DIGEST,
)


def get_git_sha():
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            text=True
        ).strip()
    except Exception:
        return "local"


def load_registry():
    if os.path.exists(REGISTRY_PATH):
        with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"active_version": None, "versions": {}}


def main():
    if not os.path.exists(POPULARITY_PATH):
        raise FileNotFoundError(f"Missing {POPULARITY_PATH}")
    if not os.path.exists(ITEM_CF_PATH):
        raise FileNotFoundError(f"Missing {ITEM_CF_PATH}")

    version = datetime.now(timezone.utc).strftime("v%Y%m%d%H%M%S")
    version_dir = Path("artifacts") / "versions" / version
    version_dir.mkdir(parents=True, exist_ok=True)

    pop_target = version_dir / "popularity.json"
    cf_target = version_dir / "item_cf.json"

    shutil.copy2(POPULARITY_PATH, pop_target)
    shutil.copy2(ITEM_CF_PATH, cf_target)

    registry = load_registry()
    registry["versions"][version] = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "models": {
            "popularity": str(pop_target).replace("\\", "/"),
            "item_cf": str(cf_target).replace("\\", "/"),
        },
        "data_snapshot_id": os.path.basename(SNAPSHOT_PATH),
        "pipeline_git_sha": get_git_sha(),
        "container_image_digest": CONTAINER_IMAGE_DIGEST,
    }
    registry["active_version"] = version

    with open(REGISTRY_PATH, "w", encoding="utf-8") as f:
        json.dump(registry, f, indent=2)

    with open(VERSION_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"{version} published at {datetime.now(timezone.utc).isoformat()}\n")

    print(f"Published version: {version}")
    print(f"Registry updated: {REGISTRY_PATH}")


if __name__ == "__main__":
    main()