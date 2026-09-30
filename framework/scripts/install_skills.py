"""Install the plugins listed in skills.json that update_skills.py syncs from.

Marketplaces must already be registered (`claude plugin marketplace add ...` /
an entry in .claude/settings.json's extraKnownMarketplaces). Run
`just update-skills` afterwards to sync the installed skills.

Usage:
    python scripts/install_skills.py
"""

import json
from pathlib import Path
import subprocess
import sys

_MANIFEST = Path(__file__).resolve().parent.parent / "skills.json"
_CACHE_ROOT = Path.home() / ".claude" / "plugins" / "cache"


def main() -> None:
    failed = False

    for source, info in json.loads(_MANIFEST.read_text()).items():
        # Marketplace-layout sources are read from the marketplace checkout itself.
        if info.get("layout") == "marketplace":
            continue

        if (_CACHE_ROOT / info["marketplace"] / info["plugin"]).is_dir():
            print(f"skip {source}: already installed")
            continue

        spec = f"{info['plugin']}@{info['marketplace']}"
        print(f"install {spec}")
        if subprocess.run(["claude", "plugin", "install", spec], check=False).returncode:
            failed = True

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
