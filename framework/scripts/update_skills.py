"""Re-sync marketplace-sourced .claude/skills/ from the local plugin cache,
per the manifest in skills.json. Requires the marketplace already installed
locally (`claude plugin marketplace add ...` / an entry in
.claude/settings.json's extraKnownMarketplaces, then installed).

Usage:
    python scripts/update_skills.py
"""

import json
from pathlib import Path
import shutil
import subprocess
import sys

_REPO_ROOT = Path(__file__).resolve().parent.parent
_MANIFEST = _REPO_ROOT / "skills.json"
_GIT_ROOT = Path(
    subprocess.run(
        ["git", "-C", str(_REPO_ROOT), "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
)
_SKILLS_DIR = _GIT_ROOT / ".claude" / "skills"
_PLUGINS_ROOT = Path.home() / ".claude" / "plugins"
_CACHE_ROOT = _PLUGINS_ROOT / "cache"
_MARKETPLACES_ROOT = _PLUGINS_ROOT / "marketplaces"


def _latest_version(plugin_dir: Path) -> Path:
    # Version dirs aren't always semver (some marketplaces cache by commit
    # hash), so "most recently updated" is the only ordering that works
    # across all of them.
    versions = [p for p in plugin_dir.iterdir() if p.is_dir()]
    if not versions:
        raise FileNotFoundError(f"no versions found in {plugin_dir}")
    return max(versions, key=lambda p: p.stat().st_mtime)


def _skills_root(info: dict) -> tuple[Path, str] | tuple[None, None]:
    """Locate a source's skills/ dir and a label for its current revision.

    Most marketplaces cache a plugin as cache/<marketplace>/<plugin>/<version>/
    skills/. A marketplace whose plugin source is the repo root itself (no
    per-plugin version dir) is checked out directly under marketplaces/<name>/
    instead -- use its git commit as the revision label there.
    """
    if info.get("layout") == "marketplace":
        base = _MARKETPLACES_ROOT / info["marketplace"]
        if not base.is_dir():
            return None, None
        rev = (
            subprocess.run(
                ["git", "-C", str(base), "rev-parse", "--short", "HEAD"],
                capture_output=True,
                text=True,
                check=False,
            ).stdout.strip()
            or "unknown"
        )
        return base / "skills", rev

    plugin_dir = _CACHE_ROOT / info["marketplace"] / info["plugin"]
    if not plugin_dir.is_dir():
        return None, None
    version_dir = _latest_version(plugin_dir)
    return version_dir / "skills", version_dir.name


def main() -> None:
    manifest = json.loads(_MANIFEST.read_text())
    changed = False

    for source, info in manifest.items():
        skills_src, revision = _skills_root(info)
        if skills_src is None:
            print(f"skip {source}: not installed locally", file=sys.stderr)
            continue

        for skill in info["skills"]:
            # A plain string when the cached skill dir already has the name
            # we want in .claude/skills/; an object when the marketplace
            # nests it elsewhere (e.g. "python/api-design").
            if isinstance(skill, str):
                src_name = dest_name = skill
            else:
                src_name, dest_name = skill["src"], skill["dest"]

            src = skills_src / src_name
            if not src.is_dir():
                print(
                    f"skip {source}/{src_name}: not found in {revision}",
                    file=sys.stderr,
                )
                continue
            dst = _SKILLS_DIR / dest_name
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
            print(f"synced {dest_name} ({source} {revision})")

        if info.get("version") != revision:
            print(f"{source}: {info.get('version')} -> {revision}")
            info["version"] = revision
            changed = True

    if changed:
        _MANIFEST.write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
