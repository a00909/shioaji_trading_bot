"""
NpyCacheManager — Redis-style key → npy file cache.

Design
──────
Key format : dot-separated segments map to path hierarchy.
             "tmf.tick.2025-11-11.time" → root/tmf/tick/2025-11-11/time.npy
             No fixed schema; any depth, any segment names.

Three-state per key:
    hit   : .npy file exists   → data available
    empty : .empty file exists → confirmed no data (source returned None)
    miss  : neither file       → never queried; caller should fetch

Empty marker uses same path, swapped extension:
    root/tmf/tick/2025-11-11/time.npy   ← data
    root/tmf/tick/2025-11-11/time.empty ← marker

Wildcard support (keys / delete):
    pattern uses * to match any single path segment.
    "tmf.tick.2025-11-11.*"  → all fields for that date
    "tmf.*.2025-11-11.time"  → time across all categories
    "tmf.**"                 → everything under tmf/ (globstar)
"""
import fnmatch
from enum import StrEnum
from pathlib import Path

import numpy as np


class CacheState(StrEnum):
    """Three-state cache result."""
    HIT = "hit"
    EMPTY = "empty"
    MISS = "miss"


class NpyCacheManager:
    """Redis-style npy cache backed by a directory tree.

    Parameters
    ----------
    root : str | Path
        Cache root directory. Defaults to ``caches`` (relative to cwd).

    Examples
    --------
    >>> cache = NpyCacheManager("caches")
    >>> cache.set("tmf.tick.2025-11-11.time", np.array([1.0, 2.0]))
    >>> cache.get("tmf.tick.2025-11-11.time")
    array([1., 2.])
    >>> cache.is_cached("tmf.tick.2025-11-11.time")
    'hit'
    >>> cache.keys("tmf.tick.2025-11-11.*")
    ['tmf.tick.2025-11-11.time']
    """

    def __init__(self, root: str | Path = None) -> None:
        if root:
            self.root = Path(root).resolve()
        else:
            self.root = Path(__file__).resolve().parent / "caches"
        self.root.mkdir(parents=True, exist_ok=True)

    # ── Key ↔ Path ─────────────────────────────────────────────────────────

    @staticmethod
    def _validate_key(key: str) -> None:
        """Raise ValueError if key is empty or contains illegal chars."""
        if not key:
            raise ValueError("Key must not be empty")
        for ch in key:
            if ch in ("/\\:*?\"<>|"):
                raise ValueError(
                    f"Key {key!r} contains illegal character {ch!r}"
                )

    def _npy_path(self, key: str) -> Path:
        """key → absolute .npy path.

        "tmf.tick.2025-11-11.time" → root/tmf/tick/2025-11-11/time.npy
        """
        self._validate_key(key)
        return (self.root / Path(*key.split("."))).with_suffix(".npy")  # type: ignore[arg-type]

    def _empty_path(self, key: str) -> Path:
        """key → absolute .empty path (same dir, swapped extension)."""
        npy = self._npy_path(key)
        return npy.with_suffix(".empty")

    @staticmethod
    def _path_to_key(path: Path, root: Path) -> str:
        """Absolute .npy path → dot-separated key."""
        rel = path.relative_to(root)
        # e.g. Path("tmf/tick/2025-11-11/time.npy") → "tmf.tick.2025-11-11.time"
        return rel.with_suffix("").as_posix().replace("/", ".")

    # ── Three-state read ───────────────────────────────────────────────────

    def get(self, key: str) -> np.ndarray | None:
        """Read cached array.

        Returns
        -------
        np.ndarray : cache hit
        None       : empty marker OR cache miss (use ``is_cached`` to distinguish)
        """
        p = self._npy_path(key)
        if p.is_file():
            return np.load(p)
        return None

    def exists(self, key: str) -> bool:
        """True iff .npy data file exists (strict hit)."""
        return self._npy_path(key).is_file()

    def has_empty(self, key: str) -> bool:
        """True iff .empty marker exists (confirmed no data)."""
        return self._empty_path(key).is_file()

    def is_cached(self, key: str) -> CacheState:
        """Three-state check: hit / empty / miss."""
        if self.exists(key):
            return CacheState.HIT
        if self.has_empty(key):
            return CacheState.EMPTY
        return CacheState.MISS

    # ── Write ──────────────────────────────────────────────────────────────

    def set(self, key: str, arr: np.ndarray) -> None:
        """Write array to cache. Removes any existing .empty marker."""
        p = self._npy_path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        self._empty_path(key).unlink(missing_ok=True)
        np.save(p, arr)

    def mark_empty(self, key: str) -> None:
        """Mark key as confirmed-no-data. Removes any existing .npy file."""
        p = self._empty_path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        self._npy_path(key).unlink(missing_ok=True)
        p.touch()

    # ── Enumerate (wildcard) ───────────────────────────────────────────────

    def keys(self, pattern: str = "*") -> list[str]:
        """List keys matching pattern.

        Pattern uses ``*`` to match any single segment and ``**`` to match
        zero or more segments (globstar).

        The implementation scans all ``.npy`` files under root and filters
        with ``fnmatch``, so pattern syntax follows ``fnmatch`` rules applied
        to the dot-separated key string.

        Examples
        --------
        >>> cache.keys()                            # everything
        >>> cache.keys("tmf.tick.2025-11-11.*")     # all fields for one date
        >>> cache.keys("tmf.*.2025-11-11.time")     # time across categories
        """
        # Collect every .npy file → key
        all_keys: list[str] = []
        for p in self.root.rglob("*.npy"):
            all_keys.append(self._path_to_key(p, self.root))

        # fnmatch on dot-separated key strings
        if "**" in pattern:
            # Convert ** to match zero+ segments:
            # "tmf.**" → "tmf.*" (fnmatch * matches everything including dots)
            # But we want "tmf.**" to mean tmf + anything after,
            # and "**.time" to mean anything ending in .time.
            # fnmatch * already matches across dots, so ** ≡ * in fnmatch.
            fn_pattern = pattern.replace("**", "*")
        else:
            fn_pattern = pattern

        return sorted(k for k in all_keys if fnmatch.fnmatch(k, fn_pattern))

    # ── Delete (wildcard) ──────────────────────────────────────────────────

    def delete(self, pattern: str = "*") -> int:
        """Delete keys matching pattern. Returns count of deleted keys.

        Removes both .npy and .empty files for matched keys.
        """
        matched = self.keys(pattern)
        count = 0
        for key in matched:
            self._npy_path(key).unlink(missing_ok=True)
            self._empty_path(key).unlink(missing_ok=True)
            count += 1
        # Clean up empty directories
        self._prune_empty_dirs()
        return count

    # ── Utilities ──────────────────────────────────────────────────────────

    def cache_info(self, pattern: str = "*") -> dict:
        """Return cache summary for keys matching pattern."""
        matched = self.keys(pattern)
        hits = [k for k in matched if self.exists(k)]
        empties = [k for k in matched if self.has_empty(k)]
        total_size = sum(self._npy_path(k).stat().st_size for k in hits)
        return {
            "root": str(self.root),
            "total_keys": len(matched),
            "hits": len(hits),
            "empties": len(empties),
            "total_bytes": total_size,
        }

    def _prune_empty_dirs(self) -> None:
        """Remove empty subdirectories under root (bottom-up)."""
        for dirpath in sorted(self.root.rglob("*"), reverse=True):
            if dirpath.is_dir() and not any(dirpath.iterdir()):
                dirpath.rmdir()

    def __repr__(self) -> str:
        return f"NpyCacheManager(root={str(self.root)!r})"
