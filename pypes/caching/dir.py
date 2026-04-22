from pathlib import Path
import json
from typing import Any

from .stringdict import CachedStringDictBase
from .jsondict import CachedJsonDictBase


class DirCachedStringDict(CachedStringDictBase):
    def __init__(
        self,
        cache_dir: Path,
        assert_exists: bool = False,
    ):
        self.cache_dir = cache_dir
        super().__init__(assert_exists=assert_exists)

    def _init_cache(self, assert_exists: bool) -> None:
        if assert_exists:
            assert self.cache_dir.exists()
        else:
            self.cache_dir.mkdir(exist_ok=True, parents=True)

        fpaths = sorted(self.cache_dir.glob("*.txt"))
        for fpath in fpaths:
            self._data[fpath.stem] = fpath.read_text().strip()

    def _update_cache(self, key: str, value: str) -> None:
        with open(self.cache_dir / f"{key}.txt", 'w') as ftxt:
            print(value, file=ftxt)


class DirCachedJsonDict(CachedJsonDictBase):
    def __init__(
        self,
        cache_dir: Path,
        assert_exists: bool = False,
    ):
        self.cache_dir = cache_dir
        super().__init__(assert_exists=assert_exists)

    def _init_cache(self, assert_exists: bool) -> None:
        if assert_exists:
            assert self.cache_dir.exists()
        else:
            self.cache_dir.mkdir(exist_ok=True, parents=True)

        fpaths = sorted(self.cache_dir.glob("*.json"))
        for fpath in fpaths:
            with fpath.open("r") as fjson:
                self._data[fpath.stem] = json.load(fjson)

    def _update_cache(self, key: str, value: dict[str, Any]) -> None:
        with open(self.cache_dir / f"{key}.json", 'w') as fjson:
            json.dump(value, fjson, indent=4, ensure_ascii=False)
