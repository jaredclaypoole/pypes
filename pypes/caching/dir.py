from pathlib import Path

from .stringdict import CachedStringDictBase


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
