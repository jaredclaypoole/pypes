from typing import Any, Iterable

from omegaconf import DictConfig


def make_tuples(label: str, the_list: list[Any]) -> Iterable[tuple[str, Any]]:
    for val in the_list:
        yield label, val


def replace(obj: DictConfig, **kwargs) -> DictConfig:
    obj = obj.copy()
    for key, value in kwargs.items():
        obj[key] = value
    return obj
