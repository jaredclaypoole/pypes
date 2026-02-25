from copy import deepcopy

from pydantic import BaseModel

from pypes.utils.hashing import myhash

import pytest


class MyModel(BaseModel):
    x: int
    s: str


class MyObject:
    pass


def test_hashing_basics():
    assert isinstance(myhash("dummy"), str)

    with pytest.raises(NotImplementedError):
        myhash(object())

    with pytest.raises(NotImplementedError):
        myhash(MyObject())

    mm1 = MyModel(x=1, s="dummy")
    mm2 = MyModel(x=1, s="dummy")
    assert myhash(mm1) == myhash(mm2)


@pytest.mark.parametrize(
    "value",
    [
        "dummy",
        1,
        1.0,
        {"a": 1, "b": 2},
        ["a", 1],
        (1, "dummy"),
    ],
)
def test_hashing_various_types(value):
    deep_copied_value =  deepcopy(value)
    assert myhash(deep_copied_value) == myhash(value)


def test_dict_hashing_is_order_dependent():
    # unlike many hashing schemes, the order of dict keys matters for hashing
    dict1 = {"a": 1, "b": 2}
    dict2 = {"b": 2, "a": 1}
    assert myhash(dict1) != myhash(dict2)
