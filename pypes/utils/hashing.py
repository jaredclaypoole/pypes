import hashlib
from typing import Any

from pydantic import BaseModel

from .pydantic_utils import get_fields_dict


def myhash(obj: Any) -> str:
    if isinstance(obj, BaseModel):
        return myhash(tuple(get_fields_dict(obj).items()))
    elif isinstance(obj, tuple):
        return myhash(str(obj))
    elif isinstance(obj, list):
        return myhash(tuple(obj))
    elif isinstance(obj, dict):
        return myhash(tuple(obj.items()))
    elif isinstance(obj, (int, float)):
        return myhash(str(obj))
    elif isinstance(obj, str):
        return hashlib.sha256(obj.encode("utf-8")).hexdigest()
    else:
        raise NotImplementedError(type(obj))
