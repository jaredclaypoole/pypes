from typing import Any

from pydantic import BaseModel


def get_fields_dict(model: BaseModel) -> dict[str, Any]:
    return {name: getattr(model, name) for name in type(model).model_fields}
