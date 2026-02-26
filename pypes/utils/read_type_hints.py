import collections.abc
import inspect
from typing import Any, get_type_hints, get_args, get_origin


def get_first_param_and_return_type(
    method: Any,
    *,
    first_param_name: str | None = None,
    require_return_type: bool = False,
    param_names_to_skip: tuple[str, ...] = ("self", "cls"),
    globalns: dict[str, Any] | None = None,
    localns: dict[str, Any] | None = None,
) -> tuple[Any, Any]:
    """
    Return (first_non_self_param_type, return_type) for a function/method.
    """
    # Unwrap common descriptors
    if isinstance(method, (staticmethod, classmethod)):
        func = method.__func__
    else:
        func = method

    # If we were passed a bound method, unwrap to the underlying function
    func = getattr(func, "__func__", func)

    sig = inspect.signature(func)
    params = list(sig.parameters.values())

    if not params:
        raise TypeError("Method has no parameters; cannot determine first argument type")

    # Decide whether to skip the leading param
    skip_first = False
    first_name = params[0].name
    if first_name in param_names_to_skip:
        skip_first = True

    index = 1 if skip_first else 0
    if index >= len(params):
        raise TypeError("Method has no non-skipped parameters")

    p = params[index]
    if p.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
        raise TypeError("First non-skipped parameter is *args/**kwargs; cannot determine a single argument type")

    hints = get_type_hints(func, globalns=globalns, localns=localns)

    if first_param_name is not None and p.name != first_param_name:
        raise TypeError(f"Expected the first (non-skipped) parameter to be named '{first_param_name}', not '{p.name}'")

    if p.name not in hints:
        raise TypeError(f"Missing type annotation for parameter '{p.name}'")

    first_arg_type = hints[p.name]
    return_type = hints.get("return", None)
    if return_type is None:
        if require_return_type:
            raise TypeError(f"Missing required return type annotation")
        else:
            return_type = Any

    return first_arg_type, return_type


def unpack_generator_type_hint(anno: object) -> tuple[Any, Any, Any]:
    """
    Given an annotation of the form Generator[Y, S, R], return (Y, S, R).

    Raises:
        ValueError if anno is not a parameterized Generator.
    """
    origin = get_origin(anno)

    if origin is not collections.abc.Generator:
        raise ValueError(f"Annotation {anno!r} is not a Generator[...] type")

    args = get_args(anno)
    if len(args) != 3:
        raise ValueError(f"Generator annotation must have three type arguments; got {args!r}")

    return args[0], args[1], args[2]
