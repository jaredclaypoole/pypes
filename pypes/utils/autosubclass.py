from typing import Any, Callable, TypeVar, cast
import types


C = TypeVar("C", bound=type[Any])


def auto_subclass(
    cls: type[Any],
    *,
    fkwargs: Callable[[type[Any]], dict[str, Any]] | None = None,
    **kwargs_for_init: Any,
) -> Callable[[C], C]:
    def deco(other_class: C) -> C:
        if issubclass(other_class, cls):
            raise TypeError(f"{other_class.__name__} is already a subclass of {cls.__name__}")
        bases = (other_class, cls)

        generated_kwargs: dict[str, Any] = fkwargs(other_class) if fkwargs is not None else {}

        ns = dict(other_class.__dict__)
        ns.pop("__dict__", None)
        ns.pop("__weakref__", None)

        new_cls = types.new_class(
            other_class.__name__,
            bases,
            exec_body=lambda d: d.update(ns),
        )

        new_cls.__module__ = other_class.__module__
        new_cls.__qualname__ = getattr(
            other_class, "__qualname__", other_class.__name__
        )

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            merged = {**kwargs_for_init, **generated_kwargs, **kwargs}
            super(new_cls, self).__init__(*args, **merged)

        new_cls.__init__ = __init__

        return cast(C, new_cls)

    return deco
