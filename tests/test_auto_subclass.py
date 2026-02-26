from typing import Any, Generator

from pypes.utils.autosubclass import auto_subclass

import pytest


class BaseClass:
    def __init__(self, base_class_str: str, **kwargs):
        super().__init__(**kwargs)
        self._s = base_class_str

    def mymethod(self) -> str:
        return self._s

    def method_to_override(self, input: Any, **kwargs) -> Any:
        raise NotImplementedError()

    def gen_method_to_override(self, input: Any, **kwargs: Any) -> Generator[Any, Any, Any]:
        raise NotImplementedError()


class OtherBaseClass:
    def __init__(self, other_base_class_str: str, **kwargs):
        super().__init__(**kwargs)
        self.other_base_class_str = other_base_class_str


def test_auto_subclass_simple():
    class A:
        pass

    @auto_subclass(A)
    class B:
        pass

    assert issubclass(B, A)
    assert isinstance(B(), A)
    assert isinstance(B(), B)


def test_auto_subclass_error():
    class A:
        pass

    with pytest.raises(TypeError):
        @auto_subclass(A)
        class B(A):
            pass


def test_auto_subclass_complex():
    @auto_subclass(
        BaseClass,
        base_class_str="This is the base class str",
        sub_class_str="This is the sub class str",
        other_base_class_str="This is the other base class str",
    )
    class SubClass(OtherBaseClass):
        def __init__(self, sub_class_str: str, other_base_class_str: str, **kwargs):
            super().__init__(other_base_class_str=other_base_class_str, **kwargs)
            self.sub_class_str = sub_class_str

        def method_to_override(self, input: str, second_arg: str, **kwargs: Any) -> str:
            return str((input, second_arg))

        def gen_method_to_override(self, input: str, other_arg: str, **kwargs: Any) -> Generator[str, str, str]:
            first = yield input
            second = yield other_arg
            return str((first, second))

    assert issubclass(SubClass, BaseClass)

    instance = SubClass()
    assert isinstance(instance, BaseClass)
    assert instance.mymethod() == "This is the base class str"
    assert instance.sub_class_str == "This is the sub class str"
    assert instance.other_base_class_str == "This is the other base class str"

    assert instance.method_to_override("A", "B") == str(("A", "B"))
    the_gen = instance.gen_method_to_override("C", "D")

    to_send: str|None = None
    ret: str|None = None
    while True:
        try:
            sent = the_gen.send(to_send)
        except StopIteration as err:
            ret = err.value
            break
        to_send = f"response({sent})"

    assert ret == str(("response(C)", "response(D)"))
