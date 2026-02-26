from typing import Any, Generator

from pypes.utils.read_type_hints import (
    get_first_param_and_return_type,
    unpack_generator_type_hint,
)

import pytest


class A: pass
class B: pass
class C: pass
class D: pass
class E: pass


def test_get_types_with_functions():
    def f_good(arg1: A, arg2: B, arg3: C) -> D:
        return D()
    assert get_first_param_and_return_type(f_good) == (A, D)
    ret = get_first_param_and_return_type(f_good, param_names_to_skip=("arg1",))
    assert ret == (B, D)
    ret = get_first_param_and_return_type(f_good, first_param_name="arg1")
    assert ret == (A, D)
    with pytest.raises(TypeError):
        get_first_param_and_return_type(f_good, first_param_name="something_else")

    def f_single_arg(my_arg1: A) -> B:
        return B()
    assert get_first_param_and_return_type(f_single_arg) == (A, B)
    with pytest.raises(TypeError):
        get_first_param_and_return_type(f_single_arg, param_names_to_skip=("my_arg1",))

    def f_no_return_type(arg1: A, arg2: B, arg3: C):
        return D()
    get_first_param_and_return_type(f_no_return_type) == (A, Any)
    with pytest.raises(TypeError):
        get_first_param_and_return_type(f_no_return_type, require_return_type=True)

    def f_splat_first(*args: Any) -> A:
        return A()
    with pytest.raises(TypeError):
        get_first_param_and_return_type(f_splat_first)

    def f_double_splat_first(**kwargs: Any) -> A:
        return A()
    with pytest.raises(TypeError):
        get_first_param_and_return_type(f_double_splat_first)

    def f_no_first_anno(arg1, arg2: B) -> C:
        return C()
    with pytest.raises(TypeError):
        get_first_param_and_return_type(f_no_first_anno)

    def f_empty():
        pass
    with pytest.raises(TypeError):
        get_first_param_and_return_type(f_empty)


def test_get_types_with_methods():
    class MyClass:
        def f_normal(self, a: A, b: B) -> C:
            return C()

        def f_funny_self(funny_self, a: A, b: B) -> C:
            return C()

        @classmethod
        def f_cls(cls, a: A, b: B) -> C:
            return C()

        @classmethod
        def f_funny_cls(funny_cls, a: A, b: B) -> C:
            return C()

        @staticmethod
        def f_static(a: A, b: B) -> C:
            return C()

    # class-level checks
    assert get_first_param_and_return_type(MyClass.f_normal) == (A, C)

    with pytest.raises(TypeError):
        get_first_param_and_return_type(MyClass.f_funny_self)
    ret = get_first_param_and_return_type(MyClass.f_funny_self, param_names_to_skip=("self", "funny_self"))
    assert ret == (A, C)

    assert get_first_param_and_return_type(MyClass.f_cls) == (A, C)

    with pytest.raises(TypeError):
        get_first_param_and_return_type(MyClass.f_funny_cls)
    ret = get_first_param_and_return_type(MyClass.f_funny_cls, param_names_to_skip=("cls", "funny_cls"))
    assert ret == (A, C)

    assert get_first_param_and_return_type(MyClass.f_static) == (A, C)


    # instance-level checks
    my_class = MyClass()
    assert get_first_param_and_return_type(my_class.f_normal) == (A, C)

    with pytest.raises(TypeError):
        get_first_param_and_return_type(my_class.f_funny_self)
    ret = get_first_param_and_return_type(my_class.f_funny_self, param_names_to_skip=("self", "funny_self"))
    assert ret == (A, C)

    assert get_first_param_and_return_type(my_class.f_cls) == (A, C)

    with pytest.raises(TypeError):
        get_first_param_and_return_type(my_class.f_funny_cls)
    ret = get_first_param_and_return_type(my_class.f_funny_cls, param_names_to_skip=("cls", "funny_cls"))
    assert ret == (A, C)

    assert get_first_param_and_return_type(my_class.f_static) == (A, C)


    # descriptor object tests
    assert get_first_param_and_return_type(MyClass.__dict__["f_cls"]) == (A, C)
    assert get_first_param_and_return_type(MyClass.__dict__["f_static"]) == (A, C)


def test_unpack_gen():
    assert unpack_generator_type_hint(Generator[A, B, C]) == (A, B, C)
    assert unpack_generator_type_hint(Generator[A, None, None]) == (A, type(None), type(None))

    with pytest.raises(ValueError):
        unpack_generator_type_hint("not_a_generator")

    with pytest.raises(ValueError):
        unpack_generator_type_hint(Generator)


def test_real_use_cases():
    class MyClass:
        def my_method(self, input: A, second_arg: B, **kwargs: Any) -> C:
            return str((input, second_arg))

        def my_gen_method(self, input: A, other_arg: B, **kwargs: Any) -> Generator[D, E, C]:
            first = yield input
            second = yield other_arg
            return str((first, second))

    assert get_first_param_and_return_type(MyClass.my_method) == (A, C)
    first_anno, ret_anno = get_first_param_and_return_type(MyClass.my_gen_method)
    assert first_anno == A
    assert ret_anno == Generator[D, E, C]
    assert unpack_generator_type_hint(ret_anno) == (D, E, C)
