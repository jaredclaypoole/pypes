from pypes.core.mytyping import FullDepsDict

from .test_deps_resolver import get_prev_results

def test_full_deps_dict_hashing():
    full_step_results = get_prev_results(3)
    assert len(full_step_results) == 2

    base_dict = full_step_results

    fdd1 = FullDepsDict(base_dict)
    fdd2 = FullDepsDict(base_dict)

    assert hash(fdd1) == hash(fdd1)
    assert hash(fdd1) != hash(fdd2)

    assert fdd1 == fdd1
    assert fdd1 != fdd2

    # ensure repr doesn't cause an error
    assert repr(fdd1)
