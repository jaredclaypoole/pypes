import pandas as pd

from pypes.utils.merging import merge_on_identity_intersection_or_cross

import pytest


class MyLabel:
    def __init__(self, label: str):
        self.label = label

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.label!r})"  # pragma: no cover


def test_bad_prefer():
    df0 = pd.DataFrame()
    df1 = pd.DataFrame()
    with pytest.raises(ValueError):
        merge_on_identity_intersection_or_cross(df0, df1, prefer="bad_input")


def test_dup_keys():
    A = MyLabel("A")
    B1 = MyLabel("B1")
    B2 = MyLabel("B2")

    df0 = pd.DataFrame(
        [
            dict(A=A, B=B1, C="C1"),
            dict(A=A, B=B1, C="C2"),
        ],
    )
    df1 = pd.DataFrame(
        [
            dict(A=A, B=B1, D="D1"),
            dict(A=A, B=B1, D="D2"),
        ],
    )
    df = pd.DataFrame(
        [
            dict(A=A, B=B1, E="E1"),
            dict(A=A, B=B2, E="E2"),
        ],
    )

    with pytest.raises(ValueError):
        merge_on_identity_intersection_or_cross(df0, df)
    with pytest.raises(ValueError):
        merge_on_identity_intersection_or_cross(df, df1)
    with pytest.raises(ValueError):
        merge_on_identity_intersection_or_cross(df0, df1)


def test_matching_keys():
    A = MyLabel("A")
    B1 = MyLabel("B1")
    B2 = MyLabel("B2")

    df0 = pd.DataFrame(
        [
            dict(A=A, B=B1, C="C1"),
            dict(A=A, B=B2, C="C2"),
        ],
    )
    df1 = pd.DataFrame(
        [
            dict(A=A, B=B1, D="D1"),
            dict(A=A, B=B2, D="D2"),
        ],
    )
    df_expected = pd.DataFrame(
        [
            dict(A=A, B=B1, C="C1", D="D1"),
            dict(A=A, B=B2, C="C2", D="D2"),
        ],
    )

    merged_df = merge_on_identity_intersection_or_cross(df0, df1, expect_same_keys=True)
    assert (merged_df == df_expected).all().all()

    merged_df = merge_on_identity_intersection_or_cross(df0, df1, expect_same_keys=False)
    assert (merged_df == df_expected).all().all()


def test_non_matching_keys():
    A = MyLabel("A")
    B1 = MyLabel("B1")
    B2 = MyLabel("B2")
    B3 = MyLabel("B3")

    df0 = pd.DataFrame(
        [
            dict(A=A, B=B1, C="C1"),
            dict(A=A, B=B2, C="C2"),
        ],
    )
    df1 = pd.DataFrame(
        [
            dict(A=A, B=B1, D="D1"),
            dict(A=A, B=B3, D="D2"),
        ],
    )
    df_expected = pd.DataFrame(
        [
            dict(A=A, B=B1, C="C1", D="D1"),
            dict(A=A, B=B2, C="C2", D=pd.NA),
            dict(A=A, B=B3, C=pd.NA, D="D2"),
        ],
    )
    with pytest.raises(ValueError):
        merge_on_identity_intersection_or_cross(df0, df1, expect_same_keys=True)

    merged_df = merge_on_identity_intersection_or_cross(df0, df1, expect_same_keys=False)
    myNA = MyLabel("NA")
    assert (merged_df.fillna(myNA) == df_expected.fillna(myNA)).all().all()
