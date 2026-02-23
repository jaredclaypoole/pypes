from dataclasses import dataclass

from omegaconf import DictConfig
import pandas as pd


ConfigType = DictConfig
SubConfigType = DictConfig


class StepInputBase:
    pass


class StepOutputBase:
    pass


DepsType = StepOutputBase
DepsSpecType = list[str]|str|None


class FullDepsDict:
    def __init__(self, upstream_by_label: dict[str, "FullStepOutput"]):
        super().__init__()
        self._data = upstream_by_label

    @property
    def data(self) -> dict[str, "FullStepOutput"]:
        return dict(self._data)

    def as_row(self) -> pd.Series:
        return pd.Series(list(self._data.values()), index=list(self._data.keys()))

    @classmethod
    def from_row(cls, row: pd.Series) -> "FullDepsDict":
        return cls(row.to_dict())

    @classmethod
    def list_from_df(cls, df: pd.DataFrame) -> list["FullDepsDict"]:
        return [
            cls.from_row(row)
            for _idx, row in df.iterrows()
        ]

    def to_simple_dict(self) -> dict[str, StepOutputBase]:
        return {
            name: full_step_output.output
            for name, full_step_output in self._data.items()
        }

    def __hash__(self):
        return object.__hash__(self)

    def __eq__(self, other: object):
        return object.__eq__(self, other)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._data!r})"


@dataclass(frozen=True, eq=False)
class FullStepOutput:
    deps: FullDepsDict
    output: StepOutputBase
    step_name: str

    def as_row(self, full_output: bool = True) -> pd.Series:
        output = self if full_output else self.output
        row0 = self.deps.as_row()
        row1 = pd.Series([output], index=[self.step_name])
        return pd.concat([row0, row1])

    @classmethod
    def list_to_df(cls, the_list: list["FullStepOutput"]) -> pd.DataFrame:
        rows = [elem.as_row() for elem in the_list]
        return pd.DataFrame(rows)


ResultsSpec = dict[str, list[FullStepOutput]]
