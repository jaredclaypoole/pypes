from typing import Iterable

from ..core.mytyping import (
    DepsSpecType,
    ResultsSpec,
    FullDepsDict,
    FullStepOutput,
)


class DepsResolver:
    def resolve_deps(self, deps_spec: DepsSpecType, prev_results: ResultsSpec) -> Iterable[FullDepsDict]:
        if not deps_spec:
            return [FullDepsDict({})]
        if isinstance(deps_spec, str):
            deps_spec = [deps_spec]

        unprocessed_deps: list[str] = list(reversed(deps_spec))

        df0 = None
        while unprocessed_deps:
            dep = unprocessed_deps.pop()
            df1 = FullStepOutput.list_to_df(prev_results[dep])
            if df0 is None:
                df0 = df1
            else:
                df0 = df0.join(df1, how="outer")

        return FullDepsDict.list_from_df(df0)
