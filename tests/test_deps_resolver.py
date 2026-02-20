from omegaconf import DictConfig

from pypes.core.mytyping import FullStepOutput, FullDepsDict
from pypes.resolvers.deps import DepsResolver

import pytest


deps_spec_by_step_name = {
    "step1": None,
    "step2": "step1",
    "step3": "step1",
    "step4": ["step2", "step3"],
    "step5": None,
    "step6": "step5",
    "step7": ["step1", "step5"],
}

all_results = {
    "step1": [
        fso_1a:=FullStepOutput(
            deps=FullDepsDict({}),
            output=DictConfig(dict(field1a="a", field1b=2)),
            step_name="step1",
        ),
        fso_1b:=FullStepOutput(
            deps=FullDepsDict({}),
            output=DictConfig(dict(field1a="b", field1b=2)),
            step_name="step1",
        ),
    ],
    "step2": [
        fso_2a:=FullStepOutput(
            deps=FullDepsDict(dict(step1=fso_1a)),
            output=DictConfig(dict(field2a=3)),
            step_name="step2",
        ),
        fso_2b:=FullStepOutput(
            deps=FullDepsDict(dict(step1=fso_1b)),
            output=DictConfig(dict(field2a=4)),
            step_name="step2",
        ),
    ],
    "step3": [
        fso_3a:=FullStepOutput(
            deps=FullDepsDict(dict(step1=fso_1a)),
            output=DictConfig(dict(field3a=5)),
            step_name="step3",
        ),
        fso_3b:=FullStepOutput(
            deps=FullDepsDict(dict(step1=fso_1b)),
            output=DictConfig(dict(field3a=6)),
            step_name="step3",
        ),
    ],
    "step4": [
        fso_4a:=FullStepOutput(
            deps=FullDepsDict(dict(step1=fso_1a, step2=fso_2a, step3=fso_3a)),
            output=DictConfig(dict(field4a=7)),
            step_name="step4",
        ),
        fso_4b:=FullStepOutput(
            deps=FullDepsDict(dict(step1=fso_1b, step2=fso_2b, step3=fso_3b)),
            output=DictConfig(dict(field4a=8)),
            step_name="step4",
        ),
    ],
    "step5": [
        fso_5a:=FullStepOutput(
            deps=FullDepsDict({}),
            output=DictConfig(dict(field5a="a", field5b=1)),
            step_name="step5",
        ),
        fso_5b:=FullStepOutput(
            deps=FullDepsDict({}),
            output=DictConfig(dict(field5a="b", field5b=1)),
            step_name="step5",
        ),
    ],
    "step6": [
        fso_6a:=FullStepOutput(
            deps=FullDepsDict(dict(step5=fso_5a)),
            output=DictConfig(dict(field6a=9)),
            step_name="step6",
        ),
        fso_6b:=FullStepOutput(
            deps=FullDepsDict(dict(step5=fso_5b)),
            output=DictConfig(dict(field6a=10)),
            step_name="step6",
        ),
    ],
    "step7": [
        FullStepOutput(
            deps=FullDepsDict(dict(step1=fso_1a, step5=fso_5a)),
            output=DictConfig(dict(field7a=1)),
            step_name="step7",
        ),
        FullStepOutput(
            deps=FullDepsDict(dict(step1=fso_1a, step5=fso_5b)),
            output=DictConfig(dict(field7a=2)),
            step_name="step7",
        ),
        FullStepOutput(
            deps=FullDepsDict(dict(step1=fso_1b, step5=fso_5a)),
            output=DictConfig(dict(field7a=3)),
            step_name="step7",
        ),
        FullStepOutput(
            deps=FullDepsDict(dict(step1=fso_1b, step5=fso_5b)),
            output=DictConfig(dict(field7a=4)),
            step_name="step7",
        ),
    ],
}


def get_prev_results(step_number: int) -> dict[str, list[FullStepOutput]]:
    prev_step_names = [f"step{x}" for x in range(1, step_number)]
    return {name: all_results[name] for name in prev_step_names}


@pytest.mark.parametrize("step_num", list(range(1, len(all_results)+1)))
def test_deps_resolver(step_num: int):
    step_name = f"step{step_num}"
    prev_results = get_prev_results(step_num)
    dr = DepsResolver()
    fdds_actual = list(dr.resolve_deps(
        deps_spec=deps_spec_by_step_name[step_name],
        prev_results=prev_results,
    ))
    if step_name in ["step1", "step5"]:
        fdds_expected = [FullDepsDict({})]
    else:
        fdds_expected = [fso.deps for fso in all_results[step_name]]
    assert fdds_actual == fdds_expected
