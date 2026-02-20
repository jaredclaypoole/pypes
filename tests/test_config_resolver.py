from omegaconf import OmegaConf, DictConfig
from pydantic import BaseModel

from pypes.resolvers.config import ConfigResolver


config_str = """
step1:
  some_field: 1
  other_field: a string
step_with_ntrials:
  ntrials: 2
  a_field: 3
step_with_list:
  field1: [1, 2, 3]
  field2: abc
step_with_list_config:
  - field1: 1
    field2: 2
  - field1: 2
    field2: 1
"""


def test_dictconfig():
    full_config = OmegaConf.create(config_str)

    if "test_step1":
        cr = ConfigResolver(
            step_name="step1",
            proto_input_type=DictConfig,
        )
        sub_configs = list(cr.get_sub_configs(full_config))
        assert sub_configs == [DictConfig(dict(some_field=1, other_field="a string"))]
        proto_inputs = list(cr.resolve_sub_config(sub_configs[0]))
        assert proto_inputs == [DictConfig(dict(trial=0, some_field=1, other_field="a string"))]

    if "test_step_with_ntrials":
        cr = ConfigResolver(
            step_name="step_with_ntrials",
            proto_input_type=DictConfig,
        )
        sub_configs = list(cr.get_sub_configs(full_config))
        assert sub_configs == [DictConfig(dict(ntrials=2, a_field=3))]
        proto_inputs = list(cr.resolve_sub_config(sub_configs[0]))
        assert proto_inputs == [DictConfig(dict(trial=itrial, a_field=3)) for itrial in range(2)]

    if "test_step_with_list":
        cr = ConfigResolver(
            step_name="step_with_list",
            proto_input_type=DictConfig,
        )
        sub_configs = list(cr.get_sub_configs(full_config))
        assert sub_configs == [DictConfig(dict(field1=[1,2,3], field2="abc"))]
        proto_inputs = list(cr.resolve_sub_config(sub_configs[0]))
        assert proto_inputs == [
            DictConfig(dict(trial=0, field1=x, field2="abc"))
            for x in [1, 2, 3]
        ]

    if "test_step_with_list_config":
        cr = ConfigResolver(
            step_name="step_with_list_config",
            proto_input_type=DictConfig,
        )
        sub_configs = list(cr.get_sub_configs(full_config))
        assert sub_configs == [
            DictConfig(dict(field1=1, field2=2)),
            DictConfig(dict(field1=2, field2=1)),
        ]
        proto_inputs = list(cr.resolve_sub_config(sub_configs[0]))
        assert proto_inputs == [DictConfig(dict(trial=0, field1=1, field2=2))]


def test_pydantic():
    full_config = OmegaConf.create(config_str)

    class Step1Input(BaseModel, frozen=True):
        trial: int
        some_field: int
        other_field: str

    if "test_step1":
        cr = ConfigResolver(
            step_name="step1",
            proto_input_type=Step1Input,
        )
        sub_configs = list(cr.get_sub_configs(full_config))
        assert sub_configs == [DictConfig(dict(some_field=1, other_field="a string"))]
        proto_inputs = list(cr.resolve_sub_config(sub_configs[0]))
        assert proto_inputs == [Step1Input(trial=0, some_field=1, other_field="a string")]
