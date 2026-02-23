import itertools
from typing import Iterable

from omegaconf import DictConfig, ListConfig

from ..core.mytyping import (
    ConfigType, SubConfigType,
    StepInputBase,
)
from ..core.interface import PipelineStepInterface
from ..utils.config import sub_config_to_dict


class ConfigResolver:
    def __init__(self, step_name: str, proto_input_type: type[StepInputBase]):
        super().__init__()
        self.step_name = step_name
        self.proto_input_type = proto_input_type

    @classmethod
    def from_step(cls, step: PipelineStepInterface) -> "ConfigResolver":
        return cls(
            step_name=step.step_name,
            proto_input_type=step.proto_input_type,
        )

    def get_sub_configs(self, full_config: ConfigType) -> Iterable[SubConfigType]:
        proto_sub_config = full_config[self.step_name]
        if isinstance(proto_sub_config, ListConfig):
            for sub_config in proto_sub_config:
                yield sub_config
        elif isinstance(proto_sub_config, SubConfigType):
            yield proto_sub_config
        else:
            raise NotImplementedError(type(proto_sub_config))  # pragma: no cover

    def resolve_sub_config(self, sub_config: SubConfigType) -> Iterable[StepInputBase]:
        proto_input_type = self.proto_input_type
        sub_config = sub_config_to_dict(sub_config).copy()
        ntrials = sub_config.pop("ntrials", 1)
        config_dict0 = {}
        scan_vals = {}
        for key, value in sub_config.items():
            if isinstance(value, list):
                scan_vals[key] = value
                config_dict0[key] = None
            else:
                config_dict0[key] = value

        keys_tup = tuple(scan_vals.keys())
        for vals_tup in itertools.product(*scan_vals.values()):
            for trial in range(ntrials):
                config_dict = {
                    **dict(trial=trial),
                    **config_dict0,
                }
                for key, val in zip(keys_tup, vals_tup, strict=True):
                    config_dict[key] = val
                if issubclass(proto_input_type, DictConfig):
                    yield proto_input_type(config_dict)
                else:
                    yield proto_input_type(**config_dict)
