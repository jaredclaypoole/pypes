import itertools
from typing import Iterable

from omegaconf import DictConfig

from ..core.mytyping import (
    ConfigType, SubConfigType,
    StepInputBase,
)
from ..core.interface import PipelineStepInterface
from ..utils.config import sub_config_to_dict


class ConfigResolver:
    def __init__(self, step: PipelineStepInterface):
        super().__init__()
        self.step = step

    def get_sub_config(self, full_config: ConfigType) -> SubConfigType:
        step_name = self.step.step_name
        sub_config = full_config[step_name]
        return sub_config

    def resolve_sub_config(self, sub_config: SubConfigType) -> Iterable[StepInputBase]:
        proto_input_type = self.step.proto_input_type
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
