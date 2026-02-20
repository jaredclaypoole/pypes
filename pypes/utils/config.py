from omegaconf import DictConfig, OmegaConf

from ..core.mytyping import SubConfigType


def sub_config_to_dict(sub_config: SubConfigType) -> dict:
    if isinstance(sub_config, DictConfig):
        return OmegaConf.to_container(sub_config)
    else:
        raise NotImplementedError(type(sub_config))  # pragma: no cover
