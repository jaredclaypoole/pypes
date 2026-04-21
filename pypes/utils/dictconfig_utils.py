from typing import Any

from omegaconf import DictConfig, OmegaConf

def get_fields_dict(config: DictConfig) -> dict[str, Any]:
    return OmegaConf.to_container(config)
