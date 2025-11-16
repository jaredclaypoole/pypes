import hydra
from omegaconf import DictConfig, OmegaConf

from redo_structured_rd.pipeline_utils import pipeline


@hydra.main(version_base=None, config_path="redo_structured_rd/config", config_name="default")
def main(config: DictConfig) -> None:
    print("Full config:")
    print(OmegaConf.to_yaml(config))
    print()

    pipeline.run(config)
    pipeline.save_results()


if __name__ == '__main__':
    main()
