import hydra
from omegaconf import DictConfig, OmegaConf

from examples.main.compound_dict_config_pipeline import pipeline


@hydra.main(version_base=None, config_path="examples/main/config", config_name="default")
def main(config: DictConfig) -> None:
    print("Full config:")
    print(OmegaConf.to_yaml(config))
    print()

    pipeline.run(config)
    pipeline.save_results()


if __name__ == '__main__':
    main()
