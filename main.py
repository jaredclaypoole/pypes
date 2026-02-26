from pathlib import Path

import hydra
from omegaconf import DictConfig, OmegaConf

# import examples.main.dict_config_pipeline as pipeline_module
# import examples.main.pydantic_pipeline as pipeline_module
import examples.artifacts.artifact_pipeline as pipeline_module


pipeline = pipeline_module.pipeline
config_path = Path(pipeline_module.__file__).parent / "config"

@hydra.main(version_base=None, config_path=str(config_path), config_name="default")
def main(config: DictConfig) -> None:
    print("Full config:")
    print(OmegaConf.to_yaml(config))
    print()

    pipeline.run(config)
    dill_path = Path("./data/dill/all_results.dill")
    pipeline.save_results(dill_path=dill_path)
    print(f"Results saved to {dill_path}")

    import dill
    with open(dill_path, 'rb') as fdill:
        results = dill.load(fdill)

    breakpoint()
    print("Done")


if __name__ == '__main__':
    main()
