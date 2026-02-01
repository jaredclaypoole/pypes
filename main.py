from pathlib import Path

import hydra
from omegaconf import DictConfig, OmegaConf

from examples.main.pydantic_pipeline import pipeline


@hydra.main(version_base=None, config_path="examples/main/config", config_name="default")
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
