from .main import (
    dict_config_pipeline,
    pydantic_pipeline_verbose,
    pydantic_pipeline,
)

from .artifacts import (
    artifact_pipeline_verbose,
    artifact_pipeline,
)

all_example_modules = [
    dict_config_pipeline,
    pydantic_pipeline_verbose,
    pydantic_pipeline,
    artifact_pipeline_verbose,
    artifact_pipeline,
]