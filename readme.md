# pypes

A Python pipelining framework.

pypes lets you define pipelines where each step can have multiple dependencies,
and you can easily access attributes of any output in a given step's dependency tree.


## Introduction by example

Here's a simple example of a pipeline step, taken from `examples/main/dict_config_pipeline.py`:


```python
@PipelineStepBase.auto_step("truncated_doc", deps_spec="doc")
class TruncatedDocStep:
    def input_to_output(self, input: DictConfig, doc: DictConfig, **kwargs) -> DictConfig:
        sentences = doc.text.split(".")[:input.nsentences]
        sentences = [s.strip() for s in sentences] + [""]
        new_text = ". ".join(sentences)[:-1]
        return replace(
            input,
            text=new_text,
        )
```

In the example above, we define a step nameed `truncated_doc` which depends on step `doc`
(and, implicitly, on any of `doc`'s dependencies).

We override the `input_to_output` method of `PipelineStepBase`, annotating it with appropriate type hints.
These type hints are required when using the `PipelineStepBase.auto_step` decorator
(at least for the first argument `input` and the return type).

`input` is read from config (using Hydra; we'll come to that soon), and
`doc` and all of its dependencies are passed as keyword arguments to the `input_to_output` method.

In `input_to_output`, the meat of the pipeline step, we access `doc.text` and `input.nsentences`.
We assume that the earlier components of the pipeline created them to be of type `str` and `int`, respectively;
later we'll see how to leverage Pydantic to enforce types, at the cost of writing some extra code.

We do some processing (truncating `doc.text` after at most `nsentences` occurrences of the `.` character and rejoining),
then we use the `replace` utility function to create a new `DictConfig` containing the same fields as `input`
with any additions or replacements specified in the following keyword arguments.
(`replace` is meant to mimic `dataclasses.replace` in the standard library.)


Let's now take a look at how the dependency, `doc`, was defined.
(We looked at `truncated_doc` first because it was simpler.)

```python
@PipelineStepBase.auto_step("doc")
class DocStep:
    def full_config_to_inputs(self, full_config: DictConfig) -> Iterable[DictConfig]:
        for proto_input in super().full_config_to_inputs(full_config):
            assert isinstance(proto_input, DictConfig)
            dir_path = Path(proto_input.dir_path)
            for fpath in sorted(dir_path.glob(proto_input.glob)):
                yield replace(
                    proto_input,
                    path=str(fpath),
                    name=fpath.stem,
                )

    def input_to_output(self, input: DictConfig, **kwargs) -> DictConfig:
        fpath = Path(input.path)
        text = fpath.read_text().strip()
        return replace(
            input,
            text=text,
        )
```

We see this class overrides an additonal method of `PipelineStepBase`, `full_config_to_inputs`.
Rather than taking the inputs directly from (preprocessed) Hydra config,
this pipeline step performs a post-processing step in between.
This allows `DocStep` to read a `proto_input.dir_path`, read the corresponding directory's contents,
and yield `input`s for any file path that matches the associated glob.

Then in `input_to_output`, the corresponding file's text contents are read and stored in the returned output.


Let's now look at an example config file, taken from `examples/main/config/default.yaml`:

```yaml
doc:
  dir_path: "data/raw_documents"
  glob: "*.txt"

truncated_doc:
  ntrials: 2
  nsentences: [2, 3]

translated_doc:
  ntrials: 1
  language: [fr, de]
```

Most config fields are read directly to their corresponding step `input`s.
There are two exceptions:

* `ntrials` (defaults to 1) creates multiple `input`s with field `trial` ranging from 0 to `ntrials-1` (inclusive).
* Any config field with a list value creates multiple `input`s which each take one value of the list.
  * If you intend to pass an actual list to a single `input`, you'll have to "escape" it inside an extra list.

In this example, `doc` will create one (proto-)input,
`truncated_doc` wil create four,
and `translated_doc` will create two.
(Recall that `doc` actually splits its proto-inputs further,
creating one input for each glob-matching file present in the `dir_path` directory.)

Because each of these pipeline steps' dependency specs are just the name of the step before,
`truncated_doc` will have as four times as many total inputs (and outputs) as there were `doc`s,
and `translated_doc` will have twice as many total inputs (and outputs) as there were `truncated_doc`s.


Let's skip over the definition of `translated_doc`
(it's not terribly different from `truncated_doc`, and it doesn't actually translate anything at present),
and look at the pipeline definition (still located in `examples/main/dict_config_pipeline.py`)

```python
pipeline = PipelineBase()
pipeline.add_steps(
    [
        DocStep(),
        TruncatedDocStep(),
        TranslatedDocStep(),
    ],
)
```


Then we take the main function from `main.py`:

```python
from pathlib import Path

import hydra
from omegaconf import DictConfig, OmegaConf

import examples.main.dict_config_pipeline as pipeline_module


pipeline = pipeline_module.pipeline
config_path = Path(pipeline_module.__file__).parent / "config"


@hydra.main(version_base=None, config_path=str(config_path), config_name="default")
def main(config: DictConfig) -> None:
    pipeline.run(config)
    dill_path = Path("./data/dill/all_results.dill")
    pipeline.save_results(dill_path=dill_path)


if __name__ == '__main__':
    main()
```

Again, you can view the full example in
`examples/main/dict_config_pipeline.py`,
`examples/main/config/default.yaml`,
and `main.py`.


## A more robust example

Consider a more robust version of `doc`, written using Pydantic,
taken from `examples/main/pydantic_pipeline.py`:

```python
class StepInput(BaseModel, frozen=True):
    trial: int

class DocProtoInput(StepInput):
    dir_path: str
    glob: str

class DocInput(DocProtoInput):
    path: str
    name: str

class DocOutput(DocInput):
    text: str

@PipelineStepBase.auto_step("doc", proto_input_type=DocProtoInput)
class DocStep:
    def full_config_to_inputs(self, full_config: ConfigType) -> Iterable[DocInput]:
        for proto_input in super().full_config_to_inputs(full_config):
            assert isinstance(proto_input, DocProtoInput)
            dir_path = Path(proto_input.dir_path)
            for fpath in sorted(dir_path.glob(proto_input.glob)):
                yield DocInput(
                    **get_fields_dict(proto_input),
                    path=str(fpath),
                    name=fpath.stem,
                )

    def input_to_output(self, input: DocInput, **kwargs) -> DocOutput:
        fpath = Path(input.path)
        text = fpath.read_text().strip()
        output = DocOutput(
            **get_fields_dict(input),
            text=text,
        )
        return output
```

Here we define Pydantic models for each input and output.
It is slightly more verbose, but the additional checks performed by Pydantic are well worth it.

(`DocProtoInput` was only needed here because `full_config_to_inputs` was overridden.
In most pipeline steps, the `proto_input_type` is automatically inferred to be the same as the input type.)
`get_fields_dict` is a utility function to obtain a dict of the field name-value pairs in a Pydantic model.

Everything else about this example is the same as for the `DictConfig` example.
