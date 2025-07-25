# Apache Beam

This repository contains sample Beam data processing pipelines written in Python.

Based on the course materials by Navdeep Kaur and the official Beam [documentation](https://tour.beam.apache.org/tour/python/). 

## Directory structure

## Setting up local environment

1. Verify that Python 3.9, 3.10, or 3.11 is installed.
2. Create a new virtual environment and activate it.

```bash
python3 -m venv apache-beam
```
```bash
source apache-beam/bin/activate
```
3. Install the requirements.
```bash
pip3 install -r requirements.txt
```

# About Apache Beam

Apache Beam is an open-source, unified, portable model for defining batch and streaming data-parallel processing pipelines.

## Beam Architecture

![beam.png](img%2Fbeam.png)

- **Beam / Runner API (Core of Apache Beam)** - The interface responsible for defining and constructing the pipeline; used during pipeline build time to translate user code into an executable graph.

- **Execution Engine** - The actual data processing backend (such as Spark, Flink, or Dataflow) that physically runs and distributes computation according to the Runner’s plan.

- **Fn API** - The protocol that standardizes runtime communication between the Runner and the SDK Worker, making cross-language execution possible.

- **Runner** - The component that manages, schedules, and orchestrates pipeline execution, adapting Beam steps for the chosen backend and coordinating all tasks.
 
- **SDK worker** - The runtime environment for executing user code in the language of choice, processing data as instructed by the Runner via the Fn API.

- **SDK** - The programming interface and libraries (e.g., Python, Java, Go) that users employ to write Beam pipelines.


## Apache Beam flow

<img width="1158" height="163" alt="image" src="https://github.com/user-attachments/assets/1a1a00b9-5e43-4ce1-8f9c-153d76230aa1" />

### Pipeline overview

- **Pipeline object** - encapsulates all the data and steps in the processing task. This includes reading input data, transforming that data, and writing output data. A Beam driver program typically starts by constructing a Pipeline object (by creating an instance of the Beam SDK class Pipeline), and then using that object as the basis for creating the pipeline’s data sets as PCollections and its operations as Transforms. When creating the pipeline object, we also set the configuration options that tell the Pipeline where and how to run.

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

beam_options = PipelineOptions(
    runner='DataflowRunner',
    project='my-project-id',
    job_name='unique-job-name',
    temp_location='gs://my-bucket/temp',
)

with beam.Pipeline(options=beam_options) as p:
  pass  # build your pipeline here
```

- **PCollection** - represents a distributed data set that your Beam pipeline operates on. The data set can be bounded, meaning it comes from a fixed source like a file, or unbounded, meaning it comes from a continuously updating source via a subscription or other mechanism. 
  
- **PTransform** - represents a data processing operation, or a step, in your pipeline. Every PTransform takes one or more PCollection objects as the input, performs a processing function that you provide on the elements of that PCollection, and then produces zero or more output PCollection objects.
  
- **I/O transforms** - Beam comes with a number of “IOs” - library PTransforms that read or write data to various external storage systems.

**A typical Beam driver program works as follows:**

1. Create a Pipeline object and set the pipeline execution options, including the Pipeline Runner.
2. Create an initial PCollection for pipeline data, either using the IOs to read data from an external storage system, or using a Create transform to build a PCollection from in-memory data.
3. Apply PTransforms to each PCollection.
   - Transforms can change, filter, group, analyze, or otherwise process the elements in a PCollection.
   - A transform creates a new output PCollection without modifying the input collection.
   - A typical pipeline applies subsequent transforms to each new output PCollection in turn until the processing is complete. However, note that a pipeline does not have to be a single straight line of transforms applied one after another: think of PCollections as variables and PTransforms as functions applied to these variables: the shape of the pipeline can be an arbitrarily complex processing graph.
5. Use IOs to write the final, transformed PCollection(s) to an external source.
6. Run the pipeline using the designated Pipeline Runner.

### Transforms

Most common transformations are described [here](https://tour.beam.apache.org/tour/python/common-transforms/filter).

**Side inputs** - additional data that your transform can access while processing each element.

**ParDo** - a Beam transform for generic parallel processing. The ParDo processing paradigm is similar to the “Map” phase of a Map/Shuffle/Reduce-style algorithm: a ParDo transform considers each element in the input PCollection, performs some processing function (your user code) on that element, and emits zero, one, or multiple elements to an output PCollection.

**DoFn** - user-defined processing logic (DoFn). When you apply a ParDo transform, you’ll need to provide user code in the form of a DoFn object. DoFn is a Beam SDK class that defines a distributed processing function. When you use Beam, often the most important pieces of code you’ll write are these DoFns - they’re what define your pipeline’s exact data processing tasks.

```python
# The input PCollection of Strings.
input = ...

# The DoFn to perform on each element in the input PCollection.

class ComputeWordLengthFn(beam.DoFn):
  def process(self, element):
    return [len(element)]

# Apply a ParDo to the PCollection [input] to compute lengths for each word.
word_lengths = input | beam.ParDo(ComputeWordLengthFn())
```

Other DoFn parameters that can be accessed:
- ```beam.DoFn.TimestampParam``` - the timestamp of input element.
- ```window=beam.DoFn.WindowParam``` - the window an input element falls into
- ```beam.DoFn.PaneInfoParam``` - when triggers are used, Beam provides a DoFn.PaneInfoParam object that contains information about the current firing.
- ```beam.DoFn.TimerParam()``` and ```beam.DoFn.StateParam()``` - user defined Timer and State parameters can DoFn.

**DoFn as a lambda function** - if your function is relatively simple, you can simplify the use of ParDo by providing a lightweight built-in DoFn as an anonymous instance of the internal class.

```python
# The input PCollection of strings.
input = ...

# Apply a lambda function to the PCollection input.
# Save the result as the PCollection word_lengths.

word_lengths = input | beam.FlatMap(lambda word: [len(word)])
```

**Map** - if your ParDo performs a one-to-one mapping of input elements to output elements–that is, for each input element, it applies a function that produces exactly one output element, you can use the higher-level Map transform. 

**FlatMap** - works like Map elements, but inside the logic you can do complex operations like dividing the list into separate elements and processing.
