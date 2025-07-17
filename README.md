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

Beam/Runner API (Core of Apache Beam)

- **Direct Runner** - executes pipelines on your machine and is designed to validate that pipelines adhere to the Apache Beam model as closely as possible. Instead of focusing on efficient pipeline execution, the Direct Runner performs additional checks to ensure that users do not rely on semantics that are not guaranteed by the model. Using the Direct Runner for testing and development helps ensure that pipelines are robust across different Beam runners ([source](https://tour.beam.apache.org/tour/python/introduction/beam-concepts/runner-concepts)). 

- **GCP Dataflow Runner** - The Google Cloud Dataflow uses the Cloud Dataflow managed service. When you run your pipeline with the Cloud Dataflow service, the runner uploads your executable code and dependencies to a Google Cloud Storage bucket and creates a Cloud Dataflow job, which executes your pipeline on managed resources in Google Cloud Platform ([source](https://tour.beam.apache.org/tour/python/introduction/beam-concepts/runner-concepts)). 


## Apache Beam flow

<img width="1158" height="163" alt="image" src="https://github.com/user-attachments/assets/1a1a00b9-5e43-4ce1-8f9c-153d76230aa1" />

**Pipeline overview**

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

ParDo transform

ParDo transformations - A transform for generic parallel processing. A ParDo transform considers each element in the input PCollection, 
performs some processing function on that element, and emits zero or more elements to an output PCollection.

DoFn class - Beam class that has a process method, that we can overwrite and provide business logic

