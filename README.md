# Apache Beam


## Setting up local environment

1. Make sure that Python 3.9, 3.10, or 3.11.
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

What is Apache Beam

Beam Architecture

Apache Beam flow

ParDo transformations - A transform for generic parallel processing. A ParDo transform considers each element in the input PCollection, 
performs some processing function on that element, and emits zero or more elements to an output PCollection.

DoFn class - Beam class that has a process method, that we can overwrite and provide business logic

