import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging

INPUT_FILE = '../res/raw/Customers_age.txt'
OUTPUT_PATH = '../res/processed/Customers_age'

"""
A transform for generic parallel processing. A ParDo transform considers each element in the input PCollection, 
performs some processing function on that element, and emits zero or more elements to an output PCollection.
"""


class SplitRow(beam.DoFn):
    def __init__(self, delimiter=','):
      self.delimiter = delimiter

    def process(self, element):
        return [ element.split(self.delimiter)]


class FilterCustomer(beam.DoFn):
    def process(self, element):
        if element[2] == 'NY' and int(element[3])>20:
            return [element]


def run():
    pipeline_options = PipelineOptions()
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Read file' >> beam.io.ReadFromText(INPUT_FILE)
            | 'Split by delimiter' >> beam.ParDo(SplitRow(','))
            | 'Filter for NY customers above 20 yo' >> beam.ParDo(FilterCustomer())
            | 'Write output to a file' >> beam.io.WriteToText(OUTPUT_PATH, file_name_suffix='.txt')
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()