import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

INPUT_FILE = '../res/raw/Peter_Piper.txt'
OUTPUT_FILE = '../res/processed/Peter_Piper.txt'
words = ['peter', 'piper', 'picked', 'peck', 'pepper']


def find_word(element):
    return element in words

pipeline_options = PipelineOptions()
with beam.Pipeline(options=pipeline_options) as p1:
    word_frequency = (
            p1
            | 'Read file' >> beam.io.ReadFromText(INPUT_FILE)
            | 'Split by delimiter' >> beam.FlatMap(lambda x: x.split(' '))
            | 'Filter for specified words' >> beam.Filter(find_word)
            | 'Assign one to each word' >> beam.Map(lambda x: (x, 1))
            | 'Count frequencies' >> beam.CombinePerKey(sum)
            | 'Write output to a file' >> beam.io.WriteToText(OUTPUT_FILE)
    )
p1.run()