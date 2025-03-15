import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

INPUT_FILE = '../res/raw/Customers_age.txt'
OUTPUT_FILE = '../res/processed/Customers_age.txt'

pipeline_options = PipelineOptions()
with beam.Pipeline(options=pipeline_options) as p1:
    customers = (
        p1
        | 'Read file' >> beam.io.ReadFromText(INPUT_FILE)
        | 'Split by delimiter' >> beam.Map(lambda x: x.split(','))
        | 'Filter for NY customers above 20 yo' >> beam.Filter(lambda x: x[2] == 'NY'
                                                                         and int(x[3]) > 20)
        | 'Write output to a file' >> beam.io.WriteToText(OUTPUT_FILE)
    )
p1.run()