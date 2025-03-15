import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

INPUT_FILE = 'res/raw/movies_rating.txt'
OUTPUT_FILE = 'res/processed/movies_rating.txt'

pipeline_options = PipelineOptions()
with beam.Pipeline(options=pipeline_options) as p1:
    rating = (
        p1
        | 'Read file' >> beam.io.ReadFromText(INPUT_FILE, skip_header_lines=1)
        | 'Split by delimiter' >> beam.Map(lambda x: x.split(','))
        | 'Filter ratings above 4' >> beam.Filter(lambda x: float(x[2]) > 4)
        | 'Write output to a file' >> beam.io.WriteToText(OUTPUT_FILE)
    )
p1.run()