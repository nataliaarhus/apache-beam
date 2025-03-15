import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

INPUT_FILE = '../res/raw/students_marks.txt'
OUTPUT_FILE = '../res/processed/students_marks.txt'
countries_list = ['US', 'IN']

# Custom PTransform to encapsulate common logic
class MyPTransform(beam.PTransform):
    def __init__(self, country):
        super().__init__()
        self.country = country

    def expand(self, input_col):
        return (
            input_col
            | f'Calculate sum for {self.country}' >> beam.Map(calculate_total_marks)
            | f'Format output for {self.country}' >> beam.Map(format_output)
        )


def parse_record(record):
    try:
        cols = record.split(',')
        if len(cols) < 5:
            raise ValueError("Invalid record format")
        return cols[0], cols[1], int(cols[2]), int(cols[3]), int(cols[4])
    except Exception as e:
        return None


def filter_country(country, record):
    return record[1] == country


def calculate_total_marks(record):
    name, _, mark1, mark2, mark3 = record                   # tuple unpacking
    total_marks = mark1 + mark2 + mark3
    return name, total_marks


def format_output(record):
    name, total_marks = record
    return f'{name} has received {total_marks} marks.'


pipeline_options = PipelineOptions()
with beam.Pipeline(options=pipeline_options) as p:

    input_data = (
        p
        | 'Read file' >> beam.io.ReadFromText(INPUT_FILE)
        | 'Split by delimiter' >> beam.Map(parse_record)
        | 'Filter Valid Records' >> beam.Filter(lambda x: x is not None)
    )

    for country in countries_list: (
        input_data
        | f'Filter {country} records' >> beam.Filter(lambda x: filter_country(country, x))
        | f'Apply PTransform for {country}' >> MyPTransform(country)
        | f'Write output to a {country} file' >> beam.io.WriteToText(OUTPUT_FILE, file_name_suffix=f'_{country.lower()}')
    )
