import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

INPUT_FILE = '../res/raw/students_marks.txt'
OUTPUT_PATH = '../res/processed/students_marks'
countries_list = ['US', 'IN']


def parse_record(record):
    try:
        cols = record.split(',')
        if len(cols) < 5:
            raise ValueError("Invalid record format")
        return cols[0], cols[1], int(cols[2]), int(cols[3]), int(cols[4])
    except Exception as e:
        return None


def make_country_filter(country_value):
    """
    The pipeline construction phase and execution phase are separate in Apache Beam, i.e. loop completes before
    the pipeline executes. The function factory approach creates a new function for each country with the country value
    supplied at creation time, rather than looking up the variable at execution time.
    """
    def filter_fn(record):
        print(f"Filtering for country: {country_value}")
        return record[1] == country_value
    return filter_fn


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

    for country in countries_list:
        print(f"Creating pipeline branch for {country}")
        (
            input_data
            | f'Filter {country} records' >>  beam.Filter(make_country_filter(country))
            | f'Calculate sum for {country}' >> beam.Map(calculate_total_marks)
            | f'Format output for {country}' >> beam.Map(format_output)
            | f'Write output to a {country} file' >> beam.io.WriteToText(f'{OUTPUT_PATH}_{country.lower()}', file_name_suffix='.txt')
        )
