import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging

INPUT_FILE = '../res/raw/students_marks.txt'
OUTPUT_PATH = '../res/processed/students_marks'
COUNTRIES = ['US', 'IN']


class ProcessStudentRecords(beam.PTransform):
    """Custom PTransform to encapsulate common logic to process student records for a specific country."""

    def __init__(self, country):
        """Initialize with the country to filter for."""
        super().__init__()
        self.country = country

    def expand(self, pcoll):
        """Process student records for the specified country. """
        return (
                pcoll
                | f'Filter {self.country} records' >> beam.Filter(lambda x: x[1] == self.country)
                | f'Calculate sum for {self.country}' >> beam.Map(self._calculate_total_marks)
                | f'Format output for {self.country}' >> beam.Map(self._format_output)
        )

    @staticmethod
    def _calculate_total_marks(record):
        """Calculate total marks from a student record."""
        name, _, mark1, mark2, mark3 = record                                 # tuple unpacking
        total_marks = mark1 + mark2 + mark3
        return name, total_marks

    @staticmethod
    def _format_output(record):
        """Format the output string."""
        name, total_marks = record
        return f'{name} has received {total_marks} marks.'


def parse_record(record):
    """Parse a comma-separated record into a tuple."""
    try:
        cols = record.split(',')
        if len(cols) < 5:
            logging.warning(f"Invalid record format: {record}")
            return None
        return cols[0], cols[1], int(cols[2]), int(cols[3]), int(cols[4])
    except Exception as e:
        logging.error(f"Error parsing record '{record}': {str(e)}")
        return None


def run():
    """Run the pipeline."""
    pipeline_options = PipelineOptions()

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Parse and filter input data
        parsed_records = (
                pipeline
                | 'Read file' >> beam.io.ReadFromText(INPUT_FILE)
                | 'Parse records' >> beam.Map(parse_record)
                | 'Filter valid records' >> beam.Filter(lambda x: x is not None)
        )

        # Process each country
        for country in COUNTRIES: (
                parsed_records
                | f'Process {country} records' >> ProcessStudentRecords(country)
                | f'Write {country} results' >> beam.io.WriteToText(f'{OUTPUT_PATH}_{country.lower()}', file_name_suffix='.txt')
            )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()