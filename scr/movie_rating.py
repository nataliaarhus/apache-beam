import apache_beam as beam

p1 = beam.Pipeline()
file_pattern = 'movies_rating.txt'

rating = (
        p1
        | beam.io.ReadFromText('raw/'+file_pattern, skip_header_lines=1)
        | beam.Map(lambda x: x.split(','))
        | beam.Filter(lambda x: float(x[2]) > 4)
        | beam.io.WriteToText('processed/'+file_pattern)
)
p1.run()