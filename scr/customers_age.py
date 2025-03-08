import apache_beam as beam

p1 = beam.Pipeline()
file_pattern = 'Customers_age.txt'

customers = (
        p1
        | beam.io.ReadFromText('../res/raw/'+file_pattern)
        | beam.Map(lambda x: x.split(','))
        | beam.Filter(lambda x: x[2] == 'NY'
                                and int(x[3]) > 20)
        | beam.io.WriteToText('../res/processed/'+file_pattern)
)
p1.run()