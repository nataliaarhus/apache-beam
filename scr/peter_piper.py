import apache_beam as beam

p1 = beam.Pipeline()
file_pattern = 'Peter_Piper.txt'
words = ['peter', 'piper', 'picked', 'peck', 'pepper']


def find_word(element):
    return element in words


word_frequency = (
        p1
        | beam.io.ReadFromText('../res/raw/' + file_pattern)
        | beam.FlatMap(lambda x: x.split(' '))
        | beam.Filter(find_word)
        | beam.Map(lambda x: (x, 1))
        | beam.CombinePerKey(sum)
        | beam.io.WriteToText('../res/processed/' + file_pattern)
)
p1.run()
