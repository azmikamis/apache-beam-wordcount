from __future__ import print_function
import re
import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText

with beam.Pipeline() as p:
    counts = (
        p
        | ReadFromText('input.txt')
        | beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x)) # Find all word matches
        | beam.Map(lambda x: (x, 1))                            # Create tuple (word,1)
        | beam.CombinePerKey(sum)                               # Reduce by key i.e. the word
        | WriteToText('output.txt')
    )
