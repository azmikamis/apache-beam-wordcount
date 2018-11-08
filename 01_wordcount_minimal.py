from __future__ import print_function
import re
import apache_beam as beam

with beam.Pipeline() as p:
    counts = (
        p
        | beam.Create(['to be or not to be'])
        | beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x)) # Find all word matches
        | beam.Map(lambda x: (x, 1))                            # Create tuple (word,1)
        | beam.CombinePerKey(sum)                               # Reduce by key i.e. the word
        | beam.ParDo(lambda x: print(x))                        # Print output
    )