from __future__ import print_function
import argparse
import re
import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='input.txt',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        default='output.txt',
                        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    with beam.Pipeline() as p:
        counts = (
            p
            | ReadFromText(known_args.input)
            | beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x)) # Find all word matches
            | beam.Map(lambda x: (x, 1))                            # Create tuple (word,1)
            | beam.CombinePerKey(sum)                               # Reduce by key i.e. the word
            | WriteToText(known_args.output)
        )

if __name__ == '__main__':
    run()
