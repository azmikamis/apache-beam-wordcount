from __future__ import print_function
import argparse
import re
from datetime import datetime
import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText, Write, BigQuerySink, BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions

class Split(beam.DoFn):
    def process(self, element):
        word, freq = element

        return [{
            'word': word,
            'freq': freq,
        }]
        
def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='gs://dataflow-samples/shakespeare/kinglear.txt',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        default='gs://BUCKET-NAME/output',
                        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    options = PipelineOptions()
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'PROJECT-NAME'
    google_cloud_options.staging_location = 'gs://BUCKET-NAME/staging'
    google_cloud_options.temp_location = 'gs://BUCKET-NAME/temp'
    google_cloud_options.job_name = 'JOBNAME-USERNAME-DATETIME'
    with beam.Pipeline(options=options) as p:
        counts = (
            p
            | ReadFromText(known_args.input)
            | beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x)) # Find all word matches
            | beam.Map(lambda x: (x, 1))                            # Create tuple (word,1)
            | beam.CombinePerKey(sum)                               # Reduce by key i.e. the word
            | beam.ParDo(Split())
            | beam.io.Write(beam.io.BigQuerySink(
                            'PROJECT-NAME:DATASET-NAME.TABLE-NAME',
                            schema='word:STRING,freq:INTEGER',
                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))
        )

if __name__ == '__main__':
    run()
