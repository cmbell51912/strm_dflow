from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import pubsub_v1
from google.cloud import bigquery
from google.cloud import storage
import apache_beam as beam
import logging
import argparse
import sys
import re

PROJECT="gcp-stl"
schema = 'job:STRING, company:STRING, name:STRING, sex:STRING, address:STRING'
TOPIC = "projects/gcp-stl/topics/cb-dflow-POC"

class Split(beam.DoFn):

    def process(self, element):

        import ast
        element = ast.literal_eval(element)

        return [{
            'job': element['job'],
            'company': element['company'],
            'name': element['name'],
            'sex': element['sex'],
            'address': element['address']
        }]

def main(argv=None):

   parser = argparse.ArgumentParser()
   parser.add_argument("--input_topic")
   parser.add_argument("--output")
   known_args = parser.parse_known_args(argv)


   p = beam.Pipeline(options=PipelineOptions())

   (p
      | 'ReadData' >> beam.io.ReadFromPubSub(topic=TOPIC).with_output_types(bytes)
      #| "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
      | 'ParseMessage' >> beam.ParDo(Split())
      | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('{0}:CbPOC_strmdflow.UsersDflow'.format(PROJECT), schema=schema,
       write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
   )
   result = p.run()
   result.wait_until_finish()

if __name__ == '__main__':
  logger = logging.getLogger().setLevel(logging.INFO)
  main()