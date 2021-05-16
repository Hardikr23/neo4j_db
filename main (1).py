from __future__ import absolute_import
import json
import os
import argparse
import logging
import base64
from py2neo import Graph,Node
from past.builtins import unicode

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class mapping(beam.DoFn):
    """Parse each line of input text into words."""

    def process(self, element):
        final = []
        element_dict = json.loads(element)
        for key, value in element_dict.items():
            processed_element = {}
            processed_element["tag"] = key
            processed_element["quality"] = value[0]["q"]
            processed_element["timestamp"] = value[0]["ts"]
            processed_element["value"] = value[0]["v"]
            final.append(processed_element)

        return final

def get_neo4j_data():
    uri='http://35.222.165.255:7687'
    user='neo4j'
    pwd='pMyp7ZVNgvmLqlP1'

    graph = Graph(uri, auth=(user, pwd), port=7474)
    # persom = Node("Person", name="person")
    # graph.create(person)
    graph.run("select asset id from asset")
    print("------------")
    print(graph)

def run(argv=None):
    # argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('--pubsub_proj', required=True)
    parser.add_argument('--subscription_name', required=True)
    parser.add_argument('--output-bq-dataset', required=False)
    parser.add_argument('--output-bq-table', required=False)

    known_args, pipeline_args = parser.parse_known_args(argv)

    pubsub_proj = known_args.pubsub_proj
    subscription_name = known_args.subscription_name
    output_bq_dataset = known_args.output_bq_dataset
    output_bq_table = known_args.output_bq_table

    # pipeline options, google_cloud_options
    # topic_full_path = 'projects/{project_id}/topics/{topic_name}'.format(project_id=pubsub_proj, topic_name=topic_name)
    subscription_full_path = "projects/{project_id}/subscriptions/{subscription_name}".format(project_id=pubsub_proj,
                                                                                              subscription_name=subscription_name)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    SCHEMA = 'tag:STRING,quality:FLOAT,value:FLOAT,timestamp:TIMESTAMP'

    with beam.Pipeline(options=pipeline_options) as p:
        p1 = (p | 'read from pubsub' >> beam.io.ReadStringsFromPubSub(subscription=subscription_full_path)
              | 'ETL Processing' >> beam.ParDo(mapping())
              | "Write to BQ" >> beam.io.WriteToBigQuery('{}.{}'.format(output_bq_dataset, output_bq_table),
                                                         schema=SCHEMA,
                                                         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))
        # | "write to text" >> beam.io.WriteToText("output.txt"))

    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    run()

# python3 mainDataflow.py --project qp-adani-2020-12 --subscription_name my-subscription --streaming --output-bq-dataset dataset1 --output-bq-table dlp_findings --gcs_proj qp-adani-2020-12 --bq_proj qp-adani-2020-12 --pubsub_proj gcp-poc-adani --machine_type n1-standard-1 --max_num_workers 1 --staging_location gs://dataflowdm/staging/ --temp_location gs://dataflowdm/temp/

