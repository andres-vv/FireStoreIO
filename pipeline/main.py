import argparse
import logging

import apache_beam as beam
from helpers.transformations import LevelUp
from helpers.options import get_pipeline_options
from apache_beam.transforms.external import ImplicitSchemaPayloadBuilder

logging.basicConfig(level='INFO',
                    format="%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s")


def run():
    """Main function that defines pipeline and runs it."""
    # Configure your personal pipeline options
    pipeline_options = get_pipeline_options(**vars(args))
    with beam.Pipeline(options=pipeline_options) as pipeline:
        (pipeline
         | 'FirestoreRead' >> beam.ExternalTransform(
             'my.beam.transform.firestore_read',
             ImplicitSchemaPayloadBuilder({'parent': f'projects/{args.project}/databases/(default)/documents',
                                           'collectionId': 'Pokedex'}),
             "localhost:12345")
         | 'Level Up' >> beam.ParDo(
             LevelUp(args.gained_levels, args.pokemon)).with_output_types(str)
         | 'FirestoreWrite' >> beam.ExternalTransform(
             'my.beam.transform.firestore_write',
             ImplicitSchemaPayloadBuilder({'parent': f'projects/{args.project}/databases/(default)/documents',
                                           'collectionId': 'Pokedex'}),
             "localhost:12345")
         )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='stackoverflow-ingest')
    parser.add_argument('-m', '--mode',
                        help='Mode to run pipeline in.',
                        choices=['local', 'cloud', 'template'],
                        default='local')
    parser.add_argument('-p', '--project',
                        help='GCP project to run pipeline on.',
                        default='wise-dispatcher-375608')
    parser.add_argument('-sa', '--service_account',
                        help='Email address of service account to use',
                        default='svc-dataflow@wise-dispatcher-375608.iam.gserviceaccount.com')
    parser.add_argument('-gl', '--gained_levels',
                        help='Amount of levels gained')
    parser.add_argument('-pk', '--pokemon',
                        help='Amount of levels gained')
    args, _ = parser.parse_known_args()
    run()
