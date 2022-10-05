import argparse
import apache_beam as beam
from helpers.transformations import Transform
from helpers.options import get_pipeline_options
from apache_beam.transforms.external import ImplicitSchemaPayloadBuilder

def run():
    """Main function that defines pipeline and runs it."""
    pipeline_options = get_pipeline_options(**vars(args))
    with beam.Pipeline(options=pipeline_options) as pipeline:
        (pipeline
         | 'FirestoreRead' >> beam.ExternalTransform(
                    'my.beam.transform.firestore_read',
                    ImplicitSchemaPayloadBuilder({'parent': f'projects/{args.project}/databases/(default)/documents',
                                                  'collectionId': 'stack-overflow-comments'}),
                    "localhost:12345")
         | 'Do something' >> beam.ParDo(Transform()).with_output_types(str)
         | 'FirestoreWrite' >> beam.ExternalTransform(
                    'my.beam.transform.firestore_write',
                    ImplicitSchemaPayloadBuilder({'parent': f'projects/{args.project}/databases/(default)/documents',
                                                  'collectionId': 'stack-overflow-comments2',
                                                  'projectId': args.project}),
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
                        default='nimble-radio-334715')
    parser.add_argument('-sa', '--service_account',
                        help='Email address of service account to use',
                        default='svc-dataflow@nimble-radio-334715.iam.gserviceaccount.com')
    args, _ = parser.parse_known_args()
    run()
