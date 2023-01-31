"""This file contains the pipeline options to configure the Dataflow pipeline."""
import os
from datetime import datetime
from typing import Any

import apache_beam as beam


JOB_NAME = 'firestore-pipeline'


class CustomOptions(beam.pipeline.PipelineOptions):
    """Custom PipelineOptions class to support additional flags."""

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('-c', '--commit-hash',
                            help='Commit hash to add as an additional '
                                 'property to the pipeline')


def get_pipeline_options(*, mode: str, project: str,
                         service_account: str = None, **kwargs: Any) \
        -> beam.pipeline.PipelineOptions:
    """Function to retrieve the pipeline options.

    Args:
        project: GCP project to run on
        mode: Indicator to run local, cloud or template
        service_account: Email address of service account to use

    Returns:
        Dataflow pipeline options

    """
    job_name = f'{JOB_NAME}-{datetime.now().strftime("%Y%m%d%H%M%S")}'

    staging_bucket = 'gs://wise-dispatcher-375608_staging'

    # For a list of available options, check:
    # https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#setting-other-cloud-dataflow-pipeline-options
    dataflow_options = {
        'runner': 'DirectRunner' if mode == 'local' else 'DataflowRunner',
        'job_name': job_name,
        'project': project,
        'region': 'europe-west1',
        'staging_location': f'{staging_bucket}/staging',
        'temp_location': f'{staging_bucket}/temp',
        'autoscaling_algorithm': 'THROUGHPUT_BASED',
        'save_main_session': True,
        'subnetwork': 'regions/europe-west1/subnetworks/europe-west1',
        'setup_file': './setup.py',
        'max_num_workers': 5,
        'use_public_ips': False
    }

    if mode == 'template':
        dataflow_options['template_location'] = f'{staging_bucket}/templates/{job_name}'

    if service_account:
        dataflow_options['service_account_email'] = service_account

    commit_hash = os.environ.get('commit_hash')
    if commit_hash:
        dataflow_options['commit_hash'] = commit_hash

    print(f'\nStarting {mode} run with following config:')
    for option, value in dataflow_options.items():
        print(f'{option}: {value}')

    return CustomOptions(**dataflow_options, **kwargs)
