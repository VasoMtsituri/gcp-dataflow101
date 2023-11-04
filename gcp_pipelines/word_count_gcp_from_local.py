import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

logging.basicConfig(level=logging.INFO)

PROJECT_ID = 'bubbly-delight-397006'
REGION = 'europe-west1'
GCS_BUCKET = 'dataflow-resources101'
GCS_INPUT = f'gs://{GCS_BUCKET}/input.txt'
GCS_OUTPUT = f'gs://{GCS_BUCKET}/output'
GCS_TEMP = f'gs://{GCS_BUCKET}/temp'
DATAFLOW_RUNNER = 'DataflowRunner'
JOB_NAME = 'word-count'


# Define a simple function to transform the data
def format_result(word_count):
    (word, count) = word_count
    return f'{word}: {count}'


# Define the pipeline options
options = PipelineOptions(
    flags=None,
    project=PROJECT_ID,
    job_name=JOB_NAME,
    temp_location=GCS_TEMP,
    region=REGION,
    runner=DATAFLOW_RUNNER  # Specifies to use DataflowRunner to run the pipeline on dataflow

)

# Define the pipeline using a context manager and the specified options
with beam.Pipeline(options=options) as pipeline:
    # Read from a text file.
    counts = (
            pipeline
            | 'Read' >> beam.io.ReadFromText(GCS_INPUT)
            | 'Split' >> beam.FlatMap(lambda x: x.split())
            | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
            | 'GroupAndSum' >> beam.CombinePerKey(sum))

    # Apply a transformation to format the results
    formatted_counts = counts | 'Format' >> beam.Map(format_result)

    # Write to an output text file
    formatted_counts | 'Write' >> beam.io.WriteToText(GCS_OUTPUT)
