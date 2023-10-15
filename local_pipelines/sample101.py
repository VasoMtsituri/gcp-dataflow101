import logging

import apache_beam as beam
import click
from apache_beam.options.pipeline_options import PipelineOptions

logging.basicConfig(level=logging.INFO)


class ParseRawData(beam.DoFn):
    def process(self, lines):
        transformed_dicts = []
        logging.info(f'Lines received: {lines}')
        logging.info(f'Type of Lines received: {type(lines)}')
        lines = [lines] if not isinstance(lines, list) else lines

        for line in lines:
            parts = line.split(',') if ',' in line else None

            if not parts:
                logging.info(f'Malformed text line detected: {line}')
                continue

            _dict = {}
            dict_parts = [x.split(':') for x in parts]

            for key in dict_parts:
                _dict[key[0].strip()] = key[1].strip()

            transformed_dicts.append(_dict)

        logging.info(f'Final result: {transformed_dicts}')

        return transformed_dicts


@click.command()
@click.option('--f_in', help='Input file')
@click.option('--f_out', required=False, help='Output file')
def main(f_in, f_out):
    logging.info(f'Input file extracted: {f_in}!')

    with beam.Pipeline('DirectRunner', options=PipelineOptions()) as p:
        # Create a PCollection from the input file.
        lines = p | 'ReadFromText' >> beam.io.ReadFromText(f_in)

        # Apply a ParDo transform to parse each line into valid dicts.
        dicts = lines | 'ParseRawData' >> beam.ParDo(ParseRawData())

        # Write the dicts to a file.
        dicts | 'WriteToText' >> beam.io.WriteToText(f_out)


if __name__ == '__main__':
    main()
